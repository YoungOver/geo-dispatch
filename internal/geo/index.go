// Package geo is an in-memory spatial index of moving objects (couriers,
// drivers, scooters) tuned for a very high update rate and sub-millisecond
// nearest-neighbour queries.
//
// Positions are bucketed into a fixed grid of roughly cellMeters×cellMeters.
// Both the grid and the object table are sharded, so writers touching
// different parts of a city never contend on the same lock.
package geo

import (
	"container/heap"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	earthR     = 6_371_000.0
	cellMeters = 500.0
	latStep    = cellMeters / 111_320.0 // degrees per cell along latitude
	shards     = 256
)

type Status uint8

const (
	Offline Status = iota
	Free
	Busy
)

type Object struct {
	ID     uint64  `json:"id"`
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Status Status  `json:"status"`
	Seen   int64   `json:"seen_unix_ms"`
	cell   uint64
}

type cellShard struct {
	mu    sync.RWMutex
	cells map[uint64]map[uint64]struct{}
}

type objShard struct {
	mu   sync.RWMutex
	objs map[uint64]*Object
}

type Index struct {
	cells   [shards]cellShard
	objs    [shards]objShard
	Updates atomic.Uint64
	Queries atomic.Uint64
	Evicted atomic.Uint64
	count   atomic.Int64
}

func NewIndex() *Index {
	ix := &Index{}
	for i := range ix.cells {
		ix.cells[i].cells = make(map[uint64]map[uint64]struct{}, 256)
		ix.objs[i].objs = make(map[uint64]*Object, 1024)
	}
	return ix
}

func cellOf(lat, lon float64) (row, col int32) {
	row = int32(math.Floor((lat + 90) / latStep))
	col = int32(math.Floor((lon + 180) / lonStepFor(row)))
	return
}

func lonStepFor(row int32) float64 {
	centre := (float64(row)+0.5)*latStep - 90
	return latStep / math.Max(math.Cos(centre*math.Pi/180), 0.01)
}

func key(row, col int32) uint64 { return uint64(uint32(row))<<32 | uint64(uint32(col)) }

func (ix *Index) cellShardOf(k uint64) *cellShard {
	return &ix.cells[(k*0x9E3779B97F4A7C15)>>56]
}

func (ix *Index) objShardOf(id uint64) *objShard {
	return &ix.objs[(id*0x9E3779B97F4A7C15)>>56]
}

// Upsert records a new position. It is safe for concurrent use.
func (ix *Index) Upsert(id uint64, lat, lon float64, st Status, now int64) {
	r, c := cellOf(lat, lon)
	k := key(r, c)
	os := ix.objShardOf(id)
	os.mu.Lock()
	o, ok := os.objs[id]
	if !ok {
		o = &Object{ID: id}
		os.objs[id] = o
		ix.count.Add(1)
	}
	old := o.cell
	moved := !ok || old != k
	o.Lat, o.Lon, o.Status, o.Seen, o.cell = lat, lon, st, now, k
	os.mu.Unlock()

	if moved {
		if ok {
			ix.removeFromCell(old, id)
		}
		cs := ix.cellShardOf(k)
		cs.mu.Lock()
		set := cs.cells[k]
		if set == nil {
			set = make(map[uint64]struct{}, 8)
			cs.cells[k] = set
		}
		set[id] = struct{}{}
		cs.mu.Unlock()
	}
	ix.Updates.Add(1)
}

func (ix *Index) removeFromCell(k, id uint64) {
	cs := ix.cellShardOf(k)
	cs.mu.Lock()
	if set := cs.cells[k]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(cs.cells, k)
		}
	}
	cs.mu.Unlock()
}

func (ix *Index) Get(id uint64) (Object, bool) {
	os := ix.objShardOf(id)
	os.mu.RLock()
	defer os.mu.RUnlock()
	o, ok := os.objs[id]
	if !ok {
		return Object{}, false
	}
	return *o, true
}

func (ix *Index) Len() int { return int(ix.count.Load()) }

// Haversine distance in metres.
func Haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const d = math.Pi / 180
	dlat := (lat2 - lat1) * d
	dlon := (lon2 - lon1) * d
	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1*d)*math.Cos(lat2*d)*math.Sin(dlon/2)*math.Sin(dlon/2)
	return 2 * earthR * math.Asin(math.Sqrt(a))
}

type Hit struct {
	Object
	Distance float64 `json:"distance_m"`
}

type maxHeap []Hit

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].Distance > h[j].Distance }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)         { *h = append(*h, x.(Hit)) }
func (h *maxHeap) Pop() any           { o := *h; n := len(o); x := o[n-1]; *h = o[:n-1]; return x }

// Nearest returns up to k objects with status want (or any status when
// want == Offline) within radius metres, closest first. It expands square
// rings of cells around the query point and stops as soon as the ring's
// inner edge is farther than both the radius and the current k-th hit.
func (ix *Index) Nearest(lat, lon float64, k int, radius float64, want Status, staleBefore int64) []Hit {
	ix.Queries.Add(1)
	r0, _ := cellOf(lat, lon)
	h := make(maxHeap, 0, k+1)
	maxRing := int32(radius/cellMeters) + 1
	ids := make([]uint64, 0, 64)
	for ring := int32(0); ring <= maxRing; ring++ {
		inner := float64(ring-1) * cellMeters
		if ring > 0 && inner > radius {
			break
		}
		if len(h) == k && ring > 0 && inner > h[0].Distance {
			break
		}
		for dr := -ring; dr <= ring; dr++ {
			// every row has its own longitude step, so the column under the
			// query point must be recomputed per row
			cq := int32(math.Floor((lon + 180) / lonStepFor(r0+dr)))
			for dc := -ring; dc <= ring; dc++ {
				if ring > 0 && dr != -ring && dr != ring && dc != -ring && dc != ring {
					continue // only the ring perimeter
				}
				ck := key(r0+dr, cq+dc)
				cs := ix.cellShardOf(ck)
				ids = ids[:0]
				cs.mu.RLock()
				for id := range cs.cells[ck] {
					ids = append(ids, id)
				}
				cs.mu.RUnlock()
				for _, id := range ids {
					o, ok := ix.Get(id)
					if !ok || o.Seen < staleBefore {
						continue
					}
					if want != Offline && o.Status != want {
						continue
					}
					d := Haversine(lat, lon, o.Lat, o.Lon)
					if d > radius {
						continue
					}
					if len(h) < k {
						heap.Push(&h, Hit{Object: o, Distance: d})
					} else if d < h[0].Distance {
						h[0] = Hit{Object: o, Distance: d}
						heap.Fix(&h, 0)
					}
				}
			}
		}
	}
	out := make([]Hit, len(h))
	for i := len(h) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&h).(Hit)
	}
	return out
}

// Evict drops objects not seen since cutoff. Call it from a janitor.
func (ix *Index) Evict(cutoff int64) int {
	n := 0
	for i := range ix.objs {
		os := &ix.objs[i]
		var dead []*Object
		os.mu.Lock()
		for id, o := range os.objs {
			if o.Seen < cutoff {
				dead = append(dead, o)
				delete(os.objs, id)
			}
		}
		os.mu.Unlock()
		for _, o := range dead {
			ix.removeFromCell(o.cell, o.ID)
		}
		n += len(dead)
	}
	ix.count.Add(-int64(n))
	ix.Evicted.Add(uint64(n))
	return n
}

// Janitor evicts stale objects every interval until stop is closed.
func (ix *Index) Janitor(ttl, every time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			ix.Evict(now.Add(-ttl).UnixMilli())
		}
	}
}

