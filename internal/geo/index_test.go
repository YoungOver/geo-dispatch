package geo

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

const mskLat, mskLon = 55.7558, 37.6173

func randomAround(r *rand.Rand, lat, lon, spreadM float64) (float64, float64) {
	dlat := (r.Float64()*2 - 1) * spreadM / 111_320
	dlon := (r.Float64()*2 - 1) * spreadM / (111_320 * 0.5628)
	return lat + dlat, lon + dlon
}

// TestNearestMatchesBruteForce compares the grid search with a full scan.
func TestNearestMatchesBruteForce(t *testing.T) {
	ix := NewIndex()
	r := rand.New(rand.NewSource(7))
	type p struct{ lat, lon float64 }
	all := map[uint64]p{}
	now := time.Now().UnixMilli()
	for id := uint64(1); id <= 20_000; id++ {
		la, lo := randomAround(r, mskLat, mskLon, 15_000)
		ix.Upsert(id, la, lo, Free, now)
		all[id] = p{la, lo}
	}
	for q := 0; q < 200; q++ {
		qla, qlo := randomAround(r, mskLat, mskLon, 12_000)
		got := ix.Nearest(qla, qlo, 10, 3000, Offline, 0)
		var want []float64
		for _, v := range all {
			if d := Haversine(qla, qlo, v.lat, v.lon); d <= 3000 {
				want = append(want, d)
			}
		}
		sort.Float64s(want)
		if len(want) > 10 {
			want = want[:10]
		}
		if len(got) != len(want) {
			t.Fatalf("query %d: got %d hits, want %d", q, len(got), len(want))
		}
		for i := range got {
			if diff := got[i].Distance - want[i]; diff > 1e-6 || diff < -1e-6 {
				t.Fatalf("query %d hit %d: %.3f vs %.3f", q, i, got[i].Distance, want[i])
			}
		}
	}
}

func TestMoveAndEvict(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(1, mskLat, mskLon, Free, 1000)
	ix.Upsert(1, mskLat+0.05, mskLon, Free, 2000) // ~5.5 km north
	if hits := ix.Nearest(mskLat, mskLon, 5, 1000, Offline, 0); len(hits) != 0 {
		t.Fatalf("object must have left the old cell: %+v", hits)
	}
	if hits := ix.Nearest(mskLat+0.05, mskLon, 5, 1000, Offline, 0); len(hits) != 1 {
		t.Fatalf("object must be found at the new place")
	}
	if n := ix.Evict(3000); n != 1 || ix.Len() != 0 {
		t.Fatalf("evict n=%d len=%d", n, ix.Len())
	}
}

func TestStatusFilter(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(1, mskLat, mskLon, Busy, 1)
	ix.Upsert(2, mskLat+0.001, mskLon, Free, 1)
	h := ix.Nearest(mskLat, mskLon, 5, 1000, Free, 0)
	if len(h) != 1 || h[0].ID != 2 {
		t.Fatalf("status filter: %+v", h)
	}
}

func TestConcurrentWriters(t *testing.T) {
	ix := NewIndex()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 20_000; i++ {
				la, lo := randomAround(r, mskLat, mskLon, 20_000)
				ix.Upsert(uint64(r.Intn(5000)), la, lo, Free, int64(i))
				if i%10 == 0 {
					ix.Nearest(la, lo, 5, 2000, Offline, 0)
				}
			}
		}(w)
	}
	wg.Wait()
	if ix.Len() > 5000 {
		t.Fatalf("len=%d exceeds distinct ids", ix.Len())
	}
}

func seeded(n int) *Index {
	ix := NewIndex()
	r := rand.New(rand.NewSource(1))
	for id := uint64(1); id <= uint64(n); id++ {
		la, lo := randomAround(r, mskLat, mskLon, 25_000)
		ix.Upsert(id, la, lo, Free, 1)
	}
	return ix
}

func BenchmarkUpsertParallel(b *testing.B) {
	ix := seeded(200_000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			la, lo := randomAround(r, mskLat, mskLon, 25_000)
			ix.Upsert(uint64(1+r.Intn(200_000)), la, lo, Free, 2)
		}
	})
}

func BenchmarkNearestParallel(b *testing.B) {
	ix := seeded(200_000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			la, lo := randomAround(r, mskLat, mskLon, 20_000)
			ix.Nearest(la, lo, 10, 2000, Free, 0)
		}
	})
}
