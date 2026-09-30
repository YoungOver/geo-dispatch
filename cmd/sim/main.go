// sim emulates a city fleet: N couriers stream UDP pings once per interval
// while dispatchers fire nearest-courier queries over HTTP. It prints the
// sustained ingest rate and query latency percentiles.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YoungOver/geo-dispatch/internal/wire"
)

type courier struct{ lat, lon, vlat, vlon float64 }

func main() {
	udp := flag.String("udp", "127.0.0.1:9091", "server UDP address")
	httpURL := flag.String("http", "http://127.0.0.1:8081", "server HTTP address")
	n := flag.Int("couriers", 200_000, "fleet size")
	senders := flag.Int("senders", 8, "UDP sender goroutines")
	queriers := flag.Int("queriers", 64, "HTTP query workers")
	dur := flag.Duration("d", 20*time.Second, "duration")
	interval := flag.Duration("interval", 0, "ping interval per courier, 0 = as fast as possible")
	flag.Parse()

	const lat0, lon0 = 55.7558, 37.6173 // Moscow centre
	fleet := make([]courier, *n)
	r := rand.New(rand.NewSource(1))
	for i := range fleet {
		fleet[i] = courier{lat0 + (r.Float64()*2-1)*0.2, lon0 + (r.Float64()*2-1)*0.35, (r.Float64() - .5) * 1e-4, (r.Float64() - .5) * 1e-4}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	var sent, queries, qerr atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now()

	chunk := (*n + *senders - 1) / *senders
	for s := 0; s < *senders; s++ {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			conn, err := net.Dial("udp", *udp)
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			buf := make([]byte, 0, wire.PingSize*wire.MaxPerDatagram)
			var tick <-chan time.Time
			if *interval > 0 {
				t := time.NewTicker(*interval)
				defer t.Stop()
				tick = t.C
			}
			for ctx.Err() == nil {
				if tick != nil {
					select {
					case <-tick:
					case <-ctx.Done():
						return
					}
				}
				for i := lo; i < hi && i < len(fleet); i++ {
					c := &fleet[i]
					c.lat += c.vlat
					c.lon += c.vlon
					buf = wire.Append(buf, wire.Ping{ID: uint64(i + 1), Lat: c.lat, Lon: c.lon, Status: 1})
					if len(buf) == cap(buf) {
						conn.Write(buf)
						sent.Add(wire.MaxPerDatagram)
						buf = buf[:0]
					}
				}
			}
		}(s*chunk, (s+1)*chunk)
	}

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *queriers * 2}, Timeout: 3 * time.Second}
	lats := make([][]time.Duration, *queriers)
	for q := 0; q < *queriers; q++ {
		wg.Add(1)
		go func(q int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(q)))
			for ctx.Err() == nil {
				url := fmt.Sprintf("%s/v1/nearest?lat=%.6f&lon=%.6f&k=10&radius_m=2000", *httpURL, lat0+(r.Float64()*2-1)*0.15, lon0+(r.Float64()*2-1)*0.25)
				t0 := time.Now()
				resp, err := client.Get(url)
				if err != nil {
					qerr.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				lats[q] = append(lats[q], time.Since(t0))
				queries.Add(1)
			}
		}(q)
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(x float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*x)]
	}
	fmt.Printf("fleet=%d  pings sent=%d (%.0f/s)\n", *n, sent.Load(), float64(sent.Load())/el)
	fmt.Printf("nearest queries=%d (%.0f/s) errors=%d\n", queries.Load(), float64(queries.Load())/el, qerr.Load())
	fmt.Printf("query latency p50=%s p90=%s p99=%s\n", p(.5), p(.9), p(.99))
}
