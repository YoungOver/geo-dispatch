package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/YoungOver/geo-dispatch/internal/geo"
	"github.com/YoungOver/geo-dispatch/internal/wire"
)

type pingJSON struct {
	ID     uint64  `json:"id"`
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Status uint8   `json:"status"`
}

func main() {
	httpAddr := flag.String("http", ":8081", "HTTP listen address")
	udpAddr := flag.String("udp", ":9091", "UDP ping listen address")
	ttl := flag.Duration("ttl", 30*time.Second, "drop objects not seen for this long")
	readers := flag.Int("udp-readers", runtime.NumCPU(), "UDP reader goroutines")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ix := geo.NewIndex()
	var badDatagrams atomic.Uint64

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "geo_updates_total", Help: "Position updates applied."}, func() float64 { return float64(ix.Updates.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "geo_queries_total", Help: "Nearest queries served."}, func() float64 { return float64(ix.Queries.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "geo_evicted_total", Help: "Objects evicted by TTL."}, func() float64 { return float64(ix.Evicted.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "geo_bad_datagrams_total", Help: "Malformed UDP datagrams."}, func() float64 { return float64(badDatagrams.Load()) }))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "geo_objects", Help: "Objects currently tracked."}, func() float64 { return float64(ix.Len()) }))
	qlat := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "geo_nearest_seconds", Help: "Nearest query latency.", Buckets: []float64{10e-6, 25e-6, 50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 5e-3}})
	reg.MustRegister(qlat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go ix.Janitor(*ttl, time.Second, ctx.Done())

	// UDP ingest: several readers on one socket, each decoding into the index directly.
	pc, err := net.ListenPacket("udp", *udpAddr)
	if err != nil {
		log.Error("udp listen", "err", err)
		os.Exit(1)
	}
	if uc, ok := pc.(*net.UDPConn); ok {
		uc.SetReadBuffer(8 << 20)
	}
	for i := 0; i < *readers; i++ {
		go func() {
			buf := make([]byte, wire.PingSize*wire.MaxPerDatagram)
			for {
				n, _, err := pc.ReadFrom(buf)
				if err != nil {
					return
				}
				now := time.Now().UnixMilli()
				if wire.Decode(buf[:n], func(p wire.Ping) { ix.Upsert(p.ID, p.Lat, p.Lon, geo.Status(p.Status), now) }) != nil {
					badDatagrams.Add(1)
				}
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/pings", func(w http.ResponseWriter, r *http.Request) {
		var batch []pingJSON
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now().UnixMilli()
		for _, p := range batch {
			ix.Upsert(p.ID, p.Lat, p.Lon, geo.Status(p.Status), now)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET /v1/nearest", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		lat, err1 := strconv.ParseFloat(q.Get("lat"), 64)
		lon, err2 := strconv.ParseFloat(q.Get("lon"), 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "lat and lon are required", http.StatusBadRequest)
			return
		}
		k, _ := strconv.Atoi(q.Get("k"))
		if k <= 0 || k > 100 {
			k = 10
		}
		radius, _ := strconv.ParseFloat(q.Get("radius_m"), 64)
		if radius <= 0 || radius > 50_000 {
			radius = 3000
		}
		want := geo.Free
		if q.Get("status") == "any" {
			want = geo.Offline
		}
		start := time.Now()
		hits := ix.Nearest(lat, lon, k, radius, want, time.Now().Add(-*ttl).UnixMilli())
		qlat.Observe(time.Since(start).Seconds())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"count": len(hits), "hits": hits})
	})
	mux.HandleFunc("GET /v1/objects/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseUint(r.PathValue("id"), 10, 64)
		o, ok := ix.Get(id)
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(o)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	srv := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("listening", "http", *httpAddr, "udp", *udpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			stop()
		}
	}()
	<-ctx.Done()
	pc.Close()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
}
