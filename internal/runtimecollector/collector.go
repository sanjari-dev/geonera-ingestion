// Package runtimecollector samples Go process metrics (CPU, memory, goroutines)
// on a fixed interval and exposes the latest snapshot for the /api/v1/runtime
// endpoint. A single background goroutine is started via Start() at startup.
package runtimecollector

import (
	"fmt"
	"math"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync"
	"time"
)

// Snapshot is a point-in-time reading of the ingestion process resources.
type Snapshot struct {
	// CPUPercent is the process CPU utilization percentage over the last sample
	// interval, computed from the Go scheduler's total CPU-seconds metric.
	// Range: 0–100. Negative indicates not yet available (first sample pending).
	CPUPercent float64 `json:"cpu_percent"`

	// HeapAllocMB is the number of megabytes of live heap objects.
	HeapAllocMB float64 `json:"heap_alloc_mb"`

	// HeapInuseMB is the number of megabytes of heap spans in use by the allocator.
	HeapInuseMB float64 `json:"heap_inuse_mb"`

	// SysMB is the total megabytes of memory obtained from the OS by the runtime.
	SysMB float64 `json:"sys_mb"`

	// NumGoroutine is the current count of live goroutines.
	NumGoroutine int `json:"num_goroutine"`

	// NumCPU is the number of logical CPUs usable by the process.
	NumCPU int `json:"num_cpu"`

	// GCCycles have been the cumulative number of completed GC cycles since startup.
	GCCycles uint32 `json:"gc_cycles"`

	// AvgGCPauseMs is the average stop-the-world GC pause in milliseconds,
	// computed over up to the last 256 pause events.
	AvgGCPauseMs float64 `json:"avg_gc_pause_ms"`

	// UptimeSeconds is the number of seconds since Start() was called.
	UptimeSeconds int64 `json:"uptime_seconds"`
}

var (
	mu        sync.RWMutex
	latest    Snapshot
	startTime time.Time
)

// PrometheusText returns the current snapshot serialized in Prometheus text
// exposition format (version 0.0.4). Safe to call before the first sample
// interval — cpu_percent will be -1 (rendered as -1) which Prometheus accepts.
func PrometheusText() string {
	s := Get()
	var b strings.Builder
	g := func(name, help string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %.6g\n", name, help, name, name, v)
	}
	c := func(name, help string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %.6g\n", name, help, name, name, v)
	}
	g("ingestion_cpu_percent", "CPU utilization percentage (0-100; -1 = first sample pending).", s.CPUPercent)
	g("ingestion_heap_alloc_bytes", "Live heap object bytes.", s.HeapAllocMB*1024*1024)
	g("ingestion_heap_inuse_bytes", "Heap bytes in use by the allocator.", s.HeapInuseMB*1024*1024)
	g("ingestion_sys_bytes", "Total bytes obtained from the OS.", s.SysMB*1024*1024)
	g("ingestion_goroutines", "Current number of live goroutines.", float64(s.NumGoroutine))
	g("ingestion_gc_pause_ms_avg", "Average stop-the-world GC pause in milliseconds (last 256 cycles).", s.AvgGCPauseMs)
	c("ingestion_gc_cycles_total", "Cumulative completed GC cycles since startup.", float64(s.GCCycles))
	c("ingestion_uptime_seconds_total", "Seconds elapsed since the process started.", float64(s.UptimeSeconds))
	return b.String()
}

// Get returns the most recent snapshot. Returns a zero Snapshot if Start has
// not been called or the first sample interval has not elapsed yet.
func Get() Snapshot {
	mu.RLock()
	defer mu.RUnlock()
	return latest
}

// Start launches the background sampling goroutine. It must be called exactly
// once at application startup. The first snapshot is available after one
// interval (5 seconds).
func Start() {
	startTime = time.Now()
	go run()
}

func run() {
	const interval = 5 * time.Second

	// Pre-allocate the metrics.Sample slice once; the runtime reuses it.
	cpuSample := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
	}

	var prevCPU float64
	var prevTick time.Time
	firstTick := true

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for tick := range ticker.C {
		// ── CPU ────────────────────────────────────────────────────────────────
		metrics.Read(cpuSample)

		var cpuPct float64 = -1 // sentinel: not yet available
		if cpuSample[0].Value.Kind() == metrics.KindFloat64 {
			currentCPU := cpuSample[0].Value.Float64()

			if !firstTick {
				elapsed := tick.Sub(prevTick).Seconds()
				if elapsed > 0 {
					delta := currentCPU - prevCPU
					cpuPct = delta / (elapsed * float64(runtime.NumCPU())) * 100
					cpuPct = math.Round(cpuPct*10) / 10
					if cpuPct < 0 {
						cpuPct = 0
					}
					if cpuPct > 100*float64(runtime.NumCPU()) {
						cpuPct = 100
					}
				}
			}

			prevCPU = currentCPU
			prevTick = tick
			firstTick = false
		}

		// ── Memory ─────────────────────────────────────────────────────────────
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms) // brief STW pause — acceptable for a 5s poller

		toMB := func(b uint64) float64 {
			return math.Round(float64(b)/1024/1024*100) / 100
		}

		// Average GC pause over the last N GC cycles (ring buffer of 256 entries).
		avgPauseMs := 0.0
		if ms.NumGC > 0 {
			n := ms.NumGC
			if n > 256 {
				n = 256
			}
			var totalNs uint64
			for i := uint32(0); i < n; i++ {
				totalNs += ms.PauseNs[(ms.NumGC+255-i)%256]
			}
			avgPauseMs = math.Round(float64(totalNs/uint64(n))/1e6*100) / 100
		}

		s := Snapshot{
			CPUPercent:    cpuPct,
			HeapAllocMB:   toMB(ms.HeapAlloc),
			HeapInuseMB:   toMB(ms.HeapInuse),
			SysMB:         toMB(ms.Sys),
			NumGoroutine:  runtime.NumGoroutine(),
			NumCPU:        runtime.NumCPU(),
			GCCycles:      ms.NumGC,
			AvgGCPauseMs:  avgPauseMs,
			UptimeSeconds: int64(time.Since(startTime).Seconds()),
		}

		mu.Lock()
		latest = s
		mu.Unlock()
	}
}
