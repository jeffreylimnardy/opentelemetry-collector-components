# Profiling gRPC Memory Retention in the OTel Collector

This guide documents how to reproduce and analyze the `bufio.NewReaderSize` memory retention pattern in the OTel Collector's OTLP gRPC receiver, and explains why this pattern behaves differently inside an Istio service mesh.

## Background

[OTel Collector issue #15086](https://github.com/open-telemetry/opentelemetry-collector/issues/15086) describes a scenario where the collector's heap grows during high gRPC connection churn and only partially releases across multiple GC cycles. The root cause is Go's `sync.Pool`: each incoming TCP connection to the gRPC server creates a `bufio.Reader` (via `newFramer` → `bufio.NewReaderSize`), and when the connection closes, that reader is placed into `sync.Pool`. Pool objects survive one GC cycle before eviction, so under sustained churn, memory accumulates.

In Kyma's production setup the collector runs inside an Istio service mesh. Envoy sidecars mediate all pod-to-pod traffic, and the pattern behaves very differently there. This guide documents both scenarios.

## Tools

- `go tool pprof` — heap and goroutine profile analysis
- `curl` with `?gc=1` on the pprof endpoint — triggers a GC before capturing the heap snapshot
- The [churn load generator](#churn-load-generator) — a small Go program that repeatedly dials gRPC, sends spans, and closes the connection
- `kubectl port-forward` — access the collector's pprof endpoint from outside the cluster
- Envoy admin API (`http://127.0.0.1:15000/clusters`) — inspect actual TCP connection counts from Istio sidecars

## Churn Load Generator

```go
// churn/main.go — repeatedly opens gRPC connections, sends spans, and closes them
package main

import (
    "context"
    "flag"
    "log"
    "sync"
    "sync/atomic"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    endpoint    := flag.String("endpoint", "127.0.0.1:4317", "collector endpoint")
    workers     := flag.Int("workers", 32, "concurrent workers")
    duration    := flag.Duration("duration", 5*time.Minute, "test duration")
    spansPerConn := flag.Int("spans", 10, "spans per connection before close")
    pause       := flag.Duration("pause", 200*time.Millisecond, "pause between connections per worker")
    flag.Parse()

    deadline := time.Now().Add(*duration)
    var wg sync.WaitGroup
    var connCount int64

    go func() {
        t := time.NewTicker(5 * time.Second)
        defer t.Stop()
        for range t.C {
            log.Printf("connections opened: %d", atomic.LoadInt64(&connCount))
        }
    }()

    for w := 0; w < *workers; w++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for time.Now().Before(deadline) {
                ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

                exporter, err := otlptracegrpc.New(ctx,
                    otlptracegrpc.WithEndpoint(*endpoint),
                    otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()),
                    otlptracegrpc.WithDialOption(grpc.WithBlock()),
                )
                if err != nil {
                    cancel()
                    time.Sleep(250 * time.Millisecond)
                    continue
                }
                atomic.AddInt64(&connCount, 1)

                tp := sdktrace.NewTracerProvider(
                    sdktrace.WithBatcher(exporter),
                    sdktrace.WithResource(resource.NewWithAttributes(
                        semconv.SchemaURL,
                        semconv.ServiceName("churn-client"),
                    )),
                )
                otel.SetTracerProvider(tp)
                tracer := otel.Tracer("churn")

                for i := 0; i < *spansPerConn; i++ {
                    _, span := tracer.Start(ctx, "test-span")
                    span.End()
                }

                _ = tp.ForceFlush(ctx)
                _ = tp.Shutdown(ctx)
                cancel()
                time.Sleep(*pause)
            }
        }(w)
    }

    wg.Wait()
    log.Printf("done. total connections: %d", atomic.LoadInt64(&connCount))
}
```

Build and run:

```bash
go mod init churn && go mod tidy
go run . -endpoint=127.0.0.1:4317 -workers=32 -duration=5m -pause=200ms -spans=10
```

## Collector Configuration

```yaml
# collector.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:

exporters:
  debug:
    verbosity: basic

extensions:
  pprof:
    endpoint: 0.0.0.0:1777

service:
  extensions: [pprof]
  telemetry:
    metrics:
      level: detailed
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

## Profiling Workflow

The double-GC technique is critical: calling `?gc=1` triggers one GC before the snapshot. To see what survives two GC cycles, capture three sequential profiles with `?gc=1` each.

```bash
COLLECTOR=127.0.0.1:1777

# Baseline (no load)
curl -s "http://$COLLECTOR/debug/pprof/heap?gc=1" -o heap-baseline.pb.gz

# Start the churn load generator, then after ~2 minutes:
curl -s "http://$COLLECTOR/debug/pprof/heap?gc=1" -o heap-load.pb.gz

# Stop the churn generator, then capture three post-load snapshots:
curl -s "http://$COLLECTOR/debug/pprof/heap?gc=1" -o heap-after-1.pb.gz
curl -s "http://$COLLECTOR/debug/pprof/heap?gc=1" -o heap-after-2.pb.gz
curl -s "http://$COLLECTOR/debug/pprof/heap?gc=1" -o heap-after-3.pb.gz
```

Analyze with:

```bash
# Show top allocations
go tool pprof -inuse_space -top heap-load.pb.gz

# Diff: what changed between load and baseline
go tool pprof -inuse_space -base heap-baseline.pb.gz -top heap-load.pb.gz

# Diff: what survives 3 GC cycles after load ends
go tool pprof -inuse_space -base heap-load.pb.gz -top heap-after-3.pb.gz
```

## Results: Bare-Metal (Direct gRPC)

Test parameters: 32 workers, 200 ms pause, 10 spans/connection, 5 minutes, ~35 000 connections total.

| Profile | Total heap | `bufio.NewReaderSize` |
|---|---|---|
| baseline | 21.8 MB | 0 |
| load (mid-run) | 27.2 MB | **6.5 MB** (23.8 % of heap) |
| after-1 GC | 27.8 MB | 6.5 MB |
| after-2 GC | 26.1 MB | 4.9 MB |
| after-3 GC | 21.6 MB | **0.81 MB** |

The `load - baseline` diff isolates the cause:

```
flat   flat%   cum
6479 kB  86 %   bufio.NewReaderSize (inline)
              google.golang.org/grpc.(*Server).Serve.func3
              google.golang.org/grpc.(*Server).handleRawConn
              google.golang.org/grpc.(*Server).newHTTP2Transport
              google.golang.org/grpc/internal/transport.NewServerTransport
              google.golang.org/grpc/internal/transport.newFramer
```

Every new TCP connection triggers this call chain once. The resulting `bufio.Reader` (32 KB default) is placed into `sync.Pool` on connection close. Pool objects survive one GC cycle, so at 35 000 connections the pool accumulates ~6.5 MB that only fully releases after the third post-load GC.

This matches the bounded pattern described in issue #15086: the retention is proportional to the churn rate, not unbounded, and clears completely within 3 GC cycles. It is not a memory leak.

## Results: Istio Service Mesh

When the collector and its clients both run in pods with Istio sidecars injected, the Envoy proxies intercept all traffic and establish mTLS connections between themselves. From the collector's gRPC server, this means all client streams arrive over a small, persistent pool of Envoy-to-Envoy HTTP/2 TCP connections rather than individual short-lived ones.

Test parameters: identical to bare-metal. Two runs, ~45 000 and ~45 000 logical gRPC client dials.

### Envoy Connection Pool — The Key Measurement

Query the inbound cluster stats on the collector's Envoy sidecar while the load generator is running:

```bash
kubectl exec -n otel-test <otel-collector-pod> -c istio-proxy -- \
  sh -c 'curl -s http://127.0.0.1:15000/clusters' | grep "inbound|4317" | grep -E "(cx_|rq_)"
```

Observed at ~20 000 logical client connections opened:

```
inbound|4317||::10.42.0.26:4317::cx_active: 2
inbound|4317||::10.42.0.26:4317::cx_total:  2      ← only 2 TCP connections ever
inbound|4317||::10.42.0.26:4317::rq_total:  67048  ← all 67k RPCs over those 2 connections
```

The `cx_total` counter never advances beyond 2 regardless of how many times the churn client calls `otlptracegrpc.New`. Envoy multiplexes every gRPC stream over the two persistent mTLS HTTP/2 connections it maintains.

### Heap Profiles Under Mesh

| Profile | Total heap | `bufio.NewReaderSize` |
|---|---|---|
| load (run 1, ~45 k conns) | 18.8 MB | 0 |
| after-1 GC | 18.8 MB | 0 |
| after-2 GC | 18.8 MB | 0 |
| after-3 GC | 18.8 MB | 0 |
| load (run 2, ~45 k conns) | 19.6 MB | 0.81 MB |
| after-1/2/3 GC | 19.6 MB | 0.81 MB |

The 0.81 MB of `bufio.NewReaderSize` visible in run 2 is the normal steady-state for the 2 permanent connections plus other internal gRPC connections (e.g. internal OTLP pipelines). It is identical to what remains after the 3rd post-load GC in the bare-metal run — the baseline for any running collector. There is no additional accumulation from the churn.

## Conclusion

| Scenario | TCP connections seen by collector | `bufio` accumulation during churn |
|---|---|---|
| Bare-metal | one per client dial (~35 k) | +6.5 MB, clears in 3 GC cycles |
| Istio mesh | 2 (Envoy pools all streams) | none beyond the steady-state 0.81 MB |

The `bufio.NewReaderSize` retention pattern described in OTel issue #15086 is real but bounded: it is proportional to the rate of new TCP connections arriving at the gRPC server. In a Kyma production environment with Istio sidecars, Envoy's connection pooling prevents new TCP connections from being created at the collector for each client dial. The collector sees only the handful of long-lived Envoy-to-Envoy HTTP/2 connections, so the sync.Pool accumulation pattern never manifests.

If you observe elevated `bufio.NewReaderSize` retention in a Kyma environment, verify that Istio sidecar injection is active on the collector pod (`kubectl get pod <pod> -o jsonpath='{.spec.containers[*].name}'` should include `istio-proxy`) and confirm that `cx_total` on the `inbound|4317` cluster is not growing unexpectedly.
