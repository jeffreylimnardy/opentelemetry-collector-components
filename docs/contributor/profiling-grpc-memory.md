# Profiling gRPC Memory Retention in the OTel Collector

This guide documents how to reproduce and analyze the `bufio.NewReaderSize` memory retention pattern in the OTel Collector's OTLP gRPC receiver and explains when Istio sidecar injection suppresses or permits the pattern.

## Background

[OTel Collector issue #15086](https://github.com/open-telemetry/opentelemetry-collector/issues/15086) describes a scenario where the collector's heap grows during high gRPC connection churn and only partially releases across multiple GC cycles. The root cause is Go's `sync.Pool`: each incoming TCP connection to the gRPC server creates a `bufio.Reader` (via `newFramer` → `bufio.NewReaderSize`), and when the connection closes, that reader is placed into `sync.Pool`. Pool objects survive one GC cycle before eviction, so under sustained churn, memory accumulates proportionally to the connection rate.

In Kyma's production setup the collector runs inside an Istio service mesh. Whether the pattern manifests depends entirely on **whether the collector container itself sees individual TCP connections or just a small pooled set from its local Envoy sidecar**. This guide documents all three relevant scenarios.

## Tools

- `go tool pprof` — heap and goroutine profile analysis
- `curl` with `?gc=1` on the pprof endpoint — triggers a GC before capturing the heap snapshot
- The [churn load generator](#churn-load-generator) — a small Go program that repeatedly dials gRPC, sends spans, and closes the connection
- `kubectl port-forward` — access the collector's pprof endpoint from outside the cluster
- Envoy admin API (`http://127.0.0.1:15000/clusters`) — inspect actual TCP connection counts
- `nsenter -n -t <pid> -- ss -tn` — inspect TCP connections inside a container's network namespace

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
    endpoint     := flag.String("endpoint", "127.0.0.1:4317", "collector endpoint")
    workers      := flag.Int("workers", 32, "concurrent workers")
    duration     := flag.Duration("duration", 5*time.Minute, "test duration")
    spansPerConn := flag.Int("spans", 10, "spans per connection before close")
    pause        := flag.Duration("pause", 200*time.Millisecond, "pause between connections per worker")
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

The double-GC technique is critical: calling `?gc=1` triggers one GC before the snapshot. Capturing three sequential profiles reveals what survives across multiple GC cycles.

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

# Diff: what changed between baseline and load
go tool pprof -inuse_space -base heap-baseline.pb.gz -top heap-load.pb.gz

# Diff: what survives 3 GC cycles after load ends
go tool pprof -inuse_space -base heap-load.pb.gz -top heap-after-3.pb.gz
```

In a Kubernetes environment, port-forward to the pprof endpoint before capturing:

```bash
kubectl port-forward -n <namespace> <pod> 1778:1777 &
curl -s "http://127.0.0.1:1778/debug/pprof/heap?gc=1" -o heap-load.pb.gz
```

## Results: Bare-Metal (Direct gRPC)

Test parameters: 32 workers, 200 ms pause, 10 spans/connection, 5 minutes, ~35 000 connections total.

| Profile | Total heap | `bufio.NewReaderSize` |
|---|---|---|
| baseline | 21.8 MB | 0 |
| load (mid-run) | 27.2 MB | **6.5 MB** (23.8% of heap) |
| after-1 GC | 27.8 MB | 6.5 MB |
| after-2 GC | 26.1 MB | 4.9 MB |
| after-3 GC | 21.6 MB | **0.81 MB** |

The `load - baseline` diff isolates the cause:

```
flat   flat%   cum
6479 kB  86%    bufio.NewReaderSize (inline)
                google.golang.org/grpc.(*Server).Serve.func3
                google.golang.org/grpc.(*Server).handleRawConn
                google.golang.org/grpc.(*Server).newHTTP2Transport
                google.golang.org/grpc/internal/transport.NewServerTransport
                google.golang.org/grpc/internal/transport.newFramer
```

Every new TCP connection triggers this call chain once. The resulting `bufio.Reader` (32 KB default) is placed into `sync.Pool` on connection close. Under sustained churn, the pool accumulates ~6.5 MB that clears in 3 GC cycles after load stops.

This is a bounded pattern (proportional to churn rate, not unbounded) but it causes persistent elevated heap usage under ongoing pod-level connection churn in production.

## Istio Mesh — Three Scenarios

The pattern's behavior in an Istio mesh depends on a single property: **how many TCP connections the collector container itself accepts**. This is determined by whether the collector's Envoy sidecar intercepts port 4317 inbound traffic and pools it, or whether connections arrive at the container directly.

### Scenario 1: Full Mesh — Sidecar with Port 4317 Intercepted (Pattern Suppressed)

This is the configuration when the collector has Istio injection enabled and no inbound port exclusions.

The collector's Envoy sidecar intercepts all traffic on port 4317 via iptables (`ISTIO_INBOUND` chain redirects to the virtualInbound listener). It then proxies to the collector container using its own local connection pool.

**Observation (45 000 logical gRPC dials from 1 source pod → 10 separate pods):**

```bash
# Envoy sidecar on collector pod
kubectl exec <otel-collector-pod> -c istio-proxy -- \
  sh -c 'curl -s http://127.0.0.1:15000/clusters' | grep "inbound|4317" | grep -E "(cx_|rq_)"

inbound|4317||::cx_active: 2
inbound|4317||::cx_total:  2      ← fixed at 2 regardless of source pod count
inbound|4317||::rq_total:  90414  ← 90k RPCs over those 2 connections
```

Envoy multiplexes all gRPC streams (from all source pods' Envoys) over 2 persistent HTTP/2 connections to the collector container. The collector container sees only 2 TCP connections regardless of how many pods are connecting or restarting.

**Heap profiles:**

| Profile | Total heap | `bufio.NewReaderSize` |
|---|---|---|
| load (~45k conns) | 18.8 MB | 0 |
| after-1/2/3 GC | 18.8 MB | 0 |

No `bufio` accumulation. The pattern is fully suppressed.

### Scenario 2: Collector Without Sidecar Injection (Pattern Active)

When the collector pod has `sidecar.istio.io/inject: "false"` (or is in a namespace without `istio-injection: enabled`), all inbound connections arrive directly at the collector container. Source pods with sidecars still create per-pod Envoy-to-collector TCP connections.

**Observation (15 source pods, each with a sidecar, targeting sidecar-less collector):**

```bash
# From the node, inspect the collector container's network namespace
COLLECTOR_PID=$(pgrep otelcol-contrib)
nsenter -n -t $COLLECTOR_PID -- ss -tn state established | grep ":4317"

# 30 ESTABLISHED connections: each source Envoy maintains 2 HTTP/2 connections
# to the collector container (15 pods × 2 connections per Envoy = 30 total)
```

Each source pod's Envoy sidecar creates its own HTTP/2 connection pool (2 connections) directly to the collector container. When pods restart, those connections close and new ones open — each new connection creates a new `bufio.Reader` in the gRPC server.

**Heap profile at 30 active direct connections:**

| Profile | Total heap | `bufio.NewReaderSize` |
|---|---|---|
| load (30 direct conns) | 32.5 MB | **16.2 MB** (49.9% of heap) |

The diff:
```
flat   flat%   cum
16199 kB  86%   bufio.NewReaderSize (inline)
 1056 kB   6%   google.golang.org/grpc/internal/transport.newBufWriter
```

Same 86% signature as bare-metal. The pattern is fully active, driven by pod-level connection churn.

### Scenario 3: Sidecar Present but Port 4317 Excluded from Interception

Setting the annotation `traffic.sidecar.istio.io/excludeInboundPorts: "4317"` causes the `istio-init` container to add a `RETURN` rule in the `ISTIO_INBOUND` iptables chain specifically for port 4317, before the standard redirect rule:

```
Chain ISTIO_INBOUND:
RETURN  tcp dpt:4317   ← traffic on port 4317 skips Envoy redirect
ISTIO_IN_REDIRECT      ← all other ports redirect to Envoy
```

In this case the Envoy sidecar is present but does not intercept port 4317 inbound traffic. The net effect is the same as Scenario 2: direct connections arrive at the collector container.

**Important:** clients using mTLS (e.g., other mesh pods with sidecars) will fail to connect because the collector container receives TLS-encrypted data without a TLS listener. This annotation is only meaningful when paired with a `DestinationRule` that sets `trafficPolicy.tls.mode: DISABLE` for the collector's port, or when the traffic source explicitly sends plaintext.

## Connection Count as a Diagnostic

The most direct way to determine which scenario applies is to compare the TCP connection count visible to the collector container against the Envoy cluster stats:

```bash
# 1. Connections the container actually sees (from host, find the container PID first)
COLLECTOR_PID=$(pgrep otelcol-contrib)
nsenter -n -t $COLLECTOR_PID -- ss -tn state established | grep -c ":4317 "

# 2. Connections the local Envoy sidecar proxies to the container
kubectl exec <pod> -c istio-proxy -- \
  sh -c 'curl -s http://127.0.0.1:15000/clusters' | grep "inbound|4317" | grep cx_total
```

| Result | Interpretation |
|---|---|
| container sees 2 connections, `cx_total` = 2 | Full mesh protection — Scenario 1 |
| container sees N connections, no `cx_total` stat | No sidecar injection — Scenario 2 |
| container sees N connections, `cx_total` absent | Port excluded from sidecar — Scenario 3 |

## Summary

| Deployment | TCP connections to collector container | `bufio` accumulation |
|---|---|---|
| No sidecar on collector | One per source Envoy (2 × pod count) | Active — proportional to pod restarts |
| Sidecar with port 4317 excluded | One per source Envoy (2 × pod count) | Active — same as no-sidecar for plaintext clients |
| Full sidecar, port 4317 intercepted | 2 (Envoy pools all sources) | Suppressed — only baseline 0.81 MB |

The pattern described in OTel issue #15086 is real but bounded. It manifests in Kyma production whenever the collector receives direct TCP connections — either because the sidecar is absent or port 4317 is excluded from interception. Under those conditions, pod-level restarts and rolling deployments act as the churn source, each pod restart contributing 2 new `bufio.Reader` objects that persist in `sync.Pool` until the next GC cycle.
