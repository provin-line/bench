package gate

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/provin-line/oss/resolver"
)

// This file is the CACHED counterpart of the gate measurements: the same
// deliveries with resolver/cache between inputcapture and the (possibly
// admission-bounded) resolver — a node with the did-cache block enabled, in
// its steady warm state.
//
// # What the cached mode models, and what it does not
//
//   - Steady warm state only. The fixture TTL is far longer than any run, so
//     TTL-expiry churn (a 3-miss refill per window) is deliberately not
//     measured — it amortizes to noise at any realistic TTL.
//   - The hit rate here is an UPPER bound. A gate fixture's chain touches
//     gateDIDDistinctDocuments distinct documents regardless of depth, so
//     after first touch the DID family leaves the synchronous path entirely.
//     A deployment whose chains span many organizations touches more distinct
//     documents; its miss rate sits between this mode and the uncached one.
//   - The credential family is untouched: it is a strictly sequential pointer
//     chase over per-occurrence content addresses — uncacheable across
//     occurrences by construction — so depth-1 round trips per delivery
//     remain, and remain the dominant latency term once DID resolution is
//     cached.
//   - Everything the base harness excludes stays excluded.

// gateDIDDistinctDocuments is how many distinct DID documents one fixture's
// chain authenticates against: the process (signer), its pipeline, and the
// owner. Depth changes how OFTEN each is consulted (4·depth resolutions),
// never how MANY documents exist — which is exactly why first-touch caching
// collapses the DID family.
const gateDIDDistinctDocuments = 3

// TestGateRoundTripAccountingCached pins what the cached benchmarks charge:
// the first delivery through a fixture resolves each distinct document once
// (gateDIDDistinctDocuments misses reach the delay model) plus the depth-1
// credential chase; every later delivery pays the credential chase alone. The
// upstream first-touch shape is pinned in provin.oss
// (TestVerifyChain_DIDResolutionBudget_Cached); this asserts the HARNESS
// charges what that shape implies.
func TestGateRoundTripAccountingCached(t *testing.T) {
	ctx := context.Background()
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			fixture := buildGateChain(t, depth, "accounting-cached").fixture(t, gateOptions{cached: true})
			fixture.deliver(t, ctx)

			if got, want := fixture.didDelay.calls.Load(), int64(gateDIDDistinctDocuments); got != want {
				t.Errorf("DID resolutions after first delivery = %d, want %d (one per distinct document)", got, want)
			}
			if got, want := fixture.localDelay.calls.Load(), int64(depth-1); got != want {
				t.Errorf("credential resolutions = %d, want %d (the chase is not cacheable)", got, want)
			}

			fixture.deliver(t, ctx)
			if got, want := fixture.didDelay.calls.Load(), int64(gateDIDDistinctDocuments); got != want {
				t.Errorf("DID resolutions after second delivery = %d, want %d (warm deliveries must not resolve)", got, want)
			}
			if got, want := fixture.localDelay.calls.Load(), int64(2*(depth-1)); got != want {
				t.Errorf("credential resolutions after second delivery = %d, want %d", got, want)
			}
		})
	}
}

// BenchmarkGateDepthCached is BenchmarkGateDepth with the cache warm: what a
// delivery costs when DID resolution is a local read. The gap to the uncached
// curve is pure DID-family decode work (the crypto and manifest terms are
// identical).
func BenchmarkGateDepthCached(b *testing.B) {
	for _, depth := range gateDepths {
		ring := newGateRing(b, depth, gateOptions{salt: "depth-cached", cached: true})
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			runGateSerial(b, ring)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
		})
	}
}

// BenchmarkGateThroughputCached repeats the concurrency-flatness question with
// the cache on the path: the cache adds one mutex around LRU bookkeeping, and
// this benchmark is what shows whether that lock is visible at gate
// concurrency.
func BenchmarkGateThroughputCached(b *testing.B) {
	ctx := context.Background()
	for _, depth := range []int{2, 32} {
		ring := newGateRing(b, depth, gateOptions{salt: "throughput-cached", cached: true, uncounted: true})
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var worker atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				fixture := ring[int(worker.Add(1)-1)%len(ring)]
				for pb.Next() {
					if err := fixture.deliverErr(ctx); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkGateResolverLatencyCached is the before/after headline: the same
// per-call delays as BenchmarkGateResolverLatency, but only the credential
// family still pays them — depth-1 charged round trips instead of 5·depth-1.
func BenchmarkGateResolverLatencyCached(b *testing.B) {
	for _, latency := range []time.Duration{100 * time.Microsecond, 1 * time.Millisecond} {
		for _, depth := range []int{2, 8, 32} {
			ring := newGateRing(b, depth, gateOptions{salt: "rtt-cached", latency: latency, cached: true})
			b.Run(fmt.Sprintf("rtt=%s/depth=%d", latency, depth), func(b *testing.B) {
				runGateSerial(b, ring)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
			})
		}
	}
}

// BenchmarkGateColdReplicationCached is the cold path with DID resolution
// cached: the cold tax becomes a pure credential-family effect (local miss +
// upstream fetch per hop), no longer diluted or amplified by the DID family.
func BenchmarkGateColdReplicationCached(b *testing.B) {
	const localRTT = 100 * time.Microsecond
	for _, upstreamRTT := range []time.Duration{100 * time.Microsecond, 1 * time.Millisecond} {
		for _, depth := range []int{2, 8, 32} {
			ring := newGateRing(b, depth, gateOptions{
				salt: "cold-cached", latency: localRTT, upstreamLatency: upstreamRTT, cold: true, cached: true,
			})
			b.Run(fmt.Sprintf("local=100µs/upstream=%s/depth=%d", upstreamRTT, depth), func(b *testing.B) {
				runGateSerial(b, ring)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
			})
		}
	}
}

// BenchmarkGateBoundedResolverCached re-runs the saturation sweep with the
// cache warm. Warm deliveries touch the admission bound only on a miss, so the
// resolver ceiling should vanish as a constraint: no busy admissions, no
// quarantine collapse, and the new limit is whatever the credential chase and
// CPU allow. The busyresolves/op metric is the direct evidence — compare it
// against the uncached sweep's tens per attempt.
func BenchmarkGateBoundedResolverCached(b *testing.B) {
	const depth = 32
	const latency = 1 * time.Millisecond
	ctx := context.Background()

	bound := newResolverBound(gateResolverSlots)
	ring := newGateRing(b, depth, gateOptions{
		salt: "bounded-cached", latency: latency, uncounted: true, cached: true,
		didWrap: func(next resolver.Resolver) resolver.Resolver {
			return &boundedDIDResolver{next: next, bound: bound}
		},
	})

	for _, workers := range []int{32, 64, 80, 96, 128} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			busyBase := bound.busy.Load()
			var attempts, acceptedN, quarantinedN, refusedN atomic.Int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				fixture := ring[w%len(ring)]
				wg.Add(1)
				go func() {
					defer wg.Done()
					for attempts.Add(1) <= int64(b.N) {
						outcome, err := fixture.deliverOutcome(ctx)
						switch {
						case err != nil:
							b.Error(err)
							return
						case outcome == outcomeAccepted:
							acceptedN.Add(1)
						case outcome == outcomeQuarantined:
							quarantinedN.Add(1)
						default:
							refusedN.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(acceptedN.Load())/elapsed, "accepted/s")
			}
			b.ReportMetric(float64(quarantinedN.Load())/float64(b.N), "quarantined/op")
			b.ReportMetric(float64(refusedN.Load())/float64(b.N), "refused/op")
			b.ReportMetric(float64(bound.busy.Load()-busyBase)/float64(b.N), "busyresolves/op")
		})
	}
}
