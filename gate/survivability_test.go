package gate

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/provin-line/oss/did"
	"github.com/provin-line/oss/pipeline/contract"
	"github.com/provin-line/oss/resolver"
)

// This file is the issuer-stop scenario: what happens to deliveries after the
// issuer-side DID infrastructure stops answering — retirement, outage, or
// decommissioning — in each resolver posture.
//
// # What the scenario models, and what it does not
//
//   - Credentials are locally held. The in-memory credential store is the
//     recipient's replica (the partial-replication premise) and stays up;
//     only the DID family's upstream stops. Cold credential replication is
//     its own axis, measured separately.
//   - The stop is injected below the cache, where production admission
//     control sits: a stopped issuer fails every resolution that reaches
//     upstream, while cache entries keep serving until their TTL.
//   - resolver/cache has no serve-stale-on-error, so in the cached posture
//     survivability ends at the freshness window's edge by construction; the
//     scenario crosses that edge with a short real TTL.
//   - One fixture, one node, functional observation only: verdict, writer
//     count, and upstream call counts. Nothing here is a latency or
//     throughput measurement.
//
// Like the accounting and busy correctness gates, this is a composed-gate
// observation no upstream package test can express: the per-package cache
// invariants are pinned in provin.oss resolver/cache's own tests —
// TestResolveCachesWithinTTLAndIsolatesCallers (within-TTL hits),
// TestTTLIsAbsoluteFromFillAndHitsDoNotRefresh (absolute expiry),
// TestExpiredEntryIsNeverServed and TestErrorsAreNotCached (no serve-stale) —
// and what is pinned HERE is what those invariants mean for a whole delivery
// through the assembled gate.

// stoppableDIDResolver serves documents until stopped, then fails every
// resolution — the issuer's DID endpoint going away.
type stoppableDIDResolver struct {
	next    resolver.Resolver
	stopped atomic.Bool
	refused atomic.Int64
}

var _ resolver.Resolver = (*stoppableDIDResolver)(nil)

func (r *stoppableDIDResolver) Resolve(ctx context.Context, didString string) (*did.DIDDocument, error) {
	if r.stopped.Load() {
		r.refused.Add(1)
		return nil, fmt.Errorf("issuer DID infrastructure stopped: %s", didString)
	}
	if r.next == nil {
		return nil, fmt.Errorf("stoppableDIDResolver: next resolver not wired (construct via gateOptions.didWrap)")
	}
	return r.next.Resolve(ctx, didString)
}

// deliverStatus runs one occurrence and returns its terminal status, so a
// scenario can pin the exact conservative-stop shape rather than "some
// failure". A transport-level error from Process itself fails the test.
func (f *gateFixture) deliverStatus(tb testing.TB, ctx context.Context) contract.Status {
	tb.Helper()
	result, err := f.processor.Process(ctx, f.wire)
	if err != nil {
		tb.Fatalf("process at depth %d: %v", f.depth, err)
	}
	return result.Status
}

// TestGateIssuerStopUncached pins the default posture: a chain that verified
// moments ago stops verifying the moment the issuer's DID infrastructure
// does, because every delivery performs 4·depth document resolutions live.
// The writer must not run for the stopped delivery.
func TestGateIssuerStopUncached(t *testing.T) {
	ctx := context.Background()
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			stop := &stoppableDIDResolver{}
			fixture := buildGateChain(t, depth, "issuer-stop-uncached").fixture(t, gateOptions{
				capture: true,
				didWrap: func(next resolver.Resolver) resolver.Resolver {
					stop.next = next
					return stop
				},
			})

			fixture.deliver(t, ctx)
			if _, got := fixture.writer.snapshot(); got != 1 {
				t.Fatalf("writer calls after live delivery = %d, want 1", got)
			}

			stop.stopped.Store(true)
			if got := fixture.deliverStatus(t, ctx); got != contract.StatusErrored {
				t.Fatalf("uncached post-stop delivery status = %v, want %v (conservative error stop)", got, contract.StatusErrored)
			}
			if _, got := fixture.writer.snapshot(); got != 1 {
				t.Errorf("writer calls after stopped delivery = %d, want 1 (no delivery on failure)", got)
			}
			if got := stop.refused.Load(); got == 0 {
				t.Error("no resolution reached the stopped upstream; the failure did not exercise the stop")
			}
		})
	}
}

// TestGateIssuerStopCached pins the cached posture on both sides of the
// freshness window: within the TTL, locally held credentials plus cached
// documents carry the delivery with zero upstream resolutions; beyond it,
// with no serve-stale-on-error, the delivery stops conservatively and the
// writer does not run.
//
// The within-window half runs against a real clock (the cache's clock hook is
// package-internal to provin.oss). A machine too loaded to hold the window
// would fail window-dependent assertions for scheduling reasons, not product
// reasons — so every assertion that depends on the window holding (including
// the warm delivery's resolution count, whose entries can legitimately expire
// mid-delivery on a stalled machine) downgrades to a skip when the window had
// already elapsed by the time it would have failed. The guard fires only on
// the already-failing path, so a healthy run asserts at full strength; the
// writer count after a delivery that already succeeded is deterministic and
// never skips.
func TestGateIssuerStopCached(t *testing.T) {
	ctx := context.Background()
	const ttl = 500 * time.Millisecond
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			stop := &stoppableDIDResolver{}
			fixture := buildGateChain(t, depth, "issuer-stop-cached").fixture(t, gateOptions{
				capture:  true,
				cached:   true,
				cacheTTL: ttl,
				didWrap: func(next resolver.Resolver) resolver.Resolver {
					stop.next = next
					return stop
				},
			})

			// tooSlow reports whether the freshness window measured from
			// before the warm delivery — a lower bound on every entry's
			// admission time — had already closed.
			admitted := time.Now()
			tooSlow := func() bool { return time.Since(admitted) >= ttl }
			skipSlow := func() {
				t.Skipf("machine too slow to hold the %v freshness window (elapsed %v)", ttl, time.Since(admitted))
			}

			// Warm delivery with the issuer alive: each distinct document is
			// resolved once and enters the cache.
			fixture.deliver(t, ctx)
			if got, want := fixture.didDelay.calls.Load(), int64(gateDIDDistinctDocuments); got != want {
				if tooSlow() {
					skipSlow()
				}
				t.Fatalf("upstream DID resolutions after warm delivery = %d, want %d", got, want)
			}

			stop.stopped.Store(true)

			// Within the freshness window: the delivery must complete without
			// a single upstream resolution.
			if err := fixture.deliverErr(ctx); err != nil {
				if tooSlow() {
					skipSlow()
				}
				t.Fatalf("within-TTL delivery failed with the issuer stopped: %v", err)
			}
			if got := stop.refused.Load(); got != 0 {
				if tooSlow() {
					skipSlow()
				}
				t.Errorf("upstream resolutions attempted within the TTL = %d, want 0 (cache must carry the delivery)", got)
			}
			if got, want := fixture.didDelay.calls.Load(), int64(gateDIDDistinctDocuments); got != want {
				if tooSlow() {
					skipSlow()
				}
				t.Errorf("upstream DID resolutions after within-TTL delivery = %d, want %d (warm deliveries must not resolve)", got, want)
			}
			if _, got := fixture.writer.snapshot(); got != 2 {
				t.Errorf("writer calls after within-TTL delivery = %d, want 2", got)
			}

			// Beyond the freshness window: entries have expired, there is no
			// serve-stale-on-error, and the issuer is gone.
			time.Sleep(ttl + 200*time.Millisecond)
			if got := fixture.deliverStatus(t, ctx); got != contract.StatusErrored {
				t.Fatalf("cached post-TTL delivery status = %v, want %v (conservative error stop)", got, contract.StatusErrored)
			}
			if _, got := fixture.writer.snapshot(); got != 2 {
				t.Errorf("writer calls after post-TTL delivery = %d, want 2 (no delivery on failure)", got)
			}
			if got := stop.refused.Load(); got == 0 {
				t.Error("no resolution reached the stopped upstream after expiry; the failure did not exercise the stop")
			}
		})
	}
}
