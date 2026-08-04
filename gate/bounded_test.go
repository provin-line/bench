package gate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chainappraisal "github.com/provin-line/oss/appraisal/chain"
	"github.com/provin-line/oss/appraisal/inputcapture"
	"github.com/provin-line/oss/did"
	"github.com/provin-line/oss/network/pkg/didresolver"
	"github.com/provin-line/oss/pipeline/contract"
	"github.com/provin-line/oss/resolver"
	"github.com/provin-line/oss/vc"
)

// This file measures the capacity ceiling that gate-scaling-results.md §5
// previously DERIVED: the production DID resolver (network/pkg/didresolver)
// admits at most maxConcurrentResolutions simultaneous outbound fetches and,
// at the bound, fails fast with ErrResolverBusy rather than queueing. The gate
// classifies that transient error INDETERMINATE, the profile maps INDETERMINATE
// to QUARANTINE, and the sink rejects the delivery without calling the writer —
// a fail-closed non-delivery the sender never sees as backpressure.
//
// Unlike every other benchmark in this package, deliveries here are EXPECTED to
// fail once the bound saturates — saturation is the measurement. To keep that
// deliberate and distinct from silently measuring a rejected path (AGENTS.md
// rule 5), every attempt is classified into a closed set of outcomes: accepted,
// quarantined (verdict INDETERMINATE), or refused (busy removed an input the
// appraiser cannot decide without). All are counted and reported; any other
// outcome fails the benchmark.
//
// # What is and is not modelled
//
//   - The admission bound and its fail-fast semantics are modelled exactly:
//     one process-wide semaphore of gateResolverSlots slots shared by every
//     delivery, non-blocking acquire, didresolver.ErrResolverBusy when full.
//   - In production the same 64 slots are ALSO consumed by L2 wire verification
//     and control/data-plane resolution; here appraisal is the only tenant, so
//     the measured ceiling is an upper bound on the appraisal share.
//   - Workers are closed-loop with zero think time and no backoff: each failed
//     attempt is immediately followed by the next delivery. This models an
//     ingress under sustained offered load, not a well-behaved retrying client.
//   - Everything the base harness excludes (bandwidth, connection setup, TLS,
//     StoreVC, receipt issuance) stays excluded.

// gateResolverSlots mirrors network/pkg/didresolver's unexported
// maxConcurrentResolutions. The literal is load-bearing: the §5 ceiling is
// 64 / (per-delivery slot-hold time). provin.oss pins the production value
// (TestDefaultResolutionBoundIsThePublishedCapacityModel), so a change there
// fails upstream in the same run with instructions to re-measure here.
const gateResolverSlots = 64

// resolverBound is the process-wide admission bound: one semaphore shared by
// every fixture in a ring, exactly as production shares one didresolver
// instance across all inbound deliveries. busy counts failed admissions — it is
// written only on the saturated path, so the unsaturated fast path stays free
// of shared writes.
type resolverBound struct {
	sem  chan struct{}
	busy atomic.Int64
}

func newResolverBound(slots int) *resolverBound {
	return &resolverBound{sem: make(chan struct{}, slots)}
}

// boundedDIDResolver models the production admission path exactly: acquire a
// slot without blocking, or fail fast with the REAL didresolver.ErrResolverBusy
// — the same error value production returns, so the verifier's transient
// classification (any non-ErrNotFound resolver error → INDETERMINATE) is
// exercised on the identical input. The slot covers delay and decode, matching
// production's semaphore window (preflight through body parse).
type boundedDIDResolver struct {
	next  resolver.Resolver
	bound *resolverBound
}

var _ resolver.Resolver = (*boundedDIDResolver)(nil)

func (r *boundedDIDResolver) Resolve(ctx context.Context, didString string) (*did.DIDDocument, error) {
	select {
	case r.bound.sem <- struct{}{}:
		defer func() { <-r.bound.sem }()
	default:
		r.bound.busy.Add(1)
		return nil, didresolver.ErrResolverBusy
	}
	return r.next.Resolve(ctx, didString)
}

// deliveryOutcome is the closed set of results a bounded delivery can have.
// Classification is typed, never inferred from success alone, so saturation can
// never silently absorb a different failure (AGENTS.md rule 5).
type deliveryOutcome int

const (
	// outcomeAccepted: the delivery passed and the writer ran.
	outcomeAccepted deliveryOutcome = iota
	// outcomeQuarantined: a verdict WAS computed and it is INDETERMINATE — busy
	// refused an admission somewhere other than the head credential's
	// signer-authenticity resolution — so the local decision is QUARANTINE and
	// the sink rejects as StatusErrored carrying that verdict.
	outcomeQuarantined
	// outcomeRefused: busy removed an input the appraiser cannot construct a
	// decidable view without, so no verdict exists at all. Two known ways:
	// the head credential's own signer-authenticity resolution was refused, so
	// the verifier reports no cryptosuite contract (chainappraisal.ErrNoSuite);
	// or EVERY resolution was refused, so inputcapture recorded no snapshots
	// (inputcapture.ErrNoSnapshots). Same fail-closed non-delivery, different
	// shape: the sink rejects with no confidence verdict attached.
	outcomeRefused
)

// deliverOutcome classifies one delivery attempt. Busy is the only failure
// source in a bounded fixture (credential resolution is unbounded and fully
// seeded), so the non-accepted shapes are exhaustively the ways busy surfaces.
// The refused shapes reach the caller only as strings (contract.Result.Error is
// serializable, not an error value), so they are matched against the exported
// sentinels' message text; anything unmatched returns an error and fails the
// caller rather than being absorbed into a rate.
func (f *gateFixture) deliverOutcome(ctx context.Context) (deliveryOutcome, error) {
	result, err := f.processor.Process(ctx, f.wire)
	if err != nil {
		return 0, fmt.Errorf("process at depth %d: %w", f.depth, err)
	}
	switch {
	case result.Status == contract.StatusPassed:
		return outcomeAccepted, nil
	case result.Status == contract.StatusErrored &&
		result.Confidence != nil && *result.Confidence == vc.ConfidenceIndeterminate:
		return outcomeQuarantined, nil
	case result.Status == contract.StatusErrored && result.Confidence == nil &&
		(strings.Contains(result.Error, chainappraisal.ErrNoSuite.Error()) ||
			strings.Contains(result.Error, inputcapture.ErrNoSnapshots.Error())):
		return outcomeRefused, nil
	default:
		return 0, fmt.Errorf(
			"delivery at depth %d has no bounded-resolver outcome: status=%v confidence=%v error=%q",
			f.depth, result.Status, result.Confidence, result.Error)
	}
}

// scriptedBusyResolver fails exactly one scripted call (1-based) with the
// production busy error and passes every other call through. It exists because
// a mid-chain busy — some resolutions admitted, then one refused — is
// inherently a concurrency phenomenon under the real semaphore: no
// deterministic test can produce it through the bound itself. Scripting the
// Nth call reproduces the resulting delivery shape (snapshots exist, verdict
// INDETERMINATE, decision QUARANTINE) without racing goroutines.
type scriptedBusyResolver struct {
	next   resolver.Resolver
	busyOn int64
	calls  atomic.Int64
	busied atomic.Int64
}

var _ resolver.Resolver = (*scriptedBusyResolver)(nil)

func (r *scriptedBusyResolver) Resolve(ctx context.Context, didString string) (*did.DIDDocument, error) {
	if r.calls.Add(1) == r.busyOn {
		r.busied.Add(1)
		return nil, didresolver.ErrResolverBusy
	}
	return r.next.Resolve(ctx, didString)
}

// TestGateBoundedResolverBusyQuarantinesWithoutDelivery is the correctness gate
// for the saturation benchmark: busy actually occurs and is counted, a busy
// delivery is a fail-closed NON-delivery (the writer is never called), and each
// of the two busy shapes maps to the deliveryOutcome the benchmark counts it
// under. The control fixture proves the wrapper is transparent below the bound.
func TestGateBoundedResolverBusyQuarantinesWithoutDelivery(t *testing.T) {
	ctx := context.Background()
	const depth = 3
	lineage := buildGateChain(t, depth, "busy")

	// Mid-chain busy: one refused admission among 4·depth resolutions. The
	// verdict is computable (snapshots exist) and is INDETERMINATE → QUARANTINE.
	scripted := &scriptedBusyResolver{busyOn: 6}
	quarantined := lineage.fixture(t, gateOptions{capture: true, didWrap: func(next resolver.Resolver) resolver.Resolver {
		scripted.next = next
		return scripted
	}})
	outcome, err := quarantined.deliverOutcome(ctx)
	if err != nil {
		t.Fatalf("mid-chain busy delivery: %v", err)
	}
	if outcome != outcomeQuarantined {
		t.Errorf("mid-chain busy outcome = %v, want outcomeQuarantined", outcome)
	}
	if got := scripted.busied.Load(); got != 1 {
		t.Errorf("scripted busy count = %d, want 1", got)
	}
	if got := scripted.calls.Load(); got <= scripted.busyOn {
		t.Errorf("resolver calls = %d, want > %d: the walk should continue past a busy "+
			"(VerifyChain does not short-circuit — the wasted work the benchmark reports is real)",
			got, scripted.busyOn)
	}
	if _, writes := quarantined.writer.snapshot(); writes != 0 {
		t.Errorf("writer calls = %d after a quarantined delivery, want 0 (fail-closed non-delivery)", writes)
	}

	// Head busy: the one admission whose refusal removes the head credential's
	// cryptosuite contract. Credentials are verified origin-first, each costing
	// 4 resolutions with signer authenticity first, so the head's
	// signer-authenticity resolution is call 4·(depth−1)+1.
	headScript := &scriptedBusyResolver{busyOn: 4*(depth-1) + 1}
	headBusy := lineage.fixture(t, gateOptions{capture: true, didWrap: func(next resolver.Resolver) resolver.Resolver {
		headScript.next = next
		return headScript
	}})
	outcome, err = headBusy.deliverOutcome(ctx)
	if err != nil {
		t.Fatalf("head-busy delivery: %v", err)
	}
	if outcome != outcomeRefused {
		t.Errorf("head-busy outcome = %v, want outcomeRefused (no cryptosuite contract, no verdict)", outcome)
	}
	if _, writes := headBusy.writer.snapshot(); writes != 0 {
		t.Errorf("writer calls = %d after a head-busy delivery, want 0 (fail-closed non-delivery)", writes)
	}

	// Starvation: zero slots, every admission refused, no snapshots — the
	// appraiser cannot construct a view and the sink still delivers nothing.
	empty := newResolverBound(0)
	starved := lineage.fixture(t, gateOptions{capture: true, didWrap: func(next resolver.Resolver) resolver.Resolver {
		return &boundedDIDResolver{next: next, bound: empty}
	}})
	outcome, err = starved.deliverOutcome(ctx)
	if err != nil {
		t.Fatalf("starved delivery: %v", err)
	}
	if outcome != outcomeRefused {
		t.Errorf("starved outcome = %v, want outcomeRefused", outcome)
	}
	if empty.busy.Load() == 0 {
		t.Error("busy admissions = 0: the bound was never hit, so this test proved nothing")
	}
	if _, writes := starved.writer.snapshot(); writes != 0 {
		t.Errorf("writer calls = %d after a starved delivery, want 0 (fail-closed non-delivery)", writes)
	}

	// Control: production capacity, no concurrency — the bound is invisible.
	relaxed := newResolverBound(gateResolverSlots)
	control := lineage.fixture(t, gateOptions{capture: true, didWrap: func(next resolver.Resolver) resolver.Resolver {
		return &boundedDIDResolver{next: next, bound: relaxed}
	}})
	control.deliver(t, ctx)
	if got := relaxed.busy.Load(); got != 0 {
		t.Errorf("busy admissions = %d on an uncontended fixture, want 0", got)
	}
	if got, want := control.roundTrips(), int64(5*depth-1); got != want {
		t.Errorf("round trips = %d with the bound in place, want %d (the wrapper must not change accounting)", got, want)
	}
	if _, writes := control.writer.snapshot(); writes != 1 {
		t.Errorf("writer calls = %d for the control delivery, want 1", writes)
	}
}

// BenchmarkGateBoundedResolver sweeps concurrent closed-loop workers across the
// admission bound and reports, per worker count, the sustained accepted
// throughput and the busy-driven non-delivery rate. A worker holds at most one
// resolution slot at a time (resolutions inside a delivery are sequential), so
// workers ≤ 64 cannot hit the bound; the knee is expected where average slot
// demand crosses 64 — near 80 workers at this depth and latency, since a
// delivery holds a slot for roughly 150 ms of its ~186 ms wall time — and the
// accepted plateau tests §5's derived R_max ≈ 64 / 0.150 ≈ 430/s.
//
// Beyond the knee, expect goodput BELOW the plateau, not at it: VerifyChain
// does not short-circuit on an indeterminate verdict, so a delivery that hits
// busy mid-chain still performs its remaining resolutions, holding slots for
// work whose outcome is already QUARANTINE. That wasted slot-hold is the
// congestion behaviour of the production fail-fast bound, not a harness
// artifact.
func BenchmarkGateBoundedResolver(b *testing.B) {
	const depth = 32
	const latency = 1 * time.Millisecond
	ctx := context.Background()

	bound := newResolverBound(gateResolverSlots)
	ring := newGateRing(b, depth, gateOptions{
		salt: "bounded", latency: latency, uncounted: true,
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
