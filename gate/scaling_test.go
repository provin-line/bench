package gate

import (
	"context"
	stded25519 "crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/provin-line/oss/appraisal"
	chainappraisal "github.com/provin-line/oss/appraisal/chain"
	"github.com/provin-line/oss/appraisal/inputcapture"
	"github.com/provin-line/oss/crypto"
	"github.com/provin-line/oss/crypto/ed25519"
	"github.com/provin-line/oss/did"
	"github.com/provin-line/oss/pipeline/contract"
	"github.com/provin-line/oss/pipeline/sink"
	"github.com/provin-line/oss/pipeline/transport/envelopecodec"
	"github.com/provin-line/oss/resolver"
	"github.com/provin-line/oss/resolver/cache"
	"github.com/provin-line/oss/vc"
	"github.com/provin-line/oss/vc/chainwalk"
)

// This file measures the SYNCHRONOUS delivery gate over its real components —
// chain walk, per-credential Ed25519 verification, the controller-chain walk,
// EvidenceView manifest construction, and sink call ordering — against chain
// depth, concurrency, resolver latency, and replication state.
//
// It differs from BenchmarkSinkDelivery (benchmark_test.go), which injects a
// PRECOMPUTED appraiser result and therefore isolates only terminal-gate
// arithmetic. That isolation is why the published 30,199 ns/op figure is not a
// per-delivery cost: it excludes resolution and cryptography, which dominate.
//
// # What a delivery actually costs in round trips
//
// Both resolver families are on the synchronous path and BOTH are modelled here:
//
//   - credential resolution: depth-1 lookups (the head arrives in the envelope),
//     a strictly sequential pointer chase — hop k+1's body address is unknown
//     until hop k is fetched and decoded, so these cannot be batched or
//     prefetched;
//   - DID resolution: 4 per credential — one in evalSignerAuthenticity plus
//     three in walkControllerChain (process -> pipeline -> owner). The
//     production resolver (network/pkg/didresolver) performs an uncached HTTP
//     GET per call, so these are real round trips, not local reads.
//
// Total is therefore 5*depth-1 round trips per delivery, and gateRoundTrips
// asserts that arithmetic against the live counters rather than trusting it.
//
// # Expected shapes
//
//   - DEPTH is linear with a small constant, and cannot be sub-linear in this
//     design: each occurrence carries its own lineage (the data-flow invariant
//     binds outputHash[n] to inputHash[n+1]), so no per-hop verification is
//     shareable across occurrences and Ω(depth) work per delivery is structural.
//     Sub-linear depth needs a different design choice (aggregate proofs,
//     incremental verification), not tuning.
//   - THROUGHPUT: per-delivery work should not grow with concurrency, because
//     the path shares no mutable state. To keep that claim about the GATE, the
//     benchmark harness itself must add none: benchmarks use a no-op writer and
//     keep testing.TB bookkeeping out of the timed loop.
//
// # What is still excluded
//
// Latency injection models per-hop delay only — not bandwidth, connection
// setup, or TLS. The ingress store is a no-op, where production performs a
// synchronous StoreVC RPC plus audit-queue registration; receipt issuance is
// absent. Absolute figures therefore remain a LOWER bound on a networked
// deployment.

const (
	gateOwnerDID      = "did:dplaax:reg:org:gatebench"
	gatePipelineDID   = gateOwnerDID + ":pipeline:depth"
	gateProcessDID    = gatePipelineDID + ":process:stage"
	gateKeyID         = "signing"
	gateClaimContract = "linear-provenance@1"
	gateSchemaVersion = "pipeline-pass-credential@1"
	gateProfileID     = "purpose-first-agent-access@1"
	gateBoundaryID    = "provin-agent-delivery@1"
	gateUpstream      = "https://upstream.example"

	// gateDIDResolvesPerCredential is the count the verifier performs per
	// credential: issuer authenticity, then the process -> pipeline -> owner
	// controller walk. TestGateRoundTripAccounting pins it.
	gateDIDResolvesPerCredential = 4
)

// gateDepths spans the range audit item #3 asks for. 2 is the depth every prior
// measurement used; 512 is past any realistic pipeline and exists to make the
// shape of the curve unambiguous, not to model a deployment.
var gateDepths = []int{2, 8, 32, 128, 512}

// rawHash is the content-address form the sink recomputes over delivered bytes.
func rawHash(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// encode produces the wire envelope a sink consumes.
func encode(tb testing.TB, cred *vc.PipelinePassCredential, payload []byte) []byte {
	tb.Helper()
	wire, err := envelopecodec.New().MarshalEnvelope(&contract.Envelope{
		Credential: cred,
		Payload:    payload,
		SequenceNo: 1,
	})
	if err != nil {
		tb.Fatalf("encode envelope: %v", err)
	}
	return wire
}

// noopIngressStore stands in for the ingress retention step. Production performs
// a synchronous StoreVC RPC plus audit-queue registration here; excluding it is
// one of the reasons these figures are a lower bound.
type noopIngressStore struct{}

func (noopIngressStore) StoreIngressVC(context.Context, *vc.PipelinePassCredential, string) error {
	return nil
}

// gateSigner is an in-memory crypto.Signer over one raw Ed25519 key: key
// custody is a separate concern, and excluding it isolates the gate's own cost.
type gateSigner struct{ priv stded25519.PrivateKey }

var _ crypto.Signer = gateSigner{}

func (s gateSigner) Sign(_, _ string, data []byte) ([]byte, error) {
	return stded25519.Sign(s.priv, data), nil
}

// gateDelay is the shared round-trip model: a fixed per-call delay plus an
// optional call counter. Counting is what lets a test prove the cold path is
// actually cold and that the modelled round-trip total is what the file claims.
//
// Counting is OPT-IN because the counter is shared across every goroutine using
// a fixture, and a depth-32 delivery makes 159 resolver calls. Under
// b.RunParallel that is 159 contended atomic read-modify-writes per delivery,
// growing with concurrency — harness contention that would be indistinguishable
// from the gate's own. The concurrency benchmark therefore runs uncounted; the
// serial benchmarks and the tests, where contention is not the question, count.
type gateDelay struct {
	latency time.Duration
	count   bool
	calls   atomic.Int64
}

func (d *gateDelay) charge(ctx context.Context) error {
	if d.count {
		d.calls.Add(1)
	}
	if d.latency <= 0 {
		return nil
	}
	timer := time.NewTimer(d.latency)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// delayingDIDResolver models the production DID resolver's uncached HTTP GET.
// It sits INSIDE inputcapture, mirroring the real composition order
// (verifier -> inputcapture -> network resolver).
//
// It serves stored document bytes and PARSES THEM PER CALL, symmetrically with
// memCredentialResolver. Production fetches and parses a DID document on every
// resolution — there is no cache in network/pkg/didresolver — and since DID
// resolution is four fifths of a delivery's round trips, returning a shared
// pre-parsed document would discount the larger of the two families.
type delayingDIDResolver struct {
	documents map[string][]byte
	delay     *gateDelay
}

var _ resolver.Resolver = (*delayingDIDResolver)(nil)

func (r *delayingDIDResolver) Resolve(ctx context.Context, didString string) (*did.DIDDocument, error) {
	if err := r.delay.charge(ctx); err != nil {
		return nil, err
	}
	raw, ok := r.documents[didString]
	if !ok {
		return nil, fmt.Errorf("gate bench DID resolver: %w: %s", resolver.ErrNotFound, didString)
	}
	doc := &did.DIDDocument{}
	if err := doc.UnmarshalJSON(raw); err != nil {
		return nil, fmt.Errorf("gate bench DID resolver: decode %s: %w", didString, err)
	}
	return doc, nil
}

// memCredentialResolver is the perfect-replication case: every predecessor is
// already held locally.
//
// Wire bytes are stored, not decoded credentials, and each resolve unmarshals
// them — what the real resolver client does. Returning pre-decoded objects
// would quietly discount every hop.
//
// The map is written only during construction and read-only afterwards, so it
// is safe under b.RunParallel without a lock on the hot path.
type memCredentialResolver struct {
	byAddress map[string][]byte
	delay     *gateDelay
	// count gates miss accounting for the same reason gateDelay.count does:
	// a shared atomic on the parallel path is harness contention.
	count  bool
	misses atomic.Int64
}

func (r *memCredentialResolver) ResolveCredential(ctx context.Context, contentAddress string) (*vc.PipelinePassCredential, error) {
	if err := r.delay.charge(ctx); err != nil {
		return nil, err
	}
	wire, ok := r.byAddress[contentAddress]
	if !ok {
		if r.count {
			r.misses.Add(1)
		}
		return nil, fmt.Errorf("gate bench resolver: no credential at %s", contentAddress)
	}
	cred := &vc.PipelinePassCredential{}
	if err := cred.UnmarshalJSON(wire); err != nil {
		return nil, fmt.Errorf("gate bench resolver: decode %s: %w", contentAddress, err)
	}
	return cred, nil
}

// fallbackCredentialResolver mirrors the composition root's cold path
// (cmd/pipeline/wiring.go): local first, then the declared upstream registry.
//
// In production local is this node's own registry and upstream is another
// organization's, so the two delays are configured independently — a symmetric
// setting understates the cold tax and is never the realistic case.
type fallbackCredentialResolver struct {
	local    *memCredentialResolver
	upstream *memCredentialResolver
}

func (r fallbackCredentialResolver) ResolveCredential(ctx context.Context, contentAddress string) (*vc.PipelinePassCredential, error) {
	if cred, err := r.local.ResolveCredential(ctx, contentAddress); err == nil {
		return cred, nil
	}
	return r.upstream.ResolveCredential(ctx, contentAddress)
}

// noopWriter is the benchmark writer. capturingWriter's mutex would serialize
// every parallel delivery, so using it under b.RunParallel would measure the
// harness's contention and report it as the gate's.
type noopWriter struct{}

func (noopWriter) Write(context.Context, sink.Record) error { return nil }

// capturingWriter records the last delivered record for assertions. Tests only.
type capturingWriter struct {
	mu   sync.Mutex
	last sink.Record
	n    int
}

func (w *capturingWriter) Write(_ context.Context, rec sink.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.last = rec
	w.n++
	return nil
}

func (w *capturingWriter) snapshot() (sink.Record, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.n
}

// gateFixture is one fully wired synchronous gate plus one encoded occurrence
// whose head sits at the end of a depth-length signed lineage.
type gateFixture struct {
	processor *sink.Processor
	wire      []byte
	writer    *capturingWriter
	depth     int

	didDelay      *gateDelay
	localDelay    *gateDelay
	upstreamDelay *gateDelay
	localResolver *memCredentialResolver
}

// roundTrips is every modelled synchronous round trip this fixture has served.
func (f *gateFixture) roundTrips() int64 {
	total := f.localDelay.calls.Load()
	if f.didDelay != nil {
		total += f.didDelay.calls.Load()
	}
	if f.upstreamDelay != nil {
		total += f.upstreamDelay.calls.Load()
	}
	return total
}

type gateOptions struct {
	depth   int
	salt    string
	latency time.Duration
	// upstreamLatency defaults to latency when zero and cold is set.
	upstreamLatency time.Duration
	// unseeded makes every predecessor lookup a hole, for the reject path.
	unseeded bool
	// cold routes resolution through the local-miss/upstream-hit fallback.
	cold bool
	// capture swaps in the recording writer. Tests only: its mutex would
	// serialize parallel benchmarks.
	capture bool
	// uncounted disables resolver call accounting. Required for the
	// concurrency benchmark, whose whole question is whether per-delivery cost
	// grows with parallelism: shared counters would answer it themselves.
	uncounted bool
	// didWrap, when set, wraps the DID resolver BELOW inputcapture — the seat
	// of the production resolver's own admission control. bounded_test.go uses
	// it for the process-wide fail-fast semaphore (shared deliberately across a
	// ring: production has one resolver instance per node, not one per
	// delivery) and for deterministic busy scripting.
	didWrap func(resolver.Resolver) resolver.Resolver
	// cached wraps the DID resolver (above any didWrap admission bound, the
	// production order: inputcapture → cache → bounded resolver) in
	// resolver/cache, modelling a node with the did-cache block enabled. A
	// fixture built directly gets its own cache; a RING (newGateRing) builds
	// distinct lineages over one shared identity and one shared cache — the
	// production shape (one node-wide cache serving many occurrences), so the
	// cache mutex is contended exactly as a deployment would contend it. See
	// cached_test.go for what the mode measures and excludes.
	cached bool
	// cacheTTL is the DID cache's entry lifetime when cached is set. Zero means
	// the benchmark default (an hour — far longer than any run, so the
	// measurements see the steady warm state, never TTL-expiry churn). The
	// issuer-stop scenario (survivability_test.go) sets a short TTL to cross
	// the freshness window's edge inside one test.
	cacheTTL time.Duration
	// sharedDID, when set, replaces the fixture-local DID stack entirely
	// (delaying resolver + optional bound + cache built once per ring).
	sharedDID resolver.Resolver
	// sharedDIDDelay is the shared stack's delay/counter; only ring slot 0
	// carries it into roundTrips so a ring total counts each call once.
	sharedDIDDelay *gateDelay
}

// effectiveCacheTTL is the DID cache TTL this option set selects: cacheTTL
// when set, else the benchmark default.
func (o gateOptions) effectiveCacheTTL() time.Duration {
	if o.cacheTTL != 0 {
		return o.cacheTTL
	}
	return time.Hour
}

// gateChain is one signed lineage plus the DID documents that authenticate it.
// It is separate from the fixture so one lineage can be served through
// different resolver wirings — the only way to compare fetch routes, since a
// second lineage would carry different keys and proof timestamps and therefore
// different body addresses.
type gateChain struct {
	byAddress map[string][]byte
	head      *vc.PipelinePassCredential
	payload   []byte
	// didDocuments holds canonical document bytes, not parsed documents, for
	// the same reason byAddress holds wire bytes: production parses per fetch.
	didDocuments map[string][]byte
	depth        int
}

// gateIdentity is one signing identity plus the DID documents that
// authenticate it. Lineages built over the SAME identity resolve the same
// three documents — which is what lets a ring share one node-wide cache, the
// production shape (many occurrences, one organization's documents).
type gateIdentity struct {
	priv         stded25519.PrivateKey
	didDocuments map[string][]byte
}

func newGateIdentity(tb testing.TB) *gateIdentity {
	tb.Helper()
	keyPair, err := (ed25519.Generator{}).Generate()
	if err != nil {
		tb.Fatalf("keygen: %v", err)
	}
	didDocuments := make(map[string][]byte, 3)
	for _, doc := range []struct {
		id, controller string
		pub            []byte
	}{
		{gateProcessDID, gatePipelineDID, keyPair.PublicKey},
		{gatePipelineDID, gateOwnerDID, nil},
		{gateOwnerDID, gateOwnerDID, nil},
	} {
		built, docErr := gateDIDDoc(doc.id, doc.controller, doc.pub)
		if docErr != nil {
			tb.Fatalf("DID document %s: %v", doc.id, docErr)
		}
		raw, marshalErr := built.MarshalJSON()
		if marshalErr != nil {
			tb.Fatalf("marshal DID document %s: %v", doc.id, marshalErr)
		}
		didDocuments[doc.id] = raw
	}
	return &gateIdentity{priv: stded25519.PrivateKey(keyPair.PrivateKey), didDocuments: didDocuments}
}

func buildGateChain(tb testing.TB, depth int, salt string) *gateChain {
	return buildGateChainAs(tb, newGateIdentity(tb), depth, salt)
}

// buildGateChainAs builds a lineage signed by an EXISTING identity, so several
// distinct lineages can share documents (and therefore a cache).
func buildGateChainAs(tb testing.TB, identity *gateIdentity, depth int, salt string) *gateChain {
	tb.Helper()
	if depth < 1 {
		tb.Fatalf("buildGateChain: depth must be >= 1, got %d", depth)
	}
	builder := vc.NewBuilder(gateSigner{priv: identity.priv})

	// The payload is what the head credential must bind: the sink recomputes
	// sha256 over it and compares against the head's outputHash.
	payload := []byte(fmt.Sprintf(`{"lot":"LOT-%s","depth":%d,"co2e_kg":12.5}`, salt, depth))
	payloadHash := rawHash(payload)

	// Stage hashes chain the data-flow invariant: credential i converts
	// stageHash(i) into stageHash(i+1), and the final output is the payload.
	stageHash := func(i int) string {
		if i == depth {
			return payloadHash
		}
		return rawHash([]byte(fmt.Sprintf("gate-stage-%s-%d", salt, i)))
	}

	verificationMethod := gateProcessDID + "#" + gateKeyID
	subjectAt := func(i int) vc.CredentialSubjectFields {
		return vc.CredentialSubjectFields{
			PipelineID:          "depth",
			ProcessID:           "stage",
			TransformationClaim: vc.ClaimConvert,
			InputHash:           stageHash(i),
			OutputHash:          stageHash(i + 1),
		}
	}

	byAddress := make(map[string][]byte, depth)
	var head *vc.PipelinePassCredential
	for i := 0; i < depth; i++ {
		var cred *vc.PipelinePassCredential
		var buildErr error
		if i == 0 {
			cred, buildErr = builder.BuildFirstDrop(gateProcessDID, gateKeyID, verificationMethod, subjectAt(i), nil)
		} else {
			cred, buildErr = builder.BuildChainPreserving(gateProcessDID, gateKeyID, verificationMethod, subjectAt(i), head, nil)
		}
		if buildErr != nil {
			tb.Fatalf("build credential %d: %v", i, buildErr)
		}
		address, hashErr := cred.Hash()
		if hashErr != nil {
			tb.Fatalf("hash credential %d: %v", i, hashErr)
		}
		wire, marshalErr := cred.MarshalJSON()
		if marshalErr != nil {
			tb.Fatalf("marshal credential %d: %v", i, marshalErr)
		}
		byAddress[address] = wire
		head = cred
	}

	return &gateChain{
		byAddress:    byAddress,
		head:         head,
		payload:      payload,
		didDocuments: identity.didDocuments,
		depth:        depth,
	}
}

// fixture wires one gate over this lineage.
func (c *gateChain) fixture(tb testing.TB, opts gateOptions) *gateFixture {
	tb.Helper()

	served := c.byAddress
	if opts.unseeded {
		served = map[string][]byte{}
	}

	count := !opts.uncounted
	var didDelay *gateDelay
	if opts.sharedDID == nil {
		didDelay = &gateDelay{latency: opts.latency, count: count}
	} else {
		didDelay = opts.sharedDIDDelay // may be nil for non-slot-0 ring fixtures
	}
	localDelay := &gateDelay{latency: opts.latency, count: count}
	localStore := &memCredentialResolver{byAddress: served, delay: localDelay, count: count}

	var credentialResolver chainwalk.CredentialResolver = localStore
	var upstreamDelay *gateDelay
	if opts.cold {
		// Nothing replicated locally yet: every hop misses local and is served
		// upstream, paying both delays.
		upstreamLatency := opts.upstreamLatency
		if upstreamLatency == 0 {
			upstreamLatency = opts.latency
		}
		upstreamDelay = &gateDelay{latency: upstreamLatency, count: count}
		localStore = &memCredentialResolver{byAddress: map[string][]byte{}, delay: localDelay, count: count}
		credentialResolver = fallbackCredentialResolver{
			local:    localStore,
			upstream: &memCredentialResolver{byAddress: served, delay: upstreamDelay, count: count},
		}
	}

	var didResolver resolver.Resolver
	if opts.sharedDID != nil {
		didResolver = opts.sharedDID
	} else {
		didResolver = &delayingDIDResolver{documents: c.didDocuments, delay: didDelay}
		if opts.didWrap != nil {
			// The wrap sits where production's admission control sits: inside
			// the resolver, below inputcapture — a refused admission never
			// reaches the delay model, exactly as production rejects before
			// any network work.
			didResolver = opts.didWrap(didResolver)
		}
		if opts.cached {
			// TTL far longer than any benchmark run unless a scenario narrows
			// it: the measured state is the steady warm cache, not TTL-expiry
			// churn (cached_test.go).
			cachedResolver, cacheErr := cache.New(didResolver, cache.Config{TTL: opts.effectiveCacheTTL()})
			if cacheErr != nil {
				tb.Fatalf("cache.New: %v", cacheErr)
			}
			didResolver = cachedResolver
		}
	}
	verifier := vc.NewVerifier(
		inputcapture.DIDResolver{Next: didResolver},
		ed25519.Verifier{},
	)
	walker, err := chainwalk.New(credentialResolver, verifier)
	if err != nil {
		tb.Fatalf("chainwalk.New: %v", err)
	}
	appraiser, err := chainappraisal.New(walker, inputcapture.Recorder{}, chainappraisal.Config{
		ClaimContractID:   gateClaimContract,
		SchemaVersion:     gateSchemaVersion,
		SelectionPolicyID: chainappraisal.ProjectedChainSelection,
		KnownScopes:       chainappraisal.KnownScopes(),
		Profile: appraisal.Profile{
			ID:             gateProfileID,
			RequiredScopes: []string{chainappraisal.LinearAttestationScope},
		},
	})
	if err != nil {
		tb.Fatalf("chainappraisal.New: %v", err)
	}

	// Appraiser is set, so sink takes the appraisal branch and never consults
	// Config.Verifier; leaving it nil keeps the wiring honest about that.
	cfg := sink.Config{
		Strategy:         contract.VerificationAdjacent,
		Kind:             contract.SinkProduction,
		Codec:            envelopecodec.New(),
		Appraiser:        appraiser,
		AgentBoundaryID:  gateBoundaryID,
		Store:            noopIngressStore{},
		Writer:           noopWriter{},
		UpstreamEndpoint: gateUpstream,
		AllowIssuers:     []string{gateProcessDID},
	}
	var capture *capturingWriter
	if opts.capture {
		capture = &capturingWriter{}
		cfg.Writer = capture
	}
	processor, err := sink.New(cfg)
	if err != nil {
		tb.Fatalf("sink.New: %v", err)
	}

	return &gateFixture{
		processor:     processor,
		wire:          encode(tb, c.head, c.payload),
		writer:        capture,
		depth:         c.depth,
		didDelay:      didDelay,
		localDelay:    localDelay,
		upstreamDelay: upstreamDelay,
		localResolver: localStore,
	}
}

func gateDIDDoc(id, controller string, pub []byte) (*did.DIDDocument, error) {
	fields := did.DocumentFields{
		Context:    did.IssuedDocumentContexts(),
		ID:         id,
		Controller: controller,
	}
	if pub != nil {
		vm, err := did.NewMultikeyVerificationMethod(id+"#"+gateKeyID, id, pub)
		if err != nil {
			return nil, err
		}
		fields.VerificationMethod = []did.VerificationMethod{vm}
		fields.AssertionMethod = []string{id + "#" + gateKeyID}
	}
	return did.New(fields), nil
}

// deliverErr runs one occurrence through the gate. It returns an error rather
// than failing, so it is safe to call from a b.RunParallel worker and adds no
// testing.TB bookkeeping (Helper takes a lock on shared benchmark state) to the
// timed loop.
func (f *gateFixture) deliverErr(ctx context.Context) error {
	result, err := f.processor.Process(ctx, f.wire)
	if err != nil {
		return fmt.Errorf("process at depth %d: %w", f.depth, err)
	}
	if result.Status != contract.StatusPassed {
		return fmt.Errorf("process at depth %d: status %v, want passed", f.depth, result.Status)
	}
	return nil
}

// deliver is deliverErr for the serial callers that may legally fail fast.
func (f *gateFixture) deliver(tb testing.TB, ctx context.Context) {
	tb.Helper()
	if err := f.deliverErr(ctx); err != nil {
		tb.Fatal(err)
	}
}

// newGateRing builds distinct lineages so a benchmark replays a stream of
// different occurrences rather than one cache-hot occurrence. Total credentials
// built is bounded so deep rings stay cheap to construct.
func newGateRing(tb testing.TB, depth int, opts gateOptions) []*gateFixture {
	tb.Helper()
	size := 2048 / depth
	if size < 2 {
		size = 2
	}
	if size > 16 {
		size = 16
	}
	ring := make([]*gateFixture, size)
	var identity *gateIdentity
	if opts.cached {
		// One identity, one DID stack, ONE cache for the whole ring — the
		// production shape: a node-wide cache serving a stream of distinct
		// occurrences that authenticate against the same documents. Building
		// the cache per fixture would shard its mutex 16 ways and hide the
		// contention a deployment actually pays.
		identity = newGateIdentity(tb)
		didDelay := &gateDelay{latency: opts.latency, count: !opts.uncounted}
		var stack resolver.Resolver = &delayingDIDResolver{documents: identity.didDocuments, delay: didDelay}
		if opts.didWrap != nil {
			stack = opts.didWrap(stack)
		}
		cachedResolver, cacheErr := cache.New(stack, cache.Config{TTL: opts.effectiveCacheTTL()})
		if cacheErr != nil {
			tb.Fatalf("cache.New: %v", cacheErr)
		}
		opts.sharedDID = cachedResolver
		opts.sharedDIDDelay = didDelay
	}
	for i := range ring {
		local := opts
		local.depth = depth
		local.salt = fmt.Sprintf("%s-%d", opts.salt, i)
		if i > 0 {
			local.sharedDIDDelay = nil // count each shared call once (slot 0)
		}
		chain := (*gateChain)(nil)
		if identity != nil {
			chain = buildGateChainAs(tb, identity, depth, local.salt)
		} else {
			chain = buildGateChain(tb, depth, local.salt)
		}
		ring[i] = chain.fixture(tb, local)
	}
	return ring
}

// runGateSerial replays the ring for b.N iterations and reports modelled round
// trips per delivery, so every table states what network it actually charged.
func runGateSerial(b *testing.B, ring []*gateFixture) {
	ctx := context.Background()
	// The ring is shared across the framework's warm-up invocations and across
	// -count repetitions, so the counters must be read as a delta over THIS
	// timed loop. Totalling them would grow with every prior run.
	baseline := ringRoundTrips(ring)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ring[i%len(ring)].deliverErr(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(ringRoundTrips(ring)-baseline)/float64(b.N), "rt/op")
}

func ringRoundTrips(ring []*gateFixture) int64 {
	var total int64
	for _, f := range ring {
		total += f.roundTrips()
	}
	return total
}

// BenchmarkGateDepth is the depth curve with resolution served from memory: the
// lower bound a deployment approaches as replication approaches perfect. What
// it reports is cryptography, canonicalization, and wire decoding, times depth.
func BenchmarkGateDepth(b *testing.B) {
	for _, depth := range gateDepths {
		ring := newGateRing(b, depth, gateOptions{salt: "depth"})
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			runGateSerial(b, ring)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
		})
	}
}

// BenchmarkGateThroughput answers the flatness question: per-delivery work must
// not grow as concurrent deliveries rise. The harness contributes no shared
// state — no-op writer, no Helper in the loop — so contention observed here is
// the gate's own.
func BenchmarkGateThroughput(b *testing.B) {
	ctx := context.Background()
	for _, depth := range []int{2, 32} {
		// Uncounted: resolver call accounting is a shared atomic per resolve
		// (159 of them at depth 32), which would grow with concurrency and be
		// indistinguishable from the gate's own contention.
		ring := newGateRing(b, depth, gateOptions{salt: "throughput", uncounted: true})
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var worker atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				// One ring slot per worker, chosen once. A shared cursor
				// advanced per iteration would add its own contended
				// read-modify-write to every delivery.
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

// BenchmarkGateResolverLatency charges every modelled round trip — credential
// AND DID — a fixed delay, making the network term visible without a container
// topology.
//
// The credential walk is a strictly sequential pointer chase, so its delays
// cannot be overlapped. Read the result as a lower bound: per-hop delay only,
// no bandwidth, connection setup, or TLS.
func BenchmarkGateResolverLatency(b *testing.B) {
	for _, latency := range []time.Duration{100 * time.Microsecond, 1 * time.Millisecond} {
		for _, depth := range []int{2, 8, 32} {
			ring := newGateRing(b, depth, gateOptions{salt: "rtt", latency: latency})
			b.Run(fmt.Sprintf("rtt=%s/depth=%d", latency, depth), func(b *testing.B) {
				runGateSerial(b, ring)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
			})
		}
	}
}

// BenchmarkGateColdReplication is the gate before asynchronous replication has
// caught up: every hop misses locally and is served upstream.
//
// Local and upstream latencies are set independently because that is the real
// topology — local is this node's registry, upstream is another organization's.
// The symmetric case is reported alongside to show how much of the cold tax is
// an artifact of assuming the upstream is as close as the local store.
func BenchmarkGateColdReplication(b *testing.B) {
	const localRTT = 100 * time.Microsecond
	for _, upstreamRTT := range []time.Duration{100 * time.Microsecond, 1 * time.Millisecond} {
		for _, depth := range []int{2, 8, 32} {
			ring := newGateRing(b, depth, gateOptions{
				salt: "cold", latency: localRTT, upstreamLatency: upstreamRTT, cold: true,
			})
			b.Run(fmt.Sprintf("local=100µs/upstream=%s/depth=%d", upstreamRTT, depth), func(b *testing.B) {
				runGateSerial(b, ring)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cred")
			})
		}
	}
}

// TestGateFixtureDeliversExactSpine is the correctness gate for the benchmarks:
// it proves the fixture exercises the real synchronous path and that the
// EvidenceView fixes one spine entry per credential, origin-to-head.
func TestGateFixtureDeliversExactSpine(t *testing.T) {
	ctx := context.Background()
	for _, depth := range []int{1, 2, 5} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			fixture := buildGateChain(t, depth, "spine").fixture(t, gateOptions{capture: true})
			fixture.deliver(t, ctx)

			record, writes := fixture.writer.snapshot()
			if writes != 1 {
				t.Fatalf("writer calls = %d, want 1", writes)
			}
			if record.EvidenceView == nil {
				t.Fatal("delivered record carries no EvidenceView")
			}
			spine := record.EvidenceView.Manifest.Spine
			if len(spine) != depth {
				t.Fatalf("spine length = %d, want %d (one entry per credential)", len(spine), depth)
			}
			if got := record.EvidenceView.Manifest.Head; got != spine[len(spine)-1].BodyAddress {
				t.Fatalf("manifest head %s is not the final spine entry %s", got, spine[len(spine)-1].BodyAddress)
			}
			if err := record.EvidenceView.ValidateID(); err != nil {
				t.Fatalf("EvidenceViewID does not name its manifest: %v", err)
			}
			if record.EvidenceView.PolicyDecision == nil ||
				record.EvidenceView.PolicyDecision.Decision != appraisal.DecisionAccept {
				t.Fatalf("policy decision = %+v, want ACCEPT", record.EvidenceView.PolicyDecision)
			}
			if len(record.EvidenceView.Manifest.InputSnapshotDigests) == 0 {
				t.Fatal("no resolver snapshots captured; the appraisal did not read the DID documents")
			}
		})
	}
}

// TestGateRoundTripAccounting pins what the latency benchmarks actually charge.
// Without it, a change to the verifier's resolution behaviour would silently
// alter every reported network figure — the exact failure that made an earlier
// draft of this file model only a fifth of the real round trips.
func TestGateRoundTripAccounting(t *testing.T) {
	ctx := context.Background()
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			fixture := buildGateChain(t, depth, "accounting").fixture(t, gateOptions{})
			fixture.deliver(t, ctx)

			gotDID := fixture.didDelay.calls.Load()
			wantDID := int64(gateDIDResolvesPerCredential * depth)
			if gotDID != wantDID {
				t.Errorf("DID resolutions = %d, want %d (%d per credential x depth %d)",
					gotDID, wantDID, gateDIDResolvesPerCredential, depth)
			}
			gotCred := fixture.localDelay.calls.Load()
			if want := int64(depth - 1); gotCred != want {
				t.Errorf("credential resolutions = %d, want %d (head arrives in the envelope)", gotCred, want)
			}
			if want := int64(5*depth - 1); fixture.roundTrips() != want {
				t.Errorf("total modelled round trips = %d, want %d", fixture.roundTrips(), want)
			}
		})
	}
}

// TestGateColdReplicationIsActuallyCold proves the cold fixture takes the
// local-miss/upstream-hit route rather than silently resolving warm, and that
// the EvidenceViewID is invariant to the fetch route — where bytes came from is
// not part of what the view commits.
func TestGateColdReplicationIsActuallyCold(t *testing.T) {
	ctx := context.Background()
	const depth = 4
	// One lineage, two routes. Two separately built chains would carry
	// different keys and proof timestamps, so their IDs would differ for
	// reasons unrelated to the route.
	lineage := buildGateChain(t, depth, "parity")
	warm := lineage.fixture(t, gateOptions{capture: true})
	cold := lineage.fixture(t, gateOptions{cold: true, capture: true})

	warm.deliver(t, ctx)
	cold.deliver(t, ctx)

	if got, want := cold.localResolver.misses.Load(), int64(depth-1); got != want {
		t.Errorf("cold local misses = %d, want %d: the cold path resolved warm", got, want)
	}
	if got, want := cold.upstreamDelay.calls.Load(), int64(depth-1); got != want {
		t.Errorf("cold upstream lookups = %d, want %d", got, want)
	}
	if warm.upstreamDelay != nil {
		t.Error("warm fixture has an upstream resolver")
	}

	warmRecord, _ := warm.writer.snapshot()
	coldRecord, _ := cold.writer.snapshot()
	if warmRecord.EvidenceView == nil || coldRecord.EvidenceView == nil {
		t.Fatal("a delivery carried no EvidenceView")
	}
	if warmRecord.EvidenceView.EvidenceViewID != coldRecord.EvidenceView.EvidenceViewID {
		t.Fatalf("EvidenceViewID differs by fetch route: warm=%s cold=%s",
			warmRecord.EvidenceView.EvidenceViewID, coldRecord.EvidenceView.EvidenceViewID)
	}
}

// TestGateFixtureRejectsUnresolvablePredecessor pins the failure mode the
// benchmarks must never silently enter: a chain hole is a non-delivery, so a
// benchmark whose store lost a hop would measure rejection, not verification.
func TestGateFixtureRejectsUnresolvablePredecessor(t *testing.T) {
	ctx := context.Background()
	fixture := buildGateChain(t, 3, "hole").fixture(t, gateOptions{unseeded: true, capture: true})

	result, err := fixture.processor.Process(ctx, fixture.wire)
	if err != nil {
		t.Fatalf("Process returned a transport error, want a reject result: %v", err)
	}
	if result.Status == contract.StatusPassed {
		t.Fatal("a chain hole produced an accepted delivery")
	}
	if _, writes := fixture.writer.snapshot(); writes != 0 {
		t.Fatalf("writer calls = %d on a chain hole, want 0", writes)
	}
}
