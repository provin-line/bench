# provin.bench — Development Guidelines

Performance measurement for provin OSS. This repo exists because
`provin.oss` declares benchmarks out of scope (its `AGENTS.md`, "Out of scope for
this repository"), keeping the published module lean — the same split already
applied to extension adapters.

It is the counterpart to `provin.e2e`: that repo measures a running deployment
black-box through its real surfaces; this one measures in-process component
composition, which `provin.e2e` explicitly forbids ("No in-process construction
of node components").

## Rules

1. **Benchmarks only.** A correctness assertion belongs in the repository that
   owns the behaviour. If a measurement depends on a behavioural invariant —
   "the verifier resolves four DID documents per credential", say — that
   invariant is pinned by a test in `provin.oss`, not here. A guard living here
   would only fail when someone bumps the pin, which is long after the change
   that broke it.

   What stays here: the timing harness, the delay/round-trip models, and the
   `Benchmark*` functions. What goes upstream: anything whose failure means the
   product is wrong rather than the measurement being stale.

2. **Public API only.** Construct everything through `provin.oss`'s exported
   packages. A benchmark that needs an unexported identifier is measuring an
   internal, and an internal's cost is not a contract — either the surface
   should be exported deliberately, or the benchmark belongs in `provin.oss` as
   a package-internal microbenchmark.

   A useful side effect: a gate assembled here from published packages alone is
   evidence that the public surface is sufficient for an external consumer to
   compose one.

3. **Every published figure carries a pin.** `pins.env` records the `provin.oss`
   revision a measurement is attributable to. Bumping it is deliberate work:
   re-run the affected benchmarks and record the new numbers in the same commit,
   so a moved number always arrives with the revision that moved it.

4. **State what is not modelled.** A substituted transport, an omitted RPC, an
   unbounded resource the product bounds — each is a reason a figure is a lower
   bound. Say so in the benchmark's own doc comment, next to the number it
   produces, not only in a results document that may be read separately.

5. **A benchmark that can measure a rejected path is broken.** Assert success
   inside the timed operation. A rejection is usually much cheaper than the work
   under measurement, so a silently failing benchmark reports an impressively
   small number.

## Layout

    gate/    the synchronous delivery gate: depth, throughput, resolver
             latency, replication state

## Build & test

```
make test    # the harness's own correctness gates
make bench   # every benchmark, 5 samples
```

`go.mod` currently carries a `replace` to the sibling `provin.oss` checkout in
this agent scope, because the measured revision is not yet on a public branch.
See `pins.env`.

## Out of scope for this repository

- Correctness tests for `provin.oss` behaviour (see rule 1)
- Black-box deployment measurement — that is `provin.e2e`
- Package-internal microbenchmarks that need unexported identifiers (see rule 2)
