# Results

The figures the current pin produces, recorded in the commit that moves the
pin (AGENTS.md rule 3). Each section names the benchmark, the sampling and what
the number excludes; the benchmark's own doc comment says what it models.

## provin.oss v0.4.0 (`a832c8e9`), measured 2026-11-09

- Environment: darwin/arm64, Apple M4 Pro (14 logical CPUs), Go 1.27.1 host
  toolchain, `GOMAXPROCS` 14.
- Sampling: `make bench` (`-benchtime=1s -count=5`), medians of the five
  samples. Round trips (`rt/op`) and allocations are deterministic.
- What is in the measured path that the previous line did not have: the
  delivery decision record at the node's default level (every decision,
  formatted as the text log would be, its output discarded), the envelope
  codec's bounds, and provin.oss 0.4.0's stricter reading of credentials and
  keys (duplicate keys folded by case, the safe-number range, small-order
  Ed25519 keys refused on every key read). Everything the harness excluded
  before is still excluded (transport, TLS, the ingress store's RPC, receipt
  issuance, the log's I/O).
- Previous line: Paper 04's figures, measured at `4174829b7549` (the v0.3.0
  line) on the same host, 2026-08-04. Where this table changes a published
  number, the Paper 04 value is in the last column.

### 1. Depth — linear, ~133 µs per credential

`BenchmarkGateDepth`, in-memory resolution (zero delay).

| depth | ns/op | ns/credential | rt/op | allocs/op | 4174829b ns/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 338,693 | 169,347 | 9 | 3,566 | 314,000 |
| 8 | 1,148,229 | 143,529 | 39 | 13,102 | 1,060,000 |
| 32 | 4,467,736 | 139,617 | 159 | 51,302 | 3,930,000 |
| 128 | 16,912,741 | 132,131 | 639 | 203,612 | 15,800,000 |
| 512 | 68,334,250 | 133,465 | 2,559 | 818,817 | 63,200,000 |

Still linear; the per-credential cost moved from ~123 µs to ~133 µs (+8 %) and
allocations per credential from ~1,490 to ~1,600 (+7 %). The shape is
unchanged: Ω(depth) work per delivery is structural (each occurrence carries
its own lineage), and the round-trip count is exactly 5·depth−1.

Cached (`BenchmarkGateDepthCached`), in memory, the cache is still pure
overhead: 4,543,460 ns/op at depth 32 against 4,467,736 uncached (+1.7 %),
with `rt/op` = depth−1.

### 2. Throughput — per-delivery work is invariant to concurrency

`BenchmarkGateThroughput`, `b.RunParallel` at `GOMAXPROCS` 14 only (the
`GOMAXPROCS` sweep of the previous line was a separate invocation).

| depth | ns/op | allocs/op | deliveries/s | 4174829b ns/op (14 threads) |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 104,628 | 3,616 | 9,560 | 104,000 |
| 32 | 1,084,682 | 51,235 | 922 | 976,000 |

Cached (`BenchmarkGateThroughputCached`): 106,948 ns/op at depth 2 and
1,026,006 at depth 32, within ±6 % of the uncached path.

### 3. Resolver latency — the dominant term

`BenchmarkGateResolverLatency`, every modelled round trip charged the stated
delay.

| per-call delay | depth | rt/op | ns/op | vs in-memory | 4174829b ns/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 µs | 2 | 9 | 1,419,357 | 4.2× | 1,390,000 |
| 100 µs | 8 | 39 | 5,878,673 | 5.1× | 5,860,000 |
| 100 µs | 32 | 159 | 23,482,991 | 5.3× | 23,500,000 |
| 1 ms | 2 | 9 | 10,673,023 | 31.5× | 10,600,000 |
| 1 ms | 8 | 39 | 46,005,183 | 40.1× | 45,900,000 |
| 1 ms | 32 | 159 | 185,716,493 | 41.6× | 186,000,000 |

Unchanged within 2 %: charged resolution delay dominates, and the computation
that grew in §1 is a few percent of a delivery here.

Cached (`BenchmarkGateResolverLatencyCached`): at 1 ms per call, depth 32
costs 39,880,085 ns/op (`rt/op` 31), against 185,716,493 uncached — the DID
family leaves the synchronous path, the credential chase stays.

### 4. Replication state — cold costs 1.19× symmetric, 2.56× asymmetric

`BenchmarkGateColdReplication`, every hop through the real cold path (local
miss, then upstream), against the warm 100 µs baseline of §3.

| depth | warm (100 µs) | cold, upstream 100 µs | cold, upstream 1 ms | rt/op (cold) |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 1,419,357 | 1,519,864 (1.07×) | 2,693,213 (1.90×) | 10 |
| 8 | 5,878,673 | 6,708,786 (1.14×) | 14,069,360 (2.39×) | 46 |
| 32 | 23,482,991 | 27,988,971 (1.19×) | 60,073,121 (2.56×) | 190 |

The previous line measured 1.18× and 2.53× at depth 32. Cached
(`BenchmarkGateColdReplicationCached`), depth 32: 12,142,306 ns/op symmetric
and 44,550,609 asymmetric against the 8,177,584 warm cached baseline (1.5× and
5.4×; `rt/op` 62).

### 5. The capacity ceiling of the uncached resolver bound

`BenchmarkGateBoundedResolver`, depth 32, 1 ms per call, the production
64-slot fail-fast bound, closed-loop workers. Sampling here is 5 × 1 s, where
the previous line used 3 × 5 s, so the overload rows carry more noise.

| workers | accepted/s | busy admissions/attempt | quarantined/attempt | refused/attempt | 4174829b accepted/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 154 | 0 | 0 | 0 | 161 |
| 64 | 315 | 0 | 0 | 0 | 329 |
| 80 | 156 | 12.6 | 0.47 | 0.20 | 248 |
| 96 | 13.5 | 33.7 | 0.46 | 0.52 | 9.3 |
| 128 | 10.7 | 47.9 | 0.26 | 0.73 | 3.2 |

The stable ceiling is ~315/s at 64 workers (the previous line: ~330/s), and
goodput collapses past it as before: at 80 workers accepted throughput halves,
at 96 it is below 5 % of the ceiling. The mechanism is unchanged
(`VerifyChain` does not short-circuit on an indeterminate verdict, so a
delivery refused one admission still holds slots for its remaining
resolutions).

Cached (`BenchmarkGateBoundedResolverCached`): no busy admission, quarantine or
refusal at any worker count up to 128; accepted throughput rises to ~1,070/s
at 128 workers (the previous line: ~1,250–1,350/s at 96–128 workers, 3 × 5 s).

### What to make of the differences

- The uncached computation per credential grew about 8 % (§1, §2 at depth
  32). The decision record and the stricter reading are in the path and were
  not before; which of them accounts for how much is not measured here.
- Everything resolution-bound (§3, §4) is unchanged within 2 %, and the
  resolver bound's stable ceiling moved by about 4 % (§5): the gate's cost is
  still its resolution, not its arithmetic.
- The cached capacity plateau reads lower than the previous line's (~1,070/s
  against ~1,250–1,350/s). The sampling differs (1 s against 5 s per sample)
  and the computation per delivery grew; the two are not separated here.
