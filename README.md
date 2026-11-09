# provin.bench

Performance measurement for [provin](https://github.com/provin-line/oss).

`provin.oss` keeps benchmarks out of its published module. This repository holds
them, alongside the delay and round-trip models a meaningful number needs.

- `gate/` — the synchronous delivery gate: cost against chain depth, concurrency,
  resolver latency, and replication state.

Every figure is attributable to the `provin.oss` revision in `pins.env`; the
figures of the current pin are in [RESULTS.md](RESULTS.md). The figures Paper 04 published were measured at
`4174829b7549`, the v0.3.0 line, and stay attributable to that revision in this
repository's history. Measurements substitute transport for in-process stores,
so they are a **lower bound** on a networked deployment; each benchmark
documents what it does not model.

```
make test    # the harness's own correctness gates
make bench   # every benchmark, 5 samples
```

## Licence

Apache License 2.0 — see [LICENSE](LICENSE).
