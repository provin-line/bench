// Package gate measures provin's synchronous delivery gate: the work a
// production sink performs, before it hands payload bytes to an Agent-facing
// writer, to assemble and appraise the exact credential lineage that produced
// those bytes.
//
// The package holds no production code. It exists so the benchmarks in
// scaling_test.go have a home, and so the harness they need — signed-lineage
// construction, in-memory resolvers with modelled round-trip delay, and the
// warm/cold replication routes — is written once against provin.oss's published
// packages only.
//
// What the numbers mean, and what they exclude, is documented at the top of
// scaling_test.go next to the benchmarks that produce them.
package gate
