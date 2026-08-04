module github.com/provin-line/bench

go 1.25.5

// The measured revision (pins.env OSS_REF) is not yet on a public branch of
// provin.oss, so the module is resolved from the sibling checkout in this agent
// scope. Replace this directive with a normal require once it is pushed.
replace github.com/provin-line/oss => ../provin.oss

require github.com/provin-line/oss v0.0.0-00010101000000-000000000000

require (
	github.com/cayleygraph/quad v1.3.0 // indirect
	github.com/o3co/go.hocon v1.8.0 // indirect
	github.com/piprate/json-gold v0.8.0 // indirect
	github.com/pquerna/cachecontrol v0.2.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
