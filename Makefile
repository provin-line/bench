GO ?= go
COUNT ?= 5
BENCHTIME ?= 1s

.PHONY: test bench bench-gate lint verify-pin

## test runs the harness's own correctness gates (not provin.oss's tests).
test:
	$(GO) test ./...

## bench runs every benchmark. Override COUNT/BENCHTIME for a quicker sample.
bench:
	$(GO) test ./... -run '^$$' -bench . -benchmem -benchtime=$(BENCHTIME) -count=$(COUNT)

bench-gate:
	$(GO) test ./gate/ -run '^$$' -bench . -benchmem -benchtime=$(BENCHTIME) -count=$(COUNT)

lint:
	$(GO) vet ./...
	gofmt -l . | tee /dev/stderr | (! read)

## verify-pin reports whether the required module revision matches pins.env.
## A tagged version names its commit through the module's origin record
## (go mod download -json); a pseudo-version carries the commit in its suffix.
verify-pin:
	@. ./pins.env; \
	version=$$($(GO) list -m -f '{{.Version}}' github.com/provin-line/oss); \
	hash=$$($(GO) mod download -json github.com/provin-line/oss@$$version | sed -n 's/^[[:space:]]*"Hash": "\([0-9a-f]*\)".*/\1/p'); \
	[ -n "$$hash" ] || hash=$${version##*-}; \
	case "$$OSS_REF" in \
		"$$hash"*) echo "pin ok: $$OSS_REF (module $$version)";; \
		*) echo "PIN DRIFT: pins.env=$$OSS_REF module=$$version at $$hash"; exit 1;; \
	esac
