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

## verify-pin reports whether the required module revision matches pins.env:
## "pin ok", "PIN DRIFT", or "cannot verify" when the module version or its
## commit cannot be determined; only the first succeeds, so an offline or cold
## run never passes for a pin it has not checked. A tagged version names its
## commit through the module's origin record (go mod download -json); a
## pseudo-version carries the commit in its suffix.
verify-pin:
	@. ./pins.env; \
	[ -n "$$OSS_REF" ] || { echo "cannot verify: pins.env sets no OSS_REF"; exit 2; }; \
	version=$$($(GO) list -m -f '{{.Version}}' github.com/provin-line/oss) && [ -n "$$version" ] \
		|| { echo "cannot verify: the required provin.oss version is not resolvable"; exit 2; }; \
	hash=$$($(GO) mod download -json github.com/provin-line/oss@$$version | sed -n 's/^[[:space:]]*"Hash": "\([0-9a-f]*\)".*/\1/p'); \
	if [ -z "$$hash" ]; then \
		if echo "$$version" | grep -Eq -- '[-.][0-9]{14}-[0-9a-f]{12}$$'; then hash=$${version##*-}; \
		else echo "cannot verify: no origin record names the commit of $$version (try GOPROXY=direct)"; exit 2; fi; \
	fi; \
	case "$$OSS_REF" in \
		"$$hash"*) echo "pin ok: $$OSS_REF (module $$version)";; \
		*) echo "PIN DRIFT: pins.env=$$OSS_REF module=$$version at $$hash"; exit 1;; \
	esac
