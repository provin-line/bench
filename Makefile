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
verify-pin:
	@. ./pins.env; \
	pseudo=$$($(GO) list -m -f '{{.Version}}' github.com/provin-line/oss); \
	short=$${pseudo##*-}; \
	case "$$OSS_REF" in \
		"$$short"*) echo "pin ok: $$OSS_REF (module $$pseudo)";; \
		*) echo "PIN DRIFT: pins.env=$$OSS_REF module=$$pseudo"; exit 1;; \
	esac
