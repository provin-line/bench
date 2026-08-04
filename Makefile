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

## verify-pin reports whether the measured checkout matches pins.env.
verify-pin:
	@. ./pins.env; \
	actual=$$(git -C ../provin.oss rev-parse HEAD 2>/dev/null); \
	if [ "$$actual" = "$$OSS_REF" ]; then \
		echo "pin ok: $$OSS_REF"; \
	else \
		echo "PIN DRIFT: pins.env=$$OSS_REF checkout=$$actual"; exit 1; \
	fi
