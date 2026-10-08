# Build context is the repo root: docker build -f test/e2e/stubmodel/Dockerfile .
FROM golang:1.26.8 AS builder
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
COPY test/e2e/stubmodel/ test/e2e/stubmodel/
RUN CGO_ENABLED=0 go build -o stubmodel ./test/e2e/stubmodel

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /workspace/stubmodel /stubmodel
USER 65532:65532
ENTRYPOINT ["/stubmodel"]
