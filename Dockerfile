# Distroless multi-arch image. Built by goreleaser — the kattic binary is
# placed at the repo root by goreleaser before docker build runs.
FROM gcr.io/distroless/static:nonroot

LABEL org.opencontainers.image.title="kafka-attic"
LABEL org.opencontainers.image.description="Read-only scanner that quantifies unused Kafka topics with the ATTIC Score."
LABEL org.opencontainers.image.source="https://github.com/sderosiaux/kafka-attic"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY kattic /usr/local/bin/kattic

USER nonroot:nonroot
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/kattic"]
CMD ["--help"]
