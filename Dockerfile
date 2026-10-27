# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
# No third-party modules, so there is no separate `go mod download` layer.
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/hookrelay ./cmd/hookrelay \
 && mkdir -p /out/data

# distroless/static has no shell or package manager; the :nonroot tag runs as UID 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hookrelay /hookrelay
COPY config.example.json /etc/hookrelay/config.json
# Pre-create the data dir owned by the non-root user so the file store can write to the volume.
COPY --from=build --chown=65532:65532 /out/data /data
ENV HOOKRELAY_CONFIG=/etc/hookrelay/config.json \
    HOOKRELAY_STORE_PATH=/data/events.jsonl
USER nonroot:nonroot
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/hookrelay"]
