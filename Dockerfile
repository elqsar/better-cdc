FROM golang:1.26.8-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /cdc-handler ./cmd/cdc-handler
RUN mkdir -p /data/spill && chmod 0700 /data/spill && chown -R 65532:65532 /data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /cdc-handler /cdc-handler
COPY --from=build --chown=65532:65532 /data /var/lib/better-cdc
USER 65532:65532
ENV SPILL_DIR=/var/lib/better-cdc/spill
EXPOSE 8080
ENTRYPOINT ["/cdc-handler"]
