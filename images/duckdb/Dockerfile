FROM ubuntu:24.04 AS tools
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

FROM ghcr.io/yolean/duckdb:443c042a1bf3c447da187580ca085facbe0f1c60

COPY --from=tools /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
