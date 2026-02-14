# kubernetes-logs-datalake

![Example usage](howto/example-usage-00001.png)

Fluent Bit DaemonSet that collects Kubernetes container logs and writes them as Parquet files to S3-compatible storage, with a UI gateway for on-demand DuckDB query views.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [k3d](https://k3d.io/) v5+
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Node.js](https://nodejs.org/) v20+
- [Go](https://go.dev/) 1.25+
- [ko](https://ko.build/) (`go install github.com/google/ko@latest`)
- [turbo](https://turbo.build/) v2+

## Quick start

```bash
bash k3d-example/setup.sh
npm install --strict-peer-deps --ignore-scripts
turbo acceptancetest --filter=e2e
```

Open a DuckDB view:

```bash
curl -s -H "Content-Type: application/json" http://localhost:30080/_api/views
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"name":"demo","subset":{"cluster":"dev"}}' \
  http://localhost:30080/_api/views

kubectl --kubeconfig=k3d-example/kubeconfig wait \
  --for=condition=ready pod -l lakeview.yolean.se/view-name=demo -n ui --timeout=120s
```

Add a hosts file entry for the view and open it in a browser:

```bash
echo "127.0.0.1 demo.logs-datalake.local" | sudo tee -a /etc/hosts
open http://demo.logs-datalake.local:30080
```

Teardown:

```bash
k3d cluster delete fluentbit-demo
```

## What the e2e tests verify

`turbo acceptancetest --filter=e2e` builds container images, imports them into k3d, deploys the gateway, and runs these tests:

1. Envoy serves "gateway ok" via xDS direct response
2. POST `/_api/views` creates a DuckDB Job, headless Service, and xDS route
3. GET `/_api/views` lists active views
4. DuckDB pod starts and becomes ready
5. Host-based routing (`viewname.gateway`) through envoy reaches DuckDB UI
6. DELETE `/_api/views/:name` removes the view and cleans up Kubernetes resources

## Project structure

```
ui/gateway-sidecar/      Go xDS sidecar (envoy control plane + view API)
ui/gateway/              Kustomize manifests (envoy + sidecar + RBAC)
images/duckdb/           DuckDB UI container image (socat proxy for IPv6 binding)
images/gateway-sidecar/  (deprecated) Docker-based sidecar image build
test/e2e/                End-to-end tests (vitest)
k3d-example/             k3d cluster setup and base infrastructure
```

## Build tasks

Turborepo pipeline:

```bash
turbo compile            # Go binary
turbo images             # Container images (ko for sidecar, docker for duckdb)
turbo acceptancetest     # e2e tests (depends on images)
```

Go unit tests:

```bash
cd ui/gateway-sidecar && go test ./...
```

## Fluent Bit

### Build

The `build/` directory contains a Dockerfile that compiles Fluent Bit v4.2.2 with Apache Arrow/Parquet support (`FLB_ARROW=On`) on a distroless debian13 nonroot runtime.

```bash
docker buildx build --load -t yolean/fluentbit:latest build/
```

### Example

The `k3d-example/` directory provides a complete k3d-based demo: container log forwarding via a DaemonSet, writing parquet files to [versitygw](https://github.com/versity/versitygw) (S3-compatible gateway).

```bash
bash k3d-example/setup.sh
```

This will:
1. Create a k3d cluster `fluentbit-demo`
2. Import the locally built `yolean/fluentbit:latest` image
3. Deploy versitygw (S3-compatible storage with posix backend)
4. Create the `fluentbit-logs` S3 bucket
5. Deploy the Fluent Bit DaemonSet (tail → kubernetes filter → S3 parquet output)
6. Deploy a busybox log-generator that emits JSON every second

### Data flow

```
busybox (log-generator) → stdout JSON
  → /var/log/containers/*.log (k3d node)
  → fluent-bit tail input → kubernetes filter → S3 output (parquet)
  → versitygw:7070 → /data/fluentbit-logs/
```
