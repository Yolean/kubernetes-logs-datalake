./ui/gateway should be a kustomize base that uses ghcr.io/yolean/envoy:distroless-v1.37.0 with an envoy.yaml to set up a gateway reached through k3d's loadbalancer.

## concepts

Gateway routes to "views".
Views specify [subsets](../types/src/Subset.type.js).
Views have a name, see [route](../types/src/Route.type.js).



## spec

- ../gateway-sidecar is a go-control-plane that uses xDS to dynamically configure envoy
- sidecar is also an [ext_proc](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/http/ext_proc/v3/ext_proc.proto)
- knows how to genereate credentials for data lake S3 access
- gateway starts configured with a base hostname, here `localhost`
  - Only `localhost` works without TLS termination.

### State

Gateway should support restarts and horizontal scaling without losing state.
Kubernetes is already a distributed system so we don't need to build one on top of it.

The only stateful part of a "view" is the headless service.
Services are listed/watched using the predefined label selector,
and must hence be created with matching label.
Orphaned services should be practically impossible.
Annotations on each service are used for additional per view state.

We avoid dual write by using the service as the state.

The actual workloads depend on "autoscaling", with scale-from-zero,
and a TTL used for automatic shutdown.
To do this the sidecar must be on the request path too (in addition to XDS), as ext_authz.
All sidecars watch the endpoints of all services, to learn which pods are up.
Headless services should tolerate unready endpoints.
XDS makes sure that envoy only routes to ready workloads.
If a request comes in for a view name that has a service but no cluster endpoints,
a new workload is created.
We use the Job type because it has out of the box support for TTL (activeDeadlineSeconds).

This mechanism is unit tested in golang using testcontainers using the `registry.k8s.io/kwok/cluster:v0.7.0-k8s.v1.33.0` image.

## code

- all go is written for 1.25+ with no legacy tooling
- "lint" is implemented using what the go SDK offers, nothing fancy
- golang tests use gomega for expectations (register helper so that lines can begin Expect instead of g.Expect)
- golang projects have a "compile" task that produces a binary to target/bin/[arch]/
- golang projects use https://github.com/omissis/go-jsonschema to generate structs from project's shared types
- type gen is run as a "schemas" task, depended on by "compile"
- scripts use a sub- go project ./scripts
- golang logging using zap with register globals

## e2e testing

Testing must be done with the [k3d example](../../k3d-example/) so we have data.
Set up a nodejs package that uses vitest in ../../test/e2e.
Test runs should be able to reuse an existing cluster, to allow fast dev loops.

## production readiness

Areas that need design attention before serving real users.

### Image lifecycle

The DuckDB view image (`yolean/duckdb-ui:latest`) is built locally and imported into k3d.
The Job spec hardcodes the image reference. In production there is no `k3d image import`;
images must come from a registry. The `:latest` tag gives no reproducibility —
a pinned digest or version tag is needed, along with a way to roll out image updates
to already-running views.

### TLS and hostnames

`GATEWAY_HOSTNAME` is `localhost`, which only works without TLS.
Real users need real hostnames and TLS termination.
The `parseViewName` wildcard pattern (`viewname.*`) was added for dev convenience
but is too permissive for production — it would match any subdomain of any domain.
Hostname policy, certificate provisioning (e.g. wildcard cert or per-view cert),
and ingress integration all need design.

### Cold-start latency and queuing

ext_authz blocks the requesting connection for up to 60 seconds during cold start.
Every concurrent request for the same cold view creates its own polling loop —
there is no request coalescing. A burst of requests to a cold view will each
independently call `CreateViewJob` (saved by AlreadyExists idempotency)
and poll `ReadyEndpoints`, adding load on the API server.
Consider a singleflight or waitgroup per view name.

### Scale-to-zero / TTL

Jobs have `activeDeadlineSeconds: 3600` but there is no mechanism to
actually scale a view back to zero based on inactivity. The Job will run
for one hour then be killed by the Job controller, regardless of whether
it is actively serving requests. A TTL-based scale-down needs idle detection,
likely via envoy access log or request counting in ext_authz.

### Resource limits and quotas

View Job pods have no resource requests or limits.
In a shared cluster, a single view could consume unbounded CPU and memory.
There is no limit on how many views a user can create.
Both per-pod resource limits and a namespace-level view quota need design.

### Health and observability

The gateway envoy has no access logging configured.
The sidecar logs via zap but there are no metrics (Prometheus, etc.).
There is no health endpoint on the sidecar itself — the envoy readiness probe
checks port 8080 (the envoy listener), which only confirms xDS is loaded,
not that the sidecar's informers are synced or the K8s API is reachable.
ext_authz cold-start durations, view counts, and endpoint churn are all
candidates for metrics.

### Horizontal scaling and leader election

The spec says the gateway should support horizontal scaling.
Multiple sidecar replicas would each run informers and push xDS snapshots
to their co-located envoy. This works for reads (each replica watches independently),
but writes (creating Services and Jobs) could race.
`CreateViewService` returns a conflict if the ViewStore already has the name,
but two sidecars that haven't synced yet could both attempt creation.
The K8s API's AlreadyExists handling covers Jobs but not Services today.

### Graceful shutdown and draining

When the gateway pod is terminated, `grpcServer.GracefulStop()` is called
but there is no envoy drain. In-flight ext_authz cold starts will be interrupted
mid-poll. Envoy itself doesn't drain — it just stops receiving xDS.
For zero-downtime deploys, envoy should be signaled to drain connections
before the pod is killed, and the ext_authz server should stop accepting
new cold starts while finishing in-progress ones.

### Error propagation in view creation

`POST /_api/views` creates a Service and returns 201, but the informer
processes the new Service asynchronously. If the informer or xDS push fails,
the client has no signal. The view appears "created" but may never become routable.
There is no reconciliation loop to detect or repair this state.

### Security

The `/_api/` routes are unauthenticated. Anyone who can reach the gateway
can create, list, and delete views. View names are user-supplied and directly
become Kubernetes resource names — the 3-8 character validation is minimal.
There is no RBAC, no multi-tenancy, and no audit trail beyond sidecar logs.
