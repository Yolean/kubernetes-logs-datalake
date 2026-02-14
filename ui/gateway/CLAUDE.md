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
