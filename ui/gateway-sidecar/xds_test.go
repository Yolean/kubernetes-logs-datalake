package main

import (
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func newTestStore() *ViewStore {
	return &ViewStore{
		views: make(map[string]*ViewState),
		onChange: func() {},
	}
}

func TestSnapshotManagerInitialize(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	sm := NewSnapshotManager("gateway.test", store)
	err := sm.Initialize()
	Expect(err).NotTo(HaveOccurred())

	snap, err := sm.Cache().GetSnapshot(nodeID)
	Expect(err).NotTo(HaveOccurred())

	listeners := snap.GetResources(resource.ListenerType)
	Expect(listeners).To(HaveLen(1))

	routes := snap.GetResources(resource.RouteType)
	Expect(routes).To(HaveLen(1))

	clusters := snap.GetResources(resource.ClusterType)
	Expect(clusters).To(HaveLen(1))
	Expect(clusters).To(HaveKey("sidecar_api"))

	endpoints := snap.GetResources(resource.EndpointType)
	Expect(endpoints).To(BeEmpty())
}

func TestSnapshotManagerWithView(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	store.views["test01"] = &ViewState{
		Name:   "test01",
		Subset: Subset{Cluster: "dev"},
		Endpoints: []ViewEndpoint{
			{Address: "10.0.0.1", Port: 8080, Ready: true},
		},
	}

	sm := NewSnapshotManager("gateway.test", store)
	Expect(sm.Initialize()).To(Succeed())

	snap, err := sm.Cache().GetSnapshot(nodeID)
	Expect(err).NotTo(HaveOccurred())

	// Should have sidecar_api + view-test01 clusters
	clusters := snap.GetResources(resource.ClusterType)
	Expect(clusters).To(HaveLen(2))
	Expect(clusters).To(HaveKey("sidecar_api"))
	Expect(clusters).To(HaveKey("view-test01"))

	// Check view cluster is EDS
	viewCluster := clusters["view-test01"].(*cluster.Cluster)
	Expect(viewCluster.GetType()).To(Equal(cluster.Cluster_EDS))
	Expect(viewCluster.GetEdsClusterConfig()).NotTo(BeNil())

	// Check EDS endpoints
	endpoints := snap.GetResources(resource.EndpointType)
	Expect(endpoints).To(HaveLen(1))
	Expect(endpoints).To(HaveKey("view-test01"))
	cla := endpoints["view-test01"].(*endpoint.ClusterLoadAssignment)
	Expect(cla.Endpoints).To(HaveLen(1))
	Expect(cla.Endpoints[0].LbEndpoints).To(HaveLen(1))
	ep := cla.Endpoints[0].LbEndpoints[0].GetEndpoint()
	Expect(ep.GetAddress().GetSocketAddress().GetAddress()).To(Equal("10.0.0.1"))
	Expect(ep.GetAddress().GetSocketAddress().GetPortValue()).To(Equal(uint32(8080)))

	// Check route config has view virtual host
	routes := snap.GetResources(resource.RouteType)
	rc := routes["local_route"].(*route.RouteConfiguration)
	Expect(rc.VirtualHosts).To(HaveLen(2)) // default + view

	var viewVH *route.VirtualHost
	for _, vh := range rc.VirtualHosts {
		if vh.Name == "view-test01" {
			viewVH = vh
			break
		}
	}
	Expect(viewVH).NotTo(BeNil())
	Expect(viewVH.Domains).To(ContainElements("test01.gateway.test", "test01.*"))

	viewRoute := viewVH.Routes[0].GetRoute()
	Expect(viewRoute).NotTo(BeNil())
	Expect(viewRoute.GetHostRewriteLiteral()).To(Equal("localhost"))
	Expect(viewRoute.GetCluster()).To(Equal("view-test01"))
}

func TestSnapshotManagerRebuild(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	sm := NewSnapshotManager("gateway.test", store)
	Expect(sm.Initialize()).To(Succeed())

	// Add a view and rebuild
	store.views["test01"] = &ViewState{Name: "test01", Subset: Subset{Cluster: "dev"}}
	Expect(sm.Rebuild()).To(Succeed())

	snap, _ := sm.Cache().GetSnapshot(nodeID)
	clusters := snap.GetResources(resource.ClusterType)
	Expect(clusters).To(HaveLen(2))

	// Remove the view and rebuild
	delete(store.views, "test01")
	Expect(sm.Rebuild()).To(Succeed())

	snap, _ = sm.Cache().GetSnapshot(nodeID)
	clusters = snap.GetResources(resource.ClusterType)
	Expect(clusters).To(HaveLen(1))
	Expect(clusters).To(HaveKey("sidecar_api"))

	routes := snap.GetResources(resource.RouteType)
	rc := routes["local_route"].(*route.RouteConfiguration)
	Expect(rc.VirtualHosts).To(HaveLen(1))
	Expect(rc.VirtualHosts[0].Name).To(Equal("local"))
}

func TestDefaultVirtualHostRoutes(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	store.views["abc"] = &ViewState{Name: "abc", Subset: Subset{Cluster: "dev"}}

	sm := NewSnapshotManager("gateway.test", store)
	Expect(sm.Initialize()).To(Succeed())

	snap, _ := sm.Cache().GetSnapshot(nodeID)
	routes := snap.GetResources(resource.RouteType)
	rc := routes["local_route"].(*route.RouteConfiguration)

	var defaultVH *route.VirtualHost
	for _, vh := range rc.VirtualHosts {
		if vh.Name == "local" {
			defaultVH = vh
			break
		}
	}
	Expect(defaultVH).NotTo(BeNil())
	Expect(defaultVH.Domains).To(ContainElement("*"))
	Expect(defaultVH.Routes).To(HaveLen(2))

	// First route: /_api/ -> sidecar_api, ext_authz disabled
	apiRoute := defaultVH.Routes[0]
	Expect(apiRoute.GetMatch().GetPrefix()).To(Equal("/_api/"))
	Expect(apiRoute.GetRoute().GetCluster()).To(Equal("sidecar_api"))
	Expect(apiRoute.TypedPerFilterConfig).To(HaveKey("envoy.filters.http.ext_authz"))

	// Second route: / -> direct response, ext_authz disabled
	directRoute := defaultVH.Routes[1]
	Expect(directRoute.GetMatch().GetPrefix()).To(Equal("/"))
	Expect(directRoute.GetDirectResponse().GetStatus()).To(Equal(uint32(200)))
	Expect(directRoute.TypedPerFilterConfig).To(HaveKey("envoy.filters.http.ext_authz"))
}

func TestExtAuthzFilter(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	sm := NewSnapshotManager("gateway.test", store)
	Expect(sm.Initialize()).To(Succeed())

	snap, _ := sm.Cache().GetSnapshot(nodeID)
	listeners := snap.GetResources(resource.ListenerType)
	l := listeners["main"].(*listener.Listener)

	// Extract HCM from filter chain
	filter := l.FilterChains[0].Filters[0]
	hcmConfig := &hcm.HttpConnectionManager{}
	Expect(proto.Unmarshal(filter.GetTypedConfig().GetValue(), hcmConfig)).To(Succeed())

	// Should have ext_authz + router filters
	Expect(hcmConfig.HttpFilters).To(HaveLen(2))
	Expect(hcmConfig.HttpFilters[0].Name).To(Equal("envoy.filters.http.ext_authz"))
	Expect(hcmConfig.HttpFilters[1].Name).To(Equal("envoy.filters.http.router"))
}

func TestValidateRoute(t *testing.T) {
	RegisterTestingT(t)

	// Valid
	Expect(ValidateRoute(Route{Name: "abc", Subset: Subset{Cluster: "dev"}})).To(Succeed())
	Expect(ValidateRoute(Route{Name: "test01", Subset: Subset{Cluster: "prod"}})).To(Succeed())
	Expect(ValidateRoute(Route{Name: "my-view", Subset: Subset{Cluster: "dev"}})).To(Succeed())

	// Too short
	Expect(ValidateRoute(Route{Name: "ab", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())

	// Too long
	Expect(ValidateRoute(Route{Name: "abcdefghi", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())

	// Invalid chars
	Expect(ValidateRoute(Route{Name: "ABC", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())
	Expect(ValidateRoute(Route{Name: "ab_c", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())

	// Starts with number
	Expect(ValidateRoute(Route{Name: "1abc", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())

	// Ends with dash
	Expect(ValidateRoute(Route{Name: "abc-", Subset: Subset{Cluster: "dev"}})).To(HaveOccurred())

	// Missing cluster
	Expect(ValidateRoute(Route{Name: "abc", Subset: Subset{}})).To(HaveOccurred())
}

func TestEDSEmptyEndpoints(t *testing.T) {
	RegisterTestingT(t)

	store := newTestStore()
	store.views["cold"] = &ViewState{
		Name:   "cold",
		Subset: Subset{Cluster: "dev"},
		// No endpoints — cold view
	}

	sm := NewSnapshotManager("gateway.test", store)
	Expect(sm.Initialize()).To(Succeed())

	snap, _ := sm.Cache().GetSnapshot(nodeID)
	endpoints := snap.GetResources(resource.EndpointType)
	Expect(endpoints).To(HaveKey("view-cold"))
	cla := endpoints["view-cold"].(*endpoint.ClusterLoadAssignment)
	Expect(cla.Endpoints).To(HaveLen(1))
	Expect(cla.Endpoints[0].LbEndpoints).To(BeEmpty())
}
