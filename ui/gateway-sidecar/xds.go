package main

import (
	"context"
	"fmt"
	"sync"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	ext_authz_filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_authz/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const nodeID = "gateway"

type SnapshotManager struct {
	mu           sync.Mutex
	cache        cachev3.SnapshotCache
	version      int
	store        *ViewStore
	baseHostname string
	logger       *zap.Logger
}

func NewSnapshotManager(baseHostname string, store *ViewStore) *SnapshotManager {
	return &SnapshotManager{
		cache:        cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil),
		store:        store,
		baseHostname: baseHostname,
		logger:       zap.L().Named("xds"),
	}
}

func (sm *SnapshotManager) Cache() cachev3.SnapshotCache {
	return sm.cache
}

func (sm *SnapshotManager) Initialize() error {
	return sm.Rebuild()
}

func (sm *SnapshotManager) Rebuild() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.pushSnapshot()
}

func (sm *SnapshotManager) pushSnapshot() error {
	sm.version++
	versionStr := fmt.Sprintf("%d", sm.version)

	var views []*ViewState
	if sm.store != nil {
		views = sm.store.ListViews()
	}

	clusters := sm.buildClusters(views)
	endpoints := sm.buildEndpoints(views)
	routes := sm.buildRouteConfig(views)
	listeners := []types.Resource{sm.buildListener()}

	snapshot, err := cachev3.NewSnapshot(versionStr,
		map[resource.Type][]types.Resource{
			resource.ListenerType: listeners,
			resource.RouteType:    {routes},
			resource.ClusterType:  clusters,
			resource.EndpointType: endpoints,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	if err := snapshot.Consistent(); err != nil {
		return fmt.Errorf("snapshot inconsistent: %w", err)
	}

	if err := sm.cache.SetSnapshot(context.Background(), nodeID, snapshot); err != nil {
		return fmt.Errorf("failed to set snapshot: %w", err)
	}

	sm.logger.Info("pushed xDS snapshot", zap.String("version", versionStr), zap.Int("views", len(views)))
	return nil
}

func (sm *SnapshotManager) buildListener() *listener.Listener {
	routerAny, err := anypb.New(&router.Router{})
	if err != nil {
		sm.logger.Fatal("failed to marshal router config", zap.Error(err))
	}

	extAuthzAny, err := anypb.New(&ext_authz_filter.ExtAuthz{
		Services: &ext_authz_filter.ExtAuthz_GrpcService{
			GrpcService: &core.GrpcService{
				TargetSpecifier: &core.GrpcService_EnvoyGrpc_{
					EnvoyGrpc: &core.GrpcService_EnvoyGrpc{
						ClusterName: "xds_cluster",
					},
				},
				Timeout: durationpb.New(65_000_000_000), // 65s, longer than ext_authz cold-start timeout
			},
		},
		TransportApiVersion: core.ApiVersion_V3,
		FailureModeAllow:    false,
	})
	if err != nil {
		sm.logger.Fatal("failed to marshal ext_authz config", zap.Error(err))
	}

	hcmConfig := &hcm.HttpConnectionManager{
		StatPrefix: "ingress_http",
		CodecType:  hcm.HttpConnectionManager_AUTO,
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				ConfigSource: &core.ConfigSource{
					ConfigSourceSpecifier: &core.ConfigSource_Ads{
						Ads: &core.AggregatedConfigSource{},
					},
					ResourceApiVersion: core.ApiVersion_V3,
				},
				RouteConfigName: "local_route",
			},
		},
		HttpFilters: []*hcm.HttpFilter{
			{
				Name: "envoy.filters.http.ext_authz",
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: extAuthzAny,
				},
			},
			{
				Name: "envoy.filters.http.router",
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: routerAny,
				},
			},
		},
	}

	hcmAny, err := anypb.New(hcmConfig)
	if err != nil {
		sm.logger.Fatal("failed to marshal HCM config", zap.Error(err))
	}

	return &listener.Listener{
		Name: "main",
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Address: "0.0.0.0",
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: 8080,
					},
				},
			},
		},
		FilterChains: []*listener.FilterChain{{
			Filters: []*listener.Filter{{
				Name: "envoy.filters.network.http_connection_manager",
				ConfigType: &listener.Filter_TypedConfig{
					TypedConfig: hcmAny,
				},
			}},
		}},
	}
}

func (sm *SnapshotManager) buildRouteConfig(views []*ViewState) *route.RouteConfiguration {
	vhosts := []*route.VirtualHost{sm.buildDefaultVirtualHost()}

	for _, v := range views {
		vhosts = append(vhosts, &route.VirtualHost{
			Name:    viewNamePrefix + v.Name,
			Domains: []string{v.Name + "." + sm.baseHostname, v.Name + ".*"},
			Routes: []*route.Route{{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: viewNamePrefix + v.Name,
						},
						HostRewriteSpecifier: &route.RouteAction_HostRewriteLiteral{
							HostRewriteLiteral: "localhost",
						},
						Timeout: durationpb.New(0), // disable route timeout for SSE streams
						RetryPolicy: &route.RetryPolicy{
							RetryOn:    "connect-failure,reset",
							NumRetries: wrapperspb.UInt32(3),
						},
					},
				},
			}},
		})
	}

	return &route.RouteConfiguration{
		Name:         "local_route",
		VirtualHosts: vhosts,
	}
}

func (sm *SnapshotManager) extAuthzDisabledPerRoute() map[string]*anypb.Any {
	perRouteAny, err := anypb.New(&ext_authz_filter.ExtAuthzPerRoute{
		Override: &ext_authz_filter.ExtAuthzPerRoute_Disabled{
			Disabled: true,
		},
	})
	if err != nil {
		sm.logger.Fatal("failed to marshal ext_authz per-route config", zap.Error(err))
	}
	return map[string]*anypb.Any{
		"envoy.filters.http.ext_authz": perRouteAny,
	}
}

func (sm *SnapshotManager) buildDefaultVirtualHost() *route.VirtualHost {
	disabled := sm.extAuthzDisabledPerRoute()
	return &route.VirtualHost{
		Name:    "local",
		Domains: []string{"*"},
		Routes: []*route.Route{
			{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/_api/"},
				},
				Action: &route.Route_Route{
					Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_Cluster{
							Cluster: "sidecar_api",
						},
					},
				},
				TypedPerFilterConfig: disabled,
			},
			{
				Match: &route.RouteMatch{
					PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"},
				},
				Action: &route.Route_DirectResponse{
					DirectResponse: &route.DirectResponseAction{
						Status: 200,
						Body: &core.DataSource{
							Specifier: &core.DataSource_InlineString{InlineString: "gateway ok\n"},
						},
					},
				},
				TypedPerFilterConfig: disabled,
			},
		},
	}
}

func (sm *SnapshotManager) buildClusters(views []*ViewState) []types.Resource {
	clusters := []types.Resource{
		&cluster.Cluster{
			Name:           "sidecar_api",
			ConnectTimeout: durationpb.New(1_000_000_000),
			ClusterDiscoveryType: &cluster.Cluster_Type{
				Type: cluster.Cluster_STATIC,
			},
			LoadAssignment: &endpoint.ClusterLoadAssignment{
				ClusterName: "sidecar_api",
				Endpoints: []*endpoint.LocalityLbEndpoints{{
					LbEndpoints: []*endpoint.LbEndpoint{{
						HostIdentifier: &endpoint.LbEndpoint_Endpoint{
							Endpoint: &endpoint.Endpoint{
								Address: &core.Address{
									Address: &core.Address_SocketAddress{
										SocketAddress: &core.SocketAddress{
											Address: "127.0.0.1",
											PortSpecifier: &core.SocketAddress_PortValue{
												PortValue: 18080,
											},
										},
									},
								},
							},
						},
					}},
				}},
			},
		},
	}

	for _, v := range views {
		clusters = append(clusters, &cluster.Cluster{
			Name:           viewNamePrefix + v.Name,
			ConnectTimeout: durationpb.New(5_000_000_000),
			ClusterDiscoveryType: &cluster.Cluster_Type{
				Type: cluster.Cluster_EDS,
			},
			EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
				EdsConfig: &core.ConfigSource{
					ConfigSourceSpecifier: &core.ConfigSource_Ads{
						Ads: &core.AggregatedConfigSource{},
					},
					ResourceApiVersion: core.ApiVersion_V3,
				},
			},
		})
	}

	return clusters
}

func (sm *SnapshotManager) buildEndpoints(views []*ViewState) []types.Resource {
	var endpoints []types.Resource

	for _, v := range views {
		var lbEndpoints []*endpoint.LbEndpoint
		if sm.store != nil {
			for _, ep := range sm.store.ReadyEndpoints(v.Name) {
				lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{
						Endpoint: &endpoint.Endpoint{
							Address: &core.Address{
								Address: &core.Address_SocketAddress{
									SocketAddress: &core.SocketAddress{
										Address:       ep.Address,
										PortSpecifier: &core.SocketAddress_PortValue{PortValue: uint32(ep.Port)},
									},
								},
							},
						},
					},
				})
			}
		}

		endpoints = append(endpoints, &endpoint.ClusterLoadAssignment{
			ClusterName: viewNamePrefix + v.Name,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: lbEndpoints,
			}},
		})
	}

	return endpoints
}
