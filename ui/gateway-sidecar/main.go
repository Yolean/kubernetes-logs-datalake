package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	logger, _ := zap.NewProduction()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseHostname := os.Getenv("GATEWAY_HOSTNAME")
	if baseHostname == "" {
		logger.Fatal("GATEWAY_HOSTNAME must be set")
	}

	// Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Fatal("failed to get in-cluster config", zap.Error(err))
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Fatal("failed to create kubernetes client", zap.Error(err))
	}
	k8s := NewK8sClient(clientset)

	// ViewStore with onChange → xDS rebuild
	var snapshots *SnapshotManager
	store := NewViewStore(clientset, viewNamespace, func() {
		if snapshots != nil {
			if err := snapshots.Rebuild(); err != nil {
				logger.Error("failed to rebuild xDS snapshot", zap.Error(err))
			}
		}
	})

	// xDS snapshot manager
	snapshots = NewSnapshotManager(baseHostname, store)
	if err := snapshots.Initialize(); err != nil {
		logger.Fatal("failed to initialize xDS", zap.Error(err))
	}

	// Start ViewStore informers
	go store.Start(ctx)

	// HTTP API
	api := NewAPIHandler(store, k8s)
	httpServer := &http.Server{
		Addr:    ":18080",
		Handler: api.Handler(),
	}

	// gRPC xDS server + ext_authz
	srv := server.NewServer(ctx, snapshots.Cache(), nil)
	grpcServer := grpc.NewServer()
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
	listenerservice.RegisterListenerDiscoveryServiceServer(grpcServer, srv)
	routeservice.RegisterRouteDiscoveryServiceServer(grpcServer, srv)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, srv)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, srv)

	authzServer := NewExtAuthzServer(store, k8s, baseHostname)
	authzServer.Register(grpcServer)

	lis, err := net.Listen("tcp", ":18000")
	if err != nil {
		logger.Fatal("failed to listen on :18000", zap.Error(err))
	}

	// Start servers
	go func() {
		logger.Info("xDS server listening on :18000")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	go func() {
		logger.Info("HTTP API listening on :18080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	logger.Info("shutting down...")
	grpcServer.GracefulStop()
	httpServer.Shutdown(ctx)
}
