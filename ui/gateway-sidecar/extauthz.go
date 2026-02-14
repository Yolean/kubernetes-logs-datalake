package main

import (
	"context"
	"strings"
	"time"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	coldStartPollInterval = 500 * time.Millisecond
	coldStartTimeout      = 60 * time.Second
)

type ExtAuthzServer struct {
	authv3.UnimplementedAuthorizationServer
	store        *ViewStore
	k8s          *K8sClient
	baseHostname string
	logger       *zap.Logger
}

func NewExtAuthzServer(store *ViewStore, k8s *K8sClient, baseHostname string) *ExtAuthzServer {
	return &ExtAuthzServer{
		store:        store,
		k8s:          k8s,
		baseHostname: baseHostname,
		logger:       zap.L().Named("ext_authz"),
	}
}

func (s *ExtAuthzServer) Register(srv *grpc.Server) {
	authv3.RegisterAuthorizationServer(srv, s)
}

func (s *ExtAuthzServer) Check(ctx context.Context, req *authv3.CheckRequest) (*authv3.CheckResponse, error) {
	host := req.GetAttributes().GetRequest().GetHttp().GetHost()
	viewName := s.parseViewName(host)

	if viewName == "" {
		return okResponse(), nil
	}

	view := s.store.GetView(viewName)
	if view == nil {
		s.logger.Info("no service for view", zap.String("view", viewName))
		return deniedResponse(codes.NotFound, 404, "view not found"), nil
	}

	if ready := s.store.ReadyEndpoints(viewName); len(ready) > 0 {
		return okResponse(), nil
	}

	// Cold start: create job and wait for ready endpoints
	startTime := time.Now()
	s.logger.Info("cold start begun", zap.String("view", viewName))
	if err := s.k8s.CreateViewJob(viewName, view.Subset); err != nil {
		s.logger.Error("cold start job creation failed", zap.String("view", viewName), zap.Error(err))
		return deniedResponse(codes.Internal, 503, "failed to create workload"), nil
	}

	deadline := time.After(coldStartTimeout)
	ticker := time.NewTicker(coldStartPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Warn("cold start cancelled", zap.String("view", viewName), zap.Duration("elapsed", time.Since(startTime)))
			return deniedResponse(codes.DeadlineExceeded, 504, "request cancelled"), nil
		case <-deadline:
			s.logger.Warn("cold start timeout", zap.String("view", viewName), zap.Duration("elapsed", time.Since(startTime)))
			return deniedResponse(codes.DeadlineExceeded, 503, "workload not ready"), nil
		case <-ticker.C:
			if ready := s.store.ReadyEndpoints(viewName); len(ready) > 0 {
				s.logger.Info("cold start complete", zap.String("view", viewName), zap.Duration("elapsed", time.Since(startTime)))
				return okResponse(), nil
			}
		}
	}
}

func (s *ExtAuthzServer) parseViewName(host string) string {
	// Strip port if present
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	// Exact base hostname is not a view
	if host == s.baseHostname {
		return ""
	}

	suffix := "." + s.baseHostname
	if strings.HasSuffix(host, suffix) {
		return strings.TrimSuffix(host, suffix)
	}

	// Also match "viewname.*" pattern — any host with a dot where the prefix is a single label
	if parts := strings.SplitN(host, ".", 2); len(parts) == 2 && !strings.Contains(parts[0], ".") {
		return parts[0]
	}

	return ""
}

func okResponse() *authv3.CheckResponse {
	return &authv3.CheckResponse{
		Status: &status.Status{Code: int32(codes.OK)},
	}
}

func deniedResponse(code codes.Code, httpStatus uint32, body string) *authv3.CheckResponse {
	return &authv3.CheckResponse{
		Status: &status.Status{Code: int32(code)},
		HttpResponse: &authv3.CheckResponse_DeniedResponse{
			DeniedResponse: &authv3.DeniedHttpResponse{
				Status: &typev3.HttpStatus{Code: typev3.StatusCode(httpStatus)},
				Body:   body,
			},
		},
	}
}
