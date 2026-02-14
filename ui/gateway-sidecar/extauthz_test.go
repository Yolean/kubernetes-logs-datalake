package main

import (
	"context"
	"testing"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestExtAuthz() (*ExtAuthzServer, *ViewStore) {
	store := &ViewStore{
		views:    make(map[string]*ViewState),
		onChange: func() {},
	}
	clientset := fake.NewSimpleClientset()
	k8s := NewK8sClient(clientset)
	srv := NewExtAuthzServer(store, k8s, "gateway.test")
	return srv, store
}

func checkRequest(host string) *authv3.CheckRequest {
	return &authv3.CheckRequest{
		Attributes: &authv3.AttributeContext{
			Request: &authv3.AttributeContext_Request{
				Http: &authv3.AttributeContext_HttpRequest{
					Host: host,
				},
			},
		},
	}
}

func TestParseViewName(t *testing.T) {
	RegisterTestingT(t)

	srv, _ := newTestExtAuthz()

	// Matches base hostname
	Expect(srv.parseViewName("test01.gateway.test")).To(Equal("test01"))

	// Matches with port
	Expect(srv.parseViewName("test01.gateway.test:8080")).To(Equal("test01"))

	// Matches wildcard pattern (any host with dot)
	Expect(srv.parseViewName("test01.anything")).To(Equal("test01"))

	// No match — bare hostname
	Expect(srv.parseViewName("gateway.test")).To(Equal(""))

	// No match — no dot
	Expect(srv.parseViewName("test01")).To(Equal(""))
}

func TestCheckPassthrough(t *testing.T) {
	RegisterTestingT(t)

	srv, _ := newTestExtAuthz()

	// Bare host with no view pattern → passthrough
	resp, err := srv.Check(context.Background(), checkRequest("gateway.test"))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.GetStatus().GetCode()).To(Equal(int32(0))) // OK
}

func TestCheckNoService(t *testing.T) {
	RegisterTestingT(t)

	srv, _ := newTestExtAuthz()

	// View name matches but no service exists → 404
	resp, err := srv.Check(context.Background(), checkRequest("test01.gateway.test"))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.GetStatus().GetCode()).NotTo(Equal(int32(0)))
	Expect(resp.GetDeniedResponse().GetStatus().GetCode()).To(BeEquivalentTo(404))
}

func TestCheckReadyEndpoints(t *testing.T) {
	RegisterTestingT(t)

	srv, store := newTestExtAuthz()
	store.views["test01"] = &ViewState{
		Name:   "test01",
		Subset: Subset{Cluster: "dev"},
		Endpoints: []ViewEndpoint{
			{Address: "10.0.0.1", Port: 8080, Ready: true},
		},
	}

	resp, err := srv.Check(context.Background(), checkRequest("test01.gateway.test"))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.GetStatus().GetCode()).To(Equal(int32(0))) // OK
}

func TestCheckColdStartTimeout(t *testing.T) {
	RegisterTestingT(t)

	srv, store := newTestExtAuthz()
	store.views["cold"] = &ViewState{
		Name:   "cold",
		Subset: Subset{Cluster: "dev"},
		// No endpoints
	}

	// Use a cancelled context to simulate timeout
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := srv.Check(ctx, checkRequest("cold.gateway.test"))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.GetStatus().GetCode()).NotTo(Equal(int32(0)))
}
