package main

import (
	"context"
	"strings"
	"sync"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type ViewEndpoint struct {
	Address string
	Port    int32
	Ready   bool
}

type ViewState struct {
	Name      string
	Subset    Subset
	Endpoints []ViewEndpoint
}

type ViewStore struct {
	mu       sync.RWMutex
	views    map[string]*ViewState
	onChange func()
	client   kubernetes.Interface
	ns       string
	logger   *zap.Logger
}

func NewViewStore(client kubernetes.Interface, ns string, onChange func()) *ViewStore {
	return &ViewStore{
		views:    make(map[string]*ViewState),
		onChange: onChange,
		client:   client,
		ns:       ns,
		logger:   zap.L().Named("viewstore"),
	}
}

func (vs *ViewStore) Start(ctx context.Context) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		vs.client, 0,
		informers.WithNamespace(vs.ns),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = "app=" + labelApp
		}),
	)

	svcInformer := factory.Core().V1().Services().Informer()
	svcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { vs.onServiceEvent(obj) },
		UpdateFunc: func(_, obj interface{}) { vs.onServiceEvent(obj) },
		DeleteFunc: func(obj interface{}) { vs.onServiceDelete(obj) },
	})

	// EndpointSlices need a separate factory without the app label filter,
	// since EndpointSlices are labeled with kubernetes.io/service-name, not app=lakeview
	epFactory := informers.NewSharedInformerFactoryWithOptions(
		vs.client, 0,
		informers.WithNamespace(vs.ns),
	)

	epInformer := epFactory.Discovery().V1().EndpointSlices().Informer()
	epInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { vs.onEndpointSliceEvent(obj) },
		UpdateFunc: func(_, obj interface{}) { vs.onEndpointSliceEvent(obj) },
		DeleteFunc: func(obj interface{}) { vs.onEndpointSliceEvent(obj) },
	})

	factory.Start(ctx.Done())
	epFactory.Start(ctx.Done())

	factory.WaitForCacheSync(ctx.Done())
	epFactory.WaitForCacheSync(ctx.Done())

	vs.logger.Info("informers synced")
}

func (vs *ViewStore) onServiceEvent(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return
	}

	name := svc.Labels[labelViewName]
	if name == "" {
		return
	}

	vs.mu.Lock()
	existing, had := vs.views[name]
	subset := Subset{
		Cluster:   svc.Annotations[annoCluster],
		Namespace: svc.Annotations[annoNamespace],
	}
	if !had {
		vs.views[name] = &ViewState{Name: name, Subset: subset}
	} else {
		existing.Subset = subset
	}
	vs.mu.Unlock()

	vs.logger.Info("service updated", zap.String("view", name))
	vs.onChange()
}

func (vs *ViewStore) onServiceDelete(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		svc, ok = tombstone.Obj.(*corev1.Service)
		if !ok {
			return
		}
	}

	name := svc.Labels[labelViewName]
	if name == "" {
		return
	}

	vs.mu.Lock()
	delete(vs.views, name)
	vs.mu.Unlock()

	vs.logger.Info("service deleted", zap.String("view", name))
	vs.onChange()
}

func (vs *ViewStore) onEndpointSliceEvent(obj interface{}) {
	var svcName string
	switch eps := obj.(type) {
	case *discoveryv1.EndpointSlice:
		svcName = eps.Labels[discoveryv1.LabelServiceName]
	case cache.DeletedFinalStateUnknown:
		if ep, ok := eps.Obj.(*discoveryv1.EndpointSlice); ok {
			svcName = ep.Labels[discoveryv1.LabelServiceName]
		}
	default:
		return
	}

	if svcName == "" {
		return
	}

	// Extract view name: service name is "view-{name}"
	if !strings.HasPrefix(svcName, viewNamePrefix) {
		return
	}
	viewName := strings.TrimPrefix(svcName, viewNamePrefix)

	vs.mu.RLock()
	_, exists := vs.views[viewName]
	vs.mu.RUnlock()

	if !exists {
		return
	}

	vs.rebuildEndpoints(viewName)
	vs.onChange()
}

func (vs *ViewStore) rebuildEndpoints(viewName string) {
	epsList, err := vs.client.DiscoveryV1().EndpointSlices(vs.ns).List(
		context.Background(),
		metav1.ListOptions{
			LabelSelector: discoveryv1.LabelServiceName + "=" + viewNamePrefix + viewName,
		},
	)
	if err != nil {
		vs.logger.Error("failed to list endpointslices", zap.String("view", viewName), zap.Error(err))
		return
	}

	var endpoints []ViewEndpoint
	for _, eps := range epsList.Items {
		port := int32(8080)
		if len(eps.Ports) > 0 && eps.Ports[0].Port != nil {
			port = *eps.Ports[0].Port
		}
		for _, ep := range eps.Endpoints {
			// Use Serving (not Ready) because PublishNotReadyAddresses overrides Ready to always true
			ready := ep.Conditions.Serving != nil && *ep.Conditions.Serving
			for _, addr := range ep.Addresses {
				endpoints = append(endpoints, ViewEndpoint{
					Address: addr,
					Port:    port,
					Ready:   ready,
				})
			}
		}
	}

	vs.mu.Lock()
	if state, ok := vs.views[viewName]; ok {
		state.Endpoints = endpoints
	}
	vs.mu.Unlock()

	vs.logger.Debug("endpoints rebuilt", zap.String("view", viewName), zap.Int("count", len(endpoints)))
}

func (vs *ViewStore) GetView(name string) *ViewState {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.views[name]
}

func (vs *ViewStore) ListViews() []*ViewState {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	views := make([]*ViewState, 0, len(vs.views))
	for _, v := range vs.views {
		views = append(views, v)
	}
	return views
}

func (vs *ViewStore) ReadyEndpoints(name string) []ViewEndpoint {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	state, ok := vs.views[name]
	if !ok {
		return nil
	}
	var ready []ViewEndpoint
	for _, ep := range state.Endpoints {
		if ep.Ready {
			ready = append(ready, ep)
		}
	}
	return ready
}

func (vs *ViewStore) HasService(name string) bool {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	_, ok := vs.views[name]
	return ok
}
