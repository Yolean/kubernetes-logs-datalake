package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

type APIHandler struct {
	store  *ViewStore
	k8s    *K8sClient
	logger *zap.Logger
}

func NewAPIHandler(store *ViewStore, k8s *K8sClient) *APIHandler {
	return &APIHandler{
		store:  store,
		k8s:    k8s,
		logger: zap.L().Named("api"),
	}
}

func (h *APIHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_api/views", h.createView)
	mux.HandleFunc("GET /_api/views", h.listViews)
	mux.HandleFunc("DELETE /_api/views/{name}", h.deleteView)
	return mux
}

func (h *APIHandler) createView(w http.ResponseWriter, r *http.Request) {
	var route Route
	if err := json.NewDecoder(r.Body).Decode(&route); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	if err := ValidateRoute(route); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if h.store.HasService(route.Name) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "view already exists"})
		return
	}

	if err := h.k8s.CreateViewService(route.Name, route.Subset); err != nil {
		h.logger.Error("failed to create service", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create service"})
		return
	}

	h.logger.Info("view created", zap.String("view", route.Name), zap.String("cluster", route.Subset.Cluster))

	view := &View{Name: route.Name, Subset: route.Subset}
	writeJSON(w, http.StatusCreated, view)
}

func (h *APIHandler) listViews(w http.ResponseWriter, r *http.Request) {
	states := h.store.ListViews()
	views := make([]*View, 0, len(states))
	for _, s := range states {
		views = append(views, &View{
			Name:   s.Name,
			Subset: s.Subset,
			Ready:  len(h.store.ReadyEndpoints(s.Name)) > 0,
		})
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *APIHandler) deleteView(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) >= 4 {
			name = parts[3]
		}
	}

	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}

	// Delete K8s resources (best effort); informer handles xDS removal
	if err := h.k8s.DeleteViewJob(name); err != nil {
		h.logger.Info("failed to delete job", zap.String("view", name), zap.Error(err))
	}
	if err := h.k8s.DeleteViewService(name); err != nil {
		h.logger.Info("failed to delete service", zap.String("view", name), zap.Error(err))
	}

	h.logger.Info("view deleted", zap.String("view", name))

	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "name": name})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
