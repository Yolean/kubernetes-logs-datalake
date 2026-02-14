package main

import (
	"fmt"
	"regexp"
)

var nameRegex = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)

type Subset struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace,omitempty"`
}

type Route struct {
	Name   string `json:"name"`
	Subset Subset `json:"subset"`
}

type View struct {
	Name   string `json:"name"`
	Subset Subset `json:"subset"`
	Ready  bool   `json:"ready"`
}

func ValidateRoute(r Route) error {
	if len(r.Name) < 3 || len(r.Name) > 8 {
		return fmt.Errorf("name must be 3-8 characters, got %d", len(r.Name))
	}
	if !nameRegex.MatchString(r.Name) {
		return fmt.Errorf("name must match %s", nameRegex.String())
	}
	if r.Subset.Cluster == "" {
		return fmt.Errorf("subset.cluster is required")
	}
	return nil
}
