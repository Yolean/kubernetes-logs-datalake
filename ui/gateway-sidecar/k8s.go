package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	viewNamespace  = "ui"
	viewNamePrefix = "view-"
	labelApp       = "lakeview"
	labelViewName  = "lakeview.yolean.se/view-name"
	annoCluster    = "lakeview.yolean.se/cluster"
	annoNamespace  = "lakeview.yolean.se/namespace"
)

type K8sClient struct {
	client    kubernetes.Interface
	namespace string
}

func NewK8sClient(client kubernetes.Interface) *K8sClient {
	return &K8sClient{
		client:    client,
		namespace: viewNamespace,
	}
}

func (k *K8sClient) CreateViewJob(name string, subset Subset) error {
	activeDeadline := int64(3600)
	backoffLimit := int32(0)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      viewNamePrefix + name,
			Namespace: k.namespace,
			Labels: map[string]string{
				"app":       labelApp,
				labelViewName: name,
			},
			Annotations: map[string]string{
				annoCluster:   subset.Cluster,
				annoNamespace: subset.Namespace,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadline,
			BackoffLimit:          &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":       labelApp,
						labelViewName: name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "duckdb",
							Image:           "yolean/duckdb-ui:latest",
							ImagePullPolicy: corev1.PullNever,
						},
						{
							Name:  "envoy",
							Image: "ghcr.io/yolean/envoy:distroless-v1.37.0@sha256:92fdb97b9fdb47da82fbe3651b83bd1419e544966d264632af4864004652685f",
							Ports: []corev1.ContainerPort{{
								Name:          "http",
								ContainerPort: 8080,
							}},
							VolumeMounts: []corev1.VolumeMount{{
								Name:      "envoy-config",
								MountPath: "/etc/envoy",
								ReadOnly:  true,
							}},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/",
										Port: intstr.FromInt32(8080),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{{
						Name: "envoy-config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "duckdb-envoy",
								},
							},
						},
					}},
				},
			},
		},
	}

	_, err := k.client.BatchV1().Jobs(k.namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		zap.L().Debug("job already exists", zap.String("view", name))
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to create job for view %s: %w", name, err)
	}
	zap.L().Info("job created", zap.String("view", name))
	return nil
}

func (k *K8sClient) CreateViewService(name string, subset Subset) error {
	publishNotReady := true
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      viewNamePrefix + name,
			Namespace: k.namespace,
			Labels: map[string]string{
				"app":         labelApp,
				labelViewName: name,
			},
			Annotations: map[string]string{
				annoCluster:   subset.Cluster,
				annoNamespace: subset.Namespace,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: publishNotReady,
			Selector: map[string]string{
				labelViewName: name,
			},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       8080,
				TargetPort: intstr.FromInt32(8080),
			}},
		},
	}

	_, err := k.client.CoreV1().Services(k.namespace).Create(context.Background(), svc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create service for view %s: %w", name, err)
	}
	zap.L().Info("service created", zap.String("view", name))
	return nil
}

func (k *K8sClient) DeleteViewJob(name string) error {
	propagation := metav1.DeletePropagationBackground
	err := k.client.BatchV1().Jobs(k.namespace).Delete(context.Background(), viewNamePrefix+name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil {
		return fmt.Errorf("failed to delete job for view %s: %w", name, err)
	}
	zap.L().Info("job deleted", zap.String("view", name))
	return nil
}

func (k *K8sClient) DeleteViewService(name string) error {
	err := k.client.CoreV1().Services(k.namespace).Delete(context.Background(), viewNamePrefix+name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete service for view %s: %w", name, err)
	}
	zap.L().Info("service deleted", zap.String("view", name))
	return nil
}
