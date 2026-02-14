package main

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateViewJob(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	err := k.CreateViewJob("test01", Subset{Cluster: "dev", Namespace: "logs"})
	g.Expect(err).NotTo(HaveOccurred())

	job, err := clientset.BatchV1().Jobs(viewNamespace).Get(context.Background(), "view-test01", metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	// Labels
	g.Expect(job.Labels["app"]).To(Equal(labelApp))
	g.Expect(job.Labels[labelViewName]).To(Equal("test01"))

	// Annotations
	g.Expect(job.Annotations[annoCluster]).To(Equal("dev"))
	g.Expect(job.Annotations[annoNamespace]).To(Equal("logs"))

	// Pod template labels
	g.Expect(job.Spec.Template.Labels[labelViewName]).To(Equal("test01"))

	// Containers: duckdb + envoy sidecar
	g.Expect(job.Spec.Template.Spec.Containers).To(HaveLen(2))
	duckdb := job.Spec.Template.Spec.Containers[0]
	g.Expect(duckdb.Name).To(Equal("duckdb"))
	g.Expect(duckdb.Image).To(Equal("yolean/duckdb-ui:latest"))

	envoy := job.Spec.Template.Spec.Containers[1]
	g.Expect(envoy.Name).To(Equal("envoy"))
	g.Expect(envoy.Ports).To(HaveLen(1))
	g.Expect(envoy.Ports[0].ContainerPort).To(Equal(int32(8080)))

	// Readiness probe on envoy sidecar
	g.Expect(envoy.ReadinessProbe).NotTo(BeNil())
	g.Expect(envoy.ReadinessProbe.HTTPGet.Path).To(Equal("/"))
	g.Expect(envoy.ReadinessProbe.HTTPGet.Port.IntValue()).To(Equal(8080))

	// DuckDB S3 secret
	g.Expect(duckdb.EnvFrom).To(HaveLen(1))
	g.Expect(duckdb.EnvFrom[0].SecretRef.Name).To(Equal("duckdb-s3"))

	// DuckDB subset env vars
	g.Expect(duckdb.Env).To(HaveLen(2))
	g.Expect(duckdb.Env[0].Name).To(Equal("SUBSET_CLUSTER"))
	g.Expect(duckdb.Env[0].Value).To(Equal("dev"))
	g.Expect(duckdb.Env[1].Name).To(Equal("SUBSET_NAMESPACE"))
	g.Expect(duckdb.Env[1].Value).To(Equal("logs"))

	// DuckDB init volume mount
	g.Expect(duckdb.VolumeMounts).To(HaveLen(1))
	g.Expect(duckdb.VolumeMounts[0].Name).To(Equal("duckdb-init"))
	g.Expect(duckdb.VolumeMounts[0].MountPath).To(Equal("/etc/duckdb"))
	g.Expect(duckdb.VolumeMounts[0].ReadOnly).To(BeTrue())

	// Volumes: envoy-config + duckdb-init
	g.Expect(job.Spec.Template.Spec.Volumes).To(HaveLen(2))
	g.Expect(job.Spec.Template.Spec.Volumes[0].ConfigMap.Name).To(Equal("duckdb-envoy"))
	g.Expect(job.Spec.Template.Spec.Volumes[1].ConfigMap.Name).To(Equal("duckdb-init"))

	// ActiveDeadlineSeconds
	g.Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(3600)))
}

func TestCreateViewService(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	err := k.CreateViewService("test01", Subset{Cluster: "dev", Namespace: "logs"})
	g.Expect(err).NotTo(HaveOccurred())

	svc, err := clientset.CoreV1().Services(viewNamespace).Get(context.Background(), "view-test01", metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	// Headless
	g.Expect(svc.Spec.ClusterIP).To(Equal("None"))

	// PublishNotReadyAddresses
	g.Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())

	// Selector
	g.Expect(svc.Spec.Selector[labelViewName]).To(Equal("test01"))

	// Annotations
	g.Expect(svc.Annotations[annoCluster]).To(Equal("dev"))
	g.Expect(svc.Annotations[annoNamespace]).To(Equal("logs"))

	// Port
	g.Expect(svc.Spec.Ports).To(HaveLen(1))
	g.Expect(svc.Spec.Ports[0].Port).To(Equal(int32(8080)))
}

func TestCreateViewJobAlreadyExists(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	g.Expect(k.CreateViewJob("test01", Subset{Cluster: "dev"})).To(Succeed())
	// Second create should succeed (AlreadyExists is ignored)
	g.Expect(k.CreateViewJob("test01", Subset{Cluster: "dev"})).To(Succeed())
}

func TestDeleteViewJob(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	g.Expect(k.CreateViewJob("test01", Subset{Cluster: "dev"})).To(Succeed())
	g.Expect(k.DeleteViewJob("test01")).To(Succeed())

	_, err := clientset.BatchV1().Jobs(viewNamespace).Get(context.Background(), "view-test01", metav1.GetOptions{})
	g.Expect(err).To(HaveOccurred())
}

func TestDeleteViewService(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	g.Expect(k.CreateViewService("test01", Subset{Cluster: "dev"})).To(Succeed())
	g.Expect(k.DeleteViewService("test01")).To(Succeed())

	_, err := clientset.CoreV1().Services(viewNamespace).Get(context.Background(), "view-test01", metav1.GetOptions{})
	g.Expect(err).To(HaveOccurred())
}

func TestCreateViewJobClusterOnlySubset(t *testing.T) {
	g := NewWithT(t)

	clientset := fake.NewSimpleClientset()
	k := NewK8sClient(clientset)

	err := k.CreateViewJob("cluster-only", Subset{Cluster: "dev"})
	g.Expect(err).NotTo(HaveOccurred())

	job, err := clientset.BatchV1().Jobs(viewNamespace).Get(context.Background(), "view-cluster-only", metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	duckdb := job.Spec.Template.Spec.Containers[0]
	g.Expect(duckdb.Env[0].Name).To(Equal("SUBSET_CLUSTER"))
	g.Expect(duckdb.Env[0].Value).To(Equal("dev"))
	g.Expect(duckdb.Env[1].Name).To(Equal("SUBSET_NAMESPACE"))
	g.Expect(duckdb.Env[1].Value).To(Equal(""))
}
