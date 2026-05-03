package scraper

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// DiscoveredTarget is one Prometheus-compatible scrape target with the node
// it represents. node-exporter runs as a DaemonSet, so we scrape each pod
// individually and label its samples with the node it lives on. Without this
// tagging step every sample looks identical and per-node CPU/disk/network
// rollups all collapse to NULL.
type DiscoveredTarget struct {
	URL  string
	Node string
}

// Discoverer pulls per-node DaemonSet pods that match the given label
// selector and constructs scrape URLs by pod IP.
type Discoverer struct {
	cs            kubernetes.Interface
	labelSelector string
	port          int
	path          string
	scheme        string
}

func NewDiscoverer(labelSelector, scheme string, port int, path string) (*Discoverer, error) {
	if labelSelector == "" {
		return nil, nil
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if scheme == "" {
		scheme = "http"
	}
	if path == "" {
		path = "/metrics"
	}
	return &Discoverer{cs: cs, labelSelector: labelSelector, port: port, path: path, scheme: scheme}, nil
}

func (d *Discoverer) Discover(ctx context.Context) ([]DiscoveredTarget, error) {
	if d == nil {
		return nil, nil
	}
	pods, err := d.cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: d.labelSelector,
	})
	if err != nil {
		return nil, err
	}
	out := make([]DiscoveredTarget, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != "Running" || p.Status.PodIP == "" {
			continue
		}
		out = append(out, DiscoveredTarget{
			URL:  fmt.Sprintf("%s://%s:%d%s", d.scheme, p.Status.PodIP, d.port, d.path),
			Node: p.Spec.NodeName,
		})
	}
	return out, nil
}
