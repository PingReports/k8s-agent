package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Item struct {
	SnapshotTs      time.Time
	Kind            string
	NS              string
	Name            string
	UID             string
	Phase           string
	Node            string
	ReplicasDesired int
	ReplicasReady   int
	Restarts        int
	Image           string
	SpecHash        string
	Labels          map[string]string
}

type Snapshot struct {
	Items          []Item
	NodeCount      int
	PodCount       int
	NamespaceCount int
}

// Collector takes a periodic inventory of the cluster's workload-shaped
// objects. It explicitly does NOT read Secrets or env-var values — anything
// that could leak credentials. Spec content is reduced to a hash so callers
// can detect drift without us shipping raw manifests.
type Collector struct {
	cs kubernetes.Interface
}

func New() (*Collector, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Collector{cs: cs}, nil
}

func (c *Collector) Snapshot(ctx context.Context) (Snapshot, error) {
	now := time.Now().UTC()
	out := Snapshot{}

	nss, err := c.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.NamespaceCount = len(nss.Items)
	for i := range nss.Items {
		ns := &nss.Items[i]
		out.Items = append(out.Items, Item{
			SnapshotTs: now,
			Kind:       "Namespace",
			Name:       ns.Name,
			UID:        string(ns.UID),
			Phase:      string(ns.Status.Phase),
			Labels:     trimLabels(ns.Labels),
		})
	}

	nodes, err := c.cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.NodeCount = len(nodes.Items)
	for i := range nodes.Items {
		n := &nodes.Items[i]
		phase := "Ready"
		for _, cd := range n.Status.Conditions {
			if cd.Type == corev1.NodeReady {
				if cd.Status != corev1.ConditionTrue {
					phase = "NotReady"
				}
				break
			}
		}
		out.Items = append(out.Items, Item{
			SnapshotTs: now,
			Kind:       "Node",
			Name:       n.Name,
			UID:        string(n.UID),
			Phase:      phase,
			Labels:     trimLabels(n.Labels),
		})
	}

	pods, err := c.cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	out.PodCount = len(pods.Items)
	for i := range pods.Items {
		p := &pods.Items[i]
		var restarts int32
		var img string
		for _, cs := range p.Status.ContainerStatuses {
			restarts += cs.RestartCount
			if img == "" {
				img = cs.Image
			}
		}
		ready, total := 0, len(p.Spec.Containers)
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Ready {
				ready++
			}
		}
		out.Items = append(out.Items, Item{
			SnapshotTs:      now,
			Kind:            "Pod",
			NS:              p.Namespace,
			Name:            p.Name,
			UID:             string(p.UID),
			Phase:           string(p.Status.Phase),
			Node:            p.Spec.NodeName,
			ReplicasDesired: total,
			ReplicasReady:   ready,
			Restarts:        int(restarts),
			Image:           img,
			SpecHash:        specHash(p.Spec),
			Labels:          trimLabels(p.Labels),
		})
	}

	deps, err := c.cs.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err == nil {
		for i := range deps.Items {
			d := &deps.Items[i]
			out.Items = append(out.Items, Item{
				SnapshotTs:      now,
				Kind:            "Deployment",
				NS:              d.Namespace,
				Name:            d.Name,
				UID:             string(d.UID),
				ReplicasDesired: int(d.Status.Replicas),
				ReplicasReady:   int(d.Status.ReadyReplicas),
				Labels:          trimLabels(d.Labels),
			})
		}
	}

	stss, err := c.cs.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	if err == nil {
		for i := range stss.Items {
			s := &stss.Items[i]
			out.Items = append(out.Items, Item{
				SnapshotTs:      now,
				Kind:            "StatefulSet",
				NS:              s.Namespace,
				Name:            s.Name,
				UID:             string(s.UID),
				ReplicasDesired: int(s.Status.Replicas),
				ReplicasReady:   int(s.Status.ReadyReplicas),
				Labels:          trimLabels(s.Labels),
			})
		}
	}

	dss, err := c.cs.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err == nil {
		for i := range dss.Items {
			d := &dss.Items[i]
			out.Items = append(out.Items, Item{
				SnapshotTs:      now,
				Kind:            "DaemonSet",
				NS:              d.Namespace,
				Name:            d.Name,
				UID:             string(d.UID),
				ReplicasDesired: int(d.Status.DesiredNumberScheduled),
				ReplicasReady:   int(d.Status.NumberReady),
				Labels:          trimLabels(d.Labels),
			})
		}
	}

	pvcs, err := c.cs.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err == nil {
		for i := range pvcs.Items {
			p := &pvcs.Items[i]
			out.Items = append(out.Items, Item{
				SnapshotTs: now,
				Kind:       "PersistentVolumeClaim",
				NS:         p.Namespace,
				Name:       p.Name,
				UID:        string(p.UID),
				Phase:      string(p.Status.Phase),
				Labels:     trimLabels(p.Labels),
			})
		}
	}

	return out, nil
}

func specHash(spec corev1.PodSpec) string {
	// Reduce a spec to a stable hash without round-tripping the manifest.
	// We hash a deliberately small subset that captures what matters for
	// drift detection: container images + restart policy.
	h := sha256.New()
	for _, c := range spec.Containers {
		fmt.Fprintf(h, "c|%s|%s|", c.Name, c.Image)
	}
	for _, c := range spec.InitContainers {
		fmt.Fprintf(h, "i|%s|%s|", c.Name, c.Image)
	}
	fmt.Fprintf(h, "rp|%s", spec.RestartPolicy)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func trimLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	count := 0
	for k, v := range in {
		if count >= 16 {
			break
		}
		if len(k) > 128 {
			k = k[:128]
		}
		if len(v) > 128 {
			v = v[:128]
		}
		out[k] = v
		count++
	}
	return out
}
