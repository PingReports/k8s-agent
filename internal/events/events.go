package events

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Event struct {
	Ts      time.Time
	NS      string
	Kind    string
	Name    string
	Reason  string
	Type    string
	Message string
	Count   int32
}

// Watcher polls the Kubernetes Events API on an interval and accumulates
// operationally-meaningful events into an in-memory buffer that the sender
// drains on each push.
//
// We poll instead of watching: a watch connection is fragile across apiserver
// outages, and 30 s freshness is more than enough for alerting on Events
// that already represent terminal conditions (OOMKill, FailedScheduling).
type Watcher struct {
	cs       kubernetes.Interface
	mu       sync.Mutex
	events   []Event
	maxRows  int
	pollFreq time.Duration
	seen     map[string]struct{}
}

func New(maxRows int) (*Watcher, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Watcher{
		cs:       cs,
		maxRows:  maxRows,
		pollFreq: 30 * time.Second,
		seen:     make(map[string]struct{}, 1024),
	}, nil
}

func (w *Watcher) Drain() []Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.events
	w.events = nil
	return out
}

func (w *Watcher) Run(ctx context.Context) error {
	t := time.NewTicker(w.pollFreq)
	defer t.Stop()
	w.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			w.poll(ctx)
		}
	}
}

func (w *Watcher) poll(ctx context.Context) {
	since := time.Now().Add(-2 * time.Minute)
	list, err := w.cs.CoreV1().Events("").List(ctx, metav1.ListOptions{Limit: 500})
	if err != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range list.Items {
		e := &list.Items[i]
		ts := e.LastTimestamp.Time
		if ts.IsZero() {
			ts = e.EventTime.Time
		}
		if ts.IsZero() {
			ts = e.CreationTimestamp.Time
		}
		if ts.Before(since) {
			continue
		}
		if !shouldShip(e.Reason) {
			continue
		}
		key := string(e.UID) + ":" + e.LastTimestamp.Time.UTC().Format(time.RFC3339)
		if _, ok := w.seen[key]; ok {
			continue
		}
		w.seen[key] = struct{}{}
		w.events = append(w.events, Event{
			Ts:      ts,
			NS:      e.InvolvedObject.Namespace,
			Kind:    e.InvolvedObject.Kind,
			Name:    e.InvolvedObject.Name,
			Reason:  e.Reason,
			Type:    e.Type,
			Message: truncate(e.Message, 4096),
			Count:   e.Count,
		})
		if len(w.events) > w.maxRows {
			drop := len(w.events) - w.maxRows
			w.events = w.events[drop:]
		}
	}
	// Trim seen map to bounded size (LRU-ish — drop oldest half when over cap).
	if len(w.seen) > 4096 {
		w.seen = make(map[string]struct{}, 1024)
	}
}

func shouldShip(reason string) bool {
	if reason == "" {
		return false
	}
	switch reason {
	case "OOMKilling", "OOMKilled", "BackOff", "Failed", "FailedScheduling",
		"FailedCreate", "FailedKillPod", "FailedMount", "FailedAttachVolume",
		"NodeNotReady", "NodeNotSchedulable", "NodeHasInsufficientMemory",
		"NodeHasDiskPressure", "NodeHasInsufficientPID",
		"Unhealthy", "Evicted", "Preempted",
		"FailedDaemonPod", "ImagePullBackOff", "ErrImagePull",
		"ProvisioningFailed", "VolumeFailedMount":
		return true
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
