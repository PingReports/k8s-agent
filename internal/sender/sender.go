package sender

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/pingreports/k8s-agent/internal/config"
	"github.com/pingreports/k8s-agent/internal/events"
	"github.com/pingreports/k8s-agent/internal/inventory"
	"github.com/pingreports/k8s-agent/internal/scraper"
)

type Payload struct {
	Schema         int             `json:"schema"`
	AgentVersion   string          `json:"agent_version"`
	AgentUptimeS   int64           `json:"agent_uptime_s"`
	NodeCount      int             `json:"node_count"`
	PodCount       int             `json:"pod_count"`
	NamespaceCount int             `json:"namespace_count"`
	ClusterVersion string          `json:"cluster_version,omitempty"`
	Metrics        []MetricPoint   `json:"metrics"`
	Events         []EventEntry    `json:"events"`
	Inventory      []InventoryItem `json:"inventory"`
}

type MetricPoint struct {
	Ts     time.Time         `json:"ts"`
	Name   string            `json:"name"`
	NS     string            `json:"ns,omitempty"`
	Kind   string            `json:"kind,omitempty"`
	Obj    string            `json:"obj,omitempty"`
	Node   string            `json:"node,omitempty"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

type EventEntry struct {
	Ts      time.Time `json:"ts"`
	NS      string    `json:"ns"`
	Kind    string    `json:"kind"`
	Name    string    `json:"name"`
	Reason  string    `json:"reason"`
	Type    string    `json:"type"`
	Message string    `json:"message"`
	Count   int32     `json:"count"`
}

type InventoryItem struct {
	SnapshotTs      time.Time         `json:"snapshot_ts"`
	Kind            string            `json:"kind"`
	NS              string            `json:"ns,omitempty"`
	Name            string            `json:"name"`
	UID             string            `json:"uid,omitempty"`
	Phase           string            `json:"phase,omitempty"`
	Node            string            `json:"node,omitempty"`
	ReplicasDesired int               `json:"replicas_desired,omitempty"`
	ReplicasReady   int               `json:"replicas_ready,omitempty"`
	Restarts        int               `json:"restarts,omitempty"`
	Image           string            `json:"image,omitempty"`
	SpecHash        string            `json:"spec_hash,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

type IngestAck struct {
	Accepted             bool      `json:"accepted"`
	ServerTime           time.Time `json:"server_time"`
	LatestAgentVersion   string    `json:"latest_agent_version"`
	RetentionDays        int       `json:"retention_days"`
	NextPushInS          int       `json:"next_push_in_s"`
	MetricsAccepted      int       `json:"metrics_accepted"`
	EventsAccepted       int       `json:"events_accepted"`
	InventoryAccepted    int       `json:"inventory_accepted"`
}

type Sender struct {
	cfg    *config.Config
	client *http.Client
}

func New(cfg *config.Config) *Sender {
	return &Sender{
		cfg: cfg,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// MetricsToPoints converts prom samples into the agent's wire format. The
// label map is subset to a small set of identifying keys per metric to keep
// the on-the-wire payload small.
func MetricsToPoints(now time.Time, samples []scraper.Sample) []MetricPoint {
	out := make([]MetricPoint, 0, len(samples))
	for _, s := range samples {
		ns := s.Labels["namespace"]
		obj := pickObjectName(s.Labels)
		kind := pickKind(s.Name, s.Labels)
		node := s.Labels["node"]
		labels := pickLabelsForMetric(s.Name, s.Labels)
		out = append(out, MetricPoint{
			Ts:     now,
			Name:   s.Name,
			NS:     ns,
			Kind:   kind,
			Obj:    obj,
			Node:   node,
			Value:  s.Value,
			Labels: labels,
		})
	}
	return out
}

func pickObjectName(l map[string]string) string {
	for _, k := range []string{"pod", "deployment", "statefulset", "daemonset", "node", "namespace", "ingress", "service", "job_name", "cronjob", "horizontalpodautoscaler", "persistentvolumeclaim", "persistentvolume"} {
		if v := l[k]; v != "" {
			return v
		}
	}
	return ""
}

func pickKind(name string, l map[string]string) string {
	switch {
	case startsWith(name, "kube_pod_"):
		return "Pod"
	case startsWith(name, "kube_deployment_"):
		return "Deployment"
	case startsWith(name, "kube_statefulset_"):
		return "StatefulSet"
	case startsWith(name, "kube_daemonset_"):
		return "DaemonSet"
	case startsWith(name, "kube_node_"):
		return "Node"
	case startsWith(name, "kube_namespace_"):
		return "Namespace"
	case startsWith(name, "kube_ingress_"):
		return "Ingress"
	case startsWith(name, "kube_service_"):
		return "Service"
	case startsWith(name, "kube_job_"):
		return "Job"
	case startsWith(name, "kube_cronjob_"):
		return "CronJob"
	case startsWith(name, "kube_horizontalpodautoscaler_"):
		return "HPA"
	case startsWith(name, "kube_persistentvolumeclaim_"):
		return "PersistentVolumeClaim"
	case startsWith(name, "kube_persistentvolume_"):
		return "PersistentVolume"
	}
	if v := l["kind"]; v != "" {
		return v
	}
	return ""
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// Keys we keep in the per-row label map. Everything else is dropped to avoid
// label-cardinality explosions on the server side.
var keepLabels = map[string]struct{}{
	"phase":           {},
	"condition":       {},
	"status":          {},
	"reason":          {},
	"resource":        {},
	"unit":            {},
	"created_by_kind": {},
	"image":           {},
	"container":       {},
	"job_name":        {},
	"role":            {},
	"type":            {},
	"verb":            {},
}

// Metrics where we keep the FULL label set — they're identifying info-style
// metrics, not high-cardinality samples.
var keepAllLabelsMetrics = map[string]struct{}{
	"kube_node_info":                 {},
	"kube_pod_info":                  {},
	"kube_pod_status_phase":          {},
	"kube_pod_status_ready":          {},
	"kube_deployment_status_replicas": {},
	"kube_deployment_spec_replicas":  {},
	"kube_node_status_condition":     {},
	"kube_node_status_capacity":      {},
	"kube_node_status_allocatable":   {},
	"kube_namespace_status_phase":    {},
	"kube_persistentvolumeclaim_status_phase": {},
	"kube_horizontalpodautoscaler_status_current_replicas": {},
	"kube_horizontalpodautoscaler_status_desired_replicas": {},
	"kube_horizontalpodautoscaler_spec_max_replicas":       {},
}

func pickLabelsForMetric(metric string, in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	if _, all := keepAllLabelsMetrics[metric]; all {
		// Cap to 24 entries / 128 chars per kv to bound payload size.
		out := make(map[string]string, len(in))
		count := 0
		for k, v := range in {
			if count >= 24 {
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
	out := make(map[string]string, 4)
	for k := range keepLabels {
		if v := in[k]; v != "" {
			if len(v) > 128 {
				v = v[:128]
			}
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EventsToEntries adapts the in-process events buffer to the wire format.
func EventsToEntries(in []events.Event) []EventEntry {
	out := make([]EventEntry, len(in))
	for i, e := range in {
		out[i] = EventEntry{
			Ts:      e.Ts,
			NS:      e.NS,
			Kind:    e.Kind,
			Name:    e.Name,
			Reason:  e.Reason,
			Type:    e.Type,
			Message: e.Message,
			Count:   e.Count,
		}
	}
	return out
}

func InventoryToItems(in []inventory.Item) []InventoryItem {
	out := make([]InventoryItem, len(in))
	for i, it := range in {
		out[i] = InventoryItem{
			SnapshotTs:      it.SnapshotTs,
			Kind:            it.Kind,
			NS:              it.NS,
			Name:            it.Name,
			UID:             it.UID,
			Phase:           it.Phase,
			Node:            it.Node,
			ReplicasDesired: it.ReplicasDesired,
			ReplicasReady:   it.ReplicasReady,
			Restarts:        it.Restarts,
			Image:           it.Image,
			SpecHash:        it.SpecHash,
			Labels:          it.Labels,
		}
	}
	return out
}

// Send signs and posts a payload. Returns the server ack on success.
func (s *Sender) Send(ctx context.Context, p *Payload) (*IngestAck, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var compressed bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&compressed, gzip.BestCompression)
	if _, err := gz.Write(raw); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	body := compressed.Bytes()
	if len(body) > s.cfg.MaxBatchBytes {
		return nil, fmt.Errorf("compressed payload %d exceeds max %d", len(body), s.cfg.MaxBatchBytes)
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := newNonce()
	sig := signBody(s.cfg.ClusterSecret, ts, nonce, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Cluster-Id", s.cfg.ClusterID)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)
	req.Header.Set("X-Agent-Version", s.cfg.AgentVersion)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ingest HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	var ack IngestAck
	if err := json.Unmarshal(respBody, &ack); err != nil {
		return nil, fmt.Errorf("decode ack: %w (body=%q)", err, string(respBody))
	}
	return &ack, nil
}

func newNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func signBody(secret, ts, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write([]byte(nonce))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
