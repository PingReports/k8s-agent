package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ClusterID     string
	ClusterSecret string
	Endpoint      string

	PushInterval     time.Duration
	ScrapeInterval   time.Duration
	BufferDir        string
	BufferRetention  time.Duration
	MaxBatchBytes    int

	KubeStateMetricsURL    string
	NodeExporterURL        string
	NodeExporterPodLabels  string
	NodeExporterPodPort    int
	NodeExporterPodPath    string
	NodeExporterPodScheme  string
	EventsEnabled          bool
	InventoryEnabled       bool

	AgentVersion  string
	AutoUpdate    bool
	OwnNamespace  string
	OwnDeployment string
}

func Load(version string) (*Config, error) {
	c := &Config{
		AgentVersion:        version,
		ClusterID:           os.Getenv("PR_CLUSTER_ID"),
		ClusterSecret:       os.Getenv("PR_CLUSTER_SECRET"),
		Endpoint:            envOr("PR_INGEST_ENDPOINT", "https://app.pingreports.com/api/v1/k8s/ingest"),
		BufferDir:           envOr("PR_BUFFER_DIR", "/var/lib/pr-k8s-agent"),
		KubeStateMetricsURL:   envOr("PR_KSM_URL", "http://kube-state-metrics.kube-system.svc:8080/metrics"),
		NodeExporterURL:       envOr("PR_NODE_EXPORTER_URL", ""),
		NodeExporterPodLabels: envOr("PR_NODE_EXPORTER_POD_LABELS", "app.kubernetes.io/name=prometheus-node-exporter"),
		NodeExporterPodPort:   envInt("PR_NODE_EXPORTER_POD_PORT", 9100),
		NodeExporterPodPath:   envOr("PR_NODE_EXPORTER_POD_PATH", "/metrics"),
		NodeExporterPodScheme: envOr("PR_NODE_EXPORTER_POD_SCHEME", "http"),
		OwnNamespace:        envOr("PR_OWN_NAMESPACE", "pingreports-agent"),
		OwnDeployment:       envOr("PR_OWN_DEPLOYMENT", "pr-k8s-agent"),
		MaxBatchBytes:       envInt("PR_MAX_BATCH_BYTES", 6*1024*1024),
		AutoUpdate:          envBool("PR_AUTO_UPDATE", true),
		EventsEnabled:       envBool("PR_EVENTS_ENABLED", true),
		InventoryEnabled:    envBool("PR_INVENTORY_ENABLED", true),
	}

	c.PushInterval = envDuration("PR_PUSH_INTERVAL", 5*time.Minute)
	c.ScrapeInterval = envDuration("PR_SCRAPE_INTERVAL", 60*time.Second)
	c.BufferRetention = envDuration("PR_BUFFER_RETENTION", 24*time.Hour)

	if c.ClusterID == "" || c.ClusterSecret == "" {
		return nil, fmt.Errorf("PR_CLUSTER_ID and PR_CLUSTER_SECRET are required")
	}
	return c, nil
}

func envOr(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func envBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "TRUE", "yes", "on":
		return true
	case "0", "false", "FALSE", "no", "off":
		return false
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
