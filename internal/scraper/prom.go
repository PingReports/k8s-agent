package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
)

type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Scrape pulls a Prometheus text-format metrics endpoint and decodes the
// samples. Bounded by ctx; the HTTP client is reused across calls.
type Scraper struct {
	URL    string
	Client *http.Client
	Filter func(name string) bool
}

func NewScraper(url string, filter func(string) bool) *Scraper {
	return &Scraper{
		URL:    url,
		Client: &http.Client{Timeout: 30 * time.Second},
		Filter: filter,
	}
}

func (s *Scraper) Scrape(ctx context.Context) ([]Sample, error) {
	if s.URL == "" {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("scrape %s: HTTP %d", s.URL, resp.StatusCode)
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}
	samples := make([]Sample, 0, 1024)
	for name, fam := range families {
		if s.Filter != nil && !s.Filter(name) {
			continue
		}
		for _, m := range fam.Metric {
			labels := make(map[string]string, len(m.Label))
			for _, lp := range m.Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			var v float64
			switch {
			case m.Gauge != nil:
				v = m.Gauge.GetValue()
			case m.Counter != nil:
				v = m.Counter.GetValue()
			case m.Untyped != nil:
				v = m.Untyped.GetValue()
			case m.Summary != nil:
				v = m.Summary.GetSampleSum()
			case m.Histogram != nil:
				v = m.Histogram.GetSampleSum()
			default:
				continue
			}
			samples = append(samples, Sample{
				Name:   name,
				Labels: labels,
				Value:  v,
			})
		}
	}
	return samples, nil
}

// IsKnownPrefix returns true for the metric prefixes we want to ship by default.
// Keeps cardinality bounded and avoids leaking unknown metrics.
func IsKnownPrefix(name string) bool {
	for _, p := range knownPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

var knownPrefixes = []string{
	"kube_pod_",
	"kube_deployment_",
	"kube_replicaset_",
	"kube_statefulset_",
	"kube_daemonset_",
	"kube_node_",
	"kube_namespace_",
	"kube_persistentvolume_",
	"kube_persistentvolumeclaim_",
	"kube_service_",
	"kube_ingress_",
	"kube_job_",
	"kube_cronjob_",
	"kube_horizontalpodautoscaler_",
	"kube_poddisruptionbudget_",
	"kube_resourcequota_",
	"kube_endpoint_",
	// node-exporter
	"node_cpu_seconds_total",
	"node_memory_MemAvailable_bytes",
	"node_memory_MemTotal_bytes",
	"node_filesystem_avail_bytes",
	"node_filesystem_size_bytes",
	"node_load1",
	"node_load5",
	"node_load15",
	"node_network_receive_bytes_total",
	"node_network_transmit_bytes_total",
	"node_disk_io_time_seconds_total",
}
