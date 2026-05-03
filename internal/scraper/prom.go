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

// ScrapeAt is identical to Scrape but uses an arbitrary URL — used by the
// per-pod discovery path so the same parser/filter pipeline is shared.
func (s *Scraper) ScrapeAt(ctx context.Context, url string) ([]Sample, error) {
	if url == "" {
		return nil, nil
	}
	prev := s.URL
	s.URL = url
	defer func() { s.URL = prev }()
	return s.Scrape(ctx)
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

// We ship the full kube-state-metrics surface plus every node-exporter
// metric that has operational signal. Cardinality is bounded by the
// per-batch row caps on the server side; "ship everything we can interpret"
// is the design choice.
// Wildcard accept matchers — every metric whose name has one of these
// prefixes is shipped. Per-prefix gating keeps cardinality bounded.
var knownPrefixes = []string{
	// Control-plane (when wired up)
	"apiserver_",
	"etcd_",
	"workqueue_",
	"rest_client_",
	"kube_apiserver_",
	"kube_controller_manager_",
	"scheduler_",
	"cluster_autoscaler_",
	// CNIs
	"felix_",
	"calico_",
	"cilium_",
	"hubble_",
	"weave_",
	"flannel_",
	// Ingress controllers
	"nginx_ingress_controller_",
	"traefik_",
	"controller_request_",
	// kube-state-metrics — every workload kind + cluster-level resource
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
	"kube_endpoint_",
	"kube_job_",
	"kube_cronjob_",
	"kube_horizontalpodautoscaler_",
	"kube_poddisruptionbudget_",
	"kube_resourcequota_",
	"kube_limitrange",
	"kube_storageclass_",
	"kube_networkpolicy_",
	"kube_lease_",
	"kube_configmap_info",
	"kube_secret_info", // METADATA ONLY (count, age) — never .data
	"kube_state_metrics_",

	// node-exporter — CPU
	"node_cpu_seconds_total",
	"node_cpu_frequency_hertz",
	"node_cpu_scaling_frequency_hertz",
	// memory
	"node_memory_MemTotal_bytes",
	"node_memory_MemAvailable_bytes",
	"node_memory_MemFree_bytes",
	"node_memory_Buffers_bytes",
	"node_memory_Cached_bytes",
	"node_memory_SwapTotal_bytes",
	"node_memory_SwapFree_bytes",
	"node_memory_Active_bytes",
	"node_memory_Inactive_bytes",
	"node_memory_Dirty_bytes",
	"node_memory_Slab_bytes",
	"node_memory_AnonPages_bytes",
	"node_memory_PageTables_bytes",
	"node_memory_HugePages_Total",
	// load + processes
	"node_load1",
	"node_load5",
	"node_load15",
	"node_procs_running",
	"node_procs_blocked",
	"node_processes_state",
	"node_processes_threads",
	"node_processes_max_threads",
	"node_context_switches_total",
	"node_intr_total",
	"node_forks_total",
	"node_uname_info",
	"node_boot_time_seconds",
	"node_time_seconds",
	"node_timex_offset_seconds",
	// filesystem
	"node_filesystem_size_bytes",
	"node_filesystem_avail_bytes",
	"node_filesystem_free_bytes",
	"node_filesystem_files",
	"node_filesystem_files_free",
	"node_filesystem_readonly",
	"node_filesystem_device_error",
	// disk I/O
	"node_disk_read_bytes_total",
	"node_disk_written_bytes_total",
	"node_disk_reads_completed_total",
	"node_disk_writes_completed_total",
	"node_disk_io_time_seconds_total",
	"node_disk_io_time_weighted_seconds_total",
	"node_disk_read_time_seconds_total",
	"node_disk_write_time_seconds_total",
	"node_disk_io_now",
	"node_disk_discard_time_seconds_total",
	// network
	"node_network_receive_bytes_total",
	"node_network_transmit_bytes_total",
	"node_network_receive_packets_total",
	"node_network_transmit_packets_total",
	"node_network_receive_errs_total",
	"node_network_transmit_errs_total",
	"node_network_receive_drop_total",
	"node_network_transmit_drop_total",
	"node_network_up",
	"node_network_speed_bytes",
	"node_network_mtu_bytes",
	// netstat / sockets
	"node_netstat_Tcp_CurrEstab",
	"node_netstat_Tcp_ActiveOpens",
	"node_netstat_Tcp_PassiveOpens",
	"node_netstat_Tcp_RetransSegs",
	"node_netstat_Tcp_OutSegs",
	"node_netstat_Tcp_InSegs",
	"node_netstat_TcpExt_TCPSynRetrans",
	"node_netstat_Udp_InDatagrams",
	"node_netstat_Udp_OutDatagrams",
	"node_sockstat_TCP_inuse",
	"node_sockstat_UDP_inuse",
	"node_sockstat_TCP_tw",
	"node_sockstat_sockets_used",
	// fd / pressure / vmstat highlights
	"node_filefd_allocated",
	"node_filefd_maximum",
	"node_pressure_cpu_waiting_seconds_total",
	"node_pressure_io_waiting_seconds_total",
	"node_pressure_io_stalled_seconds_total",
	"node_pressure_memory_waiting_seconds_total",
	"node_pressure_memory_stalled_seconds_total",
	"node_vmstat_pgpgin",
	"node_vmstat_pgpgout",
	"node_vmstat_pswpin",
	"node_vmstat_pswpout",
	"node_vmstat_oom_kill",
	// hardware sensors — useful when present, harmless when absent
	"node_hwmon_temp_celsius",
	"node_thermal_zone_temp",
	"node_power_supply_online",
	// node-exporter scrape self-metrics (telemetry)
	"node_scrape_collector_duration_seconds",
	"node_exporter_build_info",

	// kubelet cAdvisor (only present when scraped via /metrics/cadvisor — ignored otherwise)
	"container_cpu_usage_seconds_total",
	"container_cpu_cfs_throttled_seconds_total",
	"container_memory_working_set_bytes",
	"container_memory_rss",
	"container_memory_cache",
	"container_network_receive_bytes_total",
	"container_network_transmit_bytes_total",
	"container_fs_reads_bytes_total",
	"container_fs_writes_bytes_total",
}
