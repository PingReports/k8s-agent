package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingreports/k8s-agent/internal/buffer"
	"github.com/pingreports/k8s-agent/internal/config"
	"github.com/pingreports/k8s-agent/internal/events"
	"github.com/pingreports/k8s-agent/internal/inventory"
	"github.com/pingreports/k8s-agent/internal/scraper"
	"github.com/pingreports/k8s-agent/internal/selfupdate"
	"github.com/pingreports/k8s-agent/internal/sender"
)

// Set at link time via -ldflags '-X main.Version=...'.
var Version = "0.0.0-dev"

func main() {
	logFlag := flag.String("log-level", "info", "log level: debug|info|warn|error")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(*logFlag),
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load(Version)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}
	logger.Info("starting agent",
		"version", cfg.AgentVersion,
		"endpoint", cfg.Endpoint,
		"cluster_id", cfg.ClusterID,
		"push_interval", cfg.PushInterval.String(),
		"scrape_interval", cfg.ScrapeInterval.String(),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	startedAt := time.Now()

	buf, err := buffer.New(cfg.BufferDir, cfg.BufferRetention)
	if err != nil {
		logger.Error("init buffer", "err", err)
		os.Exit(1)
	}

	ksm := scraper.NewScraper(cfg.KubeStateMetricsURL, scraper.IsKnownPrefix)
	var nodeexp *scraper.Scraper
	if cfg.NodeExporterURL != "" {
		nodeexp = scraper.NewScraper(cfg.NodeExporterURL, scraper.IsKnownPrefix)
	}

	var evWatcher *events.Watcher
	if cfg.EventsEnabled {
		evWatcher, err = events.New(2000)
		if err != nil {
			logger.Warn("events watcher disabled", "err", err)
		} else {
			go func() {
				if err := evWatcher.Run(ctx); err != nil && err != context.Canceled {
					logger.Warn("events watcher exited", "err", err)
				}
			}()
		}
	}

	var inv *inventory.Collector
	if cfg.InventoryEnabled {
		inv, err = inventory.New()
		if err != nil {
			logger.Warn("inventory disabled", "err", err)
		}
	}

	var updater *selfupdate.Updater
	if cfg.AutoUpdate {
		updater, err = selfupdate.New(cfg.OwnNamespace, cfg.OwnDeployment)
		if err != nil {
			logger.Warn("self-update disabled", "err", err)
		}
	}

	send := sender.New(cfg)

	scrapeBuf := make([]sender.MetricPoint, 0, 4096)
	scrapeTick := time.NewTicker(cfg.ScrapeInterval)
	defer scrapeTick.Stop()
	pushTick := time.NewTicker(cfg.PushInterval)
	defer pushTick.Stop()
	pruneTick := time.NewTicker(15 * time.Minute)
	defer pruneTick.Stop()

	// Eager first scrape so the dashboard isn't blank for the first cycle.
	scrapeBuf = append(scrapeBuf, doScrape(ctx, ksm, nodeexp, logger)...)

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return
		case <-scrapeTick.C:
			scrapeBuf = append(scrapeBuf, doScrape(ctx, ksm, nodeexp, logger)...)
		case <-pruneTick.C:
			if err := buf.Prune(); err != nil {
				logger.Warn("prune buffer", "err", err)
			}
		case <-pushTick.C:
			doPush(ctx, send, buf, &scrapeBuf, evWatcher, inv, updater, cfg, startedAt, logger)
		}
	}
}

func doScrape(ctx context.Context, ksm, nodeexp *scraper.Scraper, logger *slog.Logger) []sender.MetricPoint {
	now := time.Now().UTC()
	out := make([]sender.MetricPoint, 0, 1024)
	if ksm != nil {
		samples, err := ksm.Scrape(ctx)
		if err != nil {
			logger.Warn("ksm scrape failed", "err", err)
		} else {
			out = append(out, sender.MetricsToPoints(now, samples)...)
		}
	}
	if nodeexp != nil {
		samples, err := nodeexp.Scrape(ctx)
		if err != nil {
			logger.Warn("node-exporter scrape failed", "err", err)
		} else {
			out = append(out, sender.MetricsToPoints(now, samples)...)
		}
	}
	return out
}

func doPush(
	ctx context.Context,
	send *sender.Sender,
	buf *buffer.FileBuffer,
	scrapeBuf *[]sender.MetricPoint,
	evWatcher *events.Watcher,
	inv *inventory.Collector,
	updater *selfupdate.Updater,
	cfg *config.Config,
	startedAt time.Time,
	logger *slog.Logger,
) {
	payload := &sender.Payload{
		Schema:       1,
		AgentVersion: cfg.AgentVersion,
		AgentUptimeS: int64(time.Since(startedAt).Seconds()),
		Metrics:      *scrapeBuf,
	}
	if evWatcher != nil {
		payload.Events = sender.EventsToEntries(evWatcher.Drain())
	}
	if inv != nil {
		snap, err := inv.Snapshot(ctx)
		if err != nil {
			logger.Warn("inventory snapshot failed", "err", err)
		} else {
			payload.Inventory = sender.InventoryToItems(snap.Items)
			payload.NodeCount = snap.NodeCount
			payload.PodCount = snap.PodCount
			payload.NamespaceCount = snap.NamespaceCount
		}
	}

	// Best-effort send. If the network or endpoint is sick, we spool to disk
	// and the next cycle replays oldest-first.
	ack, err := send.Send(ctx, payload)
	if err != nil {
		logger.Warn("ingest failed; spooling to disk", "err", err)
		if err := buf.Enqueue(payload); err != nil {
			logger.Error("buffer enqueue failed", "err", err)
		}
		// Reset the in-memory metrics buffer so the next cycle starts clean.
		*scrapeBuf = (*scrapeBuf)[:0]
		return
	}
	*scrapeBuf = (*scrapeBuf)[:0]
	logger.Info("ingest accepted",
		"metrics", ack.MetricsAccepted,
		"events", ack.EventsAccepted,
		"inventory", ack.InventoryAccepted,
		"latest_agent", ack.LatestAgentVersion,
	)

	// Replay any spooled batches now that the path is healthy. Bounded so we
	// don't burn an entire push window on backlog.
	replaySpool(ctx, send, buf, logger, 6)

	if updater != nil {
		if patched, err := updater.Apply(ctx, ack.LatestAgentVersion, cfg.AgentVersion); err != nil {
			logger.Warn("self-update apply failed", "err", err)
		} else if patched {
			logger.Info("self-update applied; rolling out", "to", ack.LatestAgentVersion)
		}
	}
}

func replaySpool(ctx context.Context, send *sender.Sender, buf *buffer.FileBuffer, logger *slog.Logger, maxBatches int) {
	paths, err := buf.ListOldest(maxBatches)
	if err != nil || len(paths) == 0 {
		return
	}
	for _, p := range paths {
		raw, err := buf.ReadAndRemove(p)
		if err != nil {
			logger.Warn("buffer read failed", "path", p, "err", err)
			continue
		}
		var spooled sender.Payload
		if err := json.Unmarshal(raw, &spooled); err != nil {
			logger.Warn("spool decode failed; dropping", "path", p, "err", err)
			continue
		}
		if _, err := send.Send(ctx, &spooled); err != nil {
			logger.Warn("replay failed; re-spooling", "path", p, "err", err)
			_ = buf.Enqueue(&spooled)
			return
		}
	}
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
