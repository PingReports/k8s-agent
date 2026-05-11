# PingReports k8s-agent

Push-only Kubernetes monitoring agent for [PingReports](https://app.pingreports.com).
Runs inside your cluster, scrapes kube-state-metrics + node-exporter, watches
Events, and ships compressed batches to PingReports every 5 minutes.

## Install

```sh
helm upgrade --install pr-k8s-agent \
  oci://ghcr.io/pingreports/charts/k8s-agent \
  --version 0.1.0 \
  --namespace pingreports-agent --create-namespace \
  --set cluster.id=<cluster-uuid> \
  --set cluster.secret=<cluster-secret>
```

Get `cluster.id` + `cluster.secret` from the PingReports UI: **Kubernetes →
Register cluster**.

## Prerequisites

The agent needs metric data. The chart can install the optional
dependencies for you, or you can point at existing services:

```yaml
metrics:
  ksmEndpoint: http://kube-state-metrics.kube-system.svc:8080/metrics
  nodeExporterEndpoint: http://node-exporter.kube-system.svc:9100/metrics
```

If you don't already run `kube-state-metrics`, install
[the upstream chart](https://github.com/prometheus-community/helm-charts).

## What it ships

- kube-state-metrics samples (kube_pod_*, kube_deployment_*, etc.)
- node-exporter samples (CPU, memory, disk, network)
- Kubernetes Events filtered to operationally meaningful reasons
  (OOMKilled, FailedScheduling, Evicted, NodeNotReady, …)
- Inventory snapshots (Pods, Deployments, StatefulSets, DaemonSets, Nodes,
  Namespaces, PVCs)

## What it does NOT ship

- Secrets — the ServiceAccount has no `get` on `secrets`.
- Env-var values from pod specs.
- ConfigMap values.
- Anything outside the scrape allowlist.

You can confirm with:

```sh
kubectl auth can-i --as=system:serviceaccount:pingreports-agent:pr-k8s-agent get secrets
# expected: no
```

## Self-update

When PingReports publishes a new agent version, the running agent patches
its own Deployment to the new image tag. RBAC limits this to the agent's
own Deployment — no other workload can be touched. Disable with
`--set agent.autoUpdate=false`.

## Local dev

```sh
go build ./cmd/agent
PR_CLUSTER_ID=... PR_CLUSTER_SECRET=... ./agent --log-level=debug
```

## License

Apache-2.0
