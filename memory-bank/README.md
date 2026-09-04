# Memory Bank

Shared, committed context for anyone (human or agent) working on this fork.
Read the file that matches your task.

| File | Contents |
|------|----------|
| [01-overview.md](01-overview.md) | Fork point, branching, project layout, server config keys, GCS integration |
| [02-build-and-test.md](02-build-and-test.md) | Build/test commands, Scala versions, style rules, per-task checklist |
| [03-virtana-changes.md](03-virtana-changes.md) | Divergence from upstream: telemetry package, modified files, merge risks |
| [04-deployment.md](04-deployment.md) | Kustomize overlays, make targets, GCS environments, env vars, CI |
| [05-gcp-egress-pricing.md](05-gcp-egress-pricing.md) | GCP egress pricing tiers and how classification is implemented |
| [06-egress-monitoring.md](06-egress-monitoring.md) | Per-share egress monitoring: tier resolution, config, Delta writer behaviour, log formats |
| [07-access-log-table-reference.md](07-access-log-table-reference.md) | `access_log_br__system` table reference: schema, locations, example queries, retention |
| [08-query-performance-metrics.md](08-query-performance-metrics.md) | **Proposal** — query taxonomy, per-stage latency boundaries, proposed metric catalog, SLOs/alerts (ZING-45093) |

Related: [../AGENTS.md](../AGENTS.md) (agent instructions), [../CHANGELOG-VIRTANA.md](../CHANGELOG-VIRTANA.md)
(authoritative divergence log).

`dev/` is local scratch tooling and is intentionally not documented here.
