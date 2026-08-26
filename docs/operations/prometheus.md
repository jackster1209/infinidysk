# Prometheus metrics [since 1.2.0](https://github.com/infinidysk/infinidysk/releases/tag/v1.2.0){ .nzbdav-since }

InfiniDysk exposes Prometheus metrics at `/metrics`. The endpoint includes standard
.NET process/runtime metrics plus `nzbdav_` metrics for active streaming, seek
latency, NNTP provider pools, circuit breakers, article outcomes, PAR2 repair,
streaming-confirmed corrupt articles, and internal metrics-pipeline health.

Metric labels are deliberately bounded. Provider metrics use the configured provider
identity; other labels are fixed enums such as `region`, `kind`, `state`, and
`status`. Paths, release names, filenames, client addresses, and article IDs are
never exported as labels.

## Authentication

By default, direct backend scrapes are anonymous. Set
`METRICS_REQUIRE_API_KEY=true` to require the normal `x-api-key` header for direct
scrapes. Requests through the frontend `/metrics` proxy always require an
authenticated InfiniDysk UI session and automatically receive the internal key.

Do not publish the backend port to untrusted networks when direct scraping is
anonymous.

## Verification (STAT) metrics [since 1.3.0](https://github.com/infinidysk/infinidysk/releases/tag/v1.3.0){ .nzbdav-since }

Background health checks and queue article validation resolve every segment with STAT.
The `nzbdav_segment_fetches_total` and `nzbdav_segment_fetch_duration_seconds` families
describe *transfers* and deliberately record nothing for a successful STAT, so they cannot
be used to reason about verification. These families do:

| Metric | Labels | Meaning |
|--------|--------|---------|
| `nzbdav_nntp_stat_attempts_total` | `provider_key`, `result` | STAT attempts per provider. `result` is `exists`, `missing`, or `error` |
| `nzbdav_nntp_stat_duration_seconds` | `provider_key`, `result` | Duration of each STAT attempt |
| `nzbdav_nntp_stat_walk_depth` | `outcome` | Providers asked before a segment resolved. `outcome` is `exists`, `missing`, or `error` |

`nzbdav_nntp_stat_walk_depth` answers the two questions that matter for verification cost:

- **Mean walk depth** — `_sum / _count`. A value near 1 means the first provider tried
  usually holds the article; a high value means most segments are paying several serial
  round trips before one answers.
- **First-provider hit rate** — the `le="1"` bucket over `_count`. Verification follows your
  configured provider order rather than measured retention, so this can be low even when
  every provider is healthy — it means the provider you put first genuinely does not hold
  much of what is being checked.

Walk depth is recorded only on the per-STAT provider walk, which is the path that
actually walks providers one at a time. Per-provider verification sweeps
(`SweepProviderPipelinedAsync`) do record `nzbdav_nntp_stat_attempts_total` and
`nzbdav_nntp_stat_duration_seconds` for every result, so attempt and latency figures
cover both paths — but a sweep asks one provider per phase and has no walk to measure,
so it contributes nothing to `nzbdav_nntp_stat_walk_depth`. Mean walk depth and
first-provider hit rate therefore describe the per-STAT walk alone. The primary-only
pipelined path used by queue imports (`StatsPipelinedAsync`) still records nothing.

### Verification coverage

Verification keeps your configured provider order and only defends against it: a provider
that definitively misses most of what verification recently asked it for is tried later
*within its own tier* until fresh answers improve. These families show which providers that
is currently happening to, and the evidence behind it.

| Metric | Labels | Meaning |
|--------|--------|---------|
| `nzbdav_nntp_verification_coverage_state` | `provider_key` | `0` = normal, `1` = deprioritized |
| `nzbdav_nntp_verification_coverage_samples` | `provider_key` | Definitive STAT answers observed. Transport errors are excluded |
| `nzbdav_nntp_verification_coverage_miss_rate` | `provider_key` | Recency-weighted share of those answers that were definitive misses |
| `nzbdav_nntp_verification_coverage_deprioritizations_total` | `provider_key` | Times the provider has been deprioritized since process start |

The miss rate is a recency-weighted rate, not a count divided by `samples`: recent answers
carry more weight than old ones, and evidence with nothing recent behind it fades. A state
of `1` alongside a rate that has since fallen means the provider is on its way back.

`deprioritizations_total` is the number to watch when tuning: a provider that keeps crossing
back and forth is a sign the thresholds do not suit the deployment, which the current state
alone cannot show. State is held in memory only — every provider starts normal after a
restart, so a demotion never outlives the process that observed it.

## Provider connection budgets

Operation-aware providers expose their live budget through the existing bounded-label
NNTP gauge families:

| Metric | Label | Values |
|--------|-------|--------|
| `nzbdav_nntp_pool_connections` | `state` | `transfer_active`, `metadata_active`, `transfer_waiting`, `metadata_waiting` |
| `nzbdav_nntp_pool_max_connections` | `limit` | `transfer_configured`, `transfer_effective`, `metadata_base`, `metadata_burst`, `metadata_max` |

Both metric families also carry `provider_key`, the provider's stable normalized identifier.
The existing pool states (`live`, `idle`, `active`, `available`, and `pending`) and limits
(`configured`, `effective`, and optional `learned`) remain available alongside the budget labels.

Providers with no configured `MaxTransferConnections` remain in legacy shared-pool mode and do
not export the transfer/metadata label values. Their absence means “budgeting disabled,” not zero
capacity.

For example, these queries compare current operation admission with its effective limits:

```promql
nzbdav_nntp_pool_connections{state=~"transfer_active|metadata_active"}
```

```promql
nzbdav_nntp_pool_max_connections{limit=~"transfer_effective|metadata_max"}
```

## Provider-aware health scheduling

Background health verification is scheduled per provider, so aggregate counters alone cannot
tell a saturated provider apart from an idle one that no session currently targets.

| Metric | Label | Values |
|--------|-------|--------|
| `nzbdav_health_check_scheduler_provider` | `state` | `active_assignments`, `runnable_sessions`, `pending_segments`, `blocked_sessions`, `legacy_shared_pool` |
| `nzbdav_health_check_scheduler_global_blocked_sessions` | — | Runnable sessions held back by the explicit aggregate ceiling |
| `nzbdav_health_check_scheduler_legacy_assignments` | — | Active assignments backed by a legacy shared-pool permit |
| `nzbdav_nntp_effective_stat_pipeline_depth` | `provider_key` | UsenetSharp `MaxPipelineDepth` that health STAT sweeps actually run at |

`nzbdav_health_check_scheduler_provider` also carries `provider_key`. No run ID, DAV item ID,
filename, or message ID is ever used as a label.

`active_assignments` counts work that already owns executable provider admission, so an
assignment is never reported active while it waits for capacity. Comparing it against that
provider's `metadata_max` shows whether health work is saturating the provider:

```promql
nzbdav_health_check_scheduler_provider{state="active_assignments"}
  / on(provider_key) nzbdav_nntp_pool_max_connections{limit="metadata_max"}
```

`nzbdav_nntp_effective_stat_pipeline_depth` reports the depth STAT genuinely runs at, which is
**not** the provider's configured pipelining depth. That setting is BODY/queue oriented:
`StatsPipelinedAsync` discards its depth argument and lets UsenetSharp window STAT at the
`MaxPipelineDepth` the physical connection was built with. InfiniDysk currently leaves that
unset, so the reported value is the UsenetSharp default (64) even for a provider configured at
depth 16. Treat the two numbers as independent until they are deliberately wired together.

`blocked_sessions` and `global_blocked_sessions` separate the two reasons work is waiting —
the provider cannot admit more, or the explicit ceiling is full. Under Auto the second is
always zero.

Scheduler-created metadata waiters should stay near zero, since the scheduler holds pending
work itself rather than queuing it inside provider admission:

```promql
nzbdav_nntp_pool_connections{state="metadata_waiting"}
```

Non-scheduler callers may still legitimately wait there, so treat a persistently high value as
a signal to check `blocked_sessions` rather than as a failure on its own.

`nzbdav_health_check_scheduler_capacity` and `nzbdav_health_check_gate_limit` report `0` in
Auto mode. An explicit ceiling is always at least 1, so 0 unambiguously means "no ceiling".

## Prometheus configuration

For the normal frontend endpoint:

```yaml
scrape_configs:
  - job_name: infinidysk
    static_configs:
      - targets: ["infinidysk:3000"]
```

On the same Docker network, scrape the backend directly on port 8080. When
`METRICS_REQUIRE_API_KEY=true`, Prometheus 3 can send the API key:

```yaml
scrape_configs:
  - job_name: infinidysk-backend
    static_configs:
      - targets: ["infinidysk:8080"]
    http_headers:
      x-api-key:
        values: ["replace-with-your-api-key"]
```
