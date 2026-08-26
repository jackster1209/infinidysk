# Usenet

Configure NNTP providers, connection budgets, cascade vs pooled routing, and queue-side NNTP pipelining.

!!! tip "Headless ENV"

    Each config key below maps to `NZBDAV_CONFIG__...` via the
    [naming algorithm](headless.md#naming-algorithm) (for example
    `usenet.providers` → `NZBDAV_CONFIG__USENET__PROVIDERS`).

## Providers

Add one or more accounts. Each provider supports:

| Control | What it does | Default / notes |
|---------|--------------|-----------------|
| Nickname | Friendly label instead of hostname | optional |
| Storage group | Same label → skip siblings after a clean article miss | optional; only same upstream |
| Host / Port | NNTP endpoint | port often `563` |
| Username / Password | Credentials | prefer SSL |
| Provider Connection Limit | Provider-wide ceiling for transfers and metadata combined | do not exceed the account allowance |
| Transfer Connections | Hard cap for concurrent `BODY` / `ARTICLE` work | blank = legacy shared pool |
| Metadata Capacity | Read-only base-to-burst range calculated from the two limits | shown when Transfer Connections is set |
| Pipeline depth | Per-provider override when pipelining on | blank = global `8` |
| Type | Disabled / Pool Connections / Backup Only | Pool |
| Use SSL | TLS for NNTP | on |
| Skip TLS certificate verification | Accept an invalid provider certificate | off |
| Data Cap | Block-account limit; auto-pauses near ~95% | uncapped |
| Already Used | Seed usage when migrating mid-block | empty |
| Auto-tune | Speed test → recommend Transfer Connections + pipelining | never changes Provider Connection Limit |

Persisted as `usenet.providers` JSON.

!!! warning "Cleartext"

    Disabling SSL stores/sends credentials in cleartext on the wire — only for trusted networks.

## Connection budgets [since 1.3.0](https://github.com/infinidysk/infinidysk/releases/tag/v1.3.0){ .nzbdav-since }

**Provider Connection Limit** is the absolute number of connections InfiniDysk may use for the
provider account. Set it no higher than the provider allows, or lower when the account is shared
with another client.

**Transfer Connections** limits article-body traffic independently. Once it is set, InfiniDysk
uses the rest of the provider budget for lightweight metadata commands such as `STAT`, `HEAD`,
and `DATE`. The editor previews **Metadata Capacity** from the configured values; after saving,
provider cards use the current effective provider limit when live runtime data is available. The
runtime range is calculated as follows:

```text
P = effective Provider Connection Limit
T = min(configured Transfer Connections, P)

base metadata = P - T
metadata burst = floor(T / 2)
metadata capacity = base metadata through base metadata + metadata burst
```

For example:

| Provider limit | Transfer connections | Metadata capacity |
|---------------:|---------------------:|------------------:|
| 50 | 20 | 30–40 |
| 50 | 50 | 0–25 |
| 40 | 16 | 24–32 |

Transfers never exceed their hard cap, and all work combined never exceeds the effective provider
limit. Metadata may borrow only the displayed burst allowance while transfer capacity is idle.
Waiting transfers reclaim borrowed capacity before metadata can burst again, but when metadata is
queued the scheduler always keeps at least one metadata operation moving. This prevents
`STAT`/`HEAD`/`DATE` starvation even when the transfer and provider limits are equal.

!!! info "Existing providers stay in legacy mode"

    A blank `MaxTransferConnections` value preserves the original shared-pool behavior. Merely
    opening or saving an existing provider does not enable budgeting. Enter **Transfer Connections**
    or apply an Auto-tune recommendation to opt in.

Auto-tune finds the transfer-throughput knee and applies its recommendation only to **Transfer
Connections**. It never rewrites **Provider Connection Limit**. If the provider later refuses its
configured ceiling, InfiniDysk lowers the effective runtime limit without changing either saved
value; provider cards and metrics then show capacities based on that learned limit.

## Invalid provider certificates [since 0.9.0](https://github.com/infinidysk/infinidysk/releases/tag/v0.9.0){ .nzbdav-since }

Leave **Skip TLS certificate verification** disabled unless a trusted provider has
a certificate it cannot correct. It keeps the NNTP connection encrypted but
accepts an untrusted, expired, or hostname-mismatched certificate. This permits
a man-in-the-middle attacker to impersonate the provider and read credentials.

## Routing and pipelining

| Control | Config key | Default | Effect |
|---------|------------|---------|--------|
| Verification routing [since 1.3.0](https://github.com/infinidysk/infinidysk/releases/tag/v1.3.0){ .nzbdav-since } | `usenet.verification-routing.enabled` | on | Provider order for background health checks and queue article validation. Follows your configured provider types and priorities, and additionally tries a provider later **within its own tier** while it is definitively missing most of what verification asks it for. Off = verification uses the same spare-capacity ordering as transfers. Does not affect BODY/ARTICLE routing |
| Enable cascade routing | `usenet.cascade.enabled` | off | Prefer providers in drag order; off = shared pool. Thinly-spared primaries (≤25% free) yield to idler peers; a larger Provider Connection Limit alone does not outrank priority. |
| Re-probe primary after miss | `usenet.cascade.retry-primary-on-miss` | on | After a clean 430/451 on the first batch attempt, try the primary once more before cascading (multi-node spool). Off = skip straight to backups. |
| Enable queue pipelining | `usenet.queue-pipelining.enabled` | off | Batch first-segment BODY during queue imports/benchmarks |
| Queue pipeline depth | `usenet.queue-pipelining.depth` | `8` | Requests in flight per connection (1–64) |

Legacy keys `usenet.pipelining.enabled` / `usenet.pipelining.depth` remain honored; env vars use `NZBDAV_CONFIG__USENET__QUEUE_PIPELINING__*` for the new names.

Run Auto-tune before enabling queue pipelining. WebDAV streaming batching is a **separate** toggle on [Streaming](streaming.md).

See [NNTP pipelining](../features/nntp-pipelining.md) and [Multi-provider](../features/multi-provider.md).

### How verification picks a provider

A STAT moves no article payload, so verification cannot cost you metered bytes the way a
BODY can — but asking a provider that does not hold the article still costs a full round
trip that returns nothing. Verification therefore keeps your configured order and only
defends against it:

- **Provider type is absolute.** Every eligible pooled provider is verified before any
  backup or block account, no matter how good the backup's retention is. A provider you
  configured as a backup stays a backup for health checks, and you do not have to set a
  data cap to make that stick — caps remain a separate, independent limit.
- **Success never promotes.** No provider is ever moved ahead of where you configured it
  because it has been answering well.
- **Sustained absence demotes, temporarily and locally.** A provider that definitively
  misses most of what verification recently asked it for is tried later among providers
  of its own type — never behind a lower tier. It stays in the walk and is still asked
  for whatever the providers before it left unresolved.
- **Transport problems never count.** Connection failures, timeouts, protocol errors and
  requests a dying connection never answered say nothing about whether a provider holds
  an article. Only definitive `exists` / missing (430) answers are counted. Provider
  availability is handled separately by the
  [circuit breaker](../features/multi-provider.md#circuit-breakers-and-storage-groups).

A demotion is not permanent. Fresh answers that improve restore the provider, and evidence
with nothing recent behind it fades after about six hours, so a provider that stopped being
asked — or whose retention improved — is reconsidered rather than stranded.

Current state per provider is exported for scraping; see
[Prometheus](../operations/prometheus.md).

## Warm connections [since 1.2.0](https://github.com/infinidysk/infinidysk/releases/tag/v1.2.0){ .nzbdav-since }

Each pooled provider keeps a small floor of pre-connected, authenticated NNTP sockets
ready so playback and queue work skip the connect/TLS/login handshake after idle
periods. Warm sockets count against the provider's connection limit but never hold
download permits. See [Connection warming](../features/connection-warming.md) for the
mechanics and the header indicator.

| Control | Config key | Default | Effect |
|---------|------------|---------|--------|
| Warm connections | `usenet.warm-connections.enabled` | on | Keep pre-connected sockets ready per provider |
| Warm floor | `usenet.warm-connections.floor` | auto | Idle sockets kept ready per provider; auto derives one sixth of Max Connections, clamped to 1–8 |

Changes take effect on the next provider save or restart — connection pools are not
rebuilt when these keys change alone.

## Article-miss negative cache [since 0.9.0](https://github.com/infinidysk/infinidysk/releases/tag/v0.9.0){ .nzbdav-since }

After a provider (or [storage group](../features/multi-provider.md)) reports a definitive article miss
(NNTP 430 or provider 451), InfiniDysk remembers that miss so later streaming/batch reads skip
re-probing the same provider for the same article until the TTL expires. Transient failures
(timeouts, network, corrupt articles) are never cached.

| Control | Config key | Default | Effect |
|---------|------------|---------|--------|
| Miss-cache TTL (seconds) | `usenet.article-miss-cache-ttl-seconds` | `300` | How long a miss stays cached (clamped 30–86400) |
| Miss-cache max entries | `usenet.article-miss-cache-max-entries` | `10000` | Cap before oldest entries are evicted (clamped 100–1000000) |

The cache clears automatically when Usenet providers are reconfigured.
