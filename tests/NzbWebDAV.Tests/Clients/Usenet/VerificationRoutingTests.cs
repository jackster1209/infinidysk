using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Config;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.Metrics;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

/// <summary>
/// Verification keeps the configured provider order and only defends against it: a provider
/// that definitively misses most of what verification asks is tried later among its own peers,
/// and no provider is ever tried earlier because it succeeds. Transfers keep the documented
/// pooled ordering regardless of what verification learned.
/// </summary>
public class VerificationRoutingTests
{
    [Fact]
    public async Task VerificationWalk_TriesAPersistentlyEmptyPeerLast()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder, holderType: ProviderType.Pooled);

        // Warm-up: both providers are pooled and equally prioritised, so the configured order
        // stands and every segment costs an attempt on the empty provider first.
        for (var i = 0; i < 150; i++)
            await StatAsVerificationAsync(client, $"warmup-{i}");

        var emptyAfterWarmup = empty.SingularRequests;
        var holderAfterWarmup = holder.SingularRequests;

        for (var i = 0; i < 40; i++)
            await StatAsVerificationAsync(client, $"measured-{i}");

        var emptyProbes = empty.SingularRequests - emptyAfterWarmup;
        var holderProbes = holder.SingularRequests - holderAfterWarmup;

        Assert.Equal(40, holderProbes);
        // Sustained definitive absence moves the empty provider behind its peer. It stays
        // eligible — the walk still reaches it when the peer leaves an id unresolved.
        Assert.True(
            emptyProbes < 10,
            $"expected the empty provider to be deprioritised, but it was probed {emptyProbes}/40 times");
    }

    [Fact]
    public async Task VerificationWalk_NeverPromotesABackupOverAPooledProvider()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder);

        // Teach verification the worst case for the tier boundary: the pooled provider holds
        // nothing being verified and the backup holds all of it.
        for (var i = 0; i < 150; i++)
            await StatAsVerificationAsync(client, $"teach-{i}");

        Assert.Equal(
            VerificationCoverageState.Deprioritized,
            client.VerificationCoverage.GetState("pooled-empty.example"));

        var emptyBefore = empty.SingularRequests;
        var holderBefore = holder.SingularRequests;
        for (var i = 0; i < 20; i++)
            await StatAsVerificationAsync(client, $"measured-{i}");

        // The pooled provider is demoted and still asked first: coverage evidence reorders a
        // provider inside its own tier and never across one. A backup that happens to have
        // excellent retention is still a backup.
        Assert.Equal(20, empty.SingularRequests - emptyBefore);
        Assert.Equal(20, holder.SingularRequests - holderBefore);
    }

    [Fact]
    public void VerificationOrder_RanksPooledThenBackupThenBlockAccount()
    {
        using var client = new MultiProviderNntpClient(
        [
            // Configured worst-first on purpose: the order below must come from the tier, not
            // from the order the provider list happens to be in.
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "backup-only.example", providerType: ProviderType.BackupOnly),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "backup-stats.example",
                providerType: ProviderType.BackupAndStats),
            MultiProviderNntpClientTests.CreateProvider(Missing(), host: "pooled.example"),
        ]);
        for (var i = 0; i < 150; i++)
        {
            client.VerificationCoverage.Record("pooled.example", exists: false);
            client.VerificationCoverage.Record("backup-stats.example", exists: true);
            client.VerificationCoverage.Record("backup-only.example", exists: true);
        }

        var order = VerificationOrder(client);

        // Both backups hold everything and the pooled provider is demoted, and the walk still
        // starts pooled. A block account stays last but stays in the walk — it is swept for
        // whatever the tiers above it left unresolved.
        Assert.Equal(
            ["pooled.example", "backup-stats.example", "backup-only.example"],
            order.Select(x => x.Host));
    }

    [Fact]
    public void VerificationOrder_KeepsConfiguredPriorityWhenCoverageMatches()
    {
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "second.example", priority: 1),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "first.example", priority: 0),
        ]);

        var order = VerificationOrder(client);

        // Nothing has been measured, so the operator's order is the whole answer.
        Assert.Equal(["first.example", "second.example"], order.Select(x => x.Host));
    }

    [Fact]
    public void VerificationOrder_PrefersNormalCoverageOverPriorityWithinATier()
    {
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "empty-primary.example", priority: 0),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "stocked-primary.example", priority: 1),
        ]);
        Deprioritize(client, "empty-primary.example");

        var order = VerificationOrder(client);

        // The limited authority coverage evidence has: spending every file's first phase on a
        // provider that is currently empty costs a whole extra phase per file, and both
        // providers are in the same tier, so nothing about the topology changes.
        Assert.Equal(
            ["stocked-primary.example", "empty-primary.example"],
            order.Select(x => x.Host));
    }

    [Fact]
    public void VerificationOrder_RanksNormalBackupAheadOfDeprioritizedBackup()
    {
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "empty-backup.example",
                providerType: ProviderType.BackupAndStats, priority: 0),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "stocked-backup.example",
                providerType: ProviderType.BackupAndStats, priority: 1),
            MultiProviderNntpClientTests.CreateProvider(Missing(), host: "pooled.example"),
        ]);
        Deprioritize(client, "empty-backup.example");

        var order = VerificationOrder(client);

        // Demotion works the same inside the backup tier, and moves nothing across it.
        Assert.Equal(
            ["pooled.example", "stocked-backup.example", "empty-backup.example"],
            order.Select(x => x.Host));
    }

    [Fact]
    public void VerificationOrder_SortsHalfOpenBehindHealthyPeersButKeepsItsTier()
    {
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "recovering.example",
                circuitBreaker: HalfOpenBreaker("recovering.example")),
            MultiProviderNntpClientTests.CreateProvider(Missing(), host: "healthy.example"),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "backup.example", providerType: ProviderType.BackupOnly),
        ]);

        var order = VerificationOrder(client);

        // A provider that may still be down should not stall verification a healthy peer
        // would serve, but it is still a primary: circuit recovery does not demote a tier.
        Assert.Equal(
            ["healthy.example", "recovering.example", "backup.example"],
            order.Select(x => x.Host));
    }

    [Fact]
    public void VerificationOrder_OmitsDisabledAndOverLimitProviders()
    {
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(
                    Missing(), host: "disabled.example",
                    providerType: ProviderType.Disabled),
                MultiProviderNntpClientTests.CreateProvider(
                    Missing(), host: "capped.example",
                    byteLimit: 1_000, bytesUsedOffset: 1_000),
                MultiProviderNntpClientTests.CreateProvider(Missing(), host: "eligible.example"),
            ],
            bytesTracker: new ProviderBytesTracker());

        var order = VerificationOrder(client);

        // A health sweep must never schedule a provider it cannot actually STAT: those ids
        // would be reported resolved without anything having asked.
        var provider = Assert.Single(order);
        Assert.Equal("eligible.example", provider.Host);
    }

    [Fact]
    public void VerificationOrder_CollapsesStorageGroupSiblingsAfterOrdering()
    {
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "empty-sibling.example", storageGroup: "Omicron", priority: 0),
            MultiProviderNntpClientTests.CreateProvider(
                Missing(), host: "stocked-sibling.example", storageGroup: "Omicron", priority: 1),
        ]);
        Deprioritize(client, "empty-sibling.example");

        var order = VerificationOrder(client);

        // Siblings share upstream storage, so only one is swept — and because collapsing
        // happens after ordering, the survivor is the one routing would have asked first.
        var provider = Assert.Single(order);
        Assert.Equal("stocked-sibling.example", provider.Host);
    }

    [Fact]
    public async Task TransferWalk_KeepsPooledOrderingRegardlessOfVerificationHistory()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder);

        // Teach verification that the backup holds everything.
        for (var i = 0; i < 100; i++)
            await StatAsVerificationAsync(client, $"teach-{i}");

        var emptyBefore = empty.SingularRequests;

        // A transfer must still try the pooled provider first: BODY moves metered bytes, so
        // the tier ordering it depends on is deliberately untouched.
        await client.DecodedBodyAsync("transfer-segment", CancellationToken.None);

        Assert.True(
            empty.SingularRequests > emptyBefore,
            "the pooled provider should still be tried first for BODY");
    }

    [Fact]
    public async Task BodyWithHealthAdmissionContext_KeepsPooledOrdering()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder);

        for (var i = 0; i < 100; i++)
            await StatAsVerificationAsync(client, $"teach-context-{i}");

        var emptyBefore = empty.SingularRequests;
        using var cts = ContextualCancellationTokenSource.CreateLinkedTokenSource(
            CancellationToken.None);
        using var gate = new HealthCheckConnectionGate(new ConfigManager());
        using var scope = cts.Token.SetContext(new HealthCheckAdmissionContext(
            gate,
            HealthCheckAdmissionPriority.Background));

        await client.DecodedBodyAsync("container-probe", cts.Token);

        Assert.True(
            empty.SingularRequests > emptyBefore,
            "a contextual BODY must retain pooled-first transfer routing");
    }

    [Fact]
    public async Task VerificationRoutingDisabled_KeepsTransferOrdering()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder, verificationRouting: false);

        for (var i = 0; i < 100; i++)
            await StatAsVerificationAsync(client, $"segment-{i}");

        // With the setting off, every verification still pays the pooled-first walk.
        Assert.Equal(100, empty.SingularRequests);
    }

    [Fact]
    public async Task StatWithoutVerificationContext_KeepsTransferOrdering()
    {
        var empty = Missing();
        var holder = Exists();
        using var client = Client(empty, holder);

        // A STAT outside health-check admission is not bulk verification and must not be
        // re-ranked; only the two verification callers carry that context.
        for (var i = 0; i < 100; i++)
            await client.StatAsync($"segment-{i}", CancellationToken.None);

        Assert.Equal(100, empty.SingularRequests);
    }

    /// <summary>Feeds enough sustained definitive absence to demote one provider.</summary>
    private static void Deprioritize(MultiProviderNntpClient client, string providerKey)
    {
        for (var i = 0; i < 150; i++) client.VerificationCoverage.Record(providerKey, exists: false);
        Assert.Equal(
            VerificationCoverageState.Deprioritized,
            client.VerificationCoverage.GetState(providerKey));
    }

    private static ProviderCircuitBreaker HalfOpenBreaker(string host)
    {
        var breaker = new ProviderCircuitBreaker(host);
        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.ExpireCooldownForTests();
        return breaker;
    }

    private static IReadOnlyList<VerificationProvider> VerificationOrder(
        MultiProviderNntpClient client)
    {
        // Ordering is verification-aware only under health-check admission, exactly as
        // HealthCheckService snapshots it.
        using var cts = ContextualCancellationTokenSource.CreateLinkedTokenSource(
            CancellationToken.None);
        using var gate = new HealthCheckConnectionGate(new ConfigManager());
        using var scope = cts.Token.SetContext(new HealthCheckAdmissionContext(
            gate,
            HealthCheckAdmissionPriority.Background));
        return client.GetVerificationProviderOrder(cts.Token);
    }

    private static async Task StatAsVerificationAsync(MultiProviderNntpClient client, string segmentId)
    {
        using var cts = ContextualCancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        using var scope = cts.Token.SetContext(new HealthCheckAdmissionContext(
            new HealthCheckConnectionGate(new ConfigManager()),
            HealthCheckAdmissionPriority.Background));
        await client.StatAsync(segmentId, cts.Token);
    }

    private static MultiProviderNntpClientTests.ScriptedNntpClient Exists() => new()
    {
        BatchResponseCode = 223,
        SingularResponseCode = (int)UsenetResponseType.ArticleExists,
    };

    private static MultiProviderNntpClientTests.ScriptedNntpClient Missing() => new()
    {
        BatchResponseCode = 430,
        SingularResponseCode = (int)UsenetResponseType.NoArticleWithThatMessageId,
    };

    private static MultiProviderNntpClient Client(
        MultiProviderNntpClientTests.ScriptedNntpClient pooledEmpty,
        MultiProviderNntpClientTests.ScriptedNntpClient holder,
        bool verificationRouting = true,
        ProviderType holderType = ProviderType.BackupOnly) => new(
        [
            MultiProviderNntpClientTests.CreateProvider(
                pooledEmpty, host: "pooled-empty.example", maxConnections: 4),
            MultiProviderNntpClientTests.CreateProvider(
                holder,
                host: holderType == ProviderType.Pooled
                    ? "pooled-holder.example"
                    : "backup-holder.example",
                providerType: holderType,
                maxConnections: 4),
        ],
        verificationRoutingEnabled: () => verificationRouting);
}
