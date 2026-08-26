using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class ProviderConnectionSnapshotTests
{
    [Fact]
    public void LegacyProviderSnapshotHasNoAdmissionState()
    {
        using var client = CreateMultiProvider(maxTransferConnections: null);

        var snapshot = Assert.Single(client.GetProviderConnectionSnapshots());

        Assert.Equal(4, snapshot.ConfiguredMaxConnections);
        Assert.Equal(4, snapshot.EffectiveMaxConnections);
        Assert.Null(snapshot.Admission);
    }

    [Fact]
    public void BudgetedProviderSnapshotIncludesEffectiveAdmissionLimits()
    {
        using var client = CreateMultiProvider(maxTransferConnections: 2);

        var snapshot = Assert.Single(client.GetProviderConnectionSnapshots());
        var admission = Assert.IsType<ProviderConnectionAdmissionSnapshot>(snapshot.Admission);

        Assert.Equal(4, snapshot.ConfiguredMaxConnections);
        Assert.Equal(4, snapshot.EffectiveMaxConnections);
        Assert.Equal(2, admission.ConfiguredTransferLimit);
        Assert.Equal(2, admission.EffectiveTransferLimit);
        Assert.Equal(2, admission.BaseMetadataCapacity);
        Assert.Equal(1, admission.MetadataBurstAllowance);
        Assert.Equal(3, admission.MaxMetadataCapacity);
    }

    [Fact]
    public void ProviderSnapshotReportsVerificationCoverage()
    {
        using var client = CreateMultiProvider(maxTransferConnections: null);
        for (var i = 0; i < 150; i++)
            client.VerificationCoverage.Record("snapshot-test", exists: false);

        var snapshot = Assert.Single(client.GetProviderConnectionSnapshots());
        var coverage = Assert.IsType<VerificationCoverageSnapshot>(snapshot.VerificationCoverage);

        // Operators need the evidence, not just the verdict: a demotion nobody can explain is
        // indistinguishable from a bug in the thresholds.
        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.State);
        Assert.Equal(150, coverage.Samples);
        Assert.InRange(coverage.MissRate, 0.85, 1d);
        Assert.Equal(1, coverage.Deprioritizations);
        Assert.NotNull(coverage.LastTransitionUtc);
    }

    private static MultiProviderNntpClient CreateMultiProvider(int? maxTransferConnections)
    {
        var pool = new ConnectionPool<INntpClient>(
            maxConnections: 4,
            _ => throw new InvalidOperationException("Snapshot tests do not acquire connections."));
        var provider = new MultiConnectionNntpClient(
            pool,
            ProviderType.Pooled,
            new ProviderCircuitBreaker("snapshot-test"),
            "snapshot-test",
            maxTransferConnections: maxTransferConnections);
        return new MultiProviderNntpClient([provider]);
    }
}
