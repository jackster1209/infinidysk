using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Models;
using UsenetSharp.Clients;

namespace NzbWebDAV.Tests.Clients.Usenet;

/// <summary>
/// A provider's configured pipelining depth is BODY/queue oriented, and
/// <c>StatsPipelinedAsync</c> discards its depth argument entirely — UsenetSharp windows STAT
/// at the MaxPipelineDepth the physical connection was constructed with. These tests pin that
/// the telemetry reports the depth STAT actually runs at rather than the configured value.
/// </summary>
public class EffectiveStatPipelineDepthTests
{
    [Fact]
    public void EffectiveStatDepth_ReportsTheDepthConnectionsAreActuallyBuiltWith()
    {
        // InfiniDysk leaves MaxPipelineDepth unset, so STAT runs at the UsenetSharp default.
        // Sourced from the named constant so the assertion cannot drift from the library.
        Assert.Equal(
            UsenetClientOptions.DefaultMaxPipelineDepth,
            BaseNntpClient.EffectiveStatPipelineDepth);
    }

    [Fact]
    public void UnsetProviderOverride_ResolvesToTheRealDefaultRatherThanZero()
    {
        using var provider = CreateProvider(pipeliningDepth: null);

        Assert.Null(provider.ConfiguredPipeliningDepth);
        Assert.NotEqual(0, provider.EffectiveStatPipelineDepth);
        Assert.Equal(
            UsenetClientOptions.DefaultMaxPipelineDepth,
            provider.EffectiveStatPipelineDepth);
    }

    [Theory]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(32)]
    public void ConfiguredBodyDepth_DoesNotMasqueradeAsEffectiveStatDepth(int configured)
    {
        using var provider = CreateProvider(pipeliningDepth: configured);

        // The configured value stays visible and unchanged...
        Assert.Equal(configured, provider.ConfiguredPipeliningDepth);
        // ...but STAT still sweeps at the physical client's window, so telemetry must not
        // echo the configured number back as though it applied to STAT.
        Assert.Equal(
            UsenetClientOptions.DefaultMaxPipelineDepth,
            provider.EffectiveStatPipelineDepth);
        Assert.NotEqual(configured, provider.EffectiveStatPipelineDepth);
    }

    [Fact]
    public void Snapshot_ReportsConfiguredAndEffectiveStatDepthSeparately()
    {
        using var provider = CreateProvider(pipeliningDepth: 16);

        var snapshot = new ProviderConnectionSnapshot(
            provider.MetricsKey,
            provider.Host,
            provider.ProviderType,
            provider.LiveConnections,
            provider.IdleConnections,
            provider.ActiveConnections,
            provider.AvailableConnections,
            provider.PendingSelections,
            provider.GetConnectionChurn(),
            provider.LearnedConnectionLimit,
            provider.MaxConnections,
            provider.EffectiveMaxConnections,
            provider.GetConnectionAdmissionSnapshot(),
            provider.EffectiveStatPipelineDepth,
            provider.ConfiguredPipeliningDepth);

        Assert.Equal(16, snapshot.ConfiguredPipelineDepth);
        Assert.Equal(
            UsenetClientOptions.DefaultMaxPipelineDepth,
            snapshot.EffectiveStatPipelineDepth);
    }

    private static MultiConnectionNntpClient CreateProvider(int? pipeliningDepth)
    {
#pragma warning disable CA2000 // ownership transfers to the returned client
        var pool = new ConnectionPool<INntpClient>(
            2,
            // Never invoked: these tests only read configuration-derived telemetry and
            // deliberately never borrow a connection.
            _ => throw new InvalidOperationException("no connection should be borrowed"),
            TimeSpan.FromMinutes(5));
#pragma warning restore CA2000
        return new MultiConnectionNntpClient(
            pool,
            ProviderType.Pooled,
            new ProviderCircuitBreaker("provider-a"),
            "provider.example",
            byteLimit: null,
            bytesUsedOffset: 0,
            priority: 0,
            pipeliningDepth: pipeliningDepth,
            storageGroup: "",
            metricsKey: "provider-a");
    }
}
