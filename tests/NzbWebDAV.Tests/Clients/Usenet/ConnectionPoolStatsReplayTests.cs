using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Config;
using NzbWebDAV.Models;
using NzbWebDAV.Websocket;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class ConnectionPoolStatsReplayTests
{
    [Fact]
    public async Task NewGeneration_ClearsRetiredProviderReplayState()
    {
        var websocketManager = new WebsocketManager();
        await websocketManager.SendMessage(
            WebsocketTopic.UsenetConnections,
            "4|8|8|8|60|8");

        _ = new ConnectionPoolStats(
            new UsenetProviderConfig
            {
                Providers =
                [
                    new UsenetProviderConfig.ConnectionDetails
                    {
                        Type = ProviderType.Pooled,
                        Host = "news.example.com",
                        Port = 563,
                        UseSsl = true,
                        User = "user",
                        Pass = "pass",
                        MaxConnections = 10,
                    },
                ],
            },
            websocketManager);

        Assert.Equal("reset", websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections));
    }

    [Fact]
    public async Task Flush_WithoutSubscribers_KeepsReplayStateFresh()
    {
        var websocketManager = new WebsocketManager();
        var connectionStats = new ConnectionPoolStats(
            new UsenetProviderConfig
            {
                Providers =
                [
                    new UsenetProviderConfig.ConnectionDetails
                    {
                        Type = ProviderType.Pooled,
                        Host = "news.example.com",
                        Port = 563,
                        UseSsl = true,
                        User = "user",
                        Pass = "pass",
                        MaxConnections = 10,
                    },
                ],
            },
            websocketManager);
        var onChanged = connectionStats.GetOnConnectionPoolChanged(0);

        // A pool event with zero subscribers (e.g. connections closing after
        // the last browser leaves) must still refresh the state-replay message,
        // otherwise a returning browser sees phantom stale connection counts.
        onChanged(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(3, 1, 10));

        await WaitUntil(() =>
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections) == "0|3|1|3|10|1");
        Assert.Equal(
            "0|3|1|3|10|1",
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections));
    }

    [Fact]
    public async Task EffectiveMax_ReflectedInTotalMax()
    {
        var websocketManager = new WebsocketManager();
        var connectionStats = new ConnectionPoolStats(
            new UsenetProviderConfig
            {
                Providers =
                [
                    new UsenetProviderConfig.ConnectionDetails
                    {
                        Type = ProviderType.Pooled,
                        Host = "news.example.com",
                        Port = 563,
                        UseSsl = true,
                        User = "user",
                        Pass = "pass",
                        MaxConnections = 150,
                    },
                ],
            },
            websocketManager);
        var onChanged = connectionStats.GetOnConnectionPoolChanged(0);

        // Simulate a learned-limit shrink: pool reports effective max 135.
        onChanged(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(5, 2, 135));

        await WaitUntil(() =>
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections) == "0|5|2|5|135|2");
        Assert.Equal(
            "0|5|2|5|135|2",
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections));
    }

    [Fact]
    public async Task FullySplitProviders_AppendAggregateTransferAndMetadataSummary()
    {
        var websocketManager = new WebsocketManager();
        var connectionStats = new ConnectionPoolStats(
            new UsenetProviderConfig
            {
                Providers =
                [
                    Provider(ProviderType.Pooled, max: 10, transfer: 6),
                    Provider(ProviderType.BackupOnly, max: 8, transfer: 5),
                ],
            },
            websocketManager);

        connectionStats.GetOnConnectionPoolChanged(0)(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(4, 1, 10));
        connectionStats.GetOnConnectionPoolChanged(1)(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(1, 0, 8));
        connectionStats.GetOnConnectionAdmissionChanged(0)(
            Admission(transferLimit: 6, metadataBase: 4, metadataMax: 7,
                activeTransfers: 3, activeMetadata: 7));
        connectionStats.GetOnConnectionAdmissionChanged(1)(
            Admission(transferLimit: 5, metadataBase: 3, metadataMax: 5,
                activeTransfers: 2, activeMetadata: 4));

        await WaitUntil(() => websocketManager.PeekLastMessage(
            WebsocketTopic.UsenetConnections) is not null);
        Assert.Equal(
            "1|1|0|4|10|1|1|5|11|11|7|12",
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections));
    }

    [Fact]
    public async Task MixedLegacyProviders_KeepLegacySummaryPayload()
    {
        var websocketManager = new WebsocketManager();
        var connectionStats = new ConnectionPoolStats(
            new UsenetProviderConfig
            {
                Providers =
                [
                    Provider(ProviderType.Pooled, max: 10, transfer: 6),
                    Provider(ProviderType.BackupOnly, max: 8, transfer: null),
                ],
            },
            websocketManager);

        connectionStats.GetOnConnectionPoolChanged(0)(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(4, 1, 10));
        connectionStats.GetOnConnectionPoolChanged(1)(
            this,
            new ConnectionPoolStats.ConnectionPoolChangedEventArgs(1, 0, 8));

        await WaitUntil(() => websocketManager.PeekLastMessage(
            WebsocketTopic.UsenetConnections) is not null);
        Assert.Equal(
            "1|1|0|4|10|1",
            websocketManager.PeekLastMessage(WebsocketTopic.UsenetConnections));
    }

    private static UsenetProviderConfig.ConnectionDetails Provider(
        ProviderType type,
        int max,
        int? transfer) => new()
    {
        Type = type,
        Host = $"{type}.example.com",
        Port = 563,
        UseSsl = true,
        User = "user",
        Pass = "pass",
        MaxConnections = max,
        MaxTransferConnections = transfer,
    };

    private static ProviderConnectionAdmissionSnapshot Admission(
        int transferLimit,
        int metadataBase,
        int metadataMax,
        int activeTransfers,
        int activeMetadata) => new(
        ConfiguredTransferLimit: transferLimit,
        EffectiveTransferLimit: transferLimit,
        BaseMetadataCapacity: metadataBase,
        MetadataBurstAllowance: metadataMax - metadataBase,
        MaxMetadataCapacity: metadataMax,
        ActiveTransferOperations: activeTransfers,
        ActiveMetadataOperations: activeMetadata,
        WaitingTransferOperations: 0,
        WaitingMetadataOperations: 0);

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }
}
