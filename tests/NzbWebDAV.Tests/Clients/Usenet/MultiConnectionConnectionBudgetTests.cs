using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.Metrics;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class MultiConnectionConnectionBudgetTests
{
    [Fact]
    public async Task HealthAdmission_IsSharedAcrossProvidersBeforePhysicalPoolAcquisition()
    {
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = "1",
            },
        ]);
        using var gate = new HealthCheckConnectionGate(config);
        var state = new BlockingStatState();
        using var firstProvider = CreateClient(state, maxTransferConnections: null);
        using var secondProvider = CreateClient(state, maxTransferConnections: null);
        using var healthContext = CancellationToken.None.SetContext(
            new HealthCheckAdmissionContext(gate, HealthCheckAdmissionPriority.Background));

        var requests = StartStats(firstProvider, 2)
            .Concat(StartStats(secondProvider, 2))
            .ToArray();
        await WaitForEnteredCount(state, expected: 1);

        Assert.Equal(1, Volatile.Read(ref state.Entered));
        Assert.Equal(1, firstProvider.LiveConnections + secondProvider.LiveConnections);
        Assert.Equal(1, gate.GetSnapshot().Active);

        state.ReleaseAll();
        await Task.WhenAll(requests).WaitAsync(TestTimeout);
        Assert.Equal(4, Volatile.Read(ref state.Entered));
        Assert.Equal(0, gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task NullTransferLimitPreservesLegacySharedPoolWidth()
    {
        var state = new BlockingStatState();
        using var client = CreateClient(state, maxTransferConnections: null);
        var requests = StartStats(client, count: 4);

        await WaitForEnteredCount(state, expected: 4);

        state.ReleaseAll();
        await Task.WhenAll(requests).WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task ExplicitTransferLimitActivatesMetadataBudget()
    {
        var state = new BlockingStatState();
        using var client = CreateClient(state, maxTransferConnections: 4);
        var requests = StartStats(client, count: 4);

        await WaitForEnteredCount(state, expected: 2);
        Assert.Equal(2, Volatile.Read(ref state.Entered));

        state.ReleaseAll();
        await Task.WhenAll(requests).WaitAsync(TestTimeout);
        Assert.Equal(4, Volatile.Read(ref state.Entered));
    }

    [Theory]
    [InlineData((int)NntpOperation.Body, (int)ProviderConnectionKind.Transfer)]
    [InlineData((int)NntpOperation.Article, (int)ProviderConnectionKind.Transfer)]
    [InlineData((int)NntpOperation.PipelinedBody, (int)ProviderConnectionKind.Transfer)]
    [InlineData((int)NntpOperation.PipelinedArticle, (int)ProviderConnectionKind.Transfer)]
    [InlineData((int)NntpOperation.Stat, (int)ProviderConnectionKind.Metadata)]
    [InlineData((int)NntpOperation.Head, (int)ProviderConnectionKind.Metadata)]
    [InlineData((int)NntpOperation.Date, (int)ProviderConnectionKind.Metadata)]
    [InlineData((int)NntpOperation.PipelinedStat, (int)ProviderConnectionKind.Metadata)]
    [InlineData((int)NntpOperation.Control, (int)ProviderConnectionKind.Metadata)]
    public void ClassifyConnectionKind_UsesOperationSemantics(
        int operation,
        int expected)
    {
        Assert.Equal(
            (ProviderConnectionKind)expected,
            MultiConnectionNntpClient.ClassifyConnectionKind((NntpOperation)operation));
    }

    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    private static MultiConnectionNntpClient CreateClient(
        BlockingStatState state,
        int? maxTransferConnections)
    {
        var pool = new ConnectionPool<INntpClient>(
            maxConnections: 4,
            _ => ValueTask.FromResult<INntpClient>(new BlockingStatClient(state)));
        return new MultiConnectionNntpClient(
            pool,
            ProviderType.Pooled,
            new ProviderCircuitBreaker("budget-test"),
            "budget-test",
            maxTransferConnections: maxTransferConnections);
    }

    private static Task<UsenetStatResponse>[] StartStats(
        MultiConnectionNntpClient client,
        int count) =>
        Enumerable.Range(0, count)
            .Select(i => client.StatAsync(
                new SegmentId($"segment-{i}"),
                CancellationToken.None))
            .ToArray();

    private static async Task WaitForEnteredCount(BlockingStatState state, int expected)
    {
        var deadline = DateTime.UtcNow + TestTimeout;
        while (Volatile.Read(ref state.Entered) < expected)
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Only {state.Entered} STAT operations entered; expected {expected}.");
            await Task.Delay(10);
        }
    }

    private sealed class BlockingStatState
    {
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Entered;
        public Task WaitForRelease() => _release.Task;
        public void ReleaseAll() => _release.TrySetResult();
    }

    private sealed class BlockingStatClient(BlockingStatState state) : NntpClient
    {
        public override Task ConnectAsync(
            string host,
            int port,
            bool useSsl,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public override Task<UsenetResponse> AuthenticateAsync(
            string user,
            string pass,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override async Task<UsenetStatResponse> StatAsync(
            SegmentId segmentId,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref state.Entered);
            await state.WaitForRelease().WaitAsync(cancellationToken);
            return new UsenetStatResponse
            {
                ResponseCode = 223,
                ResponseMessage = $"223 <{segmentId}>",
                ArticleExists = true,
            };
        }

        public override Task<UsenetHeadResponse> HeadAsync(
            SegmentId segmentId,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDecodedBodyBatch> DecodedBodiesAsync(
            IReadOnlyList<SegmentId> segmentIds,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public override Task<UsenetDateResponse> DateAsync(CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }
}
