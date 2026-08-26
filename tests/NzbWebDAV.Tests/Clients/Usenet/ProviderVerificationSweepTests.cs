using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

/// <summary>
/// One sweep is one provider, one pipelined batch, one connection. That invariant is what
/// the reverted chunked-dispatch attempt broke: it pipelined against the primary and then
/// fanned out over several connections rechecking the misses, so a scheduler lease no
/// longer corresponded to the work actually in flight.
/// </summary>
public class ProviderVerificationSweepTests
{
    [Fact]
    public async Task Sweep_ReturnsOnlyTheIdsTheProviderDidNotHave()
    {
        var provider = Holding("a", "c");
        var (client, keys) = Build(provider);
        using var _ = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["a", "b", "c", "d"], depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(["b", "d"], sweep.Missing);
        Assert.Empty(sweep.Unanswered);
    }

    [Fact]
    public async Task Sweep_IssuesOnePipelinedBatchAndNoPerSegmentStats()
    {
        // The accounting property callers depend on: a sweep may not fan out, and must not
        // silently degrade into a per-segment walk that would cost a round trip each.
        var provider = Holding();
        var (client, keys) = Build(provider);
        using var _ = client;

        var segments = Enumerable.Range(0, 200).Select(i => $"seg-{i}").ToArray();
        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], segments, depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(200, sweep.Missing.Count);
        Assert.Empty(sweep.Unanswered);
        Assert.Equal(1, provider.BatchStatRequests);
        Assert.Equal(0, provider.SingularRequests);
    }

    [Fact]
    public async Task Sweep_DoesNotFailOverToAnotherProvider()
    {
        // Failover belongs to the caller. A sweep that consulted a second provider would
        // consume a connection the caller never accounted for.
        var first = Holding();
        var second = Holding("a");
        var (client, keys) = Build(first, second);
        using var _ = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["a"], depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(["a"], sweep.Missing);
        Assert.Empty(sweep.Unanswered);
        Assert.Equal(0, second.BatchStatRequests);
        Assert.Equal(0, second.SingularRequests);
    }

    [Fact]
    public async Task Sweep_TreatsAnUnfinishedBatchAsUnresolved()
    {
        // A provider that dies partway through hands its remaining work to the next phase
        // rather than failing the file.
        var provider = Holding(["a", "b", "c"], throwAfter: 2);
        var (client, keys) = Build(provider);
        using var _ = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["a", "b", "c"], depth: 0, progress: null, CancellationToken.None);

        Assert.Empty(sweep.Missing);
        Assert.Equal(["c"], sweep.Unanswered);
    }

    [Fact]
    public async Task Sweep_OnUnknownProvider_PassesEverythingThrough()
    {
        var (client, _) = Build(Holding("a"));
        using var _c = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            "not-a-provider", ["a", "b", "a"], depth: 0, progress: null, CancellationToken.None);

        Assert.Empty(sweep.Missing);
        Assert.Equal(["a", "b"], sweep.Unanswered);
    }

    [Fact]
    public async Task Sweep_DeduplicatesRepeatedIdsInFirstSeenOrder()
    {
        var provider = Holding("present");
        var (client, keys) = Build(provider);
        using var _ = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["missing-b", "present", "missing-a", "missing-b", "present"],
            depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(["missing-b", "missing-a"], sweep.Missing);
        Assert.Empty(sweep.Unanswered);
        Assert.Equal(1, provider.BatchStatRequests);
    }

    [Fact]
    public async Task Sweep_ReportsInputPositionProgressWithinTheBatch()
    {
        var provider = Holding("present");
        var (client, keys) = Build(provider);
        using var _ = client;
        var reports = new List<int>();

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["present", "missing", "missing", "other"], depth: 0,
            new InlineProgress<int>(reports.Add), CancellationToken.None);

        Assert.Equal(["missing", "other"], sweep.Missing);
        Assert.Contains(reports, count => count is > 0 and < 4);
        Assert.Equal(4, reports[^1]);
        Assert.True(reports.SequenceEqual(reports.Order()));
    }

    [Fact]
    public async Task Sweep_ReportsCompletionWhenProviderIsUnavailable()
    {
        var (client, _) = Build(Holding("present"));
        using var _c = client;
        var reports = new List<int>();

        await client.SweepProviderPipelinedAsync(
            "not-a-provider", ["a", "b", "a"], depth: 0,
            new InlineProgress<int>(reports.Add), CancellationToken.None);

        Assert.Equal([3], reports);
    }

    [Fact]
    public void ProviderOrder_CollapsesStorageGroupSiblings()
    {
        // Siblings share upstream storage, so a second probe repeats the same answer. The
        // per-STAT walk already skips them; the sweep order must not reintroduce them.
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "a.example", storageGroup: "Omicron"),
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "b.example", storageGroup: "Omicron"),
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "c.example", storageGroup: "Eweka"),
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "d.example"),
        ]);

        var order = client.GetVerificationProviderOrder(CancellationToken.None);

        Assert.Equal(3, order.Count);
        Assert.Single(order, p => p.StorageGroup == "Omicron");
        Assert.Single(order, p => p.StorageGroup == "Eweka");
        Assert.Single(order, p => p.StorageGroup.Length == 0);
    }

    [Fact]
    public void ProviderOrder_OmitsOpenCircuitProviders()
    {
        var openBreaker = new ProviderCircuitBreaker("open.example");
        openBreaker.RecordFailure();
        openBreaker.RecordFailure();
        openBreaker.RecordFailure();
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "open.example", circuitBreaker: openBreaker),
            MultiProviderNntpClientTests.CreateProvider(
                Holding(), host: "healthy.example"),
        ]);

        var order = client.GetVerificationProviderOrder(CancellationToken.None);

        var provider = Assert.Single(order);
        Assert.Equal("healthy.example", provider.Host);
    }

    private static MultiProviderNntpClientTests.ScriptedNntpClient Holding(
        params string[] has) => Holding(has, null);

    private static MultiProviderNntpClientTests.ScriptedNntpClient Holding(
        string[] has, int? throwAfter) => new()
        {
            BatchResponseCode = 430,
            SingularResponseCode = (int)UsenetSharp.Models.UsenetResponseType.NoArticleWithThatMessageId,
            PipelinedStatHolds = has.ToHashSet(StringComparer.Ordinal),
            PipelinedStatThrowAfter = throwAfter,
        };

    [Fact]
    public async Task Sweep_WeighsEachDefinitiveAnswerExactlyOnce()
    {
        var provider = Holding("a", "c");
        var (client, keys) = Build(provider);
        using var _ = client;

        // "a" appears twice, but one message id is one logical article: it is probed once and
        // the verdict is projected back onto both positions. Weighing it twice would let a
        // malformed nzb move provider routing.
        await client.SweepProviderPipelinedAsync(
            keys[0], ["a", "b", "c", "d", "a"], depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(4, client.VerificationCoverage.GetSnapshot(keys[0]).Samples);
    }

    [Fact]
    public async Task Sweep_RecordsNoCoverageForIdsTheProviderNeverAnswered()
    {
        // The connection dies partway through the batch. The ids it never answered say
        // nothing about whether the provider holds them — that is the transport failing, not
        // the article being absent — so they must not count as evidence against it.
        var provider = Holding([], throwAfter: 2);
        var (client, keys) = Build(provider);
        using var _ = client;

        var sweep = await client.SweepProviderPipelinedAsync(
            keys[0], ["a", "b", "c", "d"], depth: 0, progress: null, CancellationToken.None);

        Assert.Equal(2, sweep.Unanswered.Count);
        Assert.Equal(2, client.VerificationCoverage.GetSnapshot(keys[0]).Samples);
    }

    private static (MultiProviderNntpClient Client, string[] Keys) Build(params MultiProviderNntpClientTests.ScriptedNntpClient[] scripted)
    {
        var providers = scripted
            .Select((c, i) => MultiProviderNntpClientTests.CreateProvider(
                c, host: $"p{i}.example", maxConnections: 8))
            .ToList();
        return (new MultiProviderNntpClient(providers), providers.Select(p => p.MetricsKey).ToArray());
    }

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }

}
