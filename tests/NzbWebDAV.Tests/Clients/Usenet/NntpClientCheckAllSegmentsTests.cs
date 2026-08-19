using System.Text.Json;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using UsenetSharp.Exceptions;
using UsenetSharp.Models;
using UsenetSharp.Streams;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class NntpClientCheckAllSegmentsTests
{
    [Fact]
    public async Task CheckAllSegmentsAsync_With451_ThrowsArticleNotFound()
    {
        var client = new StatCodeClient(451);

        var exception = await Assert.ThrowsAsync<UsenetArticleNotFoundException>(() =>
            client.CheckAllSegmentsAsync(["seg@example"], 1, null, CancellationToken.None));

        Assert.Equal("seg@example", exception.SegmentId);
    }

    [Fact]
    public async Task CheckAllSegmentsAsync_With430_ThrowsArticleNotFound()
    {
        var client = new StatCodeClient(430);

        await Assert.ThrowsAsync<UsenetArticleNotFoundException>(() =>
            client.CheckAllSegmentsAsync(["seg@example"], 1, null, CancellationToken.None));
    }

    [Fact]
    public async Task CheckAllSegmentsAsync_With400_ThrowsUnexpectedResponse()
    {
        var client = new StatCodeClient(400);

        var exception = await Assert.ThrowsAsync<UsenetUnexpectedResponseException>(() =>
            client.CheckAllSegmentsAsync(["seg@example"], 1, null, CancellationToken.None));

        Assert.IsAssignableFrom<RetryableDownloadException>(exception);
    }

    [Fact]
    public async Task CheckAllSegmentsAsync_With223_Succeeds()
    {
        var client = new StatCodeClient(223);

        await client.CheckAllSegmentsAsync(["seg@example"], 1, null, CancellationToken.None);
    }

    [Fact]
    public async Task CheckAllSegmentsAsync_KeepsWorkerSlotsFedAfterInitialBurst()
    {
        const int concurrency = 12;
        var initialStarted = 0;
        var initialFinished = 0;
        var refillActive = 0;
        var invocation = 0;
        var releaseInitial = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var refillReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRefill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new DelegateStatClient(async (segmentId, cancellationToken) =>
        {
            if (Interlocked.Increment(ref invocation) <= concurrency)
            {
                if (Interlocked.Increment(ref initialStarted) == concurrency)
                    releaseInitial.TrySetResult();
                await releaseInitial.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                if (Interlocked.Increment(ref initialFinished) == concurrency)
                    initialDrained.TrySetResult();
                await initialDrained.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return Exists(segmentId);
            }

            var current = Interlocked.Increment(ref refillActive);
            if (current >= concurrency - 1) refillReached.TrySetResult();
            try
            {
                await releaseRefill.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return Exists(segmentId);
            }
            finally
            {
                Interlocked.Decrement(ref refillActive);
            }
        });
        var refillObservedBeforeFirstProgressReturned = false;
        var progress = new CallbackProgress(value =>
        {
            if (value != 1) return;
            refillObservedBeforeFirstProgressReturned = refillReached.Task.Wait(TimeSpan.FromSeconds(5));
            releaseRefill.TrySetResult();
        });

        await client.CheckAllSegmentsAsync(
            Enumerable.Range(0, 36).Select(index => $"segment-{index}@example"),
            concurrency,
            progress,
            CancellationToken.None);

        Assert.True(refillObservedBeforeFirstProgressReturned);
    }

    [Fact]
    public async Task ConcurrentChecks_KeepSharedHealthGateFedAfterInitialBursts()
    {
        const int gateLimit = 12;
        const int checkCount = 3;
        using var gate = new HealthCheckConnectionGate(CreateGateConfig(gateLimit));
        var initialFinished = 0;
        var refillObserved = 0;
        var observer = 0;
        using var firstProgressBarrier = new CountdownEvent(checkCount);
        var allInitialFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gateRefilled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRefill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var checks = Enumerable.Range(0, checkCount).Select(checkIndex =>
        {
            var invocation = 0;
            var client = new DelegateStatClient(async (segmentId, cancellationToken) =>
            {
                await Task.Yield();
                var currentInvocation = Interlocked.Increment(ref invocation);
                if (currentInvocation > gateLimit)
                    await allInitialFinished.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                using var lease = await gate.AcquireAsync(
                    HealthCheckAdmissionPriority.Background,
                    cancellationToken).ConfigureAwait(false);
                if (currentInvocation <= gateLimit)
                {
                    if (Interlocked.Increment(ref initialFinished) == gateLimit * checkCount)
                        allInitialFinished.TrySetResult();
                    return Exists(segmentId);
                }

                if (gate.GetSnapshot().Active >= gateLimit) gateRefilled.TrySetResult();
                await releaseRefill.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return Exists(segmentId);
            });
            var progress = new CallbackProgress(value =>
            {
                if (value != 1) return;
                var initialObserved = allInitialFinished.Task.Wait(TimeSpan.FromSeconds(5));
                firstProgressBarrier.Signal();
                var barrierObserved = firstProgressBarrier.Wait(TimeSpan.FromSeconds(5));
                if (!initialObserved || !barrierObserved)
                {
                    releaseRefill.TrySetResult();
                    return;
                }
                if (Interlocked.CompareExchange(ref observer, 1, 0) == 0)
                {
                    if (gateRefilled.Task.Wait(TimeSpan.FromSeconds(5)))
                        Volatile.Write(ref refillObserved, 1);
                    releaseRefill.TrySetResult();
                }
                else
                {
                    releaseRefill.Task.GetAwaiter().GetResult();
                }
            });

            return client.CheckAllSegmentsAsync(
                Enumerable.Range(0, 36).Select(index => $"check-{checkIndex}-segment-{index}@example"),
                gateLimit,
                progress,
                CancellationToken.None);
        });

        await Task.WhenAll(checks);

        Assert.Equal(1, Volatile.Read(ref refillObserved));
    }

    [Fact]
    public async Task CheckAllSegmentsAsync_MissingSegmentCancelsAndDrainsSiblingWorkers()
    {
        var allStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;
        var drained = 0;
        var client = new DelegateStatClient(async (segmentId, cancellationToken) =>
        {
            if (Interlocked.Increment(ref started) == 3) allStarted.TrySetResult();
            await allStarted.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (segmentId.ToString() == "missing@example")
            {
                return new UsenetStatResponse
                {
                    ResponseCode = 430,
                    ResponseMessage = "430 missing",
                    ArticleExists = false,
                };
            }

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return Exists(segmentId);
            }
            finally
            {
                Interlocked.Increment(ref drained);
            }
        });

        await Assert.ThrowsAsync<UsenetArticleNotFoundException>(() =>
            client.CheckAllSegmentsAsync(
                ["missing@example", "sibling-1@example", "sibling-2@example"],
                concurrency: 3,
                progress: null,
                CancellationToken.None));

        Assert.Equal(2, drained);
    }

    [Fact]
    public async Task ArticleExistenceChecker_UsesConcurrentPoolPath()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, true],
            recheckCodes: [223, 223]);

        await ArticleExistenceChecker.CheckAsync(
            client,
            ["a@example", "b@example"],
            concurrency: 7,
            progress: null,
            CancellationToken.None);

        Assert.Equal(1, client.CheckAllSegmentsCallCount);
        Assert.Equal(0, client.PipelinedStatsCallCount);
        Assert.Equal(7, client.LastConcurrency);
        Assert.Equal(["a@example", "b@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_WithAllExists_SucceedsWithoutFailoverRecheck()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, true],
            recheckCodes: []);

        await client.CheckAllSegmentsPipelinedAsync(
            ["a@example", "b@example"], depth: 8, fallbackConcurrency: 2, progress: null,
            CancellationToken.None);

        Assert.Equal(0, client.CheckAllSegmentsCallCount);
        Assert.Empty(client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_RechecksOnlyMisses()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, false, true, false],
            recheckCodes: [223, 223]);

        await client.CheckAllSegmentsPipelinedAsync(
            ["a@example", "b@example", "c@example", "d@example"],
            depth: 8,
            fallbackConcurrency: 2,
            progress: null,
            CancellationToken.None);

        Assert.Equal(1, client.CheckAllSegmentsCallCount);
        Assert.Equal(["b@example", "d@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_MissConfirmedOnFailover_ThrowsArticleNotFound()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, false],
            recheckCodes: [430]);

        var exception = await Assert.ThrowsAsync<UsenetArticleNotFoundException>(() =>
            client.CheckAllSegmentsPipelinedAsync(
                ["a@example", "b@example"], 8, 1, null, CancellationToken.None));

        Assert.Equal("b@example", exception.SegmentId);
        Assert.Equal(["b@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_SweepThrowsUnexpected_FallsBackToFullConcurrentPath()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: null,
            recheckCodes: [223, 223],
            sweepException: new UsenetUnexpectedResponseException("a@example", "400 idle timeout"));

        await client.CheckAllSegmentsPipelinedAsync(
            ["a@example", "b@example"], 8, 2, null, CancellationToken.None);

        Assert.Equal(1, client.CheckAllSegmentsCallCount);
        Assert.Equal(["a@example", "b@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_SweepThrowsProtocol_FallsBackToFullConcurrentPath()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: null,
            recheckCodes: [223, 223],
            sweepException: new UsenetProtocolException(
                "The NNTP connection closed before all pipelined STAT responses were received."));

        await client.CheckAllSegmentsPipelinedAsync(
            ["a@example", "b@example"], 8, 2, null, CancellationToken.None);

        Assert.Equal(1, client.CheckAllSegmentsCallCount);
        Assert.Equal(["a@example", "b@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CheckAllSegmentsPipelinedAsync_SweepThrowsAfterProgress_FallbackProgressIsMonotonic()
    {
        var reports = new List<int>();
        // Collect synchronously — System.Progress<T> posts via the sync context / thread
        // pool and races List enumeration in Assert.Equal.
        var progress = new CollectingProgress(reports);
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, true, true],
            recheckCodes: [223, 223, 223],
            sweepException: new UsenetProtocolException("connection closed mid-sweep"),
            throwAfterYieldCount: 2);

        await client.CheckAllSegmentsPipelinedAsync(
            ["a@example", "b@example", "c@example"], 8, 2, progress, CancellationToken.None);

        Assert.Equal(1, client.CheckAllSegmentsCallCount);
        Assert.Equal(["a@example", "b@example", "c@example"], client.RecheckedSegmentIds);
        // Pipelined reports 1,2 then throw; fallback clamps so n=1,2 stay at 2 before advancing to 3.
        Assert.Equal([1, 2, 2, 2, 3], reports);
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_SweepThrowsAfterProgress_FallbackProgressIsMonotonic()
    {
        var reports = new List<int>();
        var progress = new CollectingProgress(reports);
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, true, true],
            recheckCodes: [223, 223, 223],
            sweepException: new UsenetProtocolException("connection closed mid-sweep"),
            throwAfterYieldCount: 2);

        var missing = await client.CollectMissingSegmentsPipelinedAsync(
            ["a@example", "b@example", "c@example"], 8, 2, progress, CancellationToken.None);

        Assert.Empty(missing);
        Assert.Equal(["a@example", "b@example", "c@example"], client.RecheckedSegmentIds);
        // Pipelined reports 1,2 then throw; fallback clamps so n=1,2 stay at 2 before advancing to 3.
        Assert.Equal([1, 2, 2, 2, 3], reports);
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_CollectsConfirmedMissesInInputOrder()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [false, true, false],
            recheckCodes: [430, 223]);

        var missing = await client.CollectMissingSegmentsPipelinedAsync(
            ["a@example", "b@example", "c@example"], 8, 2, null, CancellationToken.None);

        Assert.Equal(["a@example"], missing);
        Assert.Equal(["a@example", "c@example"], client.RecheckedSegmentIds);
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_NonDefinitiveRecheckThrows()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [false],
            recheckCodes: [400]);

        await Assert.ThrowsAsync<UsenetUnexpectedResponseException>(() =>
            client.CollectMissingSegmentsPipelinedAsync(
                ["a@example"], 8, 1, null, CancellationToken.None));
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_WithAllExists_ReturnsEmptyWithoutRecheck()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [true, true],
            recheckCodes: []);

        var missing = await client.CollectMissingSegmentsPipelinedAsync(
            ["a@example", "b@example"], 8, 2, null, CancellationToken.None);

        Assert.Empty(missing);
        Assert.Empty(client.RecheckedSegmentIds);
        Assert.Equal(0, client.CheckAllSegmentsCallCount);
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_WithEmptyInput_ReturnsEmpty()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: [],
            recheckCodes: []);

        var missing = await client.CollectMissingSegmentsPipelinedAsync(
            [], 8, 2, null, CancellationToken.None);

        Assert.Empty(missing);
        Assert.Equal(0, client.PipelinedStatsCallCount);
    }

    [Fact]
    public async Task CollectMissingSegmentsPipelinedAsync_SweepThrows_CollectingFallbackReturnsFullSet()
    {
        var client = new TrackingPipelinedStatClient(
            pipelinedExists: null,
            recheckCodes: [430, 223, 430],
            sweepException: new UsenetProtocolException("connection closed mid-sweep"));

        var missing = await client.CollectMissingSegmentsPipelinedAsync(
            ["a@example", "b@example", "c@example"], 8, 2, null, CancellationToken.None);

        // The collecting fallback STATs every segment concurrently (not just a partial
        // sweep's misses) and returns the full confirmed set in input order.
        Assert.Equal(["a@example", "c@example"], missing);
        Assert.Equal(["a@example", "b@example", "c@example"], client.RecheckedSegmentIds);
        Assert.Equal(0, client.CheckAllSegmentsCallCount);
    }

    private sealed class CollectingProgress(List<int> reports) : IProgress<int>
    {
        public void Report(int value) => reports.Add(value);
    }

    private sealed class CallbackProgress(Action<int> onReport) : IProgress<int>
    {
        public void Report(int value) => onReport(value);
    }

    private static UsenetStatResponse Exists(SegmentId segmentId) => new()
    {
        ResponseCode = (int)UsenetResponseType.ArticleExists,
        ResponseMessage = $"223 <{segmentId}>",
        ArticleExists = true,
    };

    private static ConfigManager CreateGateConfig(int limit)
    {
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.UsenetProviders,
                ConfigValue = JsonSerializer.Serialize(new UsenetProviderConfig
                {
                    Providers =
                    [
                        new UsenetProviderConfig.ConnectionDetails
                        {
                            ProviderId = Guid.NewGuid(),
                            Type = ProviderType.Pooled,
                            Host = "gate.example",
                            Port = 563,
                            UseSsl = true,
                            User = "user",
                            Pass = "pass",
                            MaxConnections = 50,
                        },
                    ],
                }),
            },
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = limit.ToString(),
            },
        ]);
        return config;
    }

    [Fact]
    public async Task MapPipelinedBodyResult_With451_ReportsNotFound()
    {
        var client = new BodyCodeClient(451);

        PipelinedBodyResult? result = null;
        await foreach (var item in client.DecodedBodiesPipelinedAsync(
                           ["seg@example"], 1, CancellationToken.None))
            result = item;

        Assert.NotNull(result);
        var body = result ?? throw new InvalidOperationException("expected result");
        Assert.False(body.Found);
        Assert.Null(body.Stream);
    }

    private sealed class TrackingPipelinedStatClient(
        bool[]? pipelinedExists,
        int[] recheckCodes,
        Exception? sweepException = null,
        int throwAfterYieldCount = 0) : NntpClient
    {
        private int _recheckIndex;

        public int CheckAllSegmentsCallCount { get; private set; }
        public int PipelinedStatsCallCount { get; private set; }
        public int? LastConcurrency { get; private set; }
        public List<string> RecheckedSegmentIds { get; } = [];

        public override async IAsyncEnumerable<PipelinedStatResult> StatsPipelinedAsync(
            IReadOnlyList<string> segmentIds,
            int depth,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            PipelinedStatsCallCount++;
            if (sweepException != null && throwAfterYieldCount <= 0)
                throw sweepException;

            for (var i = 0; i < segmentIds.Count; i++)
            {
                if (sweepException != null && i == throwAfterYieldCount)
                    throw sweepException;

                yield return new PipelinedStatResult
                {
                    SegmentId = segmentIds[i],
                    Exists = pipelinedExists![i],
                };
            }
        }

        public override async Task CheckAllSegmentsAsync(
            IEnumerable<string> segmentIds,
            int concurrency,
            IProgress<int>? progress,
            CancellationToken cancellationToken)
        {
            CheckAllSegmentsCallCount++;
            LastConcurrency = concurrency;
            var list = segmentIds.ToList();
            RecheckedSegmentIds.AddRange(list);

            var processed = 0;
            foreach (var segmentId in list)
            {
                progress?.Report(++processed);
                var code = recheckCodes[_recheckIndex++];
                if (code == (int)UsenetResponseType.ArticleExists) continue;
                if (code is 430 or 451)
                    throw new UsenetArticleNotFoundException(segmentId, $"{code} missing");
                throw new UsenetUnexpectedResponseException(segmentId, $"{code} unexpected");
            }

            await Task.CompletedTask;
        }

        public override Task ConnectAsync(
            string host, int port, bool useSsl, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task<UsenetResponse> AuthenticateAsync(
            string user, string pass, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetStatResponse> StatAsync(
            SegmentId segmentId, CancellationToken cancellationToken)
        {
            RecheckedSegmentIds.Add(segmentId);
            var code = recheckCodes[_recheckIndex++];
            return Task.FromResult(new UsenetStatResponse
            {
                ResponseCode = code,
                ResponseMessage = $"{code} <{segmentId}>",
                ArticleExists = code == (int)UsenetResponseType.ArticleExists,
            });
        }

        public override Task<UsenetHeadResponse> HeadAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyBatch> DecodedBodiesAsync(
            IReadOnlyList<SegmentId> segmentIds,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDateResponse> DateAsync(CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }

    private sealed class DelegateStatClient(
        Func<SegmentId, CancellationToken, Task<UsenetStatResponse>> stat) : NntpClient
    {
        public override Task ConnectAsync(
            string host, int port, bool useSsl, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task<UsenetResponse> AuthenticateAsync(
            string user, string pass, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetStatResponse> StatAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            stat(segmentId, cancellationToken);

        public override Task<UsenetHeadResponse> HeadAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyBatch> DecodedBodiesAsync(
            IReadOnlyList<SegmentId> segmentIds,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDateResponse> DateAsync(CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }

    private sealed class StatCodeClient(int responseCode) : NntpClient
    {
        public override Task ConnectAsync(
            string host, int port, bool useSsl, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task<UsenetResponse> AuthenticateAsync(
            string user, string pass, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetStatResponse> StatAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            Task.FromResult(new UsenetStatResponse
            {
                ResponseCode = responseCode,
                ResponseMessage = $"{responseCode} <{segmentId}>",
                ArticleExists = responseCode == (int)UsenetResponseType.ArticleExists,
            });

        public override Task<UsenetHeadResponse> HeadAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyBatch> DecodedBodiesAsync(
            IReadOnlyList<SegmentId> segmentIds,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDateResponse> DateAsync(CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }

    private sealed class BodyCodeClient(int responseCode) : NntpClient
    {
        public override Task ConnectAsync(
            string host, int port, bool useSsl, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task<UsenetResponse> AuthenticateAsync(
            string user, string pass, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetStatResponse> StatAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetHeadResponse> HeadAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            DecodedBodyAsync(segmentId, null, cancellationToken);

        public override Task<UsenetDecodedBodyResponse> DecodedBodyAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken)
        {
            var success = responseCode == (int)UsenetResponseType.ArticleRetrievedBodyFollows;
            return Task.FromResult(new UsenetDecodedBodyResponse
            {
                SegmentId = segmentId.ToString(),
                ResponseCode = responseCode,
                ResponseMessage = $"{responseCode} scripted body",
                Stream = success ? new YencStream(new MemoryStream([], writable: false)) : null,
            });
        }

        public override Task<UsenetDecodedBodyBatch> DecodedBodiesAsync(
            IReadOnlyList<SegmentId> segmentIds,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken)
        {
            var responses = segmentIds
                .Select(id => DecodedBodyAsync(id, cancellationToken))
                .ToArray();
            return Task.FromResult(new UsenetDecodedBodyBatch { Responses = responses });
        }

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDecodedArticleResponse> DecodedArticleAsync(
            SegmentId segmentId,
            ArticleBodyCompletionHandler? onConnectionReadyAgain,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override Task<UsenetDateResponse> DateAsync(CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }
}
