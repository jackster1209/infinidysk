using System.Collections.Concurrent;
using System.Text.Json;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Services;

public sealed class HealthCheckStatSchedulerTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task FiveHungrySessions_ShareCapacityEvenly()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 10);
        var held = new List<HealthCheckConnectionGate.Lease>();
        for (var index = 0; index < 10; index++)
            held.Add(await harness.Gate.AcquireAsync(
                HealthCheckAdmissionPriority.Queue, CancellationToken.None));

        var sessions = Enumerable.Range(0, 5)
            .Select(_ => harness.StartSession(segmentCount: 20))
            .ToArray();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().PendingAdmissions == 10);

        foreach (var lease in held) lease.Dispose();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 10);

        var active = harness.Scheduler.GetSnapshot().Sessions
            .Select(session => session.InFlight)
            .Order()
            .ToArray();
        Assert.Equal([2, 2, 2, 2, 2], active);

        foreach (var session in sessions) await session.CancelAsync();
    }

    [Fact]
    public async Task OneSession_BorrowsAllCapacity()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 12);
        var session = harness.StartSession(segmentCount: 50);

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 12);

        var snapshot = Assert.Single(harness.Scheduler.GetSnapshot().Sessions);
        Assert.Equal(12, snapshot.InFlight);
        Assert.Equal(12, session.Executor.InvocationCount);
        await session.CancelAsync();
    }

    [Fact]
    public async Task SmallSession_DoesNotStrandCapacity()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 10);
        var held = new List<HealthCheckConnectionGate.Lease>();
        for (var index = 0; index < 10; index++)
            held.Add(await harness.Gate.AcquireAsync(
                HealthCheckAdmissionPriority.Queue, CancellationToken.None));
        var small = harness.StartSession(segmentCount: 1);
        var large = harness.StartSession(segmentCount: 50);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().PendingAdmissions == 10);

        foreach (var lease in held) lease.Dispose();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 10);

        var byRun = harness.Scheduler.GetSnapshot().Sessions.ToDictionary(x => x.RunId);
        Assert.Equal(1, byRun[small.RunId].InFlight);
        Assert.Equal(9, byRun[large.RunId].InFlight);
        await small.CancelAsync();
        await large.CancelAsync();
    }

    [Fact]
    public async Task NewSessionAtSaturation_ReceivesFutureSlotsWithoutPreemption()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 6);
        var established = harness.StartSession(segmentCount: 30);
        await WaitUntilAsync(() => established.Executor.InvocationCount == 6);
        var newcomer = harness.StartSession(segmentCount: 30);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 2);

        Assert.Equal(0, newcomer.Executor.InvocationCount);
        for (var index = 0; index < 3; index++)
        {
            established.Executor.CompleteOne(Exists());
            var expected = index + 1;
            await WaitUntilAsync(() => newcomer.Executor.InvocationCount == expected);
        }

        var active = harness.Scheduler.GetSnapshot().Sessions
            .ToDictionary(session => session.RunId, session => session.InFlight);
        Assert.Equal(3, active[established.RunId]);
        Assert.Equal(3, active[newcomer.RunId]);
        await established.CancelAsync();
        await newcomer.CancelAsync();
    }

    [Fact]
    public async Task QueueWaiter_WinsBeforeAnonymousSchedulerAdmission()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 1);
        using var active = await harness.Gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var session = harness.StartSession(segmentCount: 10);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().PendingAdmissions == 1);
        var queue = harness.Gate.AcquireAsync(
            HealthCheckAdmissionPriority.Queue, CancellationToken.None);

        active.Dispose();
        using var queueLease = await queue.WaitAsync(TestTimeout);
        Assert.Equal(0, session.Executor.InvocationCount);

        queueLease.Dispose();
        await WaitUntilAsync(() => session.Executor.InvocationCount == 1);
        await session.CancelAsync();
    }

    [Fact]
    public async Task FailFastMissing_CancelsAndDrainsOnlyOwningSession()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 4);
        var failing = harness.StartSession(segmentCount: 20, HealthCheckStatMode.VerifyAll);
        var healthy = harness.StartSession(segmentCount: 20, HealthCheckStatMode.VerifyAll);
        await WaitUntilAsync(() => failing.Executor.InvocationCount > 0
                                   && healthy.Executor.InvocationCount > 0
                                   && harness.Scheduler.GetSnapshot().ActiveAssignments == 4);

        failing.Executor.CompleteOne(Missing());
        await Assert.ThrowsAsync<UsenetArticleNotFoundException>(() => failing.Task);
        await WaitUntilAsync(() => healthy.Executor.InvocationCount == 4);

        Assert.Single(harness.Scheduler.GetSnapshot().Sessions);
        await healthy.CancelAsync();
    }

    [Fact]
    public async Task RequestCancellation_DrainsAssignmentsAndReleasesEveryGateLease()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 4);
        var session = harness.StartSession(segmentCount: 20);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);

        await session.CancelAsync();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 0);

        var snapshot = harness.Scheduler.GetSnapshot();
        Assert.Equal(1, snapshot.Cancellations);
        Assert.Equal(0, snapshot.Failures);
        Assert.Empty(snapshot.Sessions);
        Assert.Equal(0, harness.Gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task CollectMissing_ReturnsInputIndicesInOrder()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 4);
        var missing = new HashSet<string>(["segment-1", "segment-3"]);
        var runId = Guid.NewGuid();
        var segments = Enumerable.Range(0, 5).Select(index => $"segment-{index}").ToArray();
        var result = await harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                runId, Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (segment, _) => Task.FromResult(missing.Contains(segment) ? Missing() : Exists()),
            progress: null,
            CancellationToken.None);

        Assert.Equal([1, 3], result.MissingIndices);
        Assert.Equal(5, result.Completed);
    }

    [Fact]
    public async Task HugeSession_CreatesOnlyCapacityBoundedExecutions()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 5);
        var session = harness.StartSession(segmentCount: 100_000);

        await WaitUntilAsync(() => session.Executor.InvocationCount == 5);

        Assert.Equal(5, harness.Scheduler.GetSnapshot().ActiveAssignments);
        Assert.Equal(0, harness.Scheduler.GetSnapshot().PendingAdmissions);
        await session.CancelAsync();
    }

    [Fact]
    public async Task LoweringAndRaisingLimit_DrainsThenFillsWithoutPreemption()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 3);
        var session = harness.StartSession(segmentCount: 20);
        await WaitUntilAsync(() => session.Executor.InvocationCount == 3);

        harness.SetLimit(1);
        session.Executor.CompleteOne(Exists());
        session.Executor.CompleteOne(Exists());
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 1);
        Assert.Equal(3, session.Executor.InvocationCount);

        harness.SetLimit(4);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);
        Assert.Equal(6, session.Executor.InvocationCount);
        await session.CancelAsync();
    }

    private static UsenetStatResponse Exists() => new()
    {
        ResponseCode = 223,
        ResponseMessage = "223 exists",
        ArticleExists = true,
    };

    private static UsenetStatResponse Missing() => new()
    {
        ResponseCode = 430,
        ResponseMessage = "430 missing",
        ArticleExists = false,
    };

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        var deadline = DateTimeOffset.UtcNow + TestTimeout;
        while (!predicate())
        {
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException("Condition was not reached before the test timeout.");
            await Task.Delay(10);
        }
    }

    private sealed class ControlledExecutor
    {
        private readonly ConcurrentQueue<TaskCompletionSource<UsenetStatResponse>> _pending = new();
        private int _invocationCount;

        public int InvocationCount => Volatile.Read(ref _invocationCount);

        public async Task<UsenetStatResponse> ExecuteAsync(string _, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _invocationCount);
            var completion = new TaskCompletionSource<UsenetStatResponse>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _pending.Enqueue(completion);
            return await completion.Task.WaitAsync(cancellationToken);
        }

        public void CompleteOne(UsenetStatResponse response)
        {
            Assert.True(_pending.TryDequeue(out var completion));
            completion.SetResult(response);
        }
    }

    private sealed class RunningSession(
        Guid runId,
        ControlledExecutor executor,
        CancellationTokenSource cancellation,
        Task<HealthCheckStatResult> task)
    {
        public Guid RunId { get; } = runId;
        public ControlledExecutor Executor { get; } = executor;
        public Task<HealthCheckStatResult> Task { get; } = task;

        public async Task CancelAsync()
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task);
            cancellation.Dispose();
        }
    }

    private sealed class SchedulerHarness : IAsyncDisposable
    {
        private SchedulerHarness(
            ConfigManager config,
            HealthCheckConnectionGate gate,
            HealthCheckStatScheduler scheduler)
        {
            Config = config;
            Gate = gate;
            Scheduler = scheduler;
        }

        public ConfigManager Config { get; }
        public HealthCheckConnectionGate Gate { get; }
        public HealthCheckStatScheduler Scheduler { get; }

        public static async Task<SchedulerHarness> CreateAsync(int limit)
        {
            var config = CreateConfig(limit);
            var gate = new HealthCheckConnectionGate(config);
            var scheduler = new HealthCheckStatScheduler(config, gate);
            await scheduler.StartAsync(CancellationToken.None);
            return new SchedulerHarness(config, gate, scheduler);
        }

        public RunningSession StartSession(
            int segmentCount,
            HealthCheckStatMode mode = HealthCheckStatMode.CollectMissing)
        {
            var runId = Guid.NewGuid();
            var executor = new ControlledExecutor();
            var cancellation = new CancellationTokenSource();
            var task = Scheduler.RunAsync(
                new HealthCheckStatRequest(
                    runId,
                    Guid.NewGuid(),
                    0,
                    Enumerable.Range(0, segmentCount).Select(index => $"{runId:N}-{index}").ToArray(),
                    mode),
                executor.ExecuteAsync,
                progress: null,
                cancellation.Token);
            return new RunningSession(runId, executor, cancellation, task);
        }

        public void SetLimit(int limit)
        {
            Config.UpdateValues([
                new ConfigItem
                {
                    ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                    ConfigValue = limit.ToString(),
                },
            ]);
        }

        public async ValueTask DisposeAsync()
        {
            await Scheduler.StopAsync(CancellationToken.None);
            Scheduler.Dispose();
            Gate.Dispose();
        }

        private static ConfigManager CreateConfig(int limit)
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
                                Host = "scheduler.example",
                                Port = 563,
                                UseSsl = true,
                                User = "user",
                                Pass = "pass",
                                MaxConnections = 200,
                            },
                        ],
                    }),
                },
            ]);
            config.UpdateValues([
                new ConfigItem
                {
                    ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                    ConfigValue = limit.ToString(),
                },
            ]);
            return config;
        }
    }
}
