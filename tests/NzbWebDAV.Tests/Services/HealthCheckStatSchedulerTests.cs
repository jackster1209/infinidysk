using System.Collections.Concurrent;
using System.Text.Json;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Tests.Clients.Usenet;
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
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 5);
        Assert.Equal(0, harness.Scheduler.GetSnapshot().PendingAdmissions);

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
    public async Task ProviderCapacity_BoundsActiveAssignmentsWithoutMetadataWaiters()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 20,
            new ProviderDefinition("provider-a", ConnectionLimit: 5, TransferLimit: 2));
        var heldGlobal = new List<HealthCheckConnectionGate.Lease>();
        for (var index = 0; index < 20; index++)
        {
            heldGlobal.Add(await harness.Gate.AcquireAsync(
                HealthCheckAdmissionPriority.Queue,
                CancellationToken.None));
        }
        var sessions = Enumerable.Range(0, 3)
            .Select(_ => harness.StartSession(segmentCount: 30, providerKey: "provider-a"))
            .ToArray();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 3);

        foreach (var lease in heldGlobal)
            lease.Dispose();

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);

        var scheduler = harness.Scheduler.GetSnapshot();
        var admission = Assert.IsType<ProviderConnectionAdmissionSnapshot>(
            harness.GetProviderAdmission("provider-a"));
        Assert.Equal(4, scheduler.ActiveAssignments);
        Assert.Equal(4, harness.Gate.GetSnapshot().Active);
        Assert.Equal(4, admission.ActiveMetadataOperations);
        Assert.Equal(0, admission.WaitingMetadataOperations);
        Assert.Equal([1, 1, 2], scheduler.Sessions.Select(x => x.InFlight).Order().ToArray());

        foreach (var session in sessions)
            await session.CancelAsync();
    }

    [Fact]
    public async Task IndependentProviders_UseTheirExecutableCapacityConcurrently()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 20,
            new ProviderDefinition("provider-a", ConnectionLimit: 5, TransferLimit: 2),
            new ProviderDefinition("provider-b", ConnectionLimit: 4, TransferLimit: 2));
        var providerA = harness.StartSession(segmentCount: 30, providerKey: "provider-a");
        var providerB = harness.StartSession(segmentCount: 30, providerKey: "provider-b");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 7);

        Assert.Equal(4, harness.GetProviderAdmission("provider-a")?.ActiveMetadataOperations);
        Assert.Equal(3, harness.GetProviderAdmission("provider-b")?.ActiveMetadataOperations);
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.WaitingMetadataOperations);
        Assert.Equal(0, harness.GetProviderAdmission("provider-b")?.WaitingMetadataOperations);
        Assert.Equal(7, harness.Gate.GetSnapshot().Active);

        await providerA.CancelAsync();
        await providerB.CancelAsync();
    }

    [Fact]
    public async Task ExplicitGlobalCeiling_ConstrainsAggregateProviderCapacity()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 5,
            new ProviderDefinition("provider-a", ConnectionLimit: 5, TransferLimit: 2),
            new ProviderDefinition("provider-b", ConnectionLimit: 4, TransferLimit: 2));
        var providerA = harness.StartSession(segmentCount: 30, providerKey: "provider-a");
        var providerB = harness.StartSession(segmentCount: 30, providerKey: "provider-b");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 5);

        var sessions = harness.Scheduler.GetSnapshot().Sessions;
        Assert.Equal(5, harness.Gate.GetSnapshot().Active);
        Assert.All(sessions, session => Assert.True(session.InFlight > 0));
        Assert.Equal(5, sessions.Sum(session => session.InFlight));
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.WaitingMetadataOperations);
        Assert.Equal(0, harness.GetProviderAdmission("provider-b")?.WaitingMetadataOperations);

        await providerA.CancelAsync();
        await providerB.CancelAsync();
    }

    [Fact]
    public async Task LegacySharedPool_UsesBoundedCompatibilityPath()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 5,
            new ProviderDefinition("legacy", ConnectionLimit: 8, TransferLimit: null));
        var session = harness.StartSession(segmentCount: 30, providerKey: "legacy");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 5);

        Assert.Equal(5, harness.Gate.GetSnapshot().Active);
        Assert.Null(harness.GetProviderAdmission("legacy"));
        Assert.Equal(0, harness.Scheduler.GetSnapshot().PendingAdmissions);
        await session.CancelAsync();
    }

    [Fact]
    public async Task LegacySharedPool_IsBoundedByPhysicalPoolWidth()
    {
        // Auto-style ceiling well above the provider: the physical pool, not the ceiling,
        // has to be what stops the scheduler, or legacy providers regain the old failure.
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 50,
            new ProviderDefinition("legacy", ConnectionLimit: 4, TransferLimit: null));
        var session = harness.StartSession(segmentCount: 200, providerKey: "legacy");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);
        await Task.Delay(150);

        Assert.Equal(4, harness.Scheduler.GetSnapshot().ActiveAssignments);
        Assert.Equal(4, harness.Gate.GetSnapshot().Active);
        Assert.Null(harness.GetProviderAdmission("legacy"));
        await session.CancelAsync();
    }

    [Fact]
    public async Task Snapshot_AttributesCapacityAndBlockingToTheRightProvider()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 50,
            new ProviderDefinition("provider-a", ConnectionLimit: 6, TransferLimit: 3),
            new ProviderDefinition("provider-b", ConnectionLimit: 8, TransferLimit: 4));
        var sessionA = harness.StartSession(segmentCount: 200, providerKey: "provider-a");
        var sessionB = harness.StartSession(segmentCount: 200, providerKey: "provider-b");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Providers.Count == 2);
        await WaitUntilAsync(() =>
        {
            var providers = harness.Scheduler.GetSnapshot().Providers;
            return providers.All(provider => provider.ActiveAssignments > 0);
        });

        var snapshot = harness.Scheduler.GetSnapshot();
        var byProvider = snapshot.Providers.ToDictionary(provider => provider.ProviderKey);
        var capacityA = harness.GetProviderAdmission("provider-a")!.MaxMetadataCapacity;
        var capacityB = harness.GetProviderAdmission("provider-b")!.MaxMetadataCapacity;

        // Each provider is bounded by its own metadata capacity, not by the shared ceiling,
        // and saturation is reported against the provider rather than the ceiling.
        Assert.Equal(capacityA, byProvider["provider-a"].ActiveAssignments);
        Assert.Equal(capacityB, byProvider["provider-b"].ActiveAssignments);
        Assert.Equal(capacityA + capacityB, snapshot.ActiveAssignments);
        Assert.True(capacityA + capacityB < 50, "the ceiling must not be the binding limit");
        Assert.Equal(0, snapshot.GlobalBlockedSessions);
        Assert.Equal(0, snapshot.LegacyCompatibilityAssignments);
        Assert.All(snapshot.Providers, provider => Assert.False(provider.IsLegacySharedPool));

        await sessionA.CancelAsync();
        await sessionB.CancelAsync();
    }

    [Fact]
    public async Task Snapshot_ReportsLegacyAssignmentsAsCompatibilityWork()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 50,
            new ProviderDefinition("legacy", ConnectionLimit: 3, TransferLimit: null));
        var session = harness.StartSession(segmentCount: 100, providerKey: "legacy");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 3);

        var snapshot = harness.Scheduler.GetSnapshot();
        Assert.Equal(3, snapshot.LegacyCompatibilityAssignments);
        var legacy = Assert.Single(snapshot.Providers);
        Assert.True(legacy.IsLegacySharedPool);
        Assert.Equal(3, legacy.ActiveAssignments);

        await session.CancelAsync();
    }

    [Fact]
    public async Task ProviderGrantedButCeilingFull_ReleasesProviderLeaseWithoutSpinning()
    {
        // The scheduler must never hold provider capacity while the aggregate ceiling is
        // full, and must not retry in a loop: only a gate release can make it executable.
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 2,
            new ProviderDefinition("provider-a", ConnectionLimit: 8, TransferLimit: 2));
        var blocking = new List<HealthCheckConnectionGate.Lease>();
        for (var index = 0; index < 2; index++)
        {
            blocking.Add(await harness.Gate.AcquireAsync(
                HealthCheckAdmissionPriority.Queue, CancellationToken.None));
        }

        var session = harness.StartSession(segmentCount: 100, providerKey: "provider-a");
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 1);
        await Task.Delay(200);

        var blocked = harness.Scheduler.GetSnapshot();
        Assert.Equal(0, blocked.ActiveAssignments);
        Assert.Equal(0, session.Executor.InvocationCount);
        // The provider lease was handed back rather than held or leaked behind the ceiling.
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.ActiveMetadataOperations);
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.WaitingMetadataOperations);
        // No dispatch was recorded, so the blocked pass did not spin through reconciliation.
        Assert.Equal(0, blocked.Dispatches);

        foreach (var lease in blocking) lease.Dispose();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 2);

        await session.CancelAsync();
    }

    [Fact]
    public async Task LegacyDiagnostics_ReportSharedPoolEvenWithNoActiveAssignments()
    {
        // Shared-pool status is a property of the provider generation, not of whatever is
        // running, so it must not flicker as assignments come and go.
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 50,
            new ProviderDefinition("legacy", ConnectionLimit: 1, TransferLimit: null));

        // Take the provider's only physical permit so the scheduler can never dispatch.
        using var blocking = harness.BorrowProviderSlot("legacy");

        var session = harness.StartSession(segmentCount: 50, providerKey: "legacy");
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Providers.Count == 1);
        await Task.Delay(150);

        var snapshot = harness.Scheduler.GetSnapshot();
        var legacy = Assert.Single(snapshot.Providers);
        Assert.Equal(0, legacy.ActiveAssignments);
        Assert.Equal(0, snapshot.LegacyCompatibilityAssignments);
        // Zero activity, still unambiguously a shared-pool provider.
        Assert.True(legacy.IsLegacySharedPool);

        await session.CancelAsync();
    }

    [Fact]
    public async Task Cancellation_ReleasesProviderAndGlobalLeases()
    {
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 20,
            new ProviderDefinition("provider-a", ConnectionLimit: 5, TransferLimit: 2));
        var session = harness.StartSession(segmentCount: 30, providerKey: "provider-a");
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);

        await session.CancelAsync();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 0);

        Assert.Equal(0, harness.Gate.GetSnapshot().Active);
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.ActiveMetadataOperations);
        Assert.Equal(0, harness.GetProviderAdmission("provider-a")?.WaitingMetadataOperations);
    }

    [Fact]
    public async Task SessionSnapshot_ExposesTargetProvider()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 1);
        var session = harness.StartSession(segmentCount: 10, providerKey: "provider-a");

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 1);

        Assert.Equal("provider-a", Assert.Single(harness.Scheduler.GetSnapshot().Sessions).ProviderKey);
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
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 2);
        Assert.Equal(0, harness.Scheduler.GetSnapshot().PendingAdmissions);

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
        await using var harness = await SchedulerHarness.CreateAsync(
            limit: 6,
            new ProviderDefinition("provider-a", ConnectionLimit: 8, TransferLimit: 4));
        var established = harness.StartSession(segmentCount: 30, providerKey: "provider-a");
        await WaitUntilAsync(() => established.Executor.InvocationCount == 6);
        var newcomer = harness.StartSession(segmentCount: 30, providerKey: "provider-a");
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 2);

        Assert.Equal(0, newcomer.Executor.InvocationCount);
        for (var index = 0; index < 3; index++)
        {
            established.Executor.CompleteOne();
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
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 1);
        Assert.Equal(0, harness.Scheduler.GetSnapshot().PendingAdmissions);
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
        await WaitUntilAsync(() => failing.Executor.InvocationCount == 4);

        // A lone session borrows the whole ceiling and is never preempted, so hand the
        // newcomer its share by retiring two of the first session's assignments.
        var healthy = harness.StartSession(segmentCount: 20, HealthCheckStatMode.VerifyAll);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 2);
        failing.Executor.CompleteOne();
        failing.Executor.CompleteOne();
        await WaitUntilAsync(() => healthy.Executor.InvocationCount == 2
                                   && harness.Scheduler.GetSnapshot().ActiveAssignments == 4);

        failing.Executor.FailOne(new UsenetArticleNotFoundException("segment", "430 missing"));
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
            (chunk, _, _) => Task.FromResult<IReadOnlyList<string>>(
                chunk.Where(missing.Contains).ToArray()),
            progress: null,
            CancellationToken.None);

        Assert.Equal([1, 3], result.MissingIndices);
        Assert.Equal(5, result.Completed);
    }

    [Fact]
    public async Task DetailedSweep_KeepsUnansweredIndicesSeparateFromDefinitiveMisses()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 2);
        var segments = Enumerable.Range(0, 5).Select(index => $"segment-{index}").ToArray();

        var result = await harness.Scheduler.RunDetailedAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (chunk, _, _) => Task.FromResult(new HealthCheckStatChunkResult(
                chunk.Where(id => id == "segment-1").ToArray(),
                chunk.Where(id => id == "segment-3").ToArray())),
            progress: null,
            CancellationToken.None);

        Assert.Equal([1], result.MissingIndices);
        Assert.Equal([3], result.UnansweredIndices);
        Assert.Equal(5, result.Completed);
    }

    [Fact]
    public async Task DetailedSweep_MapsLogicalVerdictsToEveryDuplicateOccurrence()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 1);
        harness.Scheduler.ChunkSizeOverride = 10;
        string[] segments = ["present", "missing", "missing", "unanswered", "unanswered"];

        var result = await harness.Scheduler.RunDetailedAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (_, _, _) => Task.FromResult(new HealthCheckStatChunkResult(
                ["missing"],
                ["unanswered"])),
            progress: null,
            CancellationToken.None);

        Assert.Equal([1, 2], result.MissingIndices);
        Assert.Equal([3, 4], result.UnansweredIndices);
        Assert.Equal(segments.Length, result.Completed);
    }

    [Fact]
    public async Task HugeSession_CreatesOnlyCapacityBoundedExecutions()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 5);
        var session = harness.StartSession(segmentCount: 100_000);

        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 5);

        Assert.Equal(5, harness.Scheduler.GetSnapshot().ActiveAssignments);
        Assert.Equal(5, session.Executor.InvocationCount);
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
        session.Executor.CompleteOne();
        session.Executor.CompleteOne();
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 1);
        Assert.Equal(3, session.Executor.InvocationCount);

        harness.SetLimit(4);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().ActiveAssignments == 4);
        Assert.Equal(6, session.Executor.InvocationCount);
        await session.CancelAsync();
    }

    [Fact]
    public async Task OutOfMemoryInAssignment_FailsSessionInsteadOfReportingVerified()
    {
        // A hard OOM is deliberately not handled inside the assignment, but it must still
        // fail the session. Publishing an unfinished chunk as a clean result would let a
        // file be recorded Healthy on segments that were never verified.
        await using var harness = await SchedulerHarness.CreateAsync(limit: 2);

        var segments = Enumerable.Range(0, 4).Select(index => $"segment-{index}").ToArray();
        var run = harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (_, _, _) => throw new OutOfMemoryException("simulated allocation failure"),
            progress: null,
            CancellationToken.None);

        await Assert.ThrowsAsync<OutOfMemoryException>(() => run);
        await WaitUntilAsync(() => harness.Scheduler.GetSnapshot().Sessions.Count == 0);
        Assert.Equal(0, harness.Gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task AssignmentFailure_SurfacesInsteadOfCompletingSilently()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 1);

        var segments = Enumerable.Range(0, 4).Select(index => $"segment-{index}").ToArray();
        var run = harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (_, _, _) => throw new UsenetUnexpectedResponseException("segment-0", "500 unexpected"),
            progress: null,
            CancellationToken.None);

        await Assert.ThrowsAsync<UsenetUnexpectedResponseException>(() => run);
        Assert.Equal(0, harness.Gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task SuccessfulSweep_AccountsForEverySegment()
    {
        // The success path asserts Completed == Total, so per-assignment accounting has to
        // add up exactly; a short-counting assignment fails the sweep instead of passing it.
        await using var harness = await SchedulerHarness.CreateAsync(limit: 3);

        var segments = Enumerable.Range(0, 50).Select(index => $"segment-{index}").ToArray();
        var result = await harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (chunk, _, _) => Task.FromResult<IReadOnlyList<string>>(
                chunk.Where(id => id == "segment-49").ToArray()),
            progress: null,
            CancellationToken.None);

        Assert.Equal(segments.Length, result.Completed);
        Assert.Equal([49], result.MissingIndices);
    }

    [Fact]
    public async Task DefaultChunking_DispatchesBatchesNotSingleSegments()
    {
        // The whole point of chunking: one gate lease must carry a batch the client can
        // pipeline. A per-segment dispatch caps a sweep at concurrency / round-trip.
        await using var harness = await SchedulerHarness.CreateAsync(limit: 4);
        harness.Scheduler.ChunkSizeOverride = null;

        var observed = new ConcurrentQueue<int>();
        var segments = Enumerable.Range(0, 4000).Select(index => $"segment-{index}").ToArray();
        var result = await harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (chunk, _, _) =>
            {
                observed.Enqueue(chunk.Count);
                return Task.FromResult<IReadOnlyList<string>>(
                    chunk.Contains("segment-2500") ? ["segment-2500"] : []);
            },
            progress: null,
            CancellationToken.None);

        Assert.All(observed, count => Assert.InRange(
            count, 1, HealthCheckStatScheduler.MaximumChunkSize));
        Assert.Contains(observed, count => count > 1);
        // Batched dispatch must cover every segment exactly once.
        Assert.Equal(segments.Length, observed.Sum());
        Assert.Equal(segments.Length, result.Completed);
        Assert.Equal([2500], result.MissingIndices);
    }

    [Fact]
    public async Task ChunkMissingIds_MapToAbsoluteSessionIndices()
    {
        await using var harness = await SchedulerHarness.CreateAsync(limit: 1);
        harness.Scheduler.ChunkSizeOverride = 10;

        var segments = Enumerable.Range(0, 30).Select(index => $"segment-{index}").ToArray();
        var missing = new HashSet<string>(["segment-3", "segment-14", "segment-29"]);
        var result = await harness.Scheduler.RunAsync(
            new HealthCheckStatRequest(
                Guid.NewGuid(), Guid.NewGuid(), 0, segments, HealthCheckStatMode.CollectMissing),
            (chunk, _, _) => Task.FromResult<IReadOnlyList<string>>(
                chunk.Where(missing.Contains).ToArray()),
            progress: null,
            CancellationToken.None);

        // Offsets are chunk-relative; the scheduler must rebase them onto the session.
        Assert.Equal([3, 14, 29], result.MissingIndices);
        Assert.Equal(30, result.Completed);
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
        private readonly ConcurrentQueue<TaskCompletionSource<IReadOnlyList<string>>> _pending = new();
        private int _invocationCount;

        public int InvocationCount => Volatile.Read(ref _invocationCount);

        public async Task<IReadOnlyList<string>> ExecuteAsync(
            IReadOnlyList<string> segmentIds,
            IProgress<int>? progress,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _invocationCount);
            var completion = new TaskCompletionSource<IReadOnlyList<string>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _pending.Enqueue(completion);
            var missing = await completion.Task.WaitAsync(cancellationToken);
            progress?.Report(segmentIds.Count);
            return missing;
        }

        /// <summary>Completes the next chunk with every segment present.</summary>
        public void CompleteOne()
        {
            Assert.True(_pending.TryDequeue(out var completion));
            completion.SetResult([]);
        }

        /// <summary>Completes the next chunk reporting the given ids as confirmed missing.</summary>
        public void CompleteOneMissing(params string[] missingIds)
        {
            Assert.True(_pending.TryDequeue(out var completion));
            completion.SetResult(missingIds);
        }

        /// <summary>Fails the next chunk. VerifyAll signals a definitive miss by throwing.</summary>
        public void FailOne(Exception exception)
        {
            Assert.True(_pending.TryDequeue(out var completion));
            completion.SetException(exception);
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
        private readonly HealthCheckProviderAdmissionRegistry _providerAdmissions;
        private readonly MultiProviderNntpClient? _providerClient;

        private SchedulerHarness(
            ConfigManager config,
            HealthCheckConnectionGate gate,
            HealthCheckProviderAdmissionRegistry providerAdmissions,
            HealthCheckStatScheduler scheduler,
            MultiProviderNntpClient? providerClient)
        {
            Config = config;
            Gate = gate;
            _providerAdmissions = providerAdmissions;
            Scheduler = scheduler;
            _providerClient = providerClient;
        }

        public ConfigManager Config { get; }
        public HealthCheckConnectionGate Gate { get; }
        public HealthCheckStatScheduler Scheduler { get; }

        public static async Task<SchedulerHarness> CreateAsync(
            int limit,
            params ProviderDefinition[] providers)
        {
            var config = CreateConfig(limit);
            var gate = new HealthCheckConnectionGate(config);
            var providerAdmissions = new HealthCheckProviderAdmissionRegistry();
            MultiProviderNntpClient? providerClient = null;
            if (providers.Length > 0)
            {
                providerClient = CreateProviderClient(providers);
                providerAdmissions.Activate(providerClient.HealthAdmissionGeneration);
            }
            var scheduler = new HealthCheckStatScheduler(config, gate, providerAdmissions)
            {
                // These tests assert per-segment fair-share rotation; pin one segment per
                // dispatch so chunking does not change the expected invocation counts.
                ChunkSizeOverride = 1,
            };
            await scheduler.StartAsync(CancellationToken.None);
            return new SchedulerHarness(
                config,
                gate,
                providerAdmissions,
                scheduler,
                providerClient);
        }

        public ProviderConnectionAdmissionSnapshot? GetProviderAdmission(string providerKey) =>
            _providerAdmissions.GetSnapshot(providerKey)?.Admission;

        /// <summary>
        /// Holds one of the provider's executable slots so the scheduler cannot dispatch,
        /// letting a test observe a provider that is known but idle.
        /// </summary>
        public HealthCheckProviderLease BorrowProviderSlot(string providerKey)
        {
            var attempt = _providerAdmissions.TryAcquireMetadata(
                providerKey, SemaphorePriority.Low);
            Assert.Equal(HealthCheckProviderAdmissionState.Acquired, attempt.State);
            return attempt.Lease!;
        }

        public RunningSession StartSession(
            int segmentCount,
            HealthCheckStatMode mode = HealthCheckStatMode.CollectMissing,
            string? providerKey = null)
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
                    mode,
                    providerKey),
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
            _providerClient?.Dispose();
            _providerAdmissions.Dispose();
            Gate.Dispose();
        }

        private static MultiProviderNntpClient CreateProviderClient(
            IReadOnlyList<ProviderDefinition> providers)
        {
            var clients = providers.Select(provider =>
            {
                var connection = new MultiProviderNntpClientTests.ScriptedNntpClient
                {
                    BatchResponseCode = 430,
                };
                var pool = new ConnectionPool<INntpClient>(
                    provider.ConnectionLimit,
                    _ => ValueTask.FromResult<INntpClient>(connection));
                return new MultiConnectionNntpClient(
                    pool,
                    ProviderType.Pooled,
                    new ProviderCircuitBreaker(provider.Key),
                    provider.Key,
                    metricsKey: provider.Key,
                    maxTransferConnections: provider.TransferLimit);
            }).ToList();
            return new MultiProviderNntpClient(clients);
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

    private sealed record ProviderDefinition(
        string Key,
        int ConnectionLimit,
        int? TransferLimit);
}
