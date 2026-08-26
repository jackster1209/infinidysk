using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Extensions;
using Serilog;
using UsenetSharp.Models;

namespace NzbWebDAV.Services;

public enum HealthCheckStatMode
{
    VerifyAll,
    CollectMissing,
}

public sealed record HealthCheckStatRequest(
    Guid RunId,
    Guid DavItemId,
    int PhaseId,
    IReadOnlyList<string> SegmentIds,
    HealthCheckStatMode Mode,
    string? ProviderKey = null);

public sealed record HealthCheckStatResult(
    IReadOnlyList<int> MissingIndices,
    int Completed)
{
    public IReadOnlyList<int> UnansweredIndices { get; init; } = [];
}

/// <summary>
/// Classifies logical ids returned by a chunk executor. Each verdict applies to every
/// matching occurrence in that executor's input chunk.
/// </summary>
public sealed record HealthCheckStatChunkResult(
    IReadOnlyList<string> MissingIds,
    IReadOnlyList<string> UnansweredIds);

/// <summary>
/// Runs one contiguous chunk of segment ids and returns the confirmed-missing ids from it.
/// Chunking is what lets the underlying client pipeline STATs on a single connection: a
/// per-segment executor pays a full round-trip each time, which caps a sweep at
/// concurrency / RTT regardless of how much pool capacity is free.
/// VerifyAll executors signal a miss by throwing instead of returning it.
/// </summary>
public delegate Task<IReadOnlyList<string>> HealthCheckStatChunkExecutor(
    IReadOnlyList<string> segmentIds,
    IProgress<int>? progress,
    CancellationToken cancellationToken);

public delegate Task<HealthCheckStatChunkResult> HealthCheckStatDetailedChunkExecutor(
    IReadOnlyList<string> segmentIds,
    IProgress<int>? progress,
    CancellationToken cancellationToken);

public sealed record HealthCheckStatSessionSnapshot(
    Guid RunId,
    Guid DavItemId,
    int PhaseId,
    string? ProviderKey,
    HealthCheckStatMode Mode,
    string State,
    int InFlight,
    int Completed,
    int Total);

/// <summary>
/// Per-provider scheduler state. Providers are the resource health work is allocated from,
/// so aggregate counters alone cannot show whether a provider is saturated or merely idle
/// because no session currently targets it.
/// </summary>
public sealed record HealthCheckStatProviderSnapshot(
    string ProviderKey,
    int ActiveAssignments,
    int RunnableSessions,
    long PendingSegments,
    /// <summary>Runnable sessions held back because this provider cannot admit more work.</summary>
    int BlockedSessions,
    bool IsLegacySharedPool);

public sealed record HealthCheckStatSchedulerSnapshot(
    /// <summary>Explicit aggregate ceiling, or null in Auto (provider-aware) mode.</summary>
    int? Capacity,
    int ActiveAssignments,
    int PendingAdmissions,
    int RunnableSessions,
    long PendingSegments,
    long Dispatches,
    long Completions,
    long Cancellations,
    long Failures,
    IReadOnlyList<HealthCheckStatSessionSnapshot> Sessions,
    IReadOnlyList<HealthCheckStatProviderSnapshot> Providers,
    /// <summary>Runnable sessions held back by the explicit aggregate ceiling.</summary>
    int GlobalBlockedSessions,
    /// <summary>Active assignments backed by a legacy shared-pool permit.</summary>
    int LegacyCompatibilityAssignments);

/// <summary>
/// Fairly assigns background health-check STAT work only after its target provider and
/// the explicit aggregate gate can both execute it without queuing at either layer.
/// All scheduler state is serialized through one channel reader; execution tasks only post results.
/// </summary>
public sealed class HealthCheckStatScheduler : BackgroundService
{
    private readonly ConfigManager _configManager;
    private readonly HealthCheckConnectionGate _gate;
    private readonly HealthCheckProviderAdmissionRegistry _providerAdmissions;
    private readonly bool _ownsProviderAdmissions;
    private readonly Channel<SchedulerEvent> _events = Channel.CreateUnbounded<SchedulerEvent>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    private readonly Dictionary<Guid, SessionState> _sessions = [];
    private readonly Dictionary<Guid, WorkAssignment> _assignments = [];
    private HealthCheckStatSchedulerSnapshot _snapshot = EmptySnapshot;
    private CancellationToken _stoppingToken;
    private long _dispatchSequence;
    private long _dispatches;
    private long _completions;
    private long _cancellations;
    private long _failures;
    private bool _shuttingDown;
    private bool _globalBlocked;
    private HashSet<string> _blockedProviders = new(StringComparer.Ordinal);

    private static readonly HealthCheckStatSchedulerSnapshot EmptySnapshot = new(
        null, 0, 0, 0, 0, 0, 0, 0, 0, [], [], 0, 0);

    // A chunk is one pipelined batch on one connection. Too small and the round-trip
    // dominates again; too large and a slow chunk delays the fair-share rotation.
    internal const int MinimumChunkSize = 32;
    internal const int MaximumChunkSize = 256;

    /// <summary>Pins the dispatch chunk size. Tests use 1 to assert per-segment fairness.</summary>
    internal int? ChunkSizeOverride { get; set; }

    public HealthCheckStatScheduler(
        ConfigManager configManager,
        HealthCheckConnectionGate gate,
        HealthCheckProviderAdmissionRegistry? providerAdmissions = null)
    {
        _configManager = configManager;
        _gate = gate;
        _providerAdmissions = providerAdmissions ?? new HealthCheckProviderAdmissionRegistry();
        _ownsProviderAdmissions = providerAdmissions is null;
        _configManager.OnConfigChanged += OnConfigChanged;
        _gate.AvailabilityChanged += OnGlobalAvailabilityChanged;
        _providerAdmissions.AvailabilityChanged += OnProviderAvailabilityChanged;
        PublishSnapshot();
    }

    public Task<HealthCheckStatResult> RunAsync(
        HealthCheckStatRequest request,
        HealthCheckStatChunkExecutor executor,
        IProgress<int>? progress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(executor);
        return RunDetailedAsync(
            request,
            async (segmentIds, chunkProgress, chunkCancellationToken) => new HealthCheckStatChunkResult(
                await executor(segmentIds, chunkProgress, chunkCancellationToken).ConfigureAwait(false),
                []),
            progress,
            cancellationToken);
    }

    public Task<HealthCheckStatResult> RunDetailedAsync(
        HealthCheckStatRequest request,
        HealthCheckStatDetailedChunkExecutor executor,
        IProgress<int>? progress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(executor);
        ArgumentNullException.ThrowIfNull(request.SegmentIds);

        var sessionId = Guid.NewGuid();
        var completion = new TaskCompletionSource<HealthCheckStatResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = new RegisterSession(
            sessionId, request, executor, progress, completion, cancellationToken);
        if (!_events.Writer.TryWrite(registration))
            throw new InvalidOperationException("The health-check STAT scheduler is not accepting work.");

        var cancellationRegistration = cancellationToken.Register(
            () => _events.Writer.TryWrite(new CancelSession(sessionId, cancellationToken)));
        return AwaitCompletionAsync(completion.Task, cancellationRegistration);
    }

    public HealthCheckStatSchedulerSnapshot GetSnapshot() => Volatile.Read(ref _snapshot);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        using var stoppingRegistration = stoppingToken.Register(
            () => _events.Writer.TryWrite(StopScheduler.Instance));

        try
        {
            // Shutdown is delivered as an event so the reader can continue draining active
            // assignments after the hosted-service token is cancelled.
            while (await _events.Reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                while (_events.Reader.TryRead(out var schedulerEvent))
                    HandleEvent(schedulerEvent);

                ReconcileAssignments();
                PublishSnapshot();
                if (_shuttingDown
                    && _sessions.Count == 0
                    && _assignments.Count == 0)
                    break;
            }
        }
        finally
        {
            _events.Writer.TryComplete();
            foreach (var session in _sessions.Values)
            {
                await session.Cancellation.CancelAsync().ConfigureAwait(false);
                session.Completion.TrySetCanceled(stoppingToken);
                session.Cancellation.Dispose();
            }
            _sessions.Clear();
            PublishSnapshot();
        }
    }

    private static async Task<HealthCheckStatResult> AwaitCompletionAsync(
        Task<HealthCheckStatResult> task,
        CancellationTokenRegistration registration)
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        finally
        {
            await registration.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void HandleEvent(SchedulerEvent schedulerEvent)
    {
        switch (schedulerEvent)
        {
            case RegisterSession registration:
                Register(registration);
                break;
            case CancelSession cancellation:
                Cancel(cancellation);
                break;
            case AssignmentFinished finished:
                CompleteAssignment(finished);
                break;
            case AssignmentProgress progress:
                AdvanceAssignmentProgress(progress);
                break;
            case GlobalAvailabilityChanged:
                _globalBlocked = false;
                break;
            case ProviderAvailabilityChanged:
            case ReconcileScheduler:
                break;
            case StopScheduler:
                BeginShutdown();
                break;
        }
    }

    private void Register(RegisterSession registration)
    {
        if (_shuttingDown)
        {
            registration.Completion.TrySetCanceled(_stoppingToken);
            return;
        }

        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            registration.CancellationToken,
            _stoppingToken);
        var session = new SessionState(registration, cancellation);
        _sessions.Add(registration.SessionId, session);
        if (registration.CancellationToken.IsCancellationRequested)
        {
            Cancel(new CancelSession(registration.SessionId, registration.CancellationToken));
            return;
        }

        TryCompleteSession(session);
    }

    private void Cancel(CancelSession cancellation)
    {
        if (!_sessions.TryGetValue(cancellation.SessionId, out var session)
            || session.Status is not SessionStatus.Running)
            return;

        session.Status = SessionStatus.Cancelling;
        session.CancellationToken = cancellation.CancellationToken;
        session.Cancellation.Cancel();
        TryCompleteSession(session);
    }

    private void Dispatch(
        SessionState session,
        HealthCheckProviderLease? providerLease,
        HealthCheckConnectionGate.Lease globalLease)
    {
        var offset = session.NextIndex;
        var length = NextChunkLength(session);
        session.NextIndex += length;
        session.InFlight++;
        session.LastDispatchSequence = ++_dispatchSequence;
        _dispatches++;
        var assignment = new WorkAssignment(
            Guid.NewGuid(),
            session,
            offset,
            length,
            providerLease,
            globalLease);
        _assignments.Add(assignment.Id, assignment);
        assignment.Execution = Observe(
            ExecuteAssignmentAsync(assignment),
            "Background health STAT assignment terminated abnormally");
    }

    private void CompleteAssignment(AssignmentFinished finished)
    {
        if (!_assignments.Remove(finished.Assignment.Id, out var assignment)) return;
        var session = assignment.Session;
        session.InFlight--;

        if (session.Status is SessionStatus.Running)
        {
            // Fold in whatever the chunk finished before it ended, so a failing or cancelled
            // batch still credits the segments it did verify.
            ApplyChunkProgress(assignment, finished.Processed);

            if (finished.Exception is not null)
            {
                if (finished.Exception is OperationCanceledException
                    && (session.RequestCancellationToken.IsCancellationRequested || _shuttingDown))
                {
                    session.Status = SessionStatus.Cancelling;
                    session.CancellationToken = session.RequestCancellationToken.IsCancellationRequested
                        ? session.RequestCancellationToken
                        : _stoppingToken;
                    session.Cancellation.Cancel();
                }
                else
                {
                    // VerifyAll executors surface a definitive miss by throwing
                    // UsenetArticleNotFoundException; that is the session's terminal result.
                    FailSession(session, finished.Exception);
                }
            }
            else
            {
                if (finished.MissingOffsets is { Count: > 0 } offsets)
                {
                    foreach (var offset in offsets)
                        session.MissingIndices.Add(assignment.Offset + offset);
                }
                if (finished.UnansweredOffsets is { Count: > 0 } unansweredOffsets)
                {
                    foreach (var offset in unansweredOffsets)
                        session.UnansweredIndices.Add(assignment.Offset + offset);
                }

                ReportProgress(session);
            }
        }

        TryCompleteSession(session);
    }

    private void ApplyChunkProgress(WorkAssignment assignment, int processedInChunk)
    {
        var clamped = Math.Clamp(processedInChunk, 0, assignment.Length);
        var delta = clamped - assignment.ReportedProgress;
        if (delta <= 0) return;
        assignment.ReportedProgress = clamped;
        assignment.Session.Completed += delta;
        _completions += delta;
    }

    private void AdvanceAssignmentProgress(AssignmentProgress progress)
    {
        if (!_assignments.ContainsKey(progress.Assignment.Id)) return;
        var session = progress.Assignment.Session;
        if (session.Status is not SessionStatus.Running) return;
        ApplyChunkProgress(progress.Assignment, progress.ProcessedInChunk);
        ReportProgress(session);
    }

    private void FailSession(SessionState session, Exception exception)
    {
        if (session.Status is not SessionStatus.Running) return;
        session.Status = SessionStatus.Failed;
        session.Failure = exception;
        _failures++;
        session.Cancellation.Cancel();
    }

    private void TryCompleteSession(SessionState session)
    {
        if (session.InFlight > 0) return;

        if (session.Status == SessionStatus.Running
            && session.NextIndex < session.Request.SegmentIds.Count)
            return;

        switch (session.Status)
        {
            case SessionStatus.Running:
                // Everything was dispatched and nothing is in flight. If fewer segments were
                // verified than requested, an assignment ended without reporting its work;
                // surface that instead of a result the caller would read as verified.
                if (session.Completed != session.Request.SegmentIds.Count)
                {
                    session.Completion.TrySetException(new IncompleteHealthCheckSweepException(
                        session.Request.DavItemId,
                        session.Request.SegmentIds.Count,
                        session.Completed));
                    _failures++;
                    break;
                }

                session.MissingIndices.Sort();
                session.UnansweredIndices.Sort();
                session.Completion.TrySetResult(new HealthCheckStatResult(
                    session.MissingIndices.ToArray(),
                    session.Completed)
                {
                    UnansweredIndices = session.UnansweredIndices.ToArray(),
                });
                break;
            case SessionStatus.Cancelling:
                _cancellations++;
                session.Completion.TrySetCanceled(session.CancellationToken);
                break;
            case SessionStatus.Failed:
                session.Completion.TrySetException(session.Failure!);
                break;
        }

        _sessions.Remove(session.Id);
        session.Cancellation.Dispose();
    }

    private void ReportProgress(SessionState session)
    {
        try
        {
            session.Progress?.Report(session.Completed);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            Log.Warning(
                exception,
                "Health-check STAT progress callback failed for {DavItemId}",
                session.Request.DavItemId);
        }
    }

    /// <summary>
    /// Chunk size for the next dispatch. Large enough that the client can pipeline a
    /// worthwhile batch on one connection, but capped at an even split of the session's
    /// remaining work across free capacity so a single file cannot claim a whole slot's
    /// worth of segments while its peers idle.
    /// </summary>
    private int NextChunkLength(SessionState session)
    {
        var remaining = session.Request.SegmentIds.Count - session.NextIndex;
        if (remaining <= 1) return Math.Max(1, remaining);
        if (ChunkSizeOverride is { } pinned) return Math.Min(Math.Max(1, pinned), remaining);

        // Size the chunk against the slots this session's own provider can execute, not a
        // global pool of interchangeable connections, and share them only with the sessions
        // actually competing for that provider.
        var capacity = Math.Max(1, GetProviderSchedulingCapacity(session.Request.ProviderKey));
        var runnable = Math.Max(1, _sessions.Values.Count(other =>
            other.Status == SessionStatus.Running
            && other.NextIndex < other.Request.SegmentIds.Count
            && string.Equals(
                other.Request.ProviderKey,
                session.Request.ProviderKey,
                StringComparison.Ordinal)));

        // Slots this session can expect to hold concurrently, so its remaining work is
        // spread across them instead of landing in one oversized chunk.
        var fairSlots = Math.Max(1, capacity / runnable);
        var fairShare = Math.Max(1, remaining / fairSlots);
        return Math.Clamp(fairShare, MinimumChunkSize, MaximumChunkSize) is var chunk && chunk > remaining
            ? remaining
            : Math.Min(chunk, remaining);
    }

    /// <summary>
    /// Metadata slots the target provider can currently execute, bounded by the explicit
    /// aggregate ceiling. Legacy shared-pool and unknown providers fall back to the ceiling
    /// rather than inventing a metadata budget for them.
    /// </summary>
    private int GetProviderSchedulingCapacity(string? providerKey)
    {
        // Auto has no aggregate ceiling, so the provider's own executable width decides.
        var ceiling = _configManager.GetHealthCheckCeiling() is { } configured
            ? Math.Max(1, configured)
            : int.MaxValue;
        if (providerKey is null)
            return ceiling == int.MaxValue ? MaximumChunkSize : ceiling;
        if (_providerAdmissions.GetSnapshot(providerKey) is not { Admission: { } admission })
            return ceiling == int.MaxValue ? MaximumChunkSize : ceiling;
        return Math.Min(ceiling, Math.Max(1, admission.MaxMetadataCapacity));
    }

    private IEnumerable<SessionState> SelectCandidateSessions(
        HashSet<string> blockedProviders) => _sessions.Values
        .Where(session => session.Status == SessionStatus.Running
                          && session.NextIndex < session.Request.SegmentIds.Count
                          && (session.Request.ProviderKey is null
                              || !blockedProviders.Contains(session.Request.ProviderKey)))
        .OrderBy(session => session.InFlight)
        .ThenBy(session => session.LastDispatchSequence);

    private void ReconcileAssignments()
    {
        // Diagnostics reflect the pass that just ran, so never leave a stale blocked set
        // behind when the pass is skipped entirely.
        var blockedProviders = new HashSet<string>(StringComparer.Ordinal);
        _blockedProviders = blockedProviders;
        if (_shuttingDown || _globalBlocked) return;

        while (SelectCandidateSessions(blockedProviders).FirstOrDefault() is { } session)
        {
            HealthCheckProviderLease? providerLease = null;
            if (session.Request.ProviderKey is { } providerKey)
            {
                var providerAttempt = _providerAdmissions.TryAcquireMetadata(
                    providerKey,
                    SemaphorePriority.Low);
                switch (providerAttempt.State)
                {
                    case HealthCheckProviderAdmissionState.Acquired:
                        providerLease = providerAttempt.Lease
                            ?? throw new InvalidOperationException(
                                "Provider admission succeeded without returning a lease.");
                        break;
                    case HealthCheckProviderAdmissionState.TemporarilyUnavailable:
                        blockedProviders.Add(providerKey);
                        continue;
                    case HealthCheckProviderAdmissionState.ProviderUnavailable:
                        providerLease = providerAttempt.Lease;
                        // A pinned terminal lease returns unanswered without touching a pool.
                        // With no active generation (test doubles/startup), retain the bounded
                        // compatibility path and let the executor settle the request.
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(providerAttempt),
                            providerAttempt.State,
                            null);
                }
            }

#pragma warning disable CA2000 // ownership transfers to WorkAssignment, or is released on every failure path
            if (!_gate.TryAcquire(
                    HealthCheckAdmissionPriority.Background,
                    out var globalLease)
                || globalLease is null)
            {
                // Avoid a retry loop caused by releasing the provider lease below. Only a
                // gate availability event can make aggregate capacity executable again.
                _globalBlocked = true;
                providerLease?.Dispose();
                return;
            }

            try
            {
                Dispatch(session, providerLease, globalLease);
            }
            catch
            {
                try
                {
                    globalLease.Dispose();
                }
                finally
                {
                    providerLease?.Dispose();
                }
                throw;
            }
#pragma warning restore CA2000
        }
    }

    private async Task ExecuteAssignmentAsync(WorkAssignment assignment)
    {
        IReadOnlyList<int>? missingOffsets = null;
        IReadOnlyList<int>? unansweredOffsets = null;
        Exception? exception = null;
        var processed = 0;
        var succeeded = false;
        var session = assignment.Session;
        var segmentIds = new string[assignment.Length];
        for (var i = 0; i < assignment.Length; i++)
            segmentIds[i] = session.Request.SegmentIds[assignment.Offset + i];

        try
        {
            using var assignmentCts = ContextualCancellationTokenSource.CreateLinkedTokenSource(
                session.RequestCancellationToken,
                session.Cancellation.Token);
            HealthCheckAdmissionContext healthAdmissionContext = assignment.ProviderLease is
                { } providerLease
                ? new ProviderAwareHealthCheckAdmissionContext(
                    _gate,
                    HealthCheckAdmissionPriority.Background,
                    GateLeasePreAcquired: true,
                    providerLease)
                : new HealthCheckAdmissionContext(
                    _gate,
                    HealthCheckAdmissionPriority.Background,
                    GateLeasePreAcquired: true);
            using var healthAdmissionScope = assignmentCts.Token.SetContext(
                healthAdmissionContext);

            // Forward intra-chunk progress so the caller's no-progress watchdog stays armed
            // across a long pipelined batch. Reports are chunk-relative and absolute.
            var chunkProgress = new SynchronousProgress<int>(count =>
            {
                var clamped = Math.Clamp(count, 0, assignment.Length);
                if (Interlocked.Exchange(ref processed, clamped) == clamped) return;
                _events.Writer.TryWrite(new AssignmentProgress(assignment, clamped));
            });

            var sweep = await session.Executor(segmentIds, chunkProgress, assignmentCts.Token)
                .ConfigureAwait(false);

            processed = assignment.Length;
            if (sweep.MissingIds.Count > 0 || sweep.UnansweredIds.Count > 0)
            {
                // Executors classify logical ids, while the session result is positional.
                // Scan the chunk so a repeated id receives the verdict at every occurrence.
                if (sweep.MissingIds.Count > 0)
                {
                    var missingIds = sweep.MissingIds.ToHashSet(StringComparer.Ordinal);
                    var offsets = new List<int>(segmentIds.Length);
                    for (var i = 0; i < segmentIds.Length; i++)
                        if (missingIds.Contains(segmentIds[i]))
                            offsets.Add(i);
                    missingOffsets = offsets;
                }

                if (sweep.UnansweredIds.Count > 0)
                {
                    var unansweredIds = sweep.UnansweredIds.ToHashSet(StringComparer.Ordinal);
                    var offsets = new List<int>(segmentIds.Length);
                    for (var i = 0; i < segmentIds.Length; i++)
                        if (unansweredIds.Contains(segmentIds[i]))
                            offsets.Add(i);
                    unansweredOffsets = offsets;
                }
            }

            succeeded = true;
        }
        catch (OutOfMemoryException oom)
        {
            // Never swallowed — rethrown below so process-level policy is unchanged.
            // Recording it here is what stops the finally from publishing an unfinished
            // chunk as a clean result, which would let the sweep resolve as verified on
            // segments that were never checked.
            exception = oom;
            throw;
        }
        catch (Exception caught)
        {
            exception = caught;
        }
        finally
        {
            // Any abnormal unwind that left no exception recorded must not look like
            // verified work either.
            if (!succeeded && exception is null)
            {
                exception = new IncompleteHealthCheckSweepException(
                    session.Request.DavItemId, assignment.Length, Volatile.Read(ref processed));
            }

            // Publish completion before releasing either lease so channel ordering lets the
            // actor retire this assignment before release events trigger reconciliation.
            _events.Writer.TryWrite(new AssignmentFinished(
                assignment,
                succeeded ? missingOffsets : null,
                succeeded ? unansweredOffsets : null,
                Volatile.Read(ref processed),
                exception));
            assignment.DisposeLeases();
        }
    }

    /// <summary>
    /// Keeps a fire-and-forget task's failure from becoming an unobserved exception. The
    /// session has already been failed through the event channel by the time this runs;
    /// this only records that the task itself unwound.
    /// </summary>
    private static Task Observe(Task execution, string message)
    {
        _ = execution.ContinueWith(
            task => Log.Error(task.Exception!.GetBaseException(), "{Message}", message),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return execution;
    }

    private void BeginShutdown()
    {
        if (_shuttingDown) return;
        _shuttingDown = true;
        foreach (var session in _sessions.Values.ToArray())
        {
            if (session.Status == SessionStatus.Running)
            {
                session.Status = SessionStatus.Cancelling;
                session.CancellationToken = _stoppingToken;
            }
            session.Cancellation.Cancel();
            TryCompleteSession(session);
        }
    }

    private void OnConfigChanged(object? sender, ConfigManager.ConfigEventArgs args)
    {
        if (args.ChangedConfig.ContainsKey(ConfigKeys.RepairHealthcheckConcurrency)
            || args.ChangedConfig.ContainsKey(ConfigKeys.UsenetProviders))
            _events.Writer.TryWrite(ReconcileScheduler.Instance);
    }

    private void OnProviderAvailabilityChanged(string providerKey)
    {
        _events.Writer.TryWrite(new ProviderAvailabilityChanged(providerKey));
    }

    private void OnGlobalAvailabilityChanged()
    {
        _events.Writer.TryWrite(GlobalAvailabilityChanged.Instance);
    }

    private void PublishSnapshot()
    {
        var sessions = _sessions.Values
            .OrderBy(session => session.Request.DavItemId)
            .Select(session => new HealthCheckStatSessionSnapshot(
                session.Request.RunId,
                session.Request.DavItemId,
                session.Request.PhaseId,
                session.Request.ProviderKey,
                session.Request.Mode,
                session.Status.ToString(),
                session.InFlight,
                session.Completed,
                session.Request.SegmentIds.Count))
            .ToArray();
        var runnable = _sessions.Values.Count(session =>
            session.Status == SessionStatus.Running
            && session.NextIndex < session.Request.SegmentIds.Count);
        var pendingSegments = _sessions.Values
            .Where(session => session.Status == SessionStatus.Running)
            .Sum(session => (long)session.Request.SegmentIds.Count - session.NextIndex);

        var activeByProvider = _assignments.Values
            .Where(assignment => assignment.Session.Request.ProviderKey is not null)
            .GroupBy(
                assignment => assignment.Session.Request.ProviderKey!,
                StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.Ordinal);
        var runnableByProvider = _sessions.Values
            .Where(session => session.Status == SessionStatus.Running
                              && session.NextIndex < session.Request.SegmentIds.Count
                              && session.Request.ProviderKey is not null)
            .GroupBy(session => session.Request.ProviderKey!, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.Ordinal);

        var providers = activeByProvider.Keys
            .Concat(runnableByProvider.Keys)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(providerKey => providerKey, StringComparer.Ordinal)
            .Select(providerKey =>
            {
                var active = activeByProvider.GetValueOrDefault(providerKey, []);
                var runnableSessions = runnableByProvider.GetValueOrDefault(providerKey, []);
                var isBlocked = _blockedProviders.Contains(providerKey);
                return new HealthCheckStatProviderSnapshot(
                    providerKey,
                    active.Length,
                    runnableSessions.Length,
                    runnableSessions.Sum(session =>
                        (long)session.Request.SegmentIds.Count - session.NextIndex),
                    // Nothing is blocked on the provider unless the last pass found it
                    // saturated while sessions still had work to dispatch.
                    isBlocked ? runnableSessions.Length : 0,
                    // Shared-pool status belongs to the provider generation, not to whatever
                    // happens to be running, so an idle or blocked legacy provider still
                    // reports as legacy.
                    _providerAdmissions.GetSnapshot(providerKey)?.IsLegacySharedPool ?? false);
            })
            .ToArray();

        Volatile.Write(ref _snapshot, new HealthCheckStatSchedulerSnapshot(
            _configManager.GetHealthCheckCeiling(),
            _assignments.Count,
            0,
            runnable,
            pendingSegments,
            _dispatches,
            _completions,
            _cancellations,
            _failures,
            sessions,
            providers,
            _globalBlocked ? runnable : 0,
            _assignments.Values.Count(assignment =>
                assignment.ProviderLease is { IsLegacySharedPool: true })));
    }

    public override void Dispose()
    {
        _configManager.OnConfigChanged -= OnConfigChanged;
        _gate.AvailabilityChanged -= OnGlobalAvailabilityChanged;
        _providerAdmissions.AvailabilityChanged -= OnProviderAvailabilityChanged;
        base.Dispose();
        if (_ownsProviderAdmissions)
            _providerAdmissions.Dispose();
    }

    private abstract record SchedulerEvent;
    private sealed record RegisterSession(
        Guid SessionId,
        HealthCheckStatRequest Request,
        HealthCheckStatDetailedChunkExecutor Executor,
        IProgress<int>? Progress,
        TaskCompletionSource<HealthCheckStatResult> Completion,
        CancellationToken CancellationToken) : SchedulerEvent;
    private sealed record CancelSession(
        Guid SessionId,
        CancellationToken CancellationToken) : SchedulerEvent;
    private sealed record AssignmentFinished(
        WorkAssignment Assignment,
        IReadOnlyList<int>? MissingOffsets,
        IReadOnlyList<int>? UnansweredOffsets,
        int Processed,
        Exception? Exception) : SchedulerEvent;
    private sealed record AssignmentProgress(
        WorkAssignment Assignment,
        int ProcessedInChunk) : SchedulerEvent;
    private sealed record ProviderAvailabilityChanged(string ProviderKey) : SchedulerEvent;
    private sealed record GlobalAvailabilityChanged : SchedulerEvent
    {
        public static readonly GlobalAvailabilityChanged Instance = new();
    }
    private sealed record ReconcileScheduler : SchedulerEvent
    {
        public static readonly ReconcileScheduler Instance = new();
    }
    private sealed record StopScheduler : SchedulerEvent
    {
        public static readonly StopScheduler Instance = new();
    }

    private sealed class WorkAssignment(
        Guid id,
        SessionState session,
        int offset,
        int length,
        HealthCheckProviderLease? providerLease,
        HealthCheckConnectionGate.Lease globalLease)
    {
        public Guid Id { get; } = id;
        public SessionState Session { get; } = session;
        public int Offset { get; } = offset;
        public int Length { get; } = length;
        public HealthCheckProviderLease? ProviderLease { get; } = providerLease;
        public HealthCheckConnectionGate.Lease GlobalLease { get; } = globalLease;
        public Task? Execution { get; set; }

        /// <summary>Chunk-relative progress already folded into the session's completed count.</summary>
        public int ReportedProgress { get; set; }

        public void DisposeLeases()
        {
            try
            {
                GlobalLease.Dispose();
            }
            finally
            {
                ProviderLease?.Dispose();
            }
        }
    }

    private sealed class SessionState
    {
        public SessionState(RegisterSession registration, CancellationTokenSource cancellation)
        {
            Id = registration.SessionId;
            Request = registration.Request;
            Executor = registration.Executor;
            Progress = registration.Progress;
            RequestCancellationToken = registration.CancellationToken;
            CancellationToken = registration.CancellationToken;
            Completion = registration.Completion;
            Cancellation = cancellation;
        }

        public Guid Id { get; }
        public HealthCheckStatRequest Request { get; }
        public HealthCheckStatDetailedChunkExecutor Executor { get; }
        public IProgress<int>? Progress { get; }
        public CancellationToken RequestCancellationToken { get; }
        public CancellationToken CancellationToken { get; set; }
        public TaskCompletionSource<HealthCheckStatResult> Completion { get; }
        public CancellationTokenSource Cancellation { get; }
        public List<int> MissingIndices { get; } = [];
        public List<int> UnansweredIndices { get; } = [];
        public SessionStatus Status { get; set; }
        public Exception? Failure { get; set; }
        public int NextIndex { get; set; }
        public int InFlight { get; set; }
        public int Completed { get; set; }
        public long LastDispatchSequence { get; set; }
    }

    /// <summary>
    /// Progress adapter that invokes inline. Progress&lt;T&gt; posts to a captured context and can
    /// reorder reports, which would corrupt the monotonic chunk counter.
    /// </summary>
    private sealed class SynchronousProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }

    private enum SessionStatus
    {
        Running,
        Cancelling,
        Failed,
    }
}
