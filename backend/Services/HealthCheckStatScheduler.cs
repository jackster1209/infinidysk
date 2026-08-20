using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using NzbWebDAV.Clients.Usenet;
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
    HealthCheckStatMode Mode);

public sealed record HealthCheckStatResult(
    IReadOnlyList<int> MissingIndices,
    int Completed);

public sealed record HealthCheckStatSessionSnapshot(
    Guid RunId,
    Guid DavItemId,
    int PhaseId,
    HealthCheckStatMode Mode,
    string State,
    int InFlight,
    int Completed,
    int Total);

public sealed record HealthCheckStatSchedulerSnapshot(
    int Capacity,
    int ActiveAssignments,
    int PendingAdmissions,
    int RunnableSessions,
    long PendingSegments,
    long Dispatches,
    long Completions,
    long Cancellations,
    long Failures,
    IReadOnlyList<HealthCheckStatSessionSnapshot> Sessions);

/// <summary>
/// Fairly assigns background health-check STAT work after shared gate capacity is granted.
/// All scheduler state is serialized through one channel reader; execution tasks only post results.
/// </summary>
public sealed class HealthCheckStatScheduler : BackgroundService
{
    private readonly ConfigManager _configManager;
    private readonly HealthCheckConnectionGate _gate;
    private readonly Channel<SchedulerEvent> _events = Channel.CreateUnbounded<SchedulerEvent>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    private readonly Dictionary<Guid, SessionState> _sessions = [];
    private readonly Dictionary<Guid, PendingAdmission> _pendingAdmissions = [];
    private readonly Dictionary<Guid, WorkAssignment> _assignments = [];
    private HealthCheckStatSchedulerSnapshot _snapshot = EmptySnapshot;
    private CancellationToken _stoppingToken;
    private long _dispatchSequence;
    private long _dispatches;
    private long _completions;
    private long _cancellations;
    private long _failures;
    private bool _shuttingDown;

    private static readonly HealthCheckStatSchedulerSnapshot EmptySnapshot = new(
        0, 0, 0, 0, 0, 0, 0, 0, 0, []);

    public HealthCheckStatScheduler(ConfigManager configManager, HealthCheckConnectionGate gate)
    {
        _configManager = configManager;
        _gate = gate;
        _configManager.OnConfigChanged += OnConfigChanged;
        PublishSnapshot();
    }

    public Task<HealthCheckStatResult> RunAsync(
        HealthCheckStatRequest request,
        Func<string, CancellationToken, Task<UsenetStatResponse>> executor,
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
            // Shutdown is delivered as an event so the reader can continue draining bounded
            // assignments and gate acquisitions after the hosted-service token is cancelled.
            while (await _events.Reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                while (_events.Reader.TryRead(out var schedulerEvent))
                    HandleEvent(schedulerEvent);

                ReconcileAdmissions();
                PublishSnapshot();
                if (_shuttingDown
                    && _sessions.Count == 0
                    && _pendingAdmissions.Count == 0
                    && _assignments.Count == 0)
                    break;
            }
        }
        finally
        {
            _events.Writer.TryComplete();
            foreach (var admission in _pendingAdmissions.Values)
                await admission.Cancellation.CancelAsync().ConfigureAwait(false);
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
            case AdmissionGranted granted:
                Admit(granted);
                break;
            case AdmissionFailed failed:
                AdmissionFinished(failed);
                break;
            case AssignmentFinished finished:
                CompleteAssignment(finished);
                break;
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

    private void Admit(AdmissionGranted granted)
    {
        if (!_pendingAdmissions.Remove(granted.Admission.Id, out var admission))
        {
            granted.Lease.Dispose();
            return;
        }
        admission.Cancellation.Dispose();

        if (_shuttingDown || SelectNextSession() is not { } session)
        {
            granted.Lease.Dispose();
            return;
        }

        var index = session.NextIndex++;
        session.InFlight++;
        session.LastDispatchSequence = ++_dispatchSequence;
        _dispatches++;
        var assignment = new WorkAssignment(Guid.NewGuid(), session, index, granted.Lease);
        _assignments.Add(assignment.Id, assignment);
        assignment.Execution = ExecuteAssignmentAsync(assignment);
    }

    private void AdmissionFinished(AdmissionFailed failed)
    {
        if (!_pendingAdmissions.Remove(failed.Admission.Id, out var admission)) return;
        admission.Cancellation.Dispose();
        if (failed.Exception is not null
            && failed.Exception is not OperationCanceledException
            && !_shuttingDown)
        {
            Log.Warning(
                failed.Exception,
                "Background health scheduler gate admission failed: {Message}",
                failed.Exception.Message);
        }
    }

    private void CompleteAssignment(AssignmentFinished finished)
    {
        if (!_assignments.Remove(finished.Assignment.Id, out var assignment)) return;
        var session = assignment.Session;
        session.InFlight--;

        if (session.Status is SessionStatus.Running)
        {
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
                    FailSession(session, finished.Exception);
                }
            }
            else if (finished.Response is { } response)
            {
                session.Completed++;
                _completions++;
                ReportProgress(session);
                if (response.ResponseType == UsenetResponseType.ArticleExists)
                {
                    // Successful terminal result.
                }
                else if (UsenetArticleAvailability.IsDefinitiveMissing(response))
                {
                    if (session.Request.Mode == HealthCheckStatMode.VerifyAll)
                    {
                        FailSession(session, new UsenetArticleNotFoundException(
                            session.Request.SegmentIds[assignment.Index],
                            response.ResponseMessage));
                    }
                    else
                    {
                        session.MissingIndices.Add(assignment.Index);
                    }
                }
                else
                {
                    FailSession(session, new UsenetUnexpectedResponseException(
                        session.Request.SegmentIds[assignment.Index],
                        response.ResponseMessage));
                }
            }
        }

        TryCompleteSession(session);
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
                session.MissingIndices.Sort();
                session.Completion.TrySetResult(new HealthCheckStatResult(
                    session.MissingIndices.ToArray(),
                    session.Completed));
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

    private SessionState? SelectNextSession() => _sessions.Values
        .Where(session => session.Status == SessionStatus.Running
                          && session.NextIndex < session.Request.SegmentIds.Count)
        .OrderBy(session => session.InFlight)
        .ThenBy(session => session.LastDispatchSequence)
        .FirstOrDefault();

    private void ReconcileAdmissions()
    {
        if (_shuttingDown)
        {
            foreach (var admission in _pendingAdmissions.Values)
                admission.Cancellation.Cancel();
            return;
        }

        var capacity = _configManager.GetHealthCheckConcurrency();
        var availableCapacity = Math.Max(0, capacity - _assignments.Count);
        var undispatched = _sessions.Values
            .Where(session => session.Status == SessionStatus.Running)
            .Sum(session => (long)session.Request.SegmentIds.Count - session.NextIndex);
        var desiredPending = (int)Math.Min(availableCapacity, undispatched);

        if (_pendingAdmissions.Count > desiredPending)
        {
            foreach (var admission in _pendingAdmissions.Values
                         .OrderByDescending(item => item.Sequence)
                         .Take(_pendingAdmissions.Count - desiredPending))
                admission.Cancellation.Cancel();
            return;
        }

        while (_pendingAdmissions.Count < desiredPending)
        {
#pragma warning disable CA2000 // ownership transfers to PendingAdmission and is disposed when its completion event is handled
            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_stoppingToken);
#pragma warning restore CA2000
            var admission = new PendingAdmission(
                Guid.NewGuid(),
                ++_dispatchSequence,
                cancellation);
            _pendingAdmissions.Add(admission.Id, admission);
            admission.Execution = AcquireAdmissionAsync(admission);
        }
    }

    private async Task AcquireAdmissionAsync(PendingAdmission admission)
    {
        try
        {
            var lease = await _gate.AcquireAsync(
                    HealthCheckAdmissionPriority.Background,
                    admission.Cancellation.Token)
                .ConfigureAwait(false);
            if (!_events.Writer.TryWrite(new AdmissionGranted(admission, lease)))
                lease.Dispose();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            _events.Writer.TryWrite(new AdmissionFailed(admission, exception));
        }
    }

    private async Task ExecuteAssignmentAsync(WorkAssignment assignment)
    {
        UsenetStatResponse? response = null;
        Exception? exception = null;
        try
        {
            using var assignmentCts = ContextualCancellationTokenSource.CreateLinkedTokenSource(
                assignment.Session.RequestCancellationToken,
                assignment.Session.Cancellation.Token);
            using var healthAdmissionScope = assignmentCts.Token.SetContext(
                new HealthCheckAdmissionContext(
                    _gate,
                    HealthCheckAdmissionPriority.Background,
                    GateLeasePreAcquired: true));
            response = await assignment.Session.Executor(
                    assignment.Session.Request.SegmentIds[assignment.Index],
                    assignmentCts.Token)
                .ConfigureAwait(false);
        }
        catch (Exception caught) when (caught is not OutOfMemoryException)
        {
            exception = caught;
        }
        finally
        {
            // Publish completion before releasing the gate lease. A release may synchronously
            // grant the next anonymous admission; channel ordering must let the actor remove this
            // assignment before it binds that newly executable slot to another session.
            _events.Writer.TryWrite(new AssignmentFinished(assignment, response, exception));
            assignment.Lease.Dispose();
        }
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
        foreach (var admission in _pendingAdmissions.Values)
            admission.Cancellation.Cancel();
    }

    private void OnConfigChanged(object? sender, ConfigManager.ConfigEventArgs args)
    {
        if (args.ChangedConfig.ContainsKey(ConfigKeys.RepairHealthcheckConcurrency)
            || args.ChangedConfig.ContainsKey(ConfigKeys.UsenetProviders))
            _events.Writer.TryWrite(ReconcileScheduler.Instance);
    }

    private void PublishSnapshot()
    {
        var sessions = _sessions.Values
            .OrderBy(session => session.Request.DavItemId)
            .Select(session => new HealthCheckStatSessionSnapshot(
                session.Request.RunId,
                session.Request.DavItemId,
                session.Request.PhaseId,
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
        Volatile.Write(ref _snapshot, new HealthCheckStatSchedulerSnapshot(
            _configManager.GetHealthCheckConcurrency(),
            _assignments.Count,
            _pendingAdmissions.Count,
            runnable,
            pendingSegments,
            _dispatches,
            _completions,
            _cancellations,
            _failures,
            sessions));
    }

    public override void Dispose()
    {
        _configManager.OnConfigChanged -= OnConfigChanged;
        base.Dispose();
    }

    private abstract record SchedulerEvent;
    private sealed record RegisterSession(
        Guid SessionId,
        HealthCheckStatRequest Request,
        Func<string, CancellationToken, Task<UsenetStatResponse>> Executor,
        IProgress<int>? Progress,
        TaskCompletionSource<HealthCheckStatResult> Completion,
        CancellationToken CancellationToken) : SchedulerEvent;
    private sealed record CancelSession(
        Guid SessionId,
        CancellationToken CancellationToken) : SchedulerEvent;
    private sealed record AdmissionGranted(
        PendingAdmission Admission,
        HealthCheckConnectionGate.Lease Lease) : SchedulerEvent;
    private sealed record AdmissionFailed(
        PendingAdmission Admission,
        Exception? Exception) : SchedulerEvent;
    private sealed record AssignmentFinished(
        WorkAssignment Assignment,
        UsenetStatResponse? Response,
        Exception? Exception) : SchedulerEvent;
    private sealed record ReconcileScheduler : SchedulerEvent
    {
        public static readonly ReconcileScheduler Instance = new();
    }
    private sealed record StopScheduler : SchedulerEvent
    {
        public static readonly StopScheduler Instance = new();
    }

    private sealed class PendingAdmission(
        Guid id,
        long sequence,
        CancellationTokenSource cancellation)
    {
        public Guid Id { get; } = id;
        public long Sequence { get; } = sequence;
        public CancellationTokenSource Cancellation { get; } = cancellation;
        public Task? Execution { get; set; }
    }

    private sealed class WorkAssignment(
        Guid id,
        SessionState session,
        int index,
        HealthCheckConnectionGate.Lease lease)
    {
        public Guid Id { get; } = id;
        public SessionState Session { get; } = session;
        public int Index { get; } = index;
        public HealthCheckConnectionGate.Lease Lease { get; } = lease;
        public Task? Execution { get; set; }
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
        public Func<string, CancellationToken, Task<UsenetStatResponse>> Executor { get; }
        public IProgress<int>? Progress { get; }
        public CancellationToken RequestCancellationToken { get; }
        public CancellationToken CancellationToken { get; set; }
        public TaskCompletionSource<HealthCheckStatResult> Completion { get; }
        public CancellationTokenSource Cancellation { get; }
        public List<int> MissingIndices { get; } = [];
        public SessionStatus Status { get; set; }
        public Exception? Failure { get; set; }
        public int NextIndex { get; set; }
        public int InFlight { get; set; }
        public int Completed { get; set; }
        public long LastDispatchSequence { get; set; }
    }

    private enum SessionStatus
    {
        Running,
        Cancelling,
        Failed,
    }
}
