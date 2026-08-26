using NzbWebDAV.Clients.Usenet.Concurrency;
using Serilog;

namespace NzbWebDAV.Clients.Usenet.Connections;

internal enum ProviderConnectionKind
{
    Transfer,
    Metadata,
}

/// <summary>
/// Operation-aware admission in front of a single physical provider pool.
/// Transfers have a hard cap; metadata can use its base allocation plus a bounded
/// burst. Waiting transfers reclaim borrowed capacity, while queued metadata retains
/// at least one progress slot even when the configured transfer limit fills the pool.
/// </summary>
internal sealed class ProviderConnectionAdmission : IDisposable
{
    private readonly Func<int> _getEffectiveProviderLimit;
    private readonly int _configuredTransferLimit;
    private readonly Action<ProviderConnectionAdmissionSnapshot>? _onChanged;
    private readonly Lock _lock = new();
    private readonly LinkedList<Waiter> _transferHighWaiters = [];
    private readonly LinkedList<Waiter> _transferLowWaiters = [];
    private readonly LinkedList<Waiter> _metadataHighWaiters = [];
    private readonly LinkedList<Waiter> _metadataLowWaiters = [];

    private SemaphorePriorityOdds _priorityOdds;
    private int _activeTransfers;
    private int _activeMetadata;
    private int _transferAccumulatedOdds;
    private int _metadataAccumulatedOdds;
    private bool _disposed;

    internal Guid InstanceId { get; } = Guid.NewGuid();
    internal event Action<ProviderConnectionAdmissionSnapshot>? AvailabilityChanged;

    internal bool IsDisposed
    {
        get
        {
            lock (_lock)
                return _disposed;
        }
    }

    public ProviderConnectionAdmission(
        Func<int> getEffectiveProviderLimit,
        int configuredTransferLimit,
        SemaphorePriorityOdds? priorityOdds = null,
        Action<ProviderConnectionAdmissionSnapshot>? onChanged = null)
    {
        _getEffectiveProviderLimit = getEffectiveProviderLimit
            ?? throw new ArgumentNullException(nameof(getEffectiveProviderLimit));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(configuredTransferLimit);
        _configuredTransferLimit = configuredTransferLimit;
        _priorityOdds = priorityOdds ?? new SemaphorePriorityOdds { HighPriorityOdds = 100 };
        _onChanged = onChanged;
    }

    public Task<Lease> AcquireAsync(
        ProviderConnectionKind kind,
        SemaphorePriority priority,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        ProviderConnectionAdmissionSnapshot? snapshot;
        Waiter? queuedWaiter = null;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (CanEnterImmediately(kind))
            {
                Enter(kind);
                snapshot = CreateSnapshotUnsafe();
            }
            else
            {
                snapshot = null;
                queuedWaiter = new Waiter(kind, priority);
                GetQueue(kind, priority).AddLast(queuedWaiter);
            }
        }

        if (queuedWaiter is not null)
        {
            RegisterCancellation(queuedWaiter, cancellationToken);
            return queuedWaiter.Completion.Task;
        }

        NotifyChanged(snapshot);
        return Task.FromResult(new Lease(this, kind));
    }

    /// <summary>
    /// Attempts to enter without joining an admission queue. Scheduler-owned work already
    /// has a pending-work queue, so enqueuing here would create nested capacity waiting.
    /// Existing transfer/metadata waiters and priority rules remain authoritative.
    /// </summary>
    public bool TryAcquire(
        ProviderConnectionKind kind,
        SemaphorePriority priority,
        out Lease? lease)
    {
        _ = priority; // A non-queued acquisition has no lane, but callers keep workload intent explicit.
        ProviderConnectionAdmissionSnapshot? snapshot;
        lock (_lock)
        {
            if (_disposed || !CanEnterImmediately(kind))
            {
                lease = null;
                return false;
            }

            Enter(kind);
            lease = new Lease(this, kind);
            snapshot = CreateSnapshotUnsafe();
        }

        NotifyChanged(snapshot);
        return true;
    }

    private void RegisterCancellation(Waiter waiter, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled) return;

        // Register outside _lock. Register invokes synchronously when cancellation won the
        // enqueue race; doing that under _lock would run CancelWaiter and _onChanged while
        // the outer admission critical section was still held.
        var registration = cancellationToken.Register(
            () => CancelWaiter(waiter, cancellationToken));
        _ = waiter.Completion.Task.ContinueWith(
            static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
            registration,
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    public void UpdatePriorityOdds(SemaphorePriorityOdds priorityOdds)
    {
        ArgumentNullException.ThrowIfNull(priorityOdds);
        lock (_lock)
        {
            if (_disposed) return;
            _priorityOdds = priorityOdds;
        }
    }

    public ProviderConnectionAdmissionSnapshot GetSnapshot()
    {
        lock (_lock)
            return CreateSnapshotUnsafe();
    }

    private bool CanEnterImmediately(ProviderConnectionKind kind)
    {
        if (HasWaiters(kind)) return false;
        if (kind == ProviderConnectionKind.Metadata
            && HasWaiters(ProviderConnectionKind.Transfer)
            && CanEnter(ProviderConnectionKind.Transfer))
        {
            return false;
        }

        return CanEnter(kind);
    }

    private bool CanEnter(ProviderConnectionKind kind)
    {
        var budget = ProviderConnectionBudget.Calculate(
            _getEffectiveProviderLimit(),
            _configuredTransferLimit);
        if (_activeTransfers + _activeMetadata >= budget.EffectiveProviderLimit)
            return false;

        return kind switch
        {
            ProviderConnectionKind.Transfer =>
                _activeTransfers < budget.EffectiveTransferLimit,
            ProviderConnectionKind.Metadata =>
                _activeMetadata < budget.MaxMetadataCapacity,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };
    }

    private void Enter(ProviderConnectionKind kind)
    {
        if (kind == ProviderConnectionKind.Transfer)
            _activeTransfers++;
        else
            _activeMetadata++;
    }

    private void Release(ProviderConnectionKind kind)
    {
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready;
        ProviderConnectionAdmissionSnapshot? snapshot;
        lock (_lock)
        {
            if (kind == ProviderConnectionKind.Transfer)
                _activeTransfers--;
            else
                _activeMetadata--;

            if (_disposed) return;
            ready = DispatchWaiters();
            snapshot = CreateSnapshotUnsafe();
        }

        NotifyChanged(snapshot);
        CompleteReadyWaiters(ready);
    }

    private void CancelWaiter(Waiter waiter, CancellationToken cancellationToken)
    {
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready;
        ProviderConnectionAdmissionSnapshot? snapshot;
        var removed = false;
        lock (_lock)
        {
            removed = GetQueue(waiter.Kind, waiter.Priority).Remove(waiter);
            if (removed && !_disposed)
            {
                ready = DispatchWaiters();
                snapshot = CreateSnapshotUnsafe();
            }
            else
            {
                ready = [];
                snapshot = null;
            }
        }

        if (removed)
            waiter.Completion.TrySetCanceled(cancellationToken);
        NotifyChanged(snapshot);
        CompleteReadyWaiters(ready);
    }

    private List<(TaskCompletionSource<Lease> Completion, Lease Lease)> DispatchWaiters()
    {
        List<(TaskCompletionSource<Lease>, Lease)> ready = [];
        while (true)
        {
            ProviderConnectionKind? kind = null;
            if (HasWaiters(ProviderConnectionKind.Metadata)
                && NeedsReservedMetadataAdmission()
                && CanEnter(ProviderConnectionKind.Metadata))
            {
                // An equal transfer/provider limit has no static metadata allocation.
                // Once metadata is waiting, reserve one live slot so a continuous transfer
                // queue cannot starve STAT/HEAD/DATE indefinitely. Configurations with a
                // larger base allocation fill that reservation before lending slots back.
                kind = ProviderConnectionKind.Metadata;
            }
            else if (HasWaiters(ProviderConnectionKind.Transfer)
                && CanEnter(ProviderConnectionKind.Transfer))
            {
                kind = ProviderConnectionKind.Transfer;
            }
            else if (HasWaiters(ProviderConnectionKind.Metadata)
                     && CanEnter(ProviderConnectionKind.Metadata))
            {
                kind = ProviderConnectionKind.Metadata;
            }

            if (kind is not { } selectedKind) break;

            var waiter = Dequeue(selectedKind);
            if (waiter is null) break;
            Enter(selectedKind);
            ready.Add((waiter.Completion, new Lease(this, selectedKind)));
        }

        return ready;
    }

    private bool NeedsReservedMetadataAdmission()
    {
        var budget = ProviderConnectionBudget.Calculate(
            _getEffectiveProviderLimit(),
            _configuredTransferLimit);
        var reservedCapacity = Math.Max(1, budget.BaseMetadataCapacity);
        return _activeMetadata < reservedCapacity;
    }

    private Waiter? Dequeue(ProviderConnectionKind kind)
    {
        var high = GetQueue(kind, SemaphorePriority.High);
        var low = GetQueue(kind, SemaphorePriority.Low);
        LinkedList<Waiter> preferred;
        LinkedList<Waiter> fallback;

        if (high.Count == 0)
        {
            preferred = low;
            fallback = high;
        }
        else if (low.Count == 0)
        {
            preferred = high;
            fallback = low;
        }
        else
        {
            ref var accumulatedOdds = ref kind == ProviderConnectionKind.Transfer
                ? ref _transferAccumulatedOdds
                : ref _metadataAccumulatedOdds;
            accumulatedOdds += _priorityOdds.LowPriorityOdds;
            preferred = high;
            fallback = low;
            if (accumulatedOdds >= 100)
            {
                (preferred, fallback) = (fallback, preferred);
                accumulatedOdds -= 100;
            }
        }

        return TakeFirst(preferred) ?? TakeFirst(fallback);
    }

    private static Waiter? TakeFirst(LinkedList<Waiter> queue)
    {
        if (queue.First is not { } first) return null;
        queue.RemoveFirst();
        return first.Value;
    }

    private bool HasWaiters(ProviderConnectionKind kind) =>
        GetQueue(kind, SemaphorePriority.High).Count > 0
        || GetQueue(kind, SemaphorePriority.Low).Count > 0;

    private ProviderConnectionAdmissionSnapshot CreateSnapshotUnsafe()
    {
        var budget = ProviderConnectionBudget.Calculate(
            _getEffectiveProviderLimit(),
            _configuredTransferLimit);
        return new ProviderConnectionAdmissionSnapshot(
            _configuredTransferLimit,
            budget.EffectiveTransferLimit,
            budget.BaseMetadataCapacity,
            budget.MetadataBurstAllowance,
            budget.MaxMetadataCapacity,
            _activeTransfers,
            _activeMetadata,
            _transferHighWaiters.Count + _transferLowWaiters.Count,
            _metadataHighWaiters.Count + _metadataLowWaiters.Count);
    }

    private void NotifyChanged(ProviderConnectionAdmissionSnapshot? snapshot)
    {
        if (snapshot is not { } current) return;

        InvokeChanged(_onChanged, current);
        if (AvailabilityChanged is not { } availabilityChanged) return;
        foreach (var handler in availabilityChanged.GetInvocationList()
                     .Cast<Action<ProviderConnectionAdmissionSnapshot>>())
            InvokeChanged(handler, current);
    }

    private static void InvokeChanged(
        Action<ProviderConnectionAdmissionSnapshot>? callback,
        ProviderConnectionAdmissionSnapshot snapshot)
    {
        if (callback is null) return;
        try
        {
            callback(snapshot);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            Log.Warning(e, "Provider connection admission change callback failed");
        }
    }

    private LinkedList<Waiter> GetQueue(
        ProviderConnectionKind kind,
        SemaphorePriority priority) => (kind, priority) switch
        {
            (ProviderConnectionKind.Transfer, SemaphorePriority.High) => _transferHighWaiters,
            (ProviderConnectionKind.Transfer, SemaphorePriority.Low) => _transferLowWaiters,
            (ProviderConnectionKind.Metadata, SemaphorePriority.High) => _metadataHighWaiters,
            (ProviderConnectionKind.Metadata, SemaphorePriority.Low) => _metadataLowWaiters,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

    private static void CompleteReadyWaiters(
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready)
    {
        foreach (var (completion, lease) in ready)
        {
            if (!completion.TrySetResult(lease))
                lease.Dispose();
        }
    }

    public void Dispose()
    {
        List<Waiter> waiters;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            waiters = _transferHighWaiters
                .Concat(_transferLowWaiters)
                .Concat(_metadataHighWaiters)
                .Concat(_metadataLowWaiters)
                .ToList();
            _transferHighWaiters.Clear();
            _transferLowWaiters.Clear();
            _metadataHighWaiters.Clear();
            _metadataLowWaiters.Clear();
        }

        foreach (var waiter in waiters)
            waiter.Completion.TrySetException(
                new ObjectDisposedException(nameof(ProviderConnectionAdmission)));
    }

    private sealed class Waiter(ProviderConnectionKind kind, SemaphorePriority priority)
    {
        public ProviderConnectionKind Kind { get; } = kind;
        public SemaphorePriority Priority { get; } = priority;
        public TaskCompletionSource<Lease> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    internal sealed class Lease : IDisposable
    {
        private ProviderConnectionAdmission? _owner;
        private readonly ProviderConnectionKind _kind;

        internal Lease(ProviderConnectionAdmission owner, ProviderConnectionKind kind)
        {
            _owner = owner;
            _kind = kind;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _owner, null)?.Release(_kind);
        }
    }
}
