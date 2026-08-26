using NzbWebDAV.Config;
using Serilog;

namespace NzbWebDAV.Services;

public enum HealthCheckAdmissionPriority
{
    Queue,
    Background,
}

public sealed record HealthCheckConnectionGateSnapshot(
    /// <summary>Explicit aggregate ceiling, or null in Auto (provider-aware) mode.</summary>
    int? Limit,
    int Active,
    int WaitingQueue,
    int WaitingBackground,
    int PeakActive,
    int PeakWaitingQueue,
    int PeakWaitingBackground);

/// <summary>
/// Process-wide admission gate for NNTP work that verifies article existence.
/// Queue verification receives newly released capacity before background health work.
/// </summary>
public sealed class HealthCheckConnectionGate : IDisposable
{
    private readonly ConfigManager _configManager;
    private readonly Lock _lock = new();
    private readonly LinkedList<Waiter> _queueWaiters = [];
    private readonly LinkedList<Waiter> _backgroundWaiters = [];
    private int _active;
    private int _peakActive;
    private int _peakWaitingQueue;
    private int _peakWaitingBackground;
    private bool _disposed;

    internal event Action? AvailabilityChanged;

    public HealthCheckConnectionGate(ConfigManager configManager)
    {
        _configManager = configManager;
        _configManager.OnConfigChanged += OnConfigChanged;
    }

    public Task<Lease> AcquireAsync(
        HealthCheckAdmissionPriority priority,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Waiter? queuedWaiter = null;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (CanEnterImmediately(priority))
            {
                _active++;
                _peakActive = Math.Max(_peakActive, _active);
                return Task.FromResult(new Lease(this));
            }

            queuedWaiter = new Waiter(priority);
            GetQueue(priority).AddLast(queuedWaiter);
            RecordWaiterPeak(priority);
        }

        RegisterCancellation(queuedWaiter, cancellationToken);
        return queuedWaiter.Completion.Task;
    }

    /// <summary>
    /// Attempts to enter without joining the gate queue. Scheduler-owned work remains
    /// pending in the scheduler when the explicit aggregate ceiling is unavailable.
    /// Existing queue/background waiters retain their priority.
    /// </summary>
    internal bool TryAcquire(
        HealthCheckAdmissionPriority priority,
        out Lease? lease)
    {
        lock (_lock)
        {
            if (_disposed || !CanEnterImmediately(priority))
            {
                lease = null;
                return false;
            }

            _active++;
            _peakActive = Math.Max(_peakActive, _active);
            lease = new Lease(this);
            return true;
        }
    }

    private void RegisterCancellation(Waiter waiter, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled) return;

        // Register outside _lock. Register invokes synchronously when cancellation won the
        // enqueue race; doing that under _lock would run CancelWaiter — and complete the
        // waiters it dispatches — while the outer admission critical section was still held.
        var registration = cancellationToken.Register(
            () => CancelWaiter(waiter, cancellationToken));
        _ = waiter.Completion.Task.ContinueWith(
            static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
            registration,
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    public HealthCheckConnectionGateSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return CreateSnapshot();
        }
    }

    internal HealthCheckConnectionGateSnapshot TakeMetricsSnapshot()
    {
        lock (_lock)
        {
            var snapshot = CreateSnapshot();
            _peakActive = _active;
            _peakWaitingQueue = _queueWaiters.Count;
            _peakWaitingBackground = _backgroundWaiters.Count;
            return snapshot;
        }
    }

    private HealthCheckConnectionGateSnapshot CreateSnapshot() => new(
        GetLimit(),
        _active,
        _queueWaiters.Count,
        _backgroundWaiters.Count,
        _peakActive,
        _peakWaitingQueue,
        _peakWaitingBackground);

    /// <summary>Null means Auto: no aggregate ceiling, so the gate never withholds capacity.</summary>
    private int? GetLimit() => _configManager.GetHealthCheckCeiling();

    private bool CanEnterImmediately(HealthCheckAdmissionPriority priority)
    {
        if (GetLimit() is { } limit && _active >= limit) return false;
        if (GetQueue(priority).Count > 0) return false;
        return priority == HealthCheckAdmissionPriority.Queue || _queueWaiters.Count == 0;
    }

    private void Release()
    {
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready;
        lock (_lock)
        {
            if (_disposed) return;
            _active--;
            ready = DispatchWaiters();
        }

        CompleteReadyWaiters(ready);
        NotifyAvailabilityChanged();
    }

    private void CancelWaiter(Waiter waiter, CancellationToken cancellationToken)
    {
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready;
        bool removed;
        lock (_lock)
        {
            removed = GetQueue(waiter.Priority).Remove(waiter);
            ready = removed && !_disposed ? DispatchWaiters() : [];
        }

        if (removed) waiter.Completion.TrySetCanceled(cancellationToken);
        CompleteReadyWaiters(ready);
        if (removed) NotifyAvailabilityChanged();
    }

    private void OnConfigChanged(object? sender, ConfigManager.ConfigEventArgs args)
    {
        if (!args.ChangedConfig.ContainsKey(ConfigKeys.RepairHealthcheckConcurrency)) return;

        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready;
        lock (_lock)
        {
            if (_disposed) return;
            ready = DispatchWaiters();
        }

        CompleteReadyWaiters(ready);
        NotifyAvailabilityChanged();
    }

    private void NotifyAvailabilityChanged()
    {
        if (AvailabilityChanged is not { } availabilityChanged) return;
        foreach (var handler in availabilityChanged.GetInvocationList().Cast<Action>())
        {
            try
            {
                handler();
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                Log.Warning(e, "Health connection gate availability callback failed");
            }
        }
    }

    private List<(TaskCompletionSource<Lease> Completion, Lease Lease)> DispatchWaiters()
    {
        List<(TaskCompletionSource<Lease>, Lease)> ready = [];
        var limit = GetLimit();
        while (limit is not { } ceiling || _active < ceiling)
        {
            var waiter = TakeFirst(_queueWaiters) ?? TakeFirst(_backgroundWaiters);
            if (waiter is null) break;
            _active++;
            _peakActive = Math.Max(_peakActive, _active);
            ready.Add((waiter.Completion, new Lease(this)));
        }

        return ready;
    }

    private void RecordWaiterPeak(HealthCheckAdmissionPriority priority)
    {
        if (priority == HealthCheckAdmissionPriority.Queue)
            _peakWaitingQueue = Math.Max(_peakWaitingQueue, _queueWaiters.Count);
        else
            _peakWaitingBackground = Math.Max(_peakWaitingBackground, _backgroundWaiters.Count);
    }

    private LinkedList<Waiter> GetQueue(HealthCheckAdmissionPriority priority) => priority switch
    {
        HealthCheckAdmissionPriority.Queue => _queueWaiters,
        HealthCheckAdmissionPriority.Background => _backgroundWaiters,
        _ => throw new ArgumentOutOfRangeException(nameof(priority), priority, null),
    };

    private static Waiter? TakeFirst(LinkedList<Waiter> queue)
    {
        if (queue.First is not { } first) return null;
        queue.RemoveFirst();
        return first.Value;
    }

    private static void CompleteReadyWaiters(
        List<(TaskCompletionSource<Lease> Completion, Lease Lease)> ready)
    {
        foreach (var (completion, lease) in ready)
        {
            if (!completion.TrySetResult(lease)) lease.Dispose();
        }
    }

    public void Dispose()
    {
        List<Waiter> waiters;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _configManager.OnConfigChanged -= OnConfigChanged;
            waiters = _queueWaiters.Concat(_backgroundWaiters).ToList();
            _queueWaiters.Clear();
            _backgroundWaiters.Clear();
            AvailabilityChanged = null;
        }

        foreach (var waiter in waiters)
        {
            waiter.Completion.TrySetException(
                new ObjectDisposedException(nameof(HealthCheckConnectionGate)));
        }
    }

    private sealed class Waiter(HealthCheckAdmissionPriority priority)
    {
        public HealthCheckAdmissionPriority Priority { get; } = priority;
        public TaskCompletionSource<Lease> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed class Lease : IDisposable
    {
        private HealthCheckConnectionGate? _owner;

        internal Lease(HealthCheckConnectionGate owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _owner, null)?.Release();
        }
    }
}
