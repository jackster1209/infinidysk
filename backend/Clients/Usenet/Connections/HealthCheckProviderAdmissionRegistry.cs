using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Models;
using Serilog;

namespace NzbWebDAV.Clients.Usenet.Connections;

internal enum HealthCheckProviderAdmissionState
{
    Acquired,
    TemporarilyUnavailable,
    ProviderUnavailable,
}

internal readonly record struct HealthCheckProviderAdmissionAttempt(
    HealthCheckProviderAdmissionState State,
    HealthCheckProviderLease? Lease = null);

internal sealed record HealthCheckProviderCapacitySnapshot(
    string ProviderKey,
    Guid GenerationId,
    bool IsAvailable,
    bool IsLegacySharedPool,
    ProviderConnectionAdmissionSnapshot? Admission);

internal readonly record struct HealthCheckProviderAdmissionClaim(
    string ProviderKey,
    Guid GenerationId,
    Guid AdmissionId);

/// <summary>
/// Publishes the active provider generation to the health scheduler. Acquisitions pin the
/// selected generation before configuration replacement can retire it, so a stable provider
/// key can never authorize admission against a different physical client generation.
/// </summary>
public sealed class HealthCheckProviderAdmissionRegistry : IDisposable
{
    private readonly Lock _lock = new();
    private HealthCheckProviderGeneration? _current;
    private bool _disposed;

    internal event Action<string>? AvailabilityChanged;

    internal HealthCheckProviderAdmissionAttempt TryAcquireMetadata(
        string providerKey,
        SemaphorePriority priority)
    {
        HealthCheckProviderGeneration generation;
        lock (_lock)
        {
            if (_disposed || _current is not { } current || !current.TryPin())
                return new(HealthCheckProviderAdmissionState.ProviderUnavailable);
            generation = current;
        }

        return generation.TryAcquireMetadataPinned(providerKey, priority);
    }

    internal HealthCheckProviderCapacitySnapshot? GetSnapshot(string providerKey)
    {
        HealthCheckProviderGeneration generation;
        lock (_lock)
        {
            if (_disposed || _current is not { } current || !current.TryPin())
                return null;
            generation = current;
        }

        try
        {
            return generation.GetSnapshotPinned(providerKey);
        }
        finally
        {
            generation.ReleasePin();
        }
    }

    internal void Activate(HealthCheckProviderGeneration generation)
    {
        ArgumentNullException.ThrowIfNull(generation);

        HealthCheckProviderGeneration? previous;
        string[] changedKeys;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (ReferenceEquals(_current, generation)) return;

            previous = _current;
            if (previous is not null)
                previous.AvailabilityChanged -= OnGenerationAvailabilityChanged;
            _current = generation;
            generation.AvailabilityChanged += OnGenerationAvailabilityChanged;
            var previousKeys = previous?.ProviderKeys ?? Array.Empty<string>();
            changedKeys = previousKeys
                .Concat(generation.ProviderKeys)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
        }

        foreach (var providerKey in changedKeys)
            NotifyAvailabilityChanged(providerKey);
    }

    internal void Deactivate(Guid generationId)
    {
        string[] changedKeys;
        lock (_lock)
        {
            if (_disposed || _current?.GenerationId != generationId) return;
            changedKeys = _current.ProviderKeys.ToArray();
            _current.AvailabilityChanged -= OnGenerationAvailabilityChanged;
            _current = null;
        }

        foreach (var providerKey in changedKeys)
            NotifyAvailabilityChanged(providerKey);
    }

    private void OnGenerationAvailabilityChanged(Guid generationId, string providerKey)
    {
        lock (_lock)
        {
            if (_disposed || _current?.GenerationId != generationId) return;
        }

        NotifyAvailabilityChanged(providerKey);
    }

    private void NotifyAvailabilityChanged(string providerKey)
    {
        if (AvailabilityChanged is not { } availabilityChanged) return;
        foreach (var handler in availabilityChanged.GetInvocationList().Cast<Action<string>>())
        {
            try
            {
                handler(providerKey);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                Log.Warning(e, "Health provider availability callback failed for {ProviderKey}", providerKey);
            }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            if (_current is not null)
                _current.AvailabilityChanged -= OnGenerationAvailabilityChanged;
            _current = null;
            AvailabilityChanged = null;
        }
    }
}

/// <summary>One immutable set of provider clients created from a configuration snapshot.</summary>
internal sealed class HealthCheckProviderGeneration
{
    private readonly MultiProviderNntpClient _owner;
    private readonly Dictionary<string, MultiConnectionNntpClient> _providers;
    private readonly List<(MultiConnectionNntpClient Provider,
        Action<ProviderConnectionAdmissionSnapshot> Handler)> _subscriptions = [];
    private readonly List<(MultiConnectionNntpClient Provider,
        EventHandler<ConnectionPoolStats.ConnectionPoolChangedEventArgs> Handler)>
        _poolSubscriptions = [];
    private readonly Lock _lock = new();
    private int _activePins;
    private bool _retired;
    private bool _disposed;

    internal HealthCheckProviderGeneration(
        MultiProviderNntpClient owner,
        IReadOnlyList<MultiConnectionNntpClient> providers)
    {
        _owner = owner;
        GenerationId = Guid.NewGuid();
        _providers = providers
            .GroupBy(provider => provider.MetricsKey, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);

        foreach (var (providerKey, provider) in _providers)
        {
            Action<ProviderConnectionAdmissionSnapshot> handler =
                _ => AvailabilityChanged?.Invoke(GenerationId, providerKey);
            provider.HealthAdmissionAvailabilityChanged += handler;
            _subscriptions.Add((provider, handler));

            // Legacy shared-pool providers admit against the physical pool, so pool
            // occupancy - not a metadata budget - is what makes them schedulable again.
            if (provider.HasConnectionAdmission) continue;
            EventHandler<ConnectionPoolStats.ConnectionPoolChangedEventArgs> poolHandler =
                (_, _) => AvailabilityChanged?.Invoke(GenerationId, providerKey);
            provider.HealthPoolChanged += poolHandler;
            _poolSubscriptions.Add((provider, poolHandler));
        }
    }

    internal Guid GenerationId { get; }
    internal IReadOnlyCollection<string> ProviderKeys => _providers.Keys;
    internal int ActivePins => Volatile.Read(ref _activePins);
    internal event Action<Guid, string>? AvailabilityChanged;

    internal bool TryPin()
    {
        lock (_lock)
        {
            if (_retired || _disposed) return false;
            _activePins++;
            return true;
        }
    }

    internal HealthCheckProviderAdmissionAttempt TryAcquireMetadataPinned(
        string providerKey,
        SemaphorePriority priority)
    {
        if (!_providers.TryGetValue(providerKey, out var provider)
            || !_owner.IsProviderAvailableForHealthAdmission(provider))
        {
#pragma warning disable CA2000 // ownership transfers to the returned attempt; the caller disposes the lease
            return new(
                HealthCheckProviderAdmissionState.ProviderUnavailable,
                new HealthCheckProviderLease(this, providerKey));
#pragma warning restore CA2000
        }

        if (!provider.HasConnectionAdmission)
        {
            // Legacy shared-pool providers have no transfer/metadata split to admit against.
            // Reserve one real physical permit instead of inventing a budget for them, so
            // Auto still never schedules health work the pool cannot execute.
#pragma warning disable CA2000 // ownership transfers to the lease returned below
            if (!provider.TryReserveHealthConnection(out var poolReservation)
                || poolReservation is null)
            {
                ReleasePin();
                return new(HealthCheckProviderAdmissionState.TemporarilyUnavailable);
            }

#pragma warning disable CA2000 // ownership transfers to the returned attempt; the caller disposes the lease
            return new(
                HealthCheckProviderAdmissionState.Acquired,
                new HealthCheckProviderLease(
                    this,
                    poolReservation,
                    new HealthCheckProviderAdmissionClaim(providerKey, GenerationId, Guid.Empty)));
#pragma warning restore CA2000
#pragma warning restore CA2000
        }

#pragma warning disable CA2000 // ownership transfers to the returned lease/attempt below
        if (!provider.TryAcquireHealthMetadata(priority, out var admissionLease)
            || admissionLease is null)
        {
            ReleasePin();
            return new(HealthCheckProviderAdmissionState.TemporarilyUnavailable);
        }

        if (provider.ConnectionAdmissionId is not { } admissionId)
        {
            admissionLease.Dispose();
            ReleasePin();
            return new(HealthCheckProviderAdmissionState.ProviderUnavailable);
        }

        var claim = new HealthCheckProviderAdmissionClaim(
            providerKey,
            GenerationId,
            admissionId);
#pragma warning disable CA2000 // ownership transfers to the returned attempt; the caller disposes the lease
        return new(
            HealthCheckProviderAdmissionState.Acquired,
            new HealthCheckProviderLease(this, admissionLease, claim));
#pragma warning restore CA2000
#pragma warning restore CA2000
    }

    internal HealthCheckProviderCapacitySnapshot? GetSnapshotPinned(string providerKey)
    {
        if (!_providers.TryGetValue(providerKey, out var provider)) return null;
        return new HealthCheckProviderCapacitySnapshot(
            providerKey,
            GenerationId,
            _owner.IsProviderAvailableForHealthAdmission(provider),
            !provider.HasConnectionAdmission,
            provider.GetConnectionAdmissionSnapshot());
    }

    internal Task<ProviderVerificationSweepResult> SweepProviderPipelinedAsync(
        string providerKey,
        IReadOnlyList<string> segmentIds,
        int depth,
        IProgress<int>? progress,
        CancellationToken cancellationToken) =>
        _owner.SweepProviderPipelinedAsync(
            providerKey,
            segmentIds,
            depth,
            progress,
            cancellationToken);

    internal void ReleasePin()
    {
        lock (_lock)
        {
            _activePins--;
            if (_activePins < 0)
            {
                _activePins = 0;
                Log.Error("Health provider generation {GenerationId} was over-released", GenerationId);
            }
        }
    }

    internal void Retire()
    {
        lock (_lock)
            _retired = true;
    }

    internal void Close()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _retired = true;
        }

        foreach (var (provider, handler) in _subscriptions)
            provider.HealthAdmissionAvailabilityChanged -= handler;
        _subscriptions.Clear();
        foreach (var (provider, handler) in _poolSubscriptions)
            provider.HealthPoolChanged -= handler;
        _poolSubscriptions.Clear();
        AvailabilityChanged = null;
    }
}

internal sealed class HealthCheckProviderLease : IDisposable
{
    private HealthCheckProviderGeneration? _generation;
    private readonly bool _returnUnanswered;
#pragma warning disable CA2213 // released exactly once through the Interlocked exchanges below
    private ConnectionPool<INntpClient>.Reservation? _poolReservation;
#pragma warning restore CA2213
#pragma warning disable CA2213 // disposed exactly once through the Interlocked.Exchange in Dispose
    private ProviderConnectionAdmission.Lease? _admissionLease;
#pragma warning restore CA2213

    internal HealthCheckProviderLease(
        HealthCheckProviderGeneration generation,
        ProviderConnectionAdmission.Lease admissionLease,
        HealthCheckProviderAdmissionClaim claim)
    {
        _generation = generation;
        _admissionLease = admissionLease;
        Claim = claim;
    }

    internal HealthCheckProviderLease(
        HealthCheckProviderGeneration generation,
        ConnectionPool<INntpClient>.Reservation poolReservation,
        HealthCheckProviderAdmissionClaim claim)
    {
        _generation = generation;
        _poolReservation = poolReservation;
        IsLegacySharedPool = true;
        Claim = claim;
    }

    internal HealthCheckProviderLease(
        HealthCheckProviderGeneration generation,
        string providerKey)
    {
        _generation = generation;
        _returnUnanswered = true;
        Claim = new HealthCheckProviderAdmissionClaim(
            providerKey,
            generation.GenerationId,
            Guid.Empty);
    }

    internal HealthCheckProviderAdmissionClaim Claim { get; }
    internal bool IsLegacySharedPool { get; }

    /// <summary>
    /// Hands this lease's physical pool permit to the pool that issued it, exactly once.
    /// </summary>
    internal ConnectionPool<INntpClient>.Reservation? TryTakePoolReservation(
        ConnectionPool<INntpClient> pool)
    {
        var reservation = Volatile.Read(ref _poolReservation);
        if (reservation is null || !reservation.Owns(pool)) return null;
        return Interlocked.CompareExchange(ref _poolReservation, null, reservation) == reservation
            ? reservation
            : null;
    }

    internal HealthCheckProviderAdmissionClaim? PreAcquiredClaim =>
        _returnUnanswered ? null : Claim;
    internal string ProviderKey => Claim.ProviderKey;

    internal Task<ProviderVerificationSweepResult> SweepProviderPipelinedAsync(
        IReadOnlyList<string> segmentIds,
        int depth,
        IProgress<int>? progress,
        CancellationToken cancellationToken)
    {
        var generation = Volatile.Read(ref _generation)
            ?? throw new ObjectDisposedException(nameof(HealthCheckProviderLease));
        if (_returnUnanswered)
        {
            cancellationToken.ThrowIfCancellationRequested();
            progress?.Report(segmentIds.Count);
            return Task.FromResult(ProviderVerificationSweepResult.AllUnanswered(segmentIds));
        }
        return generation.SweepProviderPipelinedAsync(
            ProviderKey,
            segmentIds,
            depth,
            progress,
            cancellationToken);
    }

    public void Dispose()
    {
        var generation = Interlocked.Exchange(ref _generation, null);
        if (generation is null) return;
        try
        {
            Interlocked.Exchange(ref _admissionLease, null)?.Dispose();
        }
        finally
        {
            try
            {
                // Never consumed by a connection acquisition (cancelled, failed handshake,
                // or a sweep that never borrowed) - hand the permit back to the pool.
                Interlocked.Exchange(ref _poolReservation, null)?.Dispose();
            }
            finally
            {
                generation.ReleasePin();
            }
        }
    }
}
