using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Clients.Usenet;

public sealed class HealthCheckProviderAdmissionRegistryTests
{
    [Fact]
    public void SaturatedProvider_RemainsPendingInSchedulerLayerWithoutAdmissionWaiter()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var generation = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        registry.Activate(generation.HealthAdmissionGeneration);
        var leases = new List<HealthCheckProviderLease>();

        try
        {
            for (var index = 0; index < 4; index++)
            {
                var acquired = registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low);
                Assert.Equal(HealthCheckProviderAdmissionState.Acquired, acquired.State);
                leases.Add(Assert.IsType<HealthCheckProviderLease>(acquired.Lease));
            }

            var blocked = registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low);

            Assert.Equal(HealthCheckProviderAdmissionState.TemporarilyUnavailable, blocked.State);
            Assert.Null(blocked.Lease);
            var admission = Assert.IsType<ProviderConnectionAdmissionSnapshot>(
                registry.GetSnapshot("provider-a")?.Admission);
            Assert.Equal(4, admission.ActiveMetadataOperations);
            Assert.Equal(0, admission.WaitingMetadataOperations);
        }
        finally
        {
            foreach (var lease in leases)
                lease.Dispose();
        }
    }

    [Fact]
    public void SameProviderKeyAcrossGenerations_ProducesDistinctAdmissionClaims()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var oldGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        using var newGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        registry.Activate(oldGeneration.HealthAdmissionGeneration);
        using var oldLease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);

        registry.Activate(newGeneration.HealthAdmissionGeneration);
        oldGeneration.Retire();
        using var newLease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);

        Assert.Equal(oldLease.ProviderKey, newLease.ProviderKey);
        Assert.NotEqual(oldLease.Claim.GenerationId, newLease.Claim.GenerationId);
        Assert.NotEqual(oldLease.Claim.AdmissionId, newLease.Claim.AdmissionId);
        Assert.Equal(1, oldGeneration.HealthAdmissionGeneration.ActivePins);
        Assert.Equal(1, newGeneration.HealthAdmissionGeneration.ActivePins);

        oldLease.Dispose();
        Assert.Equal(0, oldGeneration.HealthAdmissionGeneration.ActivePins);
        Assert.Equal(0, oldGeneration.InFlightConnections);
    }

    [Fact]
    public async Task OutstandingAdmissionLease_PinsRetiringProviderGeneration()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        var oldGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        var newGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        using var wrapper = new TestWrappingClient(oldGeneration);
        registry.Activate(oldGeneration.HealthAdmissionGeneration);
        using var lease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);

        registry.Activate(newGeneration.HealthAdmissionGeneration);
        var retirement = wrapper.ReplaceUnderlyingClientForTestsAsync(newGeneration);

        Assert.False(retirement.IsCompleted);
        Assert.Equal(1, oldGeneration.InFlightConnections);

        lease.Dispose();
        await retirement.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, oldGeneration.InFlightConnections);
    }

    [Fact]
    public async Task ProviderLease_ExecutesAgainstPinnedGenerationAfterReplacement()
    {
        var oldConnection = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            PipelinedStatHolds = ["segment@example"],
        };
        var newConnection = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            PipelinedStatHolds = [],
        };
        using var registry = new HealthCheckProviderAdmissionRegistry();
        var oldGeneration = CreateGeneration(
            "provider-a", providerLimit: 5, transferLimit: 2, connection: oldConnection);
        var newGeneration = CreateGeneration(
            "provider-a", providerLimit: 5, transferLimit: 2, connection: newConnection);
        using var streaming = new UsenetStreamingClient(
            oldGeneration,
            healthProviderAdmissions: registry);
        using var providerLease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);

        registry.Activate(newGeneration.HealthAdmissionGeneration);
        var retirement = streaming.ReplaceUnderlyingClientForTestsAsync(newGeneration);
        var config = new ConfigManager();
        using var gate = new HealthCheckConnectionGate(config);
        using var cts = new CancellationTokenSource();
        using var context = cts.Token.SetContext<HealthCheckAdmissionContext>(
            new ProviderAwareHealthCheckAdmissionContext(
                gate,
                HealthCheckAdmissionPriority.Background,
                GateLeasePreAcquired: true,
                providerLease));

        var result = await streaming.SweepProviderPipelinedAsync(
            "provider-a",
            ["segment@example"],
            depth: 1,
            progress: null,
            cancellationToken: cts.Token);

        Assert.Empty(result.Missing);
        Assert.Empty(result.Unanswered);
        Assert.Equal(1, oldConnection.BatchStatRequests);
        Assert.Equal(0, newConnection.BatchStatRequests);

        providerLease.Dispose();
        await retirement.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void RetiredGenerationNotifications_DoNotWakeTheActiveGeneration()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var oldGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        using var newGeneration = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        var notifications = new List<string>();
        registry.AvailabilityChanged += notifications.Add;
        registry.Activate(oldGeneration.HealthAdmissionGeneration);
        using var oldLease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);
        registry.Activate(newGeneration.HealthAdmissionGeneration);
        oldGeneration.Retire();
        notifications.Clear();

        oldLease.Dispose();

        Assert.Empty(notifications);
        using var newLease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("provider-a", SemaphorePriority.Low).Lease);
        notifications.Clear();
        newLease.Dispose();
        Assert.Equal(["provider-a"], notifications);
    }

    [Fact]
    public void LegacyProvider_ReservesRealPoolPermitWithoutInventingMetadataCapacity()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var generation = CreateGeneration("legacy", providerLimit: 5, transferLimit: null);
        registry.Activate(generation.HealthAdmissionGeneration);

        var attempt = registry.TryAcquireMetadata("legacy", SemaphorePriority.Low);
        var snapshot = Assert.IsType<HealthCheckProviderCapacitySnapshot>(
            registry.GetSnapshot("legacy"));

        // No transfer/metadata split is synthesised; the lease is backed by one real permit.
        Assert.Equal(HealthCheckProviderAdmissionState.Acquired, attempt.State);
        var lease = Assert.IsType<HealthCheckProviderLease>(attempt.Lease);
        Assert.True(lease.IsLegacySharedPool);
        Assert.True(snapshot.IsLegacySharedPool);
        Assert.Null(snapshot.Admission);
        Assert.Equal(1, generation.HealthAdmissionGeneration.ActivePins);

        lease.Dispose();
        Assert.Equal(0, generation.HealthAdmissionGeneration.ActivePins);
    }

    [Fact]
    public void LegacyProvider_StopsAdmittingAtPhysicalPoolWidth()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var generation = CreateGeneration("legacy", providerLimit: 3, transferLimit: null);
        registry.Activate(generation.HealthAdmissionGeneration);

        var leases = new List<HealthCheckProviderLease>();
        for (var index = 0; index < 3; index++)
        {
            var attempt = registry.TryAcquireMetadata("legacy", SemaphorePriority.Low);
            Assert.Equal(HealthCheckProviderAdmissionState.Acquired, attempt.State);
            leases.Add(Assert.IsType<HealthCheckProviderLease>(attempt.Lease));
        }

        // The pool is fully reserved, so the scheduler is told to keep the work pending
        // instead of queueing a fourth waiter on the pool gate.
        var saturated = registry.TryAcquireMetadata("legacy", SemaphorePriority.Low);
        Assert.Equal(HealthCheckProviderAdmissionState.TemporarilyUnavailable, saturated.State);
        Assert.Null(saturated.Lease);
        Assert.Equal(3, generation.HealthAdmissionGeneration.ActivePins);

        // Releasing an unconsumed reservation hands the permit back.
        leases[0].Dispose();
        var reacquired = registry.TryAcquireMetadata("legacy", SemaphorePriority.Low);
        Assert.Equal(HealthCheckProviderAdmissionState.Acquired, reacquired.State);
        reacquired.Lease?.Dispose();
        foreach (var lease in leases.Skip(1)) lease.Dispose();
    }

    [Fact]
    public void LegacyLease_SurvivesRetirementAndReleasesItsPermit()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var generation = CreateGeneration("legacy", providerLimit: 1, transferLimit: null);
        registry.Activate(generation.HealthAdmissionGeneration);

        var lease = Assert.IsType<HealthCheckProviderLease>(
            registry.TryAcquireMetadata("legacy", SemaphorePriority.Low).Lease);

        // Retirement must not strand the reserved permit or the generation pin.
        generation.HealthAdmissionGeneration.Retire();
        Assert.Equal(1, generation.HealthAdmissionGeneration.ActivePins);

        lease.Dispose();
        Assert.Equal(0, generation.HealthAdmissionGeneration.ActivePins);
        Assert.Equal(0, generation.InFlightConnections);
    }

    [Fact]
    public async Task MissingProvider_ReturnsPinnedUnansweredLease()
    {
        using var registry = new HealthCheckProviderAdmissionRegistry();
        using var generation = CreateGeneration("provider-a", providerLimit: 5, transferLimit: 2);
        registry.Activate(generation.HealthAdmissionGeneration);

        var attempt = registry.TryAcquireMetadata("missing", SemaphorePriority.Low);

        Assert.Equal(HealthCheckProviderAdmissionState.ProviderUnavailable, attempt.State);
        using var lease = Assert.IsType<HealthCheckProviderLease>(attempt.Lease);
        Assert.Equal(1, generation.HealthAdmissionGeneration.ActivePins);
        var sweep = await lease.SweepProviderPipelinedAsync(
            ["missing@example"],
            depth: 1,
            progress: null,
            cancellationToken: CancellationToken.None);
        Assert.Equal(["missing@example"], sweep.Unanswered);
        lease.Dispose();
        Assert.Equal(0, generation.HealthAdmissionGeneration.ActivePins);
    }

    private static MultiProviderNntpClient CreateGeneration(
        string providerKey,
        int providerLimit,
        int? transferLimit,
        MultiProviderNntpClientTests.ScriptedNntpClient? connection = null)
    {
        connection ??= new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
        };
        var pool = new ConnectionPool<INntpClient>(
            providerLimit,
            _ => ValueTask.FromResult<INntpClient>(connection));
        var provider = new MultiConnectionNntpClient(
            pool,
            ProviderType.Pooled,
            new ProviderCircuitBreaker(providerKey),
            providerKey,
            metricsKey: providerKey,
            maxTransferConnections: transferLimit);
        return new MultiProviderNntpClient([provider]);
    }

    private sealed class TestWrappingClient(INntpClient inner) : WrappingNntpClient(inner);
}
