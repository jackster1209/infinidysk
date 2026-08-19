using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Extensions;
using NzbWebDAV.Services;

namespace NzbWebDAV.Tests.Services;

public sealed class HealthCheckConnectionGateTests
{
    [Fact]
    public void ContextualTokenSource_PreservesHealthAdmissionContext()
    {
        var config = CreateConfig(1);
        using var gate = new HealthCheckConnectionGate(config);
        using var parent = new CancellationTokenSource();
        var context = new HealthCheckAdmissionContext(
            gate,
            HealthCheckAdmissionPriority.Background);
        using var registration = parent.Token.SetContext(context);
        using var linked = ContextualCancellationTokenSource.CreateLinkedTokenSource(parent.Token);

        Assert.Same(context, linked.Token.GetContext<HealthCheckAdmissionContext>());
    }

    [Fact]
    public async Task AcquireAsync_EnforcesAggregateLimit()
    {
        var config = CreateConfig(2);
        using var gate = new HealthCheckConnectionGate(config);
        using var first = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        using var second = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);

        var waiting = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        Assert.False(waiting.IsCompleted);

        first.Dispose();
        using var third = await waiting.WaitAsync(TimeSpan.FromSeconds(1));

        var snapshot = gate.GetSnapshot();
        Assert.Equal(2, snapshot.Active);
        Assert.Equal(0, snapshot.WaitingBackground);
    }

    [Fact]
    public async Task Release_AdmitsQueueBeforeBackground()
    {
        var config = CreateConfig(1);
        using var gate = new HealthCheckConnectionGate(config);
        using var active = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var background = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var queue = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Queue, CancellationToken.None);

        active.Dispose();
        using var queueLease = await queue.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(background.IsCompleted);

        queueLease.Dispose();
        using var backgroundLease = await background.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Decrease_DrainsWithoutCancellingActiveLeases()
    {
        var config = CreateConfig(3);
        using var gate = new HealthCheckConnectionGate(config);
        var leases = new[]
        {
            await gate.AcquireAsync(HealthCheckAdmissionPriority.Background, CancellationToken.None),
            await gate.AcquireAsync(HealthCheckAdmissionPriority.Background, CancellationToken.None),
            await gate.AcquireAsync(HealthCheckAdmissionPriority.Background, CancellationToken.None),
        };
        var waiting = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);

        SetLimit(config, 1);
        leases[0].Dispose();
        leases[1].Dispose();
        Assert.False(waiting.IsCompleted);

        leases[2].Dispose();
        using var admitted = await waiting.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(1, gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task Increase_WakesWaitingWorkImmediately()
    {
        var config = CreateConfig(1);
        using var gate = new HealthCheckConnectionGate(config);
        using var active = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var second = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        var third = gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);

        SetLimit(config, 3);

        using var secondLease = await second.WaitAsync(TimeSpan.FromSeconds(1));
        using var thirdLease = await third.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(3, gate.GetSnapshot().Active);
    }

    [Fact]
    public async Task AcquireAsync_CancelsWaitingAdmissionCleanly()
    {
        var config = CreateConfig(1);
        using var gate = new HealthCheckConnectionGate(config);
        using var active = await gate.AcquireAsync(
            HealthCheckAdmissionPriority.Background, CancellationToken.None);
        using var cts = new CancellationTokenSource();
        var waiting = gate.AcquireAsync(HealthCheckAdmissionPriority.Background, cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(0, gate.GetSnapshot().WaitingBackground);
    }

    private static ConfigManager CreateConfig(int limit)
    {
        var config = new ConfigManager();
        SetLimit(config, limit);
        return config;
    }

    private static void SetLimit(ConfigManager config, int limit)
    {
        config.UpdateValues([
            new ConfigItem
            {
                ConfigName = ConfigKeys.RepairHealthcheckConcurrency,
                ConfigValue = limit.ToString(),
            },
        ]);
    }
}
