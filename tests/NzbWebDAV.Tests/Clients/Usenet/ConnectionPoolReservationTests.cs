using NzbWebDAV.Clients.Usenet.Concurrency;
using NzbWebDAV.Clients.Usenet.Connections;

namespace NzbWebDAV.Tests.Clients.Usenet;

/// <summary>
/// The health scheduler reserves one real physical permit for legacy shared-pool providers
/// before it marks an assignment active, then hands that permit to the borrow that follows.
/// These tests pin the transfer, the fairness rule, and the release paths.
/// </summary>
public class ConnectionPoolReservationTests
{
    private static readonly TimeSpan WaitBudget = TimeSpan.FromSeconds(10);

    private static ConnectionPool<object> CreatePool(int maxConnections) =>
        new(maxConnections, _ => ValueTask.FromResult(new object()), TimeSpan.FromMinutes(5));

    [Fact]
    public async Task Reservation_IsConsumedInsteadOfQueueingOnTheGateTwice()
    {
        await using var pool = CreatePool(maxConnections: 1);

        Assert.True(pool.TryReserve(out var reservation));
        Assert.NotNull(reservation);

        // The permit is already held, so a second walk-up must fail.
        Assert.False(pool.TryReserve(out var second));
        Assert.Null(second);

        using var borrowed = await pool
            .GetConnectionLockAsync(SemaphorePriority.Low, reservation, CancellationToken.None)
            .WaitAsync(WaitBudget);

        Assert.Equal(1, pool.ActiveConnections);
    }

    [Fact]
    public void TryReserve_StopsAtPoolWidth()
    {
        using var pool = CreatePool(maxConnections: 3);

        var reservations = new List<ConnectionPool<object>.Reservation>();
        for (var index = 0; index < 3; index++)
        {
            Assert.True(pool.TryReserve(out var reservation));
            reservations.Add(reservation!);
        }

        Assert.False(pool.TryReserve(out _));

        reservations[0].Dispose();
        Assert.True(pool.TryReserve(out var reclaimed));
        reclaimed!.Dispose();
        foreach (var reservation in reservations.Skip(1)) reservation.Dispose();
    }

    [Fact]
    public async Task TryReserve_NeverBypassesAnExistingPoolWaiter()
    {
        // Fairness: capacity that exists while a borrower is parked belongs to that
        // borrower, so a scheduler reservation must not overtake the queue.
        await using var pool = CreatePool(maxConnections: 1);

        using var held = await pool
            .GetConnectionLockAsync(SemaphorePriority.Low, CancellationToken.None)
            .WaitAsync(WaitBudget);

        var queued = pool.GetConnectionLockAsync(SemaphorePriority.High, CancellationToken.None);
        Assert.False(queued.IsCompleted);
        Assert.False(pool.TryReserve(out _));

        held.Dispose();
        using var promoted = await queued.WaitAsync(WaitBudget);

        // The waiter took the freed permit, not the walk-up reservation.
        Assert.False(pool.TryReserve(out _));
    }

    [Fact]
    public async Task DisposedReservation_ReturnsThePermitAndCannotBeConsumed()
    {
        await using var pool = CreatePool(maxConnections: 1);

        Assert.True(pool.TryReserve(out var reservation));
        reservation!.Dispose();

        // Released, so the pool is borrowable again...
        using var borrowed = await pool
            .GetConnectionLockAsync(SemaphorePriority.Low, CancellationToken.None)
            .WaitAsync(WaitBudget);

        // ...and the spent reservation can never be redeemed a second time.
        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.GetConnectionLockAsync(
            SemaphorePriority.Low, reservation, CancellationToken.None));
    }

    [Fact]
    public async Task LearnedConnectionLimitShrink_StopsGrantingNewReservations()
    {
        var shouldFail = true;
#pragma warning disable CA2000 // the pool owns nothing disposable here
        await using var pool = new ConnectionPool<object>(
            6,
            _ => shouldFail
                ? ValueTask.FromException<object>(new InvalidOperationException("502 too many"))
                : ValueTask.FromResult(new object()),
            TimeSpan.FromMinutes(5),
            connectionLimitDetector: _ => 2);
#pragma warning restore CA2000

        // Learn the lower limit from a failed handshake, which shrinks the effective width.
        Assert.True(pool.TryReserve(out var probe));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.GetConnectionLockAsync(
            SemaphorePriority.Low, probe, CancellationToken.None));
        Assert.Equal(1, pool.EffectiveMaxConnections);

        shouldFail = false;
        Assert.True(pool.TryReserve(out var allowed));

        // Nothing beyond the newly learned width may be reserved.
        Assert.False(pool.TryReserve(out _));
        allowed!.Dispose();
    }

    [Fact]
    public async Task FailedHandshake_ReleasesTheReservedPermit()
    {
        var shouldFail = true;
#pragma warning disable CA2000 // the pool owns nothing disposable here
        await using var pool = new ConnectionPool<object>(
            1,
            _ => shouldFail
                ? ValueTask.FromException<object>(new InvalidOperationException("handshake"))
                : ValueTask.FromResult(new object()),
            TimeSpan.FromMinutes(5));
#pragma warning restore CA2000

        Assert.True(pool.TryReserve(out var reservation));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.GetConnectionLockAsync(
            SemaphorePriority.Low, reservation, CancellationToken.None));

        // The permit must go back to the pool rather than leaking with the failed attempt.
        shouldFail = false;
        using var borrowed = await pool
            .GetConnectionLockAsync(SemaphorePriority.Low, CancellationToken.None)
            .WaitAsync(WaitBudget);
        Assert.Equal(1, pool.ActiveConnections);
    }
}
