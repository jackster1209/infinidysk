using NzbWebDAV.Services;

namespace NzbWebDAV.Tests.Services;

public sealed class HealthCheckFailoverBufferTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task PartialBatch_FlushesOnlyWhenInternalTimerExpires()
    {
        var timeProvider = new ManualTimeProvider();
        var appended = new TaskCompletionSource<IReadOnlyList<string>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var buffer = new HealthCheckFailoverBuffer(
            ids =>
            {
                appended.TrySetResult(ids);
                return Task.CompletedTask;
            },
            timeProvider,
            CancellationToken.None);

        buffer.Add(Enumerable.Range(0, 10).Select(index => $"segment-{index}").ToArray());
        await WaitUntilAsync(() => timeProvider.ActiveTimers == 1);
        timeProvider.Advance(HealthCheckFailoverBuffer.PartialFlushDelay - TimeSpan.FromTicks(1));
        Assert.False(appended.Task.IsCompleted);

        timeProvider.Advance(TimeSpan.FromTicks(1));
        var batch = await appended.Task.WaitAsync(TestTimeout);
        Assert.Equal(10, batch.Count);
        await buffer.CompleteAsync();
    }

    [Fact]
    public async Task UsefulBatch_FlushesWithoutWaitingForTimer()
    {
        var timeProvider = new ManualTimeProvider();
        var appended = new TaskCompletionSource<IReadOnlyList<string>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var buffer = new HealthCheckFailoverBuffer(
            ids =>
            {
                appended.TrySetResult(ids);
                return Task.CompletedTask;
            },
            timeProvider,
            CancellationToken.None);

        buffer.Add(Enumerable.Range(0, HealthCheckFailoverBuffer.UsefulBatchSize)
            .Select(index => $"segment-{index}")
            .ToArray());

        var batch = await appended.Task.WaitAsync(TestTimeout);
        Assert.Equal(HealthCheckFailoverBuffer.UsefulBatchSize, batch.Count);
        Assert.Equal(0, timeProvider.ActiveTimers);
        await buffer.CompleteAsync();
    }

    [Fact]
    public async Task CompleteAsync_FlushesPendingPartialBatchWithoutWaitingForTimer()
    {
        var timeProvider = new ManualTimeProvider();
        var appended = new TaskCompletionSource<IReadOnlyList<string>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var buffer = new HealthCheckFailoverBuffer(
            ids =>
            {
                appended.TrySetResult(ids);
                return Task.CompletedTask;
            },
            timeProvider,
            CancellationToken.None);

        buffer.Add(["segment-0", "segment-1"]);
        var completion = buffer.CompleteAsync();

        Assert.Equal(["segment-0", "segment-1"], await appended.Task.WaitAsync(TestTimeout));
        await completion.WaitAsync(TestTimeout);
        Assert.Equal(0, timeProvider.ActiveTimers);
    }

    [Fact]
    public async Task Add_AfterCompletionIsRejected()
    {
        var buffer = new HealthCheckFailoverBuffer(
            _ => Task.CompletedTask,
            TimeProvider.System,
            CancellationToken.None);

        await buffer.CompleteAsync();

        var exception = Assert.Throws<InvalidOperationException>(
            () => buffer.Add(["too-late"]));
        Assert.Contains("no longer accepting work", exception.Message);
    }

    [Fact]
    public async Task AppendFailure_PropagatesFromCompletionAndRejectsLaterWork()
    {
        var expected = new InvalidOperationException("downstream append failed");
        var buffer = new HealthCheckFailoverBuffer(
            _ => Task.FromException(expected),
            TimeProvider.System,
            CancellationToken.None);
        buffer.Add(Enumerable.Range(0, HealthCheckFailoverBuffer.UsefulBatchSize)
            .Select(index => $"segment-{index}")
            .ToArray());

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => buffer.CompleteAsync());

        Assert.Same(expected, actual);
        Assert.Throws<InvalidOperationException>(() => buffer.Add(["too-late"]));
    }

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        var deadline = DateTimeOffset.UtcNow + TestTimeout;
        while (!predicate())
        {
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException("Condition was not reached before the test timeout.");
            await Task.Yield();
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly Lock _lock = new();
        private readonly List<ManualTimer> _timers = [];
        private TimeSpan _elapsed;

        public int ActiveTimers
        {
            get
            {
                lock (_lock) return _timers.Count(timer => timer.IsActive);
            }
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_lock) return DateTimeOffset.UnixEpoch + _elapsed;
        }

        public override long GetTimestamp()
        {
            lock (_lock) return _elapsed.Ticks;
        }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state);
            lock (_lock)
            {
                _timers.Add(timer);
                timer.Period = period;
                timer.IsActive = dueTime != Timeout.InfiniteTimeSpan;
                timer.DueAt = _elapsed + dueTime;
            }
            return timer;
        }

        public void Advance(TimeSpan delta)
        {
            List<(TimerCallback Callback, object? State)> callbacks = [];
            lock (_lock)
            {
                _elapsed += delta;
                foreach (var timer in _timers.ToArray())
                {
                    if (!timer.IsActive || timer.DueAt > _elapsed) continue;
                    callbacks.Add((timer.Callback, timer.State));
                    if (timer.Period == Timeout.InfiniteTimeSpan)
                        timer.IsActive = false;
                    else
                        timer.DueAt += timer.Period;
                }
            }

            foreach (var (callback, state) in callbacks)
                callback(state);
        }

        private bool Change(ManualTimer timer, TimeSpan dueTime, TimeSpan period)
        {
            lock (_lock)
            {
                if (timer.IsDisposed) return false;
                timer.Period = period;
                timer.IsActive = dueTime != Timeout.InfiniteTimeSpan;
                timer.DueAt = _elapsed + dueTime;
                return true;
            }
        }

        private void Remove(ManualTimer timer)
        {
            lock (_lock)
            {
                timer.IsDisposed = true;
                timer.IsActive = false;
                _timers.Remove(timer);
            }
        }

        private sealed class ManualTimer(
            ManualTimeProvider owner,
            TimerCallback callback,
            object? state) : ITimer
        {
            public TimerCallback Callback { get; } = callback;
            public object? State { get; } = state;
            public TimeSpan DueAt { get; set; }
            public TimeSpan Period { get; set; }
            public bool IsActive { get; set; }
            public bool IsDisposed { get; set; }

            public bool Change(TimeSpan dueTime, TimeSpan period) =>
                owner.Change(this, dueTime, period);

            public void Dispose() => owner.Remove(this);

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
