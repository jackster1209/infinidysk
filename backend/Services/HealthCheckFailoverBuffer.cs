using System.Threading.Channels;

namespace NzbWebDAV.Services;

/// <summary>
/// Coalesces unresolved ids from concurrent upstream assignments before appending them to
/// the next provider session. Full buffers move immediately; a short internal delay lets
/// partial buffers absorb adjacent completions without turning the delay into configuration.
/// </summary>
internal sealed class HealthCheckFailoverBuffer
{
    internal const int UsefulBatchSize = 64;
    internal static readonly TimeSpan PartialFlushDelay = TimeSpan.FromMilliseconds(10);

    private readonly Channel<IReadOnlyList<string>> _input =
        Channel.CreateUnbounded<IReadOnlyList<string>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    private readonly Func<IReadOnlyList<string>, Task> _appendAsync;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationToken _cancellationToken;
    private readonly Task _pump;
    private int _completed;

    public HealthCheckFailoverBuffer(
        Func<IReadOnlyList<string>, Task> appendAsync,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(appendAsync);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _appendAsync = appendAsync;
        _timeProvider = timeProvider;
        _cancellationToken = cancellationToken;
        _pump = PumpAsync();
    }

    public void Add(IReadOnlyList<string> segmentIds)
    {
        ArgumentNullException.ThrowIfNull(segmentIds);
        if (segmentIds.Count == 0) return;
        if (!_input.Writer.TryWrite(segmentIds.ToArray()))
            throw new InvalidOperationException(
                "The health-check failover buffer is no longer accepting work.");
    }

    public async Task CompleteAsync()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
            _input.Writer.TryComplete();
        await _pump.ConfigureAwait(false);
    }

    private async Task PumpAsync()
    {
        try
        {
            var pending = new List<string>(UsefulBatchSize);
            while (await _input.Reader.WaitToReadAsync(_cancellationToken).ConfigureAwait(false))
            {
                if (!_input.Reader.TryRead(out var first)) continue;
                pending.AddRange(first);

                if (pending.Count < UsefulBatchSize)
                    await FillPartialBufferAsync(pending).ConfigureAwait(false);

                await _appendAsync(pending.ToArray()).ConfigureAwait(false);
                pending.Clear();
            }
        }
        catch (Exception exception)
        {
            _input.Writer.TryComplete(exception);
            throw;
        }
    }

    private async Task FillPartialBufferAsync(List<string> pending)
    {
        using var fillCts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken);
        Task<bool>? inputAvailable = null;
        try
        {
            var delay = Task.Delay(PartialFlushDelay, _timeProvider, fillCts.Token);
            while (pending.Count < UsefulBatchSize)
            {
                while (pending.Count < UsefulBatchSize && _input.Reader.TryRead(out var next))
                    pending.AddRange(next);
                if (pending.Count >= UsefulBatchSize) return;

                inputAvailable = _input.Reader.WaitToReadAsync(fillCts.Token).AsTask();
                var completed = await Task.WhenAny(delay, inputAvailable).ConfigureAwait(false);
                if (completed == delay)
                {
                    await delay.ConfigureAwait(false);
                    return;
                }

                var hasInput = await inputAvailable.ConfigureAwait(false);
                inputAvailable = null;
                if (!hasInput) return;
            }
        }
        finally
        {
            await fillCts.CancelAsync().ConfigureAwait(false);
            if (inputAvailable is not null)
            {
                try { await inputAvailable.ConfigureAwait(false); }
                catch (OperationCanceledException) when (fillCts.IsCancellationRequested)
                {
                    // Release the channel waiter's cancellation registration when the timer wins.
                }
            }
        }
    }
}
