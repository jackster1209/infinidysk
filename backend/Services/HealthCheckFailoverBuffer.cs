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
        var delay = Task.Delay(PartialFlushDelay, _timeProvider, _cancellationToken);
        while (pending.Count < UsefulBatchSize)
        {
            while (pending.Count < UsefulBatchSize && _input.Reader.TryRead(out var next))
                pending.AddRange(next);
            if (pending.Count >= UsefulBatchSize) return;

            var inputAvailable = _input.Reader.WaitToReadAsync(_cancellationToken).AsTask();
            var completed = await Task.WhenAny(delay, inputAvailable).ConfigureAwait(false);
            if (completed == delay)
            {
                await delay.ConfigureAwait(false);
                return;
            }

            if (!await inputAvailable.ConfigureAwait(false)) return;
        }
    }
}
