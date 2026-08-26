namespace NzbWebDAV.Services;

/// <summary>
/// Reports how many source segments have reached a terminal verification outcome. Transport
/// activity is kept separate so a provider attempt can re-arm the watchdog without claiming
/// that an unresolved segment moved closer to completion.
/// </summary>
internal sealed class LogicalSweepProgress(
    IProgress<int>? progress,
    int total,
    Action? onActivity = null)
{
    private readonly Lock _lock = new();
    private int _terminal;
    private int _accepted = -1;
    private int _reported = -1;
    private bool _draining;

    /// <summary>
    /// Reports the initial zero explicitly so an active sweep is distinguishable from an item
    /// that is merely waiting in the health queue.
    /// </summary>
    public void Start() => Publish(0);

    /// <summary>
    /// Counts newly terminal source positions. Percentage presentation reserves 100% for the
    /// outer completion event without distorting this logical count for small sweeps.
    /// </summary>
    public void AdvanceTerminal(int count)
    {
        if (count <= 0) return;
        int value;
        lock (_lock)
        {
            _terminal = Math.Min(total, _terminal + count);
            value = _terminal;
        }
        Publish(value);
    }

    internal static int ToInFlightPercentage(int terminal, int total)
    {
        if (total <= 0) return 0;
        var clamped = Math.Clamp(terminal, 0, total);
        return Math.Min((int)((long)clamped * 100 / total), 99);
    }

    /// <summary>
    /// Scheduler progress is physical work attempted within a provider session. It keeps the
    /// no-progress watchdog alive but never changes the logical progress display.
    /// </summary>
    public IProgress<int> Activity { get; } = new InlineProgress(_ => onActivity?.Invoke());

    private void Publish(int value)
    {
        lock (_lock)
        {
            if (value <= _accepted) return;
            _accepted = value;
            if (_draining) return;
            _draining = true;
        }

        try
        {
            while (true)
            {
                int next;
                lock (_lock)
                {
                    if (_reported == _accepted)
                    {
                        _draining = false;
                        return;
                    }

                    next = _accepted;
                    _reported = next;
                }

                progress?.Report(next);
            }
        }
        catch
        {
            lock (_lock) _draining = false;
            throw;
        }
    }

    private sealed class InlineProgress(Action<int> report) : IProgress<int>
    {
        public void Report(int value) => report(value);
    }
}
