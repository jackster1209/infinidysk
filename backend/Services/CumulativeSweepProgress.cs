namespace NzbWebDAV.Services;

/// <summary>
/// File-relative progress for a verification sweep that runs one provider per phase.
///
/// Only the first phase covers the whole file; later phases carry the shrinking remainder,
/// so each phase counts against a different denominator. Reporting a phase-relative count
/// straight through would run the bar backwards on every phase change, and letting the
/// first phase reach the whole file would claim the sweep was finished while later phases
/// were still resolving leftovers.
///
/// Phases therefore report file-relative processed work, never below what has already been
/// shown, and never the whole file — <see cref="Complete"/> is the only path to 100%, and
/// the sweep calls it once every phase that was going to run has run.
///
/// The count a phase contributes is segments it processed, not segments it resolved, so a
/// phase whose provider missed some of them still advances the bar to the held-back
/// ceiling. Resolution is known only per chunk inside the executor, while the watchdog is
/// re-armed by intra-chunk progress; splitting those two signals to gain accuracy would
/// risk cancelling a long chunk as stalled.
/// </summary>
internal sealed class CumulativeSweepProgress(
    IProgress<int>? progress,
    int total,
    Action? onActivity = null)
{
    private readonly Lock _lock = new();

    /// <summary>Held back so an unfinished sweep cannot display as a finished one.</summary>
    private readonly int _phaseCeiling = Math.Max(total - 1, 0);

    private int _accepted;
    private int _reported;
    private bool _draining;

    /// <summary>
    /// Progress sink for a phase that begins with <paramref name="resolvedBefore"/> of the
    /// file's segments already resolved by earlier phases — an exact count, taken at the
    /// phase boundary. The values it reports are segments processed within the phase, which
    /// is why the ceiling exists. Every report re-arms the no-progress watchdog, including
    /// one that does not move the displayed value.
    /// </summary>
    public IProgress<int> ForPhase(int resolvedBefore) => new InlineProgress(completedInPhase =>
    {
        onActivity?.Invoke();
        Publish(Math.Min(resolvedBefore + completedInPhase, _phaseCeiling));
    });

    /// <summary>Marks the whole sweep finished. Safe to call more than once.</summary>
    public void Complete() => Publish(total);

    /// <summary>
    /// Hands <paramref name="value"/> to the caller's progress callback, dropping anything
    /// that would not advance the bar.
    ///
    /// Only one thread forwards at a time and it always forwards the newest accepted value,
    /// so a slow callback coalesces the reports queued behind it instead of letting a second
    /// reporter overtake it and display a lower count than the one already shown. Forwarding
    /// happens outside the lock so a slow callback cannot stall the STAT workers behind it.
    /// </summary>
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

    /// <summary>
    /// Invokes inline. Progress&lt;T&gt; posts to a captured context, which would let two
    /// reports land out of order and undo the ordering enforced above.
    /// </summary>
    private sealed class InlineProgress(Action<int> report) : IProgress<int>
    {
        public void Report(int value) => report(value);
    }
}
