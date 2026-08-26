using NzbWebDAV.Services;

namespace NzbWebDAV.Tests.Services;

/// <summary>
/// A sweep runs one provider per phase, and only the first phase covers the whole file.
/// These pin the two properties the health UI depends on: the bar never goes backwards, and
/// 100% means the sweep finished rather than "the first provider ran out of segments".
/// </summary>
public class CumulativeSweepProgressTests
{
    [Fact]
    public void FirstPhase_DoesNotReachTheWholeFile()
    {
        // The regression this exists for: phase 0 answering every segment used to display a
        // finished sweep while later phases were still resolving the leftovers.
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 100);

        var phase = sweep.ForPhase(resolvedBefore: 0);
        for (var completed = 1; completed <= 100; completed++) phase.Report(completed);

        Assert.Equal(99, reports[^1]);
        Assert.DoesNotContain(100, reports);
    }

    [Fact]
    public void Complete_IsTheOnlyPathToTheWholeFile()
    {
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 100);

        sweep.ForPhase(resolvedBefore: 0).Report(100);
        sweep.Complete();

        Assert.Equal(100, reports[^1]);
    }

    [Fact]
    public void LaterPhases_ContinueFromTheResolvedCount()
    {
        // Phase 1 counts against the remainder, not the file. Reported straight through, its
        // first report would drop the bar from 90 back to 1.
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 100);

        var first = sweep.ForPhase(resolvedBefore: 0);
        for (var completed = 1; completed <= 100; completed++) first.Report(completed);
        var reportsAfterFirstPhase = reports.Count;

        var second = sweep.ForPhase(resolvedBefore: 90);
        for (var completed = 1; completed <= 10; completed++) second.Report(completed);

        Assert.All(reports.Skip(reportsAfterFirstPhase), value => Assert.Equal(99, value));
        Assert.Equal(reports.OrderBy(x => x), reports);
    }

    [Fact]
    public void ProgressIsMonotonic_AcrossEveryPhase()
    {
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 1_000);

        var remaining = 1_000;
        for (var phase = 0; phase < 4; phase++)
        {
            var resolvedBefore = 1_000 - remaining;
            var sink = sweep.ForPhase(resolvedBefore);
            for (var completed = 1; completed <= remaining; completed++) sink.Report(completed);
            remaining /= 4;
        }

        sweep.Complete();

        Assert.Equal(reports.OrderBy(x => x), reports);
        Assert.Equal(1_000, reports[^1]);
    }

    [Fact]
    public void RepeatedValues_AreNotResent()
    {
        // The websocket send is debounced, not free, and a phase that resolves nothing new
        // still reports on every chunk to keep the watchdog armed.
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 100);

        var phase = sweep.ForPhase(resolvedBefore: 50);
        phase.Report(10);
        phase.Report(10);
        phase.Report(10);

        Assert.Equal([60], reports);
    }

    [Fact]
    public void EveryReport_ArmsTheWatchdog_EvenWhenTheValueIsUnchanged()
    {
        // A later phase pinned at the ceiling reports no new value, but the STATs behind it
        // are live work — losing those re-arms would cancel the sweep as stalled.
        var activity = 0;
        var sweep = new CumulativeSweepProgress(progress: null, total: 100, () => activity++);

        var phase = sweep.ForPhase(resolvedBefore: 99);
        phase.Report(1);
        phase.Report(2);
        phase.Report(3);

        Assert.Equal(3, activity);
    }

    [Fact]
    public void Complete_IsIdempotent()
    {
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 10);

        sweep.Complete();
        sweep.Complete();

        Assert.Equal([10], reports);
    }

    [Fact]
    public void SingleSegmentFile_StillCompletes()
    {
        // total - 1 leaves no room to report inside the phase; completion must still land.
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new Recorder(reports), total: 1);

        sweep.ForPhase(resolvedBefore: 0).Report(1);
        sweep.Complete();

        Assert.Equal([1], reports);
    }

    [Fact]
    public async Task SlowCallback_CannotReorderReports()
    {
        // Forwarding runs outside the lock so a slow UI callback cannot stall STAT workers.
        // That is exactly the window where a second reporter could overtake the first and
        // display a count lower than the one already on screen.
        var reports = new List<int>();
        var gate = new Lock();
        var sweep = new CumulativeSweepProgress(
            new Recorder(reports, gate, slow: true), total: 10_000);

        var phase = sweep.ForPhase(resolvedBefore: 0);
        await Task.WhenAll(Enumerable.Range(0, 8).Select(worker => Task.Run(() =>
        {
            for (var i = 1; i <= 500; i++) phase.Report((worker * 500) + i);
        })));
        sweep.Complete();

        lock (gate)
        {
            Assert.Equal(reports.OrderBy(x => x), reports);
            Assert.Equal(10_000, reports[^1]);
        }
    }

    [Fact]
    public void ThrowingCallback_DoesNotWedgeLaterReports()
    {
        // A progress callback that throws must not leave the forwarder marked busy forever,
        // which would silently freeze the bar for the rest of the sweep.
        var reports = new List<int>();
        var sweep = new CumulativeSweepProgress(new ThrowOnceRecorder(reports), total: 100);
        var phase = sweep.ForPhase(resolvedBefore: 0);

        Assert.Throws<InvalidOperationException>(() => phase.Report(10));
        phase.Report(20);

        Assert.Equal([20], reports);
    }

    private sealed class Recorder(List<int> reports, Lock? gate = null, bool slow = false)
        : IProgress<int>
    {
        private readonly Lock _gate = gate ?? new Lock();

        public void Report(int value)
        {
            if (slow) Thread.SpinWait(200);
            lock (_gate) reports.Add(value);
        }
    }

    private sealed class ThrowOnceRecorder(List<int> reports) : IProgress<int>
    {
        private bool _thrown;

        public void Report(int value)
        {
            if (!_thrown)
            {
                _thrown = true;
                throw new InvalidOperationException("callback failed");
            }

            reports.Add(value);
        }
    }
}
