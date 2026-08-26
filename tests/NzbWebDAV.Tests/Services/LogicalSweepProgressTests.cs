using NzbWebDAV.Services;

namespace NzbWebDAV.Tests.Services;

public sealed class LogicalSweepProgressTests
{
    [Fact]
    public void Start_ReportsZeroForAnActiveSweep()
    {
        var reports = new List<int>();
        var sweep = new LogicalSweepProgress(new Recorder(reports), total: 4_060);

        sweep.Start();

        Assert.Equal([0], reports);
    }

    [Fact]
    public void AdvanceTerminal_ReportsResolvedSourcePositions()
    {
        var reports = new List<int>();
        var sweep = new LogicalSweepProgress(new Recorder(reports), total: 4_060);
        sweep.Start();

        sweep.AdvanceTerminal(3_900);

        Assert.Equal([0, 3_900], reports);
        Assert.Equal(96, reports[^1] * 100 / 4_060);
    }

    [Fact]
    public void AllTerminal_HoldsBackOnlyTheFinalUnit()
    {
        var reports = new List<int>();
        var sweep = new LogicalSweepProgress(new Recorder(reports), total: 4_060);
        sweep.Start();

        sweep.AdvanceTerminal(4_060);

        Assert.Equal(4_059, reports[^1]);
        Assert.Equal(99, reports[^1] * 100 / 4_060);
    }

    [Fact]
    public void SingleSegmentSweep_RemainsAtZeroUntilOuterCompletion()
    {
        var reports = new List<int>();
        var sweep = new LogicalSweepProgress(new Recorder(reports), total: 1);
        sweep.Start();

        sweep.AdvanceTerminal(1);

        Assert.Equal([0], reports);
    }

    [Fact]
    public void PhysicalActivity_ArmsWatchdogWithoutChangingLogicalProgress()
    {
        var reports = new List<int>();
        var activity = 0;
        var sweep = new LogicalSweepProgress(
            new Recorder(reports),
            total: 100,
            () => activity++);
        sweep.Start();

        sweep.Activity.Report(10);
        sweep.Activity.Report(20);
        sweep.Activity.Report(30);

        Assert.Equal(3, activity);
        Assert.Equal([0], reports);
    }

    [Fact]
    public async Task ConcurrentTerminalUpdates_RemainMonotonic()
    {
        var reports = new List<int>();
        var gate = new Lock();
        var sweep = new LogicalSweepProgress(
            new Recorder(reports, gate, slow: true),
            total: 1_000);
        sweep.Start();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var count = 0; count < 100; count++)
                sweep.AdvanceTerminal(1);
        })));

        lock (gate)
        {
            Assert.Equal(reports.OrderBy(value => value), reports);
            Assert.Equal(800, reports[^1]);
        }
    }

    [Fact]
    public void ThrowingCallback_DoesNotWedgeLaterReports()
    {
        var reports = new List<int>();
        var sweep = new LogicalSweepProgress(new ThrowOnceRecorder(reports), total: 100);

        Assert.Throws<InvalidOperationException>(sweep.Start);
        sweep.AdvanceTerminal(20);

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
