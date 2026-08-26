using System.Collections.Concurrent;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Tests.TestUtils;

namespace NzbWebDAV.Tests.Clients.Usenet;

public class ProviderVerificationCoverageTests
{
    [Fact]
    public void UnseenProvider_IsNormal()
    {
        var coverage = new ProviderVerificationCoverage();

        var snapshot = coverage.GetSnapshot("never-asked");

        Assert.Equal(VerificationCoverageState.Normal, snapshot.State);
        Assert.Equal(0, snapshot.Samples);
        Assert.Null(snapshot.LastTransitionUtc);
    }

    [Fact]
    public void PerfectCoverage_EarnsNoPromotion()
    {
        // The whole point of the negative-only model: a provider that answers everything is
        // normal, not preferred. There is deliberately no score here for routing to rank on,
        // so a backup with excellent retention cannot climb over a configured primary.
        var transitions = new ConcurrentQueue<VerificationCoverageTransition>();
        var coverage = new ProviderVerificationCoverage(transitions.Enqueue);
        for (var i = 0; i < 500; i++) coverage.Record("holder", exists: true);

        Assert.Equal(VerificationCoverageState.Normal, coverage.GetState("holder"));
        Assert.Empty(transitions);
    }

    [Fact]
    public void ShortMissBurst_DoesNotDemote()
    {
        // One release the provider happens not to carry must not change routing. Demotion
        // takes sustained absence, not an unlucky handful of ids.
        var coverage = new ProviderVerificationCoverage();
        for (var i = 0; i < 30; i++) coverage.Record("provider", exists: false);

        Assert.Equal(VerificationCoverageState.Normal, coverage.GetState("provider"));
    }

    [Fact]
    public void AmbientMissRate_DoesNotDemote()
    {
        // 60% of STAT attempts missed across the deployment this threshold was chosen from.
        // A threshold anywhere near ambient would demote every provider at once, which is
        // the same as demoting none of them.
        var coverage = new ProviderVerificationCoverage();
        for (var i = 0; i < 1000; i++) coverage.Record("ambient", exists: i % 5 >= 3);

        var snapshot = coverage.GetSnapshot("ambient");

        Assert.Equal(VerificationCoverageState.Normal, snapshot.State);
        Assert.InRange(snapshot.MissRate, 0.5, 0.7);
    }

    [Fact]
    public void SustainedDefinitiveMisses_Deprioritize()
    {
        var transitions = new ConcurrentQueue<VerificationCoverageTransition>();
        var coverage = new ProviderVerificationCoverage(transitions.Enqueue);
        for (var i = 0; i < 300; i++) coverage.Record("empty", exists: false);

        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.GetState("empty"));
        var transition = Assert.Single(transitions);
        Assert.Equal("empty", transition.ProviderKey);
        Assert.Equal(VerificationCoverageState.Deprioritized, transition.State);
        Assert.True(
            transition.MissRate >= ProviderVerificationCoverage.DeprioritizeMissRate,
            $"expected the announced rate to justify the demotion, got {transition.MissRate}");
        Assert.NotNull(coverage.GetSnapshot("empty").LastTransitionUtc);
    }

    [Fact]
    public void FreshHits_RecoverADeprioritizedProvider()
    {
        // A demoted provider stays in the walk, so fallback traffic keeps producing evidence.
        // When that evidence improves, the demotion has to end on its own.
        var transitions = new ConcurrentQueue<VerificationCoverageTransition>();
        var coverage = new ProviderVerificationCoverage(transitions.Enqueue);
        for (var i = 0; i < 300; i++) coverage.Record("provider", exists: false);
        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.GetState("provider"));

        for (var i = 0; i < 40; i++) coverage.Record("provider", exists: true);

        Assert.Equal(VerificationCoverageState.Normal, coverage.GetState("provider"));
        Assert.Equal(2, transitions.Count);
    }

    [Fact]
    public void ABurstOfHits_DoesNotUndoSustainedAbsence()
    {
        // Recovery needs enough fresh evidence to mean something. A provider that answers a
        // few ids out of a batch it otherwise misses is still the wrong first phase.
        var coverage = new ProviderVerificationCoverage();
        for (var i = 0; i < 300; i++) coverage.Record("provider", exists: false);

        for (var i = 0; i < ProviderVerificationCoverage.MinimumRecoveryEvidence - 1; i++)
            coverage.Record("provider", exists: true);

        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.GetState("provider"));
    }

    [Fact]
    public void EvidenceOscillatingAtTheThreshold_DoesNotFlap()
    {
        // Hysteresis: the rate that ends a demotion is well below the one that starts it, so
        // a provider sitting near the line cannot toggle the walk order batch after batch.
        var transitions = new ConcurrentQueue<VerificationCoverageTransition>();
        var coverage = new ProviderVerificationCoverage(transitions.Enqueue);
        for (var i = 0; i < 300; i++) coverage.Record("borderline", exists: false);
        Assert.Single(transitions);

        for (var round = 0; round < 50; round++)
        {
            for (var i = 0; i < 10; i++) coverage.Record("borderline", exists: true);
            for (var i = 0; i < 10; i++) coverage.Record("borderline", exists: false);
            coverage.GetState("borderline");
        }

        // At most the one recovery the improving average genuinely earned — never a stream
        // of transitions tracking the oscillation.
        Assert.InRange(transitions.Count, 1, 2);
    }

    [Fact]
    public void StaleEvidence_ForgivesADeprioritizedProvider()
    {
        // Demotion is self-reinforcing: a provider tried later gets asked less, so its own
        // average freezes at the value that demoted it. Staleness is the way out that does
        // not depend on the provider being asked.
        var clock = new ControllableTimeProvider();
        var transitions = new ConcurrentQueue<VerificationCoverageTransition>();
        var coverage = new ProviderVerificationCoverage(transitions.Enqueue, clock);
        for (var i = 0; i < 300; i++) coverage.Record("stranded", exists: false);
        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.GetState("stranded"));

        clock.Advance(ProviderVerificationCoverage.StalenessHalfLife);

        Assert.Equal(VerificationCoverageState.Normal, coverage.GetState("stranded"));
        Assert.Equal(2, transitions.Count);
    }

    [Fact]
    public void FreshMisses_HoldADemotionAgainstDecay()
    {
        // Decay is an exploration path, not a leak. A provider that keeps missing what
        // verification asks stays demoted however long the run lasts.
        var clock = new ControllableTimeProvider();
        var coverage = new ProviderVerificationCoverage(timeProvider: clock);
        for (var i = 0; i < 300; i++) coverage.Record("empty", exists: false);

        // A full day of ordinary sweeping — four half-lives — one batch per hour.
        for (var hour = 0; hour < 24; hour++)
        {
            clock.Advance(TimeSpan.FromHours(1));
            for (var i = 0; i < 50; i++) coverage.Record("empty", exists: false);
        }

        Assert.Equal(VerificationCoverageState.Deprioritized, coverage.GetState("empty"));
    }

    [Fact]
    public void MissRateResumesFromTheDecayedValue_AfterAGap()
    {
        // The value a reader saw during the gap is the one the next observation must build
        // on; snapping back to the pre-gap rate would undo the forgiveness the gap earned.
        var clock = new ControllableTimeProvider();
        var coverage = new ProviderVerificationCoverage(timeProvider: clock);
        for (var i = 0; i < 300; i++) coverage.Record("stranded", exists: false);

        clock.Advance(ProviderVerificationCoverage.StalenessHalfLife * 4);
        var decayed = coverage.GetSnapshot("stranded").MissRate;
        coverage.Record("stranded", exists: false);

        // One miss folds the decayed rate toward 1 by Alpha, so the result is a step off the
        // decayed value rather than off the frozen pre-gap one.
        Assert.Equal(
            decayed + (0.02 * (1 - decayed)),
            coverage.GetSnapshot("stranded").MissRate,
            precision: 9);
    }

    [Fact]
    public void ReadTimeDecay_IsNotAppliedTwice()
    {
        // Reads decay from LastObserved without writing the result back. Reading twice must
        // report the same rate, or simply watching a provider would forgive it.
        var clock = new ControllableTimeProvider();
        var coverage = new ProviderVerificationCoverage(timeProvider: clock);
        for (var i = 0; i < 300; i++) coverage.Record("stranded", exists: false);

        clock.Advance(ProviderVerificationCoverage.StalenessHalfLife);
        var first = coverage.GetSnapshot("stranded").MissRate;
        var second = coverage.GetSnapshot("stranded").MissRate;

        Assert.Equal(first, second, precision: 12);
    }

    [Fact]
    public void ClockGoingBackwards_DoesNotChangeTheState()
    {
        // Host clocks jump. A backwards step must not invent decay in reverse and sharpen a
        // rate past what its observations support.
        var clock = new RewindableTimeProvider(DateTimeOffset.UnixEpoch.AddDays(10));
        var coverage = new ProviderVerificationCoverage(timeProvider: clock);
        for (var i = 0; i < 300; i++) coverage.Record("empty", exists: false);
        var measured = coverage.GetSnapshot("empty").MissRate;

        clock.Set(DateTimeOffset.UnixEpoch);

        var snapshot = coverage.GetSnapshot("empty");
        Assert.Equal(measured, snapshot.MissRate, precision: 9);
        Assert.Equal(VerificationCoverageState.Deprioritized, snapshot.State);
    }

    [Fact]
    public async Task ConcurrentRecording_IsStable()
    {
        // Every verification worker records into this on the STAT hot path.
        var coverage = new ProviderVerificationCoverage();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(worker => Task.Run(() =>
        {
            for (var i = 0; i < 500; i++)
                coverage.Record($"provider-{worker % 3}", exists: i % 2 == 0);
        })));

        for (var p = 0; p < 3; p++)
        {
            var snapshot = coverage.GetSnapshot($"provider-{p}");
            Assert.InRange(snapshot.MissRate, 0d, 1d);
            Assert.Equal(VerificationCoverageState.Normal, snapshot.State);
        }

        Assert.Equal(
            8 * 500,
            Enumerable.Range(0, 3).Sum(p => coverage.GetSnapshot($"provider-{p}").Samples));
    }

    private sealed class RewindableTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Set(DateTimeOffset now) => _now = now;
    }
}
