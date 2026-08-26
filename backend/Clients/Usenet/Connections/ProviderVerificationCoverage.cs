using System.Collections.Concurrent;
using NzbWebDAV.Clients.Usenet.Models;

namespace NzbWebDAV.Clients.Usenet.Connections;

/// <summary>
/// Tracks whether a provider is currently worth asking first for verification, and answers
/// with a defensive state rather than a rank.
///
/// A STAT moves no payload, so the only thing that makes a verification attempt expensive is
/// asking a provider that does not have the article: a full round trip that returns nothing.
/// Measured on an eight-provider deployment, 60% of STAT attempts were misses and 81% of
/// verification connection-time was spent on providers holding none of the workload.
///
/// This deliberately does not rank providers by success. Ranking by hit rate let a backup or
/// block account become the first provider tried for every new health check purely because it
/// had the best retention, which silently redefines the configured provider topology — and it
/// is self-reinforcing, because the top provider sees the most work and so records the most
/// hits. Success is normal and earns no promotion. Only sustained recent definitive absence
/// earns anything, and all it earns is being tried later within the provider's own configured
/// tier.
///
/// There are two ways out of a demotion: fresh definitive evidence that improves, and staleness
/// — evidence with nothing recent behind it decays toward forgiven, so a provider that stops
/// receiving work cannot be stranded as deprioritized by a historical workload.
/// </summary>
internal sealed class ProviderVerificationCoverage(
    Action<VerificationCoverageTransition>? onTransition = null,
    TimeProvider? timeProvider = null)
{
    /// <summary>
    /// Weight of the newest observation. ~0.02 gives a half-life of about 34 observations,
    /// slow enough that a handful of anomalous misses cannot demote a good provider.
    /// </summary>
    private const double Alpha = 0.02;

    /// <summary>
    /// Definitive observations required before evidence may change routing at all. Below this
    /// every provider reads <see cref="VerificationCoverageState.Normal"/>, which keeps cold
    /// start on the configured order.
    /// </summary>
    internal const int MinimumDefinitiveSamples = 20;

    /// <summary>
    /// Miss rate at which a provider is tried later within its tier. Well above the 60% ambient
    /// miss rate measured across a real multi-provider deployment: a threshold near ambient
    /// would demote every provider, so this only fires on one that is close to empty for the
    /// workload actually being verified.
    /// </summary>
    internal const double DeprioritizeMissRate = 0.85;

    /// <summary>
    /// Miss rate a deprioritized provider must improve past to return to normal. The gap to
    /// <see cref="DeprioritizeMissRate"/> is the hysteresis that stops a provider hovering at
    /// the threshold from flapping between states.
    /// </summary>
    internal const double RecoverMissRate = 0.65;

    /// <summary>
    /// Fresh definitive observations required since a demotion before improving evidence may
    /// recover the provider, so one short burst of hits cannot undo sustained absence.
    /// </summary>
    internal const int MinimumRecoveryEvidence = 10;

    /// <summary>
    /// How long a miss rate with no new observations takes to lose half its weight. Long enough
    /// that a demotion holds across a normal verification run, short enough that a day of
    /// receiving no work restores the provider to normal on its own.
    /// </summary>
    internal static readonly TimeSpan StalenessHalfLife = TimeSpan.FromHours(6);

    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly ConcurrentDictionary<string, State> _byProvider = new(StringComparer.Ordinal);

    /// <summary>
    /// Records one definitive verification answer. Transport failures, timeouts, cancellations
    /// and unanswered pipelined ids must never reach this: they mean the provider did not
    /// answer, not that it lacks the article.
    /// </summary>
    public void Record(string providerKey, bool exists)
    {
        var now = _timeProvider.GetUtcNow();
        var state = _byProvider.GetOrAdd(providerKey, static _ => new State());
        VerificationCoverageTransition? transition;
        lock (state)
        {
            // Fold accrued staleness in before the new observation, so a provider returning
            // after a long gap resumes from the rate its state was actually built on rather
            // than from a value no reader has seen since the gap started.
            state.MissRate = DecayTowardForgiven(state.MissRate, state.LastObservedUtc, now);
            state.LastObservedUtc = now;
            state.Samples++;
            state.SamplesSinceTransition++;
            state.MissRate += Alpha * ((exists ? 0d : 1d) - state.MissRate);
            transition = Reevaluate(providerKey, state, state.MissRate, now);
        }

        // Never raise the callback under the lock: it logs, and a tracker lock must not span
        // work owned by someone else.
        if (transition is not null) onTransition?.Invoke(transition);
    }

    /// <summary>
    /// The routing-critical answer. <see cref="VerificationCoverageState.Normal"/> until a
    /// provider has definitively missed enough recent verification requests to be worth trying
    /// later within its own tier, and again once that evidence improves or goes stale.
    /// </summary>
    public VerificationCoverageState GetState(string providerKey) => Evaluate(providerKey).State;

    /// <summary>Diagnostic view: the state plus the evidence standing behind it.</summary>
    public VerificationCoverageSnapshot GetSnapshot(string providerKey) => Evaluate(providerKey);

    private VerificationCoverageSnapshot Evaluate(string providerKey)
    {
        if (!_byProvider.TryGetValue(providerKey, out var state))
            return new VerificationCoverageSnapshot(VerificationCoverageState.Normal, 0, 0d, null);

        var now = _timeProvider.GetUtcNow();
        VerificationCoverageTransition? transition;
        VerificationCoverageSnapshot snapshot;
        lock (state)
        {
            // Read-time decay is the recovery path for a provider that receives no work: a
            // demotion must not outlive the evidence behind it just because being deprioritized
            // is what stopped the provider from being asked. The decayed rate is deliberately
            // not written back — LastObservedUtc stays the anchor, so the same elapsed time is
            // never decayed twice.
            var missRate = DecayTowardForgiven(state.MissRate, state.LastObservedUtc, now);
            transition = Reevaluate(providerKey, state, missRate, now);
            snapshot = new VerificationCoverageSnapshot(
                state.CoverageState,
                state.Samples,
                missRate,
                state.LastTransitionUtc,
                state.Deprioritizations);
        }

        if (transition is not null) onTransition?.Invoke(transition);
        return snapshot;
    }

    /// <summary>
    /// Applies the demotion and recovery rules to <paramref name="missRate"/>, returning the
    /// transition to announce if the state changed. Caller must hold the state lock.
    /// </summary>
    private static VerificationCoverageTransition? Reevaluate(
        string providerKey,
        State state,
        double missRate,
        DateTimeOffset now)
    {
        var next = state.CoverageState switch
        {
            VerificationCoverageState.Normal
                when state.Samples >= MinimumDefinitiveSamples && missRate >= DeprioritizeMissRate
                => VerificationCoverageState.Deprioritized,

            // Improving evidence recovers the provider only once enough of it has arrived since
            // the demotion; staleness recovers it regardless, because a provider with no recent
            // observations has no recent evidence against it either.
            VerificationCoverageState.Deprioritized
                when missRate <= RecoverMissRate
                     && (state.SamplesSinceTransition >= MinimumRecoveryEvidence
                         || IsStale(state.LastObservedUtc, now))
                => VerificationCoverageState.Normal,

            _ => state.CoverageState,
        };

        if (next == state.CoverageState) return null;

        state.CoverageState = next;
        state.SamplesSinceTransition = 0;
        state.LastTransitionUtc = now;
        if (next == VerificationCoverageState.Deprioritized) state.Deprioritizations++;
        return new VerificationCoverageTransition(providerKey, next, state.Samples, missRate);
    }

    private static bool IsStale(DateTimeOffset? lastObservedUtc, DateTimeOffset now) =>
        lastObservedUtc is { } lastObserved && now - lastObserved >= StalenessHalfLife;

    private static double DecayTowardForgiven(
        double missRate,
        DateTimeOffset? lastObservedUtc,
        DateTimeOffset now)
    {
        if (lastObservedUtc is not { } lastObserved) return missRate;
        var elapsed = now - lastObserved;
        // A non-monotonic clock must never manufacture evidence the observations do not
        // support, so only forward time decays.
        if (elapsed <= TimeSpan.Zero) return missRate;
        return missRate * Math.Pow(0.5, elapsed / StalenessHalfLife);
    }

    private sealed class State
    {
        public int Samples;
        public int SamplesSinceTransition;
        public int Deprioritizations;
        public double MissRate;
        public VerificationCoverageState CoverageState = VerificationCoverageState.Normal;
        public DateTimeOffset? LastObservedUtc;
        public DateTimeOffset? LastTransitionUtc;
    }
}
