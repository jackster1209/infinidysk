namespace NzbWebDAV.Clients.Usenet.Models;

/// <summary>
/// Whether verification routing should currently try a provider later within its own
/// configured tier. Deliberately two-valued: coverage evidence is defensive only, so there
/// is no state that promotes a provider above the order the operator configured.
/// </summary>
public enum VerificationCoverageState
{
    Normal,
    Deprioritized,
}

/// <summary>Read-only verification coverage view for APIs and live dashboards.</summary>
public sealed record VerificationCoverageSnapshot(
    VerificationCoverageState State,
    int Samples,
    double MissRate,
    DateTimeOffset? LastTransitionUtc,
    /// <summary>
    /// How many times this provider has been deprioritized since the process started. A
    /// number that keeps climbing is the signal that the thresholds are too tight for this
    /// deployment, which a current state alone cannot show.
    /// </summary>
    int Deprioritizations = 0);

/// <summary>
/// One coverage state change, raised so the caller can log it against the provider's host
/// rather than its metrics key.
/// </summary>
public sealed record VerificationCoverageTransition(
    string ProviderKey,
    VerificationCoverageState State,
    int Samples,
    double MissRate);
