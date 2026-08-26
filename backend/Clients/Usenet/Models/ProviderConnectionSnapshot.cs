using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Models;

namespace NzbWebDAV.Clients.Usenet.Models;

/// <summary>
/// Live connection-pool state plus lifetime churn for one configured provider
/// account, keyed by metrics ProviderId.
/// </summary>
public sealed record ProviderConnectionSnapshot(
    string MetricsKey,
    string Host,
    ProviderType ProviderType,
    int LiveConnections,
    int IdleConnections,
    int ActiveConnections,
    int AvailableConnections,
    int PendingSelections,
    ConnectionPoolChurn Churn,
    int? LearnedConnectionLimit,
    int ConfiguredMaxConnections,
    int EffectiveMaxConnections,
    ProviderConnectionAdmissionSnapshot? Admission,
    /// <summary>
    /// Depth health STAT sweeps actually run at, from the physical client's
    /// UsenetSharp MaxPipelineDepth. Independent of <see cref="ConfiguredPipelineDepth"/>.
    /// </summary>
    int EffectiveStatPipelineDepth = 0,
    /// <summary>
    /// The provider's configured BODY/queue pipelining depth, or null when unset. Reported
    /// alongside the STAT depth because the two are genuinely different values.
    /// </summary>
    int? ConfiguredPipelineDepth = null,
    /// <summary>
    /// Health-verification coverage state and the evidence behind it. Diagnostic only —
    /// verification routing reads the tracker directly, and nothing consumes this to make
    /// scheduling decisions.
    /// </summary>
    VerificationCoverageSnapshot? VerificationCoverage = null);
