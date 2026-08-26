namespace NzbWebDAV.Clients.Usenet.Models;

/// <summary>
/// Result of sweeping one provider without failover. Missing ids received a definitive
/// 430/451 response. Unanswered ids received no article verdict because the provider was
/// unavailable, returned a connection-level response, or stopped partway through the batch.
/// Each collection contains distinct logical ids in first-seen input order.
/// </summary>
public sealed record ProviderVerificationSweepResult(
    IReadOnlyList<string> Missing,
    IReadOnlyList<string> Unanswered)
{
    public static ProviderVerificationSweepResult AllUnanswered(IReadOnlyList<string> segmentIds) =>
        new([], segmentIds.Distinct(StringComparer.Ordinal).ToArray());
}
