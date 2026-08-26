namespace NzbWebDAV.Exceptions;

/// <summary>
/// Thrown when a health-check STAT sweep dispatches every segment and drains every
/// assignment, yet fewer segments were verified than requested. That can only happen
/// when an assignment ended without reporting its work, so the sweep must not resolve
/// as verified: a file recorded Healthy on unverified segments hides real damage.
/// </summary>
public class IncompleteHealthCheckSweepException(Guid davItemId, int expectedSegments, int verifiedSegments)
    : Exception(BuildMessage(davItemId, expectedSegments, verifiedSegments))
{
    public Guid DavItemId { get; } = davItemId;
    public int ExpectedSegments { get; } = expectedSegments;
    public int VerifiedSegments { get; } = verifiedSegments;

    private static string BuildMessage(Guid davItemId, int expectedSegments, int verifiedSegments) =>
        $"Health-check STAT sweep for {davItemId} verified {verifiedSegments} of " +
        $"{expectedSegments} segments; refusing to report an incomplete sweep as verified.";
}
