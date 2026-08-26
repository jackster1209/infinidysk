namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

public class GetHealthCheckGateResponse : BaseApiResponse
{
    /// <summary>Explicit aggregate ceiling, or null in Auto (provider-aware) mode.</summary>
    public int? Limit { get; init; }
    /// <summary>"auto" or "explicit" - lets the UI avoid implying a ceiling is a target.</summary>
    public string CeilingMode { get; init; } = "auto";
    public int Active { get; init; }
    public int PeakActive { get; init; }
    public int WaitingQueue { get; init; }
    public int WaitingBackground { get; init; }
    public int PeakWaitingQueue { get; init; }
    public int PeakWaitingBackground { get; init; }
    public HealthCheckStatSchedulerResponse Scheduler { get; init; } = new();
}

public class HealthCheckStatSchedulerResponse
{
    /// <summary>Explicit aggregate ceiling, or null in Auto (provider-aware) mode.</summary>
    public int? Capacity { get; init; }
    public int ActiveAssignments { get; init; }
    public int PendingAdmissions { get; init; }
    public int RunnableSessions { get; init; }
    public long PendingSegments { get; init; }
    public long Dispatches { get; init; }
    public long Completions { get; init; }
    public long Cancellations { get; init; }
    public long Failures { get; init; }
    public IReadOnlyList<HealthCheckStatSessionResponse> Sessions { get; init; } = [];
    public IReadOnlyList<HealthCheckStatProviderResponse> Providers { get; init; } = [];
    /// <summary>Runnable sessions held back by the explicit aggregate ceiling.</summary>
    public int GlobalBlockedSessions { get; init; }
    /// <summary>Active assignments backed by a legacy shared-pool permit.</summary>
    public int LegacyCompatibilityAssignments { get; init; }
}

public class HealthCheckStatProviderResponse
{
    /// <summary>Stable provider identity used by scheduling and metrics.</summary>
    public string ProviderKey { get; init; } = string.Empty;
    /// <summary>Human-facing name resolved for display: nickname, else host, else key.</summary>
    public string ProviderLabel { get; init; } = string.Empty;
    public int ActiveAssignments { get; init; }
    public int RunnableSessions { get; init; }
    public long PendingSegments { get; init; }
    /// <summary>Runnable sessions held back because this provider cannot admit more work.</summary>
    public int BlockedSessions { get; init; }
    public bool IsLegacySharedPool { get; init; }
}

public class HealthCheckStatSessionResponse
{
    public Guid RunId { get; init; }
    public Guid DavItemId { get; init; }
    public int PhaseId { get; init; }
    /// <summary>Stable provider identity used by scheduling and metrics.</summary>
    public string? ProviderKey { get; init; }
    /// <summary>Human-facing name resolved for display: nickname, else host, else key.</summary>
    public string? ProviderLabel { get; init; }
    public string Mode { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public int InFlight { get; init; }
    public int Completed { get; init; }
    public int Total { get; init; }
}
