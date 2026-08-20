namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

public class GetHealthCheckGateResponse : BaseApiResponse
{
    public int Limit { get; init; }
    public int Active { get; init; }
    public int PeakActive { get; init; }
    public int WaitingBackground { get; init; }
    public int PeakWaitingBackground { get; init; }
    public HealthCheckStatSchedulerResponse Scheduler { get; init; } = new();
}

public class HealthCheckStatSchedulerResponse
{
    public int Capacity { get; init; }
    public int ActiveAssignments { get; init; }
    public int PendingAdmissions { get; init; }
    public int RunnableSessions { get; init; }
    public long PendingSegments { get; init; }
    public long Dispatches { get; init; }
    public long Completions { get; init; }
    public long Cancellations { get; init; }
    public long Failures { get; init; }
    public IReadOnlyList<HealthCheckStatSessionResponse> Sessions { get; init; } = [];
}

public class HealthCheckStatSessionResponse
{
    public Guid RunId { get; init; }
    public Guid DavItemId { get; init; }
    public int PhaseId { get; init; }
    public string Mode { get; init; } = string.Empty;
    public string State { get; init; } = string.Empty;
    public int InFlight { get; init; }
    public int Completed { get; init; }
    public int Total { get; init; }
}
