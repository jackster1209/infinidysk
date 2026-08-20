namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

public class GetHealthCheckGateResponse : BaseApiResponse
{
    public int Limit { get; init; }
    public int Active { get; init; }
    public int PeakActive { get; init; }
    public int WaitingBackground { get; init; }
    public int PeakWaitingBackground { get; init; }
}
