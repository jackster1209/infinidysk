using Microsoft.AspNetCore.Mvc;
using NzbWebDAV.Services;

namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

[ApiController]
[Route("api/get-health-check-gate")]
public class GetHealthCheckGateController(
    HealthCheckConnectionGate gate,
    HealthCheckStatScheduler scheduler) : BaseApiController
{
    protected override Task<IActionResult> HandleRequest()
    {
        var snapshot = gate.GetSnapshot();
        var schedulerSnapshot = scheduler.GetSnapshot();
        return Task.FromResult<IActionResult>(Ok(new GetHealthCheckGateResponse
        {
            Limit = snapshot.Limit,
            Active = snapshot.Active,
            PeakActive = snapshot.PeakActive,
            WaitingBackground = snapshot.WaitingBackground,
            PeakWaitingBackground = snapshot.PeakWaitingBackground,
            Scheduler = new HealthCheckStatSchedulerResponse
            {
                Capacity = schedulerSnapshot.Capacity,
                ActiveAssignments = schedulerSnapshot.ActiveAssignments,
                PendingAdmissions = schedulerSnapshot.PendingAdmissions,
                RunnableSessions = schedulerSnapshot.RunnableSessions,
                PendingSegments = schedulerSnapshot.PendingSegments,
                Dispatches = schedulerSnapshot.Dispatches,
                Completions = schedulerSnapshot.Completions,
                Cancellations = schedulerSnapshot.Cancellations,
                Failures = schedulerSnapshot.Failures,
                Sessions = schedulerSnapshot.Sessions
                    .Select(session => new HealthCheckStatSessionResponse
                    {
                        RunId = session.RunId,
                        DavItemId = session.DavItemId,
                        PhaseId = session.PhaseId,
                        Mode = session.Mode.ToString(),
                        State = session.State,
                        InFlight = session.InFlight,
                        Completed = session.Completed,
                        Total = session.Total,
                    })
                    .ToArray(),
            },
        }));
    }
}
