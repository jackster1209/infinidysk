using Microsoft.AspNetCore.Mvc;
using NzbWebDAV.Services;

namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

[ApiController]
[Route("api/get-health-check-gate")]
public class GetHealthCheckGateController(HealthCheckConnectionGate gate) : BaseApiController
{
    protected override Task<IActionResult> HandleRequest()
    {
        var snapshot = gate.GetSnapshot();
        return Task.FromResult<IActionResult>(Ok(new GetHealthCheckGateResponse
        {
            Limit = snapshot.Limit,
            Active = snapshot.Active,
            PeakActive = snapshot.PeakActive,
            WaitingBackground = snapshot.WaitingBackground,
            PeakWaitingBackground = snapshot.PeakWaitingBackground,
        }));
    }
}
