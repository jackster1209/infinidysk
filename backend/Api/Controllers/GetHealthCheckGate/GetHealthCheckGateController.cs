using Microsoft.AspNetCore.Mvc;
using NzbWebDAV.Config;
using NzbWebDAV.Services;
using NzbWebDAV.Services.Metrics;

namespace NzbWebDAV.Api.Controllers.GetHealthCheckGate;

[ApiController]
[Route("api/get-health-check-gate")]
public class GetHealthCheckGateController(
    HealthCheckConnectionGate gate,
    HealthCheckStatScheduler scheduler,
    ConfigManager configManager) : BaseApiController
{
    protected override Task<IActionResult> HandleRequest()
    {
        var snapshot = gate.GetSnapshot();
        var schedulerSnapshot = scheduler.GetSnapshot();
        // The scheduler deliberately knows only the stable provider identity. Resolve the
        // human-facing name here, at the presentation boundary, so it never has to read
        // provider configuration.
        var labels = ProviderUsageHelper.BuildLabelsByMetricsKey(
            configManager.GetUsenetProviderConfig().Providers);
        return Task.FromResult<IActionResult>(Ok(new GetHealthCheckGateResponse
        {
            Limit = snapshot.Limit,
            CeilingMode = snapshot.Limit is null ? "auto" : "explicit",
            Active = snapshot.Active,
            PeakActive = snapshot.PeakActive,
            WaitingQueue = snapshot.WaitingQueue,
            WaitingBackground = snapshot.WaitingBackground,
            PeakWaitingQueue = snapshot.PeakWaitingQueue,
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
                GlobalBlockedSessions = schedulerSnapshot.GlobalBlockedSessions,
                LegacyCompatibilityAssignments = schedulerSnapshot.LegacyCompatibilityAssignments,
                Providers = schedulerSnapshot.Providers
                    .Select(provider => new HealthCheckStatProviderResponse
                    {
                        ProviderKey = provider.ProviderKey,
                        ProviderLabel = labels.GetValueOrDefault(provider.ProviderKey)
                                        ?? provider.ProviderKey,
                        ActiveAssignments = provider.ActiveAssignments,
                        RunnableSessions = provider.RunnableSessions,
                        PendingSegments = provider.PendingSegments,
                        BlockedSessions = provider.BlockedSessions,
                        IsLegacySharedPool = provider.IsLegacySharedPool,
                    })
                    .ToList(),
                Sessions = schedulerSnapshot.Sessions
                    .Select(session => new HealthCheckStatSessionResponse
                    {
                        RunId = session.RunId,
                        DavItemId = session.DavItemId,
                        PhaseId = session.PhaseId,
                        ProviderKey = session.ProviderKey,
                        ProviderLabel = session.ProviderKey is { } sessionProvider
                            ? labels.GetValueOrDefault(sessionProvider) ?? sessionProvider
                            : null,
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
