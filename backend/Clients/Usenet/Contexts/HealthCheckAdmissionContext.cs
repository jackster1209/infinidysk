using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Services;

namespace NzbWebDAV.Clients.Usenet.Contexts;

public record HealthCheckAdmissionContext(
    HealthCheckConnectionGate Gate,
    HealthCheckAdmissionPriority Priority,
    bool GateLeasePreAcquired = false);

internal sealed record ProviderAwareHealthCheckAdmissionContext(
    HealthCheckConnectionGate Gate,
    HealthCheckAdmissionPriority Priority,
    bool GateLeasePreAcquired,
    HealthCheckProviderLease ProviderLease)
    : HealthCheckAdmissionContext(Gate, Priority, GateLeasePreAcquired);
