import type {
  HealthCheckGateSnapshot,
  HealthCheckQueueItem,
} from "~/clients/backend-client.server";
import { Truncate } from "~/components/truncate/truncate";
import { Badge, Icon } from "~/components/ui";

export type HealthTableProps = {
  isEnabled: boolean;
  healthCheckItems: HealthCheckQueueItem[];
  verificationLoad: HealthCheckGateSnapshot;
};

const desktopHeaderClass =
  "hidden min-[900px]:table-cell text-center text-xs font-semibold uppercase tracking-wide";
const desktopCellClass =
  "hidden min-[900px]:table-cell max-w-[160px] whitespace-nowrap px-1 py-3 text-center align-middle font-mono text-xs tabular-nums text-base-content/70";

export function HealthTable({ isEnabled, healthCheckItems, verificationLoad }: HealthTableProps) {
  return (
    <section className="card w-full border border-base-content/10 bg-base-100 shadow-sm">
      <div className="card-body gap-0 p-0">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-base-content/10 px-4 py-4 md:px-6">
          <h2 className="card-title text-xl">Schedule</h2>
          {isEnabled && healthCheckItems.length > 0 && (
            <Badge className="badge-ghost badge-sm font-mono tabular-nums">
              Showing {healthCheckItems.length}
            </Badge>
          )}
        </div>

                {isEnabled && <VerificationLoad snapshot={verificationLoad} />}

        {!isEnabled ? (
          <EmptyState
            title="Enable repairs in settings"
            body="Once you enable repairs, mounted usenet files are queued for continuous health monitoring."
          />
        ) : healthCheckItems.length === 0 ? (
          <EmptyState
            title="No items to health-check"
            body="Once you begin processing NZBs, mounted usenet files are queued for continuous health monitoring."
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="table table-zebra table-sm mb-0 w-full min-w-0 text-base-content min-[900px]:min-w-[720px]">
              <thead>
                <tr className="border-base-content/10 [&_th]:bg-base-200 [&_th]:text-base-content/70">
                  <th className="py-3 pl-4 text-left text-xs font-semibold uppercase tracking-wide md:pl-6">
                    Name
                  </th>
                  <th className={desktopHeaderClass}>Created</th>
                  <th className={desktopHeaderClass}>Last check</th>
                  <th className={`${desktopHeaderClass} pr-4 md:pr-6`}>Next check</th>
                </tr>
              </thead>
              <tbody>
                {healthCheckItems.map((item) => {
                  const statSession = verificationLoad.scheduler.sessions.find(
                    (session) => session.davItemId === item.id,
                  );
                  return (
                  <tr key={item.id} className="border-base-content/10">
                    <td className="max-w-[280px] py-3 pl-4 align-middle md:pl-6 max-[899px]:max-w-none">
                      <div className="flex min-w-0 flex-col gap-1">
                        <div className="break-all text-sm font-medium leading-snug text-base-content">
                          <Truncate>{item.name}</Truncate>
                        </div>
                        <div className="break-all text-xs leading-snug text-base-content/45">
                          <Truncate>{item.path}</Truncate>
                        </div>
                                                {statSession && (
                                                    <div className="font-mono text-[11px] tabular-nums text-info/75">
                                                        {statSession.inFlight} active STAT · {statSession.completed.toLocaleString()} / {statSession.total.toLocaleString()} complete
                                                    </div>
                                                )}
                        <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 min-[900px]:hidden">
                          <MetaChip
                            label="Created"
                            value={formatAge(item.releaseDate, "Unknown")}
                          />
                          <MetaChip label="Last" value={formatAge(item.lastHealthCheck, "Never")} />
                          <MetaChip
                            label="Next"
                            value={
                              item.progress != null && item.progress > 0
                                ? null
                                : formatWhen(item.nextHealthCheck, "ASAP")
                            }
                                                        {...(item.progress != null && item.progress > 0
                                                            ? { progress: item.progress }
                                                            : {})}
                          />
                        </div>
                      </div>
                    </td>
                    <td className={desktopCellClass}>{formatAge(item.releaseDate, "Unknown")}</td>
                    <td className={desktopCellClass}>{formatAge(item.lastHealthCheck, "Never")}</td>
                    <td className={`${desktopCellClass} pr-4 md:pr-6`}>
                      {item.progress != null && item.progress > 0 ? (
                        <HealthProgressBadge percentage={item.progress} />
                      ) : (
                        formatWhen(item.nextHealthCheck, "ASAP")
                      )}
                    </td>
                  </tr>
                                    );
                                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}

function VerificationLoad({ snapshot }: { snapshot: HealthCheckGateSnapshot }) {
    const utilization = snapshot.limit > 0
        ? Math.min(100, Math.round((snapshot.active / snapshot.limit) * 100))
        : 0;

    return (
        <div className="grid gap-4 border-b border-base-content/10 bg-base-200/30 px-4 py-3 md:grid-cols-[minmax(220px,1fr)_auto_auto] md:items-center md:px-6">
            <div className="min-w-0">
                <div className="mb-1.5 flex items-center justify-between gap-3 text-xs">
                    <span className="inline-flex items-center gap-1.5 font-semibold text-base-content/70">
                        <Icon name="speed" className="!text-[16px] text-base-content/50" />
                        Verification load
                    </span>
                    <span className="font-mono tabular-nums text-base-content/70">
                        {snapshot.active} / {snapshot.limit} active
                    </span>
                </div>
                <progress
                    aria-label="Active health-check verification connections"
                    className="progress progress-info h-1.5 w-full"
                    value={utilization}
                    max={100}
                />
            </div>
            <LoadValue
                label="Health Scheduler"
                value={`${snapshot.scheduler.activeAssignments} active`}
                detail={`${snapshot.scheduler.runnableSessions} runnable · ${snapshot.scheduler.pendingSegments.toLocaleString()} pending`}
            />
            <LoadValue
                label="Background waiting"
                value={snapshot.waitingBackground.toLocaleString()}
            />
        </div>
    );
}

function LoadValue({ label, value, detail }: { label: string; value: string; detail?: string }) {
    return (
        <div className="flex items-baseline justify-between gap-4 md:min-w-[130px] md:flex-col md:items-start md:gap-0">
            <span className="text-[11px] uppercase tracking-wide text-base-content/45">{label}</span>
            <span className="font-mono text-sm font-semibold tabular-nums text-base-content/80">{value}</span>
            {detail && <span className="text-[11px] text-base-content/45">{detail}</span>}
        </div>
    );
}

function EmptyState({ title, body }: { title: string; body: string }) {
  return (
    <div className="hero min-h-[220px] py-8">
      <div className="hero-content">
        <div className="flex max-w-md flex-col items-center text-center">
          <Icon name="health_and_safety" className="mb-3 !text-[48px] text-base-content/40" />
          <h3 className="text-base font-semibold text-base-content">{title}</h3>
          <p className="mt-1 text-xs leading-relaxed text-base-content/60">{body}</p>
        </div>
      </div>
    </div>
  );
}

function MetaChip({
  label,
  value,
  progress,
}: {
  label: string;
  value: string | null;
  progress?: number;
}) {
  return (
    <span className="inline-flex items-center gap-1.5 text-[11px] text-base-content/55">
      <span className="uppercase tracking-wide text-base-content/40">{label}</span>
      {progress != null ? (
        <HealthProgressBadge percentage={progress} compact />
      ) : (
        <span className="font-mono tabular-nums text-base-content/70">{value}</span>
      )}
    </span>
  );
}

function formatAge(dateString: string | null, fallback: string) {
  if (!dateString) return fallback;
  const age = Math.max(0, Math.floor((Date.now() - new Date(dateString).getTime()) / 1000));
  if (Number.isNaN(age)) return fallback;
  if (age < 5) return "just now";
  if (age < 60) return `${age}s ago`;
  if (age < 3600) return `${Math.floor(age / 60)}m ago`;
  if (age < 86400) return `${Math.floor(age / 3600)}h ago`;
  return `${Math.floor(age / 86400)}d ago`;
}

function formatWhen(dateString: string | null, fallback: string) {
  if (!dateString) return fallback;
  const delta = Math.floor((new Date(dateString).getTime() - Date.now()) / 1000);
  if (Number.isNaN(delta)) return fallback;
  if (delta <= 0) return "soon";
  if (delta < 60) return `in ${delta}s`;
  if (delta < 3600) return `in ${Math.floor(delta / 60)}m`;
  if (delta < 86400) return `in ${Math.floor(delta / 3600)}h`;
  return `in ${Math.floor(delta / 86400)}d`;
}

function HealthProgressBadge({
  percentage,
  compact = false,
}: {
  percentage: number;
  compact?: boolean;
}) {
  const progress = Math.max(0, Math.min(percentage, 100));
  return (
    <div className={`inline-flex flex-col gap-0.5 ${compact ? "w-[72px]" : "w-[85px]"}`}>
      <Badge className="badge-sm w-full justify-center font-semibold tabular-nums">
        {percentage}%
      </Badge>
      <progress className="progress progress-success h-1 w-full" value={progress} max={100} />
    </div>
  );
}
