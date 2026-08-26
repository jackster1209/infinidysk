import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { HealthTable } from "./health-table";

describe("HealthTable", () => {
  it("integrates live verification pressure into the schedule card", () => {
    const markup = renderToStaticMarkup(
      <HealthTable
        isEnabled
        healthCheckItems={[
          {
            id: "item-1",
            name: "Example.mkv",
            path: "/view/Example.mkv",
            releaseDate: null,
            lastHealthCheck: null,
            nextHealthCheck: null,
            progress: 25,
          },
          {
            id: "item-zero",
            name: "Starting.mkv",
            path: "/view/Starting.mkv",
            releaseDate: null,
            lastHealthCheck: null,
            nextHealthCheck: null,
            progress: 0,
          },
          {
            id: "item-96",
            name: "Resolving.mkv",
            path: "/view/Resolving.mkv",
            releaseDate: null,
            lastHealthCheck: null,
            nextHealthCheck: null,
            progress: 96,
          },
          {
            id: "item-99",
            name: "Finishing.mkv",
            path: "/view/Finishing.mkv",
            releaseDate: null,
            lastHealthCheck: null,
            nextHealthCheck: null,
            progress: 99,
          },
        ]}
        verificationLoad={{
          limit: 50,
          ceilingMode: "explicit",
          active: 18,
          peakActive: 42,
          waitingQueue: 56,
          waitingBackground: 1_234,
          peakWaitingQueue: 72,
          peakWaitingBackground: 1_600,
          scheduler: {
            capacity: 50,
            activeAssignments: 18,
            pendingAdmissions: 4,
            runnableSessions: 3,
            pendingSegments: 12_345,
            dispatches: 500,
            completions: 450,
            cancellations: 2,
            failures: 3,
            providers: [],

            globalBlockedSessions: 0,

            legacyCompatibilityAssignments: 0,

            sessions: [
              {
                runId: "run-1",
                davItemId: "item-1",
                phaseId: 0,
                providerKey: "provider-a",
                providerLabel: "provider-a",
                mode: "VerifyAll",
                state: "Running",
                inFlight: 7,
                completed: 125,
                total: 1_000,
              },
            ],
          },
        }}
      />,
    );

    expect(markup).toContain("Verification load");
    expect(markup).toContain("18 / 50 active");
    expect(markup).not.toContain("Recent peak");
    expect(markup).toContain("7 active STAT · 125 / 1,000 complete");
    expect(markup).toContain('value="36"');

    // Admission queue depth and scheduler internals are diagnostics, not something the
    // schedule card should spend space on: the per-provider breakdown below already says
    // where health work actually is.
    expect(markup).not.toContain("Queue waiting");
    expect(markup).not.toContain("Background waiting");
    expect(markup).not.toContain("Health Scheduler");
    expect(markup).not.toContain("runnable");
    expect(markup).not.toContain("12,345");

    // An active check is explicit even before its first logical segment resolves, while
    // ordinary and final-window progress retain their exact percentages.
    expect(markup).toContain("0%");
    expect(markup).toContain('value="0"');
    expect(markup).toContain("96%");
    expect(markup).toContain('value="96"');
    expect(markup).toContain("99%");
    expect(markup).toContain('value="99"');
  });

  it("shows Auto capacity instead of a synthetic ceiling", () => {
    const markup = renderToStaticMarkup(
      <HealthTable
        isEnabled
        healthCheckItems={[]}
        verificationLoad={{
          limit: null,
          ceilingMode: "auto",
          active: 48,
          peakActive: 48,
          waitingQueue: 0,
          waitingBackground: 0,
          peakWaitingQueue: 0,
          peakWaitingBackground: 0,
          scheduler: {
            capacity: null,
            activeAssignments: 48,
            pendingAdmissions: 0,
            runnableSessions: 8,
            pendingSegments: 900,
            dispatches: 100,
            completions: 52,
            cancellations: 0,
            failures: 0,
            providers: [],

            globalBlockedSessions: 0,

            legacyCompatibilityAssignments: 0,

            sessions: [],
          },
        }}
      />,
    );

    expect(markup).toContain("Capacity: Auto (provider-aware)");
    // With no bar to read the load off, the live count sits directly under the capacity line
    // rather than beside a ratio that does not exist.
    expect(markup).toContain("48 active");
    expect(markup.indexOf("Capacity: Auto (provider-aware)")).toBeLessThan(
      markup.indexOf("48 active"),
    );
    // No ceiling exists, so no "N / M" ratio and no utilization bar may be implied.
    expect(markup).not.toContain("48 / ");
    expect(markup).not.toContain("progress-info");
  });

  it("breaks capacity down per provider and names what work is waiting on", () => {
    const markup = renderToStaticMarkup(
      <HealthTable
        isEnabled
        healthCheckItems={[]}
        verificationLoad={{
          limit: null,
          ceilingMode: "auto",
          active: 48,
          peakActive: 48,
          waitingQueue: 0,
          waitingBackground: 0,
          peakWaitingQueue: 0,
          peakWaitingBackground: 0,
          scheduler: {
            capacity: null,
            activeAssignments: 48,
            pendingAdmissions: 0,
            runnableSessions: 8,
            pendingSegments: 900,
            dispatches: 100,
            completions: 52,
            cancellations: 0,
            failures: 0,
            providers: [
              {
                providerKey: "prov-key-xyz",
                providerLabel: "Fast Provider",
                activeAssignments: 48,
                runnableSessions: 8,
                pendingSegments: 900,
                blockedSessions: 8,
                isLegacySharedPool: false,
              },
              {
                providerKey: "legacy-b",
                providerLabel: "legacy-b",
                activeAssignments: 0,
                runnableSessions: 0,
                pendingSegments: 0,
                blockedSessions: 0,
                isLegacySharedPool: true,
              },
            ],
            globalBlockedSessions: 0,
            legacyCompatibilityAssignments: 0,
            sessions: [],
          },
        }}
      />,
    );

    // The resolved friendly label renders, not the raw identity key.
    expect(markup).toContain("Fast Provider");
    expect(markup).not.toContain("prov-key-xyz");
    expect(markup).toContain("48 active");
    // Saturation is attributed to the provider, not to the (absent) aggregate ceiling.
    expect(markup).toContain("8 waiting on provider");
    expect(markup).not.toContain("waiting on ceiling");
    // An idle provider that no session targets is shown without being flagged as blocked.
    expect(markup).toContain("legacy-b");
    expect(markup).toContain("shared pool");
  });

  it("hides verification load when health checks are disabled", () => {
    const markup = renderToStaticMarkup(
      <HealthTable
        isEnabled={false}
        healthCheckItems={[]}
        verificationLoad={{
          limit: 50,
          ceilingMode: "explicit",
          active: 0,
          peakActive: 0,
          waitingQueue: 0,
          waitingBackground: 0,
          peakWaitingQueue: 0,
          peakWaitingBackground: 0,
          scheduler: {
            capacity: 50,
            activeAssignments: 0,
            pendingAdmissions: 0,
            runnableSessions: 0,
            pendingSegments: 0,
            dispatches: 0,
            completions: 0,
            cancellations: 0,
            failures: 0,
            providers: [],

            globalBlockedSessions: 0,

            legacyCompatibilityAssignments: 0,

            sessions: [],
          },
        }}
      />,
    );

    expect(markup).not.toContain("Verification load");
    expect(markup).toContain("Enable repairs in settings");
  });
});
