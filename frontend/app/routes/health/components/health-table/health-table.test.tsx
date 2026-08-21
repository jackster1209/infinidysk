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
        ]}
        verificationLoad={{
          limit: 50,
          active: 18,
          peakActive: 42,
          waitingBackground: 1_234,
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
            sessions: [
              {
                runId: "run-1",
                davItemId: "item-1",
                phaseId: 0,
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
    expect(markup).toContain("1,234");
    expect(markup).not.toContain("Recent peak");
    expect(markup).toContain("Health Scheduler");
    expect(markup).toContain("3 runnable · 12,345 pending");
    expect(markup).toContain("7 active STAT · 125 / 1,000 complete");
    expect(markup).toContain('value="36"');
  });

  it("hides verification load when health checks are disabled", () => {
    const markup = renderToStaticMarkup(
      <HealthTable
        isEnabled={false}
        healthCheckItems={[]}
        verificationLoad={{
          limit: 50,
          active: 0,
          peakActive: 0,
          waitingBackground: 0,
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
            sessions: [],
          },
        }}
      />,
    );

    expect(markup).not.toContain("Verification load");
    expect(markup).toContain("Enable repairs in settings");
  });
});
