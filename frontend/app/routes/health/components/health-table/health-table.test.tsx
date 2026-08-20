import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { HealthTable } from "./health-table";

describe("HealthTable", () => {
    it("integrates live verification pressure into the schedule card", () => {
        const markup = renderToStaticMarkup(
            <HealthTable
                isEnabled
                healthCheckItems={[]}
                verificationLoad={{
                    limit: 50,
                    active: 18,
                    peakActive: 42,
                    waitingBackground: 1_234,
                    peakWaitingBackground: 1_600,
                }}
            />,
        );

        expect(markup).toContain("Verification load");
        expect(markup).toContain("18 / 50 active");
        expect(markup).toContain("42 / 50");
        expect(markup).toContain("1,234");
        expect(markup).toContain("Recent peak 1,600");
        expect(markup).toContain("value=\"36\"");
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
                }}
            />,
        );

        expect(markup).not.toContain("Verification load");
        expect(markup).toContain("Enable repairs in settings");
    });
});
