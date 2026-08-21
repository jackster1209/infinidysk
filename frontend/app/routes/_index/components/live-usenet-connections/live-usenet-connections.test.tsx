// @vitest-environment jsdom

import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveUsenetConnections } from "./live-usenet-connections";

const useWebsocketTopicMock = vi.hoisted(() => vi.fn());

vi.mock("~/utils/shared-websocket", () => ({
    useWebsocketTopic: useWebsocketTopicMock,
}));

describe("LiveUsenetConnections", () => {
    let receiveConnections!: (message: string) => void;

    beforeEach(() => {
        useWebsocketTopicMock.mockReset();
        useWebsocketTopicMock.mockImplementation(
            (_topic: string, _kind: string, onMessage: (message: string) => void) => {
                receiveConnections = onMessage;
            },
        );
    });

    afterEach(cleanup);

    it("keeps the existing summary for legacy or mixed provider pools", () => {
        render(<LiveUsenetConnections hasUsenetProviders />);

        act(() => receiveConnections("1|1|0|5|10|2"));

        expect(screen.getByLabelText("Usenet connections").textContent)
            .toContain("Connections");
        expect(screen.getByLabelText("Usenet connections").textContent)
            .toContain("5/10");
        expect(screen.getByLabelText("Usenet connections").textContent)
            .toContain("3 active");
        expect(screen.queryByLabelText("Transfer connections")).toBeNull();
        expect(screen.queryByLabelText("Metadata connections")).toBeNull();
    });

    it("splits transfer and metadata pools and marks metadata bursting", () => {
        render(<LiveUsenetConnections hasUsenetProviders />);

        act(() => receiveConnections("1|1|0|4|10|1|1|5|11|11|7|12"));

        expect(screen.getByLabelText("Transfer connections").textContent)
            .toContain("5/11");
        expect(screen.getByLabelText("Metadata connections").textContent)
            .toContain("11/7");
        expect(screen.getByLabelText("Metadata connections").textContent)
            .toContain("Burst +4");
    });
});
