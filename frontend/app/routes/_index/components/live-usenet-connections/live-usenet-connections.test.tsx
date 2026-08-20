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

  it("shows total active for legacy or mixed provider pools", () => {
    render(<LiveUsenetConnections hasUsenetProviders />);

    act(() => receiveConnections("1|1|0|5|10|2"));

    const pill = screen.getByLabelText("Usenet connections");
    expect(pill.textContent).toContain("Connections");
    expect(pill.textContent).toContain("5/10");
    expect(pill.textContent).toContain("3 active");
  });

  it("shows active transfer count when split scheduling is enabled", () => {
    render(<LiveUsenetConnections hasUsenetProviders />);

    act(() => receiveConnections("1|1|0|4|10|1|1|5|11|11|7|12"));

    const pill = screen.getByLabelText("Usenet connections");
    expect(pill.textContent).toContain("4/10");
    expect(pill.textContent).toContain("5 transfer");
  });
});
