import type { HealthCheckQueueItem } from "~/clients/backend-client.server";

export type HealthQueueState = {
  items: HealthCheckQueueItem[];
  uncheckedCount: number;
};

export function completeHealthCheck(state: HealthQueueState, davItemId: string): HealthQueueState {
  const completedItem = state.items.find((item) => item.id === davItemId);
  if (!completedItem) return state;

  return {
    items: state.items.filter((item) => item.id !== davItemId),
    uncheckedCount:
      completedItem.nextHealthCheck === null
        ? Math.max(0, state.uncheckedCount - 1)
        : state.uncheckedCount,
  };
}

export function updateHealthCheckProgress(
    state: HealthQueueState,
    davItemId: string,
    progress: number,
): HealthQueueState {
    if (!state.items.some(item => item.id === davItemId)) return state;

    return {
        ...state,
        items: state.items.map(item => item.id === davItemId
            ? { ...item, progress }
            : item),
    };
}
