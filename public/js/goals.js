/**
 * Legacy goals UI helpers were removed but some consumers still import this.
 * Provide no-op implementations to keep the module graph stable.
 */
export async function renderGoalsAndSubitems() {
  return Promise.resolve();
}

export function addCalendarGoal() {}
