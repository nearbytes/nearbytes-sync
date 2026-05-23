import type { Log } from 'nearbytes-log';

/** Structured benchmark line appended to {@link Log.sync} activity log. */
export async function appendBenchMarker(
  log: Log,
  event: string,
  fields: Record<string, string | number | boolean> = {},
): Promise<void> {
  const payload = JSON.stringify({ bench: event, t: Date.now(), ...fields });
  await log.sync.appendMarker(`bench ${payload}`);
}
