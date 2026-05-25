/** Log sync-layer failures to stderr (never swallow silently). */
export function logSyncError(scope: string, err: unknown): void {
  if (err instanceof Error) {
    console.error(`[nearbytes-sync:${scope}]`, err.stack ?? err.message);
    return;
  }
  console.error(`[nearbytes-sync:${scope}]`, err);
}
