/** Structured benchmark line appended to {@link Log.sync} activity log. */
export async function appendBenchMarker(log, event, fields = {}) {
    const payload = JSON.stringify({ bench: event, t: Date.now(), ...fields });
    await log.sync.appendMarker(`bench ${payload}`);
}
//# sourceMappingURL=benchMarker.js.map