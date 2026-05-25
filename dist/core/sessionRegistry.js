const announcers = new Set();
const patchedLogs = new WeakSet();
export function registerLocalHaveAnnouncer(announcer) {
    announcers.add(announcer);
    return () => announcers.delete(announcer);
}
export function broadcastLocalHave(refs) {
    if (refs.length === 0) {
        return;
    }
    for (const announcer of announcers) {
        announcer.pushLocalHave(refs);
    }
}
/** After each local reception append, push {@code have} to every open peer session (SYNC-10). */
export function patchLogForReactiveHave(log) {
    if (patchedLogs.has(log)) {
        return;
    }
    patchedLogs.add(log);
    const append = log.reception.appendReception.bind(log.reception);
    log.reception.appendReception = async (ref) => {
        const cursor = await append(ref);
        broadcastLocalHave([ref]);
        return cursor;
    };
}
//# sourceMappingURL=sessionRegistry.js.map