/** Survives peer reconnects — partial large blocks must not be lost on session stop. */
const pendingByLog = new WeakMap();
export function getPendingBlocks(log) {
    let map = pendingByLog.get(log);
    if (map === undefined) {
        map = new Map();
        pendingByLog.set(log, map);
    }
    return map;
}
//# sourceMappingURL=pendingBlocks.js.map