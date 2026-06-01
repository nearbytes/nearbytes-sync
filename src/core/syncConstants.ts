/**
 * Reception journal paging. Small journals fit one page; large journals paginate.
 */
export const RECEPTION_PAGE_LIMIT = Number(
  process.env.NEARBYTES_SYNC_RECEPTION_PAGE ?? 4_096,
);

/** Tail length on attach when the journal exceeds {@link RECEPTION_PAGE_LIMIT}. */
export const RECEPTION_ATTACH_TAIL = Number(
  process.env.NEARBYTES_SYNC_RECEPTION_TAIL ?? 256,
);

/** Urgent attach / empty-have dependency scan depth (not full orphan repair). */
export const RECEPTION_DEPENDENCY_SCAN_TAIL = Number(
  process.env.NEARBYTES_SYNC_DEPENDENCY_TAIL ?? 32,
);

/** At or below this remote/local max `seq`, anti-entropy uses one full `have` page. */
export function useInstantReceptionSync(maxSeq: number): boolean {
  return maxSeq > 0 && maxSeq <= RECEPTION_PAGE_LIMIT;
}
