/**
 * Reception journal paging.
 */
export const RECEPTION_PAGE_LIMIT = Number(
  process.env.NEARBYTES_SYNC_RECEPTION_PAGE ?? 256,
);

/** Max objects per resume / urgent delta response (one have frame). */
export const RECEPTION_RESUME_PAGE = Number(
  process.env.NEARBYTES_SYNC_RESUME_PAGE ?? 64,
);

/** Tail length announced on attach. */
export const RECEPTION_ATTACH_TAIL = Number(
  process.env.NEARBYTES_SYNC_RECEPTION_TAIL ?? 256,
);
