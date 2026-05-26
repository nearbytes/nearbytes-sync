export { start, type SyncHandle, type SyncSnapshot, type StartOptions } from './start.js';
export {
  probeSyncLock,
  SyncAlreadyRunningError,
  DATADIR_LOCK_FILENAME,
  type SyncLockStatus,
} from './dataDirLock.js';
