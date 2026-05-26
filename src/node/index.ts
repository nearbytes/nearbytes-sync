export { start, type SyncHandle, type SyncSnapshot, type StartOptions } from './start.js';
export {
  probeSyncLock,
  SyncAlreadyRunningError,
  DATADIR_LOCK_FILENAME,
  type SyncLockStatus,
} from './dataDirLock.js';
export { runDaemon, type DaemonOptions } from './daemon.js';
export {
  readDaemonConfig,
  defaultDaemonConfigPath,
  defaultDaemonDataDir,
  configsEquivalent,
  type SyncDaemonConfig,
  type DaemonProfileConfig,
} from './daemonConfig.js';
