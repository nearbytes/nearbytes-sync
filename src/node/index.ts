export {
  start,
  type SyncHandle,
  type SyncSnapshot,
  type ConnectedPeer,
  type StartOptions,
} from './start.js';
export {
  type SyncEvent,
  type SyncEventKind,
  type PeerConnectedEvent,
  type PeerDisconnectedEvent,
  type BlockSentEvent,
  type BlockReceivedEvent,
  type EventReceivedEvent,
} from '../core/syncEvents.js';
export {
  readSyncStateBeacon,
  type SyncStateBeaconPayload,
  type ReadBeaconResult,
  STATE_BEACON_FILENAME,
} from './stateBeacon.js';
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
