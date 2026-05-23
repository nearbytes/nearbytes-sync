import { randomBytes } from 'crypto';
import type { Log } from 'nearbytes-log';
import { attachPeerSession } from '../core/peerLoop.js';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
import { appendBenchMarker } from '../benchMarker.js';

export interface StartOptions {
  /** Join this profile subject so followers can sync with you (your public key hex). */
  readonly serveProfilePublicKey?: string;
}

export interface SyncHandle {
  readonly friends: readonly string[];
  stop(): Promise<void>;
}

export async function start(
  log: Log,
  friends: readonly string[],
  options: StartOptions = {},
): Promise<SyncHandle> {
  const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length} serve=${options.serveProfilePublicKey ? 'yes' : 'no'}`;
  await log.sync.appendMarker(marker);

  const topics: Uint8Array[] = [];
  const topicHexes = new Set<string>();

  const addTopic = async (subject: ReturnType<typeof profileSubject>): Promise<void> => {
    const topic = await syncTopic(subject);
    const hex = Buffer.from(topic).toString('hex');
    if (!topicHexes.has(hex)) {
      topicHexes.add(hex);
      topics.push(topic);
    }
  };

  for (const pk of friends) {
    await addTopic(profileSubject(pk));
  }
  if (options.serveProfilePublicKey) {
    await addTopic(profileSubject(options.serveProfilePublicKey));
  }

  if (topics.length === 0) {
    return { friends, async stop() {} };
  }

  const primarySubject = options.serveProfilePublicKey
    ? profileSubject(options.serveProfilePublicKey)
    : friends.length > 0
      ? profileSubject(friends[0]!)
      : profileSubject(options.serveProfilePublicKey!);

  const peerId = randomBytes(16).toString('hex');
  const discovery = createCompositeDiscovery([
    createHyperswarmDiscovery(topics),
    createMdnsDiscovery({ peerId }),
  ]);

  const sessions: Array<{ close(): void }> = [];

  discovery.onPeer((discovered) => {
    void (async () => {
      try {
        const duplex = await connectDiscoveredPeer(discovered);
        await appendBenchMarker(log, 'peer-connected', {
          transport: discovered.transport,
          label: discovered.label.slice(0, 64),
        });
        // One framed session per transport; v0 uses the first configured friend subject on this duplex.
        attachPeerSession(log, primarySubject, duplex);
        await appendBenchMarker(log, 'peer-session-attached', {
          subject: primarySubject.kind,
        });
        sessions.push(duplex);
      } catch {
        // ignore unreachable LAN / swarm peers
      }
    })();
  });

  await discovery.start();
  await appendBenchMarker(log, 'discovery-started', {
    topics: topics.length,
    friends: friends.length,
    serve: options.serveProfilePublicKey ? 1 : 0,
  });

  return {
    friends,
    async stop(): Promise<void> {
      for (const session of sessions) {
        session.close();
      }
      await discovery.stop();
      await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
    },
  };
}
