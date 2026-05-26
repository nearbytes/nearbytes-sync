#!/usr/bin/env node
// Thin shim that boots the compiled `nbsync` CLI from dist/.
// Kept in JS (not TS) so it works without a build step when installed
// from npm or referenced via package.json "bin".
import('../dist/node/cli.js');
