// peerjs-node.mjs — loads the peerjs library under Node with libdatachannel.
// Sets up the minimal browser globals peerjs expects, then resolves the
// actual Peer constructor across ESM/CJS interop shapes.
import { createRequire } from 'node:module';
import process from 'node:process';

const require = createRequire(import.meta.url);
const rtc = require('node-datachannel/polyfill');
for (const k of [
  'RTCPeerConnection', 'RTCSessionDescription', 'RTCIceCandidate',
  'RTCCertificate', 'RTCDataChannel',
]) {
  if (rtc[k]) globalThis[k] = rtc[k];
}
globalThis.navigator ??= { platform: process.platform, userAgent: 'node-jam-bridge' };
globalThis.window ??= globalThis;
globalThis.location ??= { protocol: 'https:' };

const mod = await import('peerjs');
const Peer = mod.Peer
  ?? (typeof mod.default === 'function' ? mod.default : mod.default?.Peer);
if (typeof Peer !== 'function') throw new Error('could not resolve Peer constructor from peerjs');

export { Peer };
