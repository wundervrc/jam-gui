#!/usr/bin/env node
/**
 * jam-bridge — join a Spicetify Jam session from Spotifast (or any MPRIS player)
 *
 * Speaks the same PeerJS/WebRTC protocol as the spicetify-jam extension
 * (Kyzenkms/spicetify-jam v1.4.1) and mirrors playback into a local MPRIS
 * player (spotifast's bus name is org.mpris.MediaPlayer2.fastpotify).
 *
 * Mode:
 *   guest (default)  — join her Jam code, follow playback in spotifast
 *   host (--host)    — start a Jam from spotifast; she joins with the code
 *                      from her spicetify-jam extension as usual
 *
 * Usage:
 *   node jam-bridge.mjs <CODE> [--name NAME] [--player fastpotify] [--dry-run]
 *   node jam-bridge.mjs --host   [--name NAME] [--player fastpotify] [--dry-run]
 */

import process from 'node:process';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);

// Browser-global stubs + WebRTC polyfill, then load peerjs (see lib/)
const { Peer } = await import('./lib/peerjs-node.mjs');

// ---------------------------------------------------------------------------
// 2. Jam protocol constants (decoded from spicetify-jam 1.4.1)
// ---------------------------------------------------------------------------
const ICE_SERVERS = [{
  urls: 'stun:stun.l.google.com:19302',
}, {
  urls: ['turn:eu-0.turn.peerjs.com:3478', 'turn:us-0.turn.peerjs.com:3478'],
  username: 'peerjs',
  credential: 'peerjsp',
}];
const PEER_OPTS = { debug: 1 };

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

// ---------------------------------------------------------------------------
// 3. MPRIS adapter — controls spotifast over D-Bus
// ---------------------------------------------------------------------------
class MprisPlayer {
  constructor(busName) {
    this.busName = busName.startsWith('org.mpris.') ? busName : `org.mpris.MediaPlayer2.${busName}`;
    this.dbus = require('dbus-next');
    this.bus = this.dbus.sessionBus();
    this.iface = null;
    this.props = null;
    this._uriToTrackId = new Map(); // spotify:track:xxx -> mpris trackid
  }

  async connect() {
    const obj = await this.bus.getProxyObject(this.busName, '/org/mpris/MediaPlayer2');
    this.iface = obj.getInterface('org.mpris.MediaPlayer2.Player');
    this.props = obj.getInterface('org.freedesktop.DBus.Properties');
  }

  async call(name, ...args) {
    try {
      return await this.iface[name](...args);
    } catch (e) {
      log(`mpris ${name} error:`, e.message);
    }
  }

  /** Current state snapshot: { playing, positionMs, durationMs, uri, title, artist, trackId, artUrl } */
  async state() {
    const [statusV, posV, metaV] = await Promise.all([
      this.props.Get('org.mpris.MediaPlayer2.Player', 'PlaybackStatus'),
      this.props.Get('org.mpris.MediaPlayer2.Player', 'Position'),
      this.props.Get('org.mpris.MediaPlayer2.Player', 'Metadata'),
    ]).catch(() => [null, null, null]);
    if (statusV === null) return null;
    const meta = metaV?.value ?? {};       // unwrap Variant -> dict of Variants
    const m = {};
    for (const k of Object.keys(meta)) m[k] = meta[k]?.value;
    const lengthUs = m['mpris:length'] ? Number(m['mpris:length']) : 0;
    // spotifast gives the full uri in xesam:url; trackid is a custom path
    let uri = m['xesam:url'];
    if (typeof uri !== 'string') {
      uri = String(m['mpris:trackid'] ?? '');
      uri = uri.replace(/^.*[/]track_?/, 'spotify:track:');
    }
    return {
      playing: statusV.value === 'Playing',
      positionMs: Number(posV?.value ?? 0) / 1000,
      durationMs: lengthUs / 1000,
      uri,
      trackId: m['mpris:trackid'],
      title: m['xesam:title'] ?? '',
      artist: Array.isArray(m['xesam:artist']) ? m['xesam:artist'].map((x) => x?.value ?? x).join(', ') : '',
      artUrl: m['mpris:artUrl'] ?? '',
    };
  }

  async openUri(uri) {
    this._uriToTrackId.set(uri, null);
    await this.call('OpenUri', uri);
  }

  /** Seek within the current track. Retries briefly after OpenUri while
   *  the player swaps to the new track. */
  async seekTo(positionMs, { expectUri = null, waitNewTrackMs = 0 } = {}) {
    const deadline = Date.now() + (waitNewTrackMs || 0);
    let trackId = null;
    for (;;) {
      const st = await this.state();
      trackId = st?.trackId;
      if (st && expectUri) {
        if (st.uri === expectUri) break; // new track is in
      } else if (trackId) break;
      if (Date.now() > deadline) { trackId ??= st?.trackId; break; }
      await sleep(150);
    }
    if (!trackId) return;
    this._uriToTrackId.set(expectUri ?? '', trackId);
    await this.call('SetPosition', trackId, Math.max(0, Math.round(positionMs * 1000)));
  }

  play() { return this.call('Play'); }
  pause() { return this.call('Pause'); }
  stop() { return this.call('Stop'); }
}

// ---------------------------------------------------------------------------
// 4. Jam session — the PeerJS + spicetify-jam protocol
// ---------------------------------------------------------------------------
class JamSession {
  constructor({ name, player, dryRun, guestControls = false }) {
    this.name = name;
    this.player = player;
    this.dryRun = dryRun;
    this.peer = null;
    this.conn = null;
    this.ping = 0;
    this.isHost = false;
    this.jamId = null;
    this.hostName = '';
    this.guestControls = !!guestControls;
    this.members = [];
    this.queue = [];            // host-mode queue [{uri,title,artist,artUrl,addedBy}]
    this.targetUri = null;      // track the host says we should be on
    this.targetStart = null;    // {posMs, atMs, playing} last host sync
    this.driftEma = 0;
    this.driftCount = 0;
    this.lastApplied = 0;       // ts of last seek we applied
    this.lastSyncReq = 0;
    this.closed = false;
    this.reconnects = 0;
    this.timers = [];
  }

  // ---- helpers ----------------------------------------------------------
  targetNow() {
    if (!this.targetStart) return 0;
    const { posMs, atMs, playing } = this.targetStart;
    return playing ? posMs + (Date.now() - atMs) : posMs;
  }

  send(msg) {
    if (this.conn?.open) this.conn.send(msg);
  }

  broadcast(msg) {
    for (const c of this.guests?.values() ?? []) if (c.open) c.send(msg);
  }

  code() {
    return Math.random().toString(36).slice(2, 8).toUpperCase();
  }

  // ---- lifecycle --------------------------------------------------------
  async start({ host = false, code = null } = {}) {
    this.isHost = host;
    this.jamId = host ? (code || this.code()) : null;
    this.peer = new Peer(host ? this.jamId : undefined, PEER_OPTS);
    this.peer.on('open', (id) => {
      log(`peer open as ${id}`);
      if (host) {
        log(`Jam code: ${this.jamId}  — share it, then keep this running`);
        this.guests = new Map();
        this.startHostLoops();
      } else {
        this.connectToHost(code);
      }
    });
    this.peer.on('connection', (c) => this.isHost && this.onGuestConnection(c));
    this.peer.on('error', (e) => {
      log('peer error:', e.type, e.message ?? '');
      if (e.type === 'unavailable-id' && host) {
        log('jam code taken, retrying with a new one…');
        this.peer.destroy();
        return this.start({ host });
      }
      if (e.type === 'peer-unavailable') {
        log('Jam not found — check the code. (Is her Jam still open?)');
      }
    });
  }

  connectToHost(code) {
    log(`connecting to Jam ${code} …`);
    const conn = this.peer.connect(code, { reliable: true });
    this.conn = conn;
    conn.on('open', () => {
      log('connected! joining…');
      this.reconnects = 0;
      this.send({ type: 'JOIN', name: this.name, image: '' });
      this.startGuestLoops();
    });
    conn.on('data', (d) => this.onGuestData(d));
    conn.on('close', () => {
      if (this.closed) return;
      if (++this.reconnects > 3) {
        log('lost the host and retries ran out — session over');
        return this.stop();
      }
      log(`connection lost, reconnecting (${this.reconnects}/3)…`);
      this.clearTimers();
      setTimeout(() => !this.closed && this.connectToHost(code), 1500 * this.reconnects);
    });
    conn.on('error', (e) => log('conn error:', e.message ?? e.type ?? e));
  }

  stop() {
    this.closed = true;
    this.clearTimers();
    try { this.conn?.close(); } catch {}
    try { this.peer?.destroy(); } catch {}
    log('left the Jam. bye :3');
    process.exit(0);
  }

  clearTimers() {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  every(ms, fn) { this.timers.push(setInterval(fn, ms)); }

  // ---- guest side -------------------------------------------------------
  startGuestLoops() {
    this.every(5000, () => this.send({ type: 'PING', ts: Date.now() }));
    this.every(6000, () => {
      if (Date.now() - this.lastApplied < 3000 || Date.now() - this.lastSyncReq < 5000) return;
      this.lastSyncReq = Date.now();
      this.send({ type: 'SYNC' });
    });
    // lock loop — keep our player on whatever the host says
    this.every(2000, () => this.enforceLock());
  }

  /** Mirror the host's playback into the local player. */
  async applyPlayback({ uri, posMs, paused, durMs = null }) {
    const comp = this.ping > 0 ? this.ping / 2 : 0; // latency compensation
    this.targetUri = uri;
    this.targetStart = { posMs, atMs: Date.now(), playing: !paused };
    this.lastApplied = Date.now();

    const st = await this.player.state().catch(() => null);
    const sameTrack = st && st.uri === uri;

    if (this.dryRun) {
      log(`[dry-run] PLAY ${uri} @${Math.round(posMs + comp)}ms ${paused ? '(paused)' : ''}${sameTrack ? ' (same track)' : ''}`);
      return;
    }

    if (!sameTrack) {
      await this.player.openUri(uri);
      await sleep(400); // let the player connect & start
      await this.player.seekTo(posMs + comp + 250, { expectUri: uri, waitNewTrackMs: 4000 });
      if (paused) { await sleep(250); await this.player.pause(); }
    } else {
      const drift = Math.abs(st.positionMs - (posMs + comp));
      if (paused) {
        await this.player.pause();
      } else if (drift > 400) {
        await this.player.seekTo(posMs + comp, { expectUri: uri, waitNewTrackMs: 2000 });
        if (!st.playing) await this.player.play();
      } else if (!st.playing) {
        await this.player.play();
      }
    }
  }

  /** If our track drifts off the host's target, snap back ("🔒 Locked to Jam"). */
  async enforceLock() {
    if (!this.targetUri || this.isHost) return;
    const st = await this.player.state().catch(() => null);
    if (!st || this.dryRun) return;
    if (st.uri !== this.targetUri) {
      log(`🔒 locked to Jam (was playing ${st.title || st.uri})`);
      await this.applyPlayback({
        uri: this.targetUri,
        posMs: this.targetNow(),
        paused: !this.targetStart?.playing,
      });
    }
  }

  async onGuestData(d) {
    const n = d;
    switch (n.type) {
      case 'INIT':
        this.hostName = n.host ?? 'Host';
        this.guestControls = !!n.gc;
        this.queue = n.queue ?? [];
        if (n.playing !== undefined) this.targetStart = { posMs: n.progress ?? 0, atMs: Date.now(), playing: n.playing };
        log(`in a Jam with “${this.hostName}”${this.guestControls ? ' (guest controls on)' : ''}`);
        if (n.np) log(`now: ${n.np.title} — ${n.np.artist}`);
        break;
      case 'PLAY':
        await this.applyPlayback({
          uri: n.uri, posMs: Number(n.pos || 0), paused: !!n.paused, durMs: n.dur,
        });
        break;
      case 'PAUSE':
        this.targetStart && (this.targetStart.playing = false);
        this.dryRun ? log('[dry-run] PAUSE') : await this.player.pause();
        break;
      case 'PS':
        if (this.targetStart && n.pos !== undefined) this.targetStart = { posMs: n.pos, atMs: Date.now(), playing: !!n.p };
        break;
      case 'SEEK': {
        const comp = this.ping > 0 ? this.ping / 2 : 0;
        this.targetStart = { posMs: n.pos, atMs: Date.now(), playing: this.targetStart?.playing ?? true };
        this.lastApplied = Date.now();
        this.dryRun ? log(`[dry-run] SEEK ${Math.round(n.pos + comp)}ms`) : await this.player.seekTo(n.pos + comp, {});
        break;
      }
      case 'SYNC_TICK': {
        // drift correction with smoothing, like the extension
        if (!this.targetStart?.playing || this.dryRun) break;
        const st = await this.player.state().catch(() => null);
        if (!st || !st.playing) break;
        if (Date.now() - this.lastApplied < 2000) break;
        const comp = this.ping > 0 ? this.ping / 2 : clamp(Date.now() - n.ts, 0, 500);
        const want = Number(n.pos || 0) + comp;
        const drift = Math.abs(st.positionMs - want);
        this.driftEma = this.driftEma === 0 ? drift : 0.7 * this.driftEma + 0.3 * drift;
        if (this.driftEma < 160) { this.driftCount = 0; break; }
        if (this.driftEma < 650 && ++this.driftCount < 2) break;
        this.driftCount = 0;
        this.driftEma = 0; // reset smoothing after acting, like the extension
        this.lastApplied = Date.now();
        await this.player.seekTo(want, {});
        log(`drift ${Math.round(drift)}ms — corrected`);
        break;
      }
      case 'PONG':
        this.ping = Math.max(0, Date.now() - n.ts);
        break;
      case 'MEMBERS':
        this.members = n.members ?? [];
        break;
      case 'GCTRL':
        this.guestControls = !!n.on;
        log(`guest controls ${this.guestControls ? 'enabled' : 'disabled'}`);
        break;
      case 'Q':
        this.queue = n.queue ?? [];
        break;
      case 'KICK':
        log('removed from the Jam by the host :c');
        this.stop();
        break;
    }
  }

  // ---- host side --------------------------------------------------------
  onGuestConnection(conn) {
    conn.on('data', (d) => this.onHostData(d, conn));
    conn.on('close', () => {
      this.guests.delete(conn.peer);
      this.pushMembers();
      log(`a listener left (${this.guests.size + 1} still here)`);
    });
    this.guests.set(conn.peer, { conn, name: 'Listener', image: '' });
  }

  pushMembers() {
    const members = [{ id: 'host', name: this.name, isHost: true },
      ...[...this.guests.values()].map((g, i) => ({ id: g.conn.peer, name: g.name, image: g.image }))];
    this.members = members;
    this.broadcast({ type: 'MEMBERS', members });
  }

  hostNowPlaying(st) {
    return st && st.uri ? { uri: st.uri, title: st.title, artist: st.artist, artUrl: st.artUrl } : null;
  }

  async hostBroadcastPlay() {
    const st = await this.player.state().catch(() => null);
    if (!st || !st.uri) return;
    this.targetUri = st.uri;
    this.lastPlay = { uri: st.uri, posMs: st.positionMs, playing: st.playing };
    this.broadcast({
      type: 'PLAY', uri: st.uri, pos: st.positionMs, ts: Date.now(),
      np: this.hostNowPlaying(st), paused: !st.playing, dur: st.durationMs,
    });
    // advance our own queue when the track ends naturally
  }

  startHostLoops() {
    let lastTrackId = null;
    // watch the local player; broadcast on song change / play-pause
    this.every(1000, async () => {
      const st = await this.player.state().catch(() => null);
      if (!st) return;
      if (st.trackId !== lastTrackId) {
        const first = lastTrackId === null;
        lastTrackId = st.trackId;
        if (!first) await this.onHostSongChange(st);
      } else {
        // play/pause edge detection
        if (this._lastPlaying !== undefined && this._lastPlaying !== st.playing) {
          this.broadcast({ type: st.playing ? 'PLAY' : 'PAUSE', ...(st.playing ? {
            uri: st.uri, pos: st.positionMs, ts: Date.now(), np: this.hostNowPlaying(st), paused: false, dur: st.durationMs,
          } : {}) });
          if (!st.playing) this.broadcast({ type: 'PS', p: false, pos: st.positionMs, dur: st.durationMs, ts: Date.now() });
        }
        this._lastPlaying = st.playing;
      }
    });
    // periodic drift tick, like the extension's 5s SYNC_TICK
    this.every(5000, async () => {
      const st = await this.player.state().catch(() => null);
      if (st?.playing && st.uri) {
        this.broadcast({ type: 'SYNC_TICK', pos: st.positionMs, ts: Date.now() });
      }
    });
    this.pushMembers();
  }

  async onHostSongChange(st) {
    // was this a natural end-of-queue advance, or the user picking something?
    const q = this.queue;
    const idx = q.findIndex((t) => t.uri === st.uri);
    if (idx >= 0) this.queue = q.slice(idx + 1);
    await this.hostBroadcastPlay();
    if (this.queue.length === 0 && !st.uri) return;
    log(`host: now playing ${st.title || st.uri} (queue: ${this.queue.length})`);
  }

  async playNextFromQueue() {
    const [next, ...rest] = this.queue;
    if (!next) return log('host: queue empty');
    this.queue = rest;
    this.dryRun ? log(`[dry-run] would play ${next.title ?? next.uri}`) : await this.player.openUri(next.uri);
  }

  async onHostData(n, guest) {
    const g = this.guests.get(guest.peer);
    switch (n.type) {
      case 'JOIN': {
        if (g) { g.name = n.name || 'Listener'; g.image = n.image || ''; }
        const st = await this.player.state().catch(() => null);
        guest.send({
          type: 'INIT', np: this.hostNowPlaying(st), queue: this.queue, host: this.name,
          gc: this.guestControls, playing: st?.playing ?? false, members: this.members,
          progress: st?.positionMs ?? 0, duration: st?.durationMs ?? 0,
        });
        if (st?.uri) {
          guest.send({ type: 'PLAY', uri: st.uri, pos: st.positionMs, ts: Date.now(), np: this.hostNowPlaying(st), paused: !st.playing });
        }
        this.pushMembers();
        log(`${n.name || 'someone'} joined the Jam :3`);
        break;
      }
      case 'PING': guest.send({ type: 'PONG', ts: n.ts }); break;
      case 'SYNC': {
        const st = await this.player.state().catch(() => null);
        if (st?.uri) guest.send({
          type: 'PLAY', uri: st.uri, pos: st.positionMs, ts: Date.now(),
          np: this.hostNowPlaying(st), paused: !st.playing, dur: st.durationMs,
        });
        break;
      }
      case 'ADD_Q': {
        this.queue.push({ uri: n.uri, title: n.title ?? '', artist: n.artist ?? '', artUrl: n.artUrl ?? '', addedBy: n.addedBy });
        this.broadcast({ type: 'Q', queue: this.queue });
        log(`queue += ${n.title || n.uri} (from ${n.addedBy?.name ?? 'guest'}) — ${this.queue.length} up next`);
        // if nothing is playing, kick the queue off
        const st = await this.player.state().catch(() => null);
        if (!st?.uri && !this.dryRun) await this.playNextFromQueue();
        break;
      }
      case 'RM_Q': {
        this.queue = this.queue.filter((t) => t.uri !== n.uri);
        this.broadcast({ type: 'Q', queue: this.queue });
        break;
      }
      case 'MOVE_Q': {
        const [it] = this.queue.splice(n.from, 1);
        if (it) this.queue.splice(clamp(n.to, 0, this.queue.length), 0, it);
        this.broadcast({ type: 'Q', queue: this.queue });
        break;
      }
      case 'GCTRL': break;
      case 'CMD': {
        if (!this.guestControls) break;
        const now = Date.now();
        if (now - (this._lastCmd ?? 0) < 500) break;
        this._lastCmd = now;
        log(`cmd from ${g?.name ?? 'guest'}: ${n.a}`);
        if (this.dryRun) break;
        if (n.a === 'play') await this.player.play();
        else if (n.a === 'pause') await this.player.pause();
        else if (n.a === 'seek') await this.player.seekTo(n.pos, {});
        else if (n.a === 'playuri') await this.player.openUri(n.uri);
        else if (n.a === 'next') await this.playNextFromQueue();
        break;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// 5. CLI
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : dflt;
};
const dryRun = args.includes('--dry-run');
const wantHost = args.includes('--host');
// positional args = args that are not flags and not a flag's value
const VALUE_FLAGS = new Set(['--name', '--player', '--code']);
const positional = args.filter((a, i) => i > 0 && VALUE_FLAGS.has(args[i - 1]) ? false : !a.startsWith('--'));
const codeArg = flag('code', positional[0]);

const name = flag('name', process.env.USER || 'spotifast');
const playerBus = flag('player', 'fastpotify');
const guestControls = args.includes('--gc') || args.includes('--guest-controls');

if (!wantHost && !codeArg) {
  console.log('usage: jam-bridge.mjs <CODE>   |   jam-bridge.mjs --host');
  console.log('  --name NAME     display name (default: $USER)');
  console.log('  --player BUS    MPRIS bus suffix (default: fastpotify; e.g. spotify)');
  console.log('  --dry-run       log actions without touching playback');
  process.exit(1);
}

const player = new MprisPlayer(`org.mpris.MediaPlayer2.${playerBus}`);
await player.connect().catch((e) => {
  console.error(`could not reach MPRIS player “${playerBus}”: ${e.message}`);
  console.error('is spotifast running? (bus names must match org.mpris.MediaPlayer2.<suffix>)');
  process.exit(1);
});
const st = await player.state();
log(`attached to ${playerBus}${st?.title ? ` — currently: ${st.title}` : ''}`);

const session = new JamSession({ name, player, dryRun, guestControls });
process.on('SIGINT', () => session.stop());
await session.start({ host: wantHost, code: codeArg });
