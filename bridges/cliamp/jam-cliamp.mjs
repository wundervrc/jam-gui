#!/usr/bin/env node
/**
 * jam-cliamp — join a Spicetify Jam session from cliamp
 *
 * Speaks the same PeerJS/WebRTC protocol as the spicetify-jam extension
 * (Kyzenkms/spicetify-jam v1.4.1) and mirrors playback into cliamp through
 * its v2 IPC API (`cliamp remote call ...` / `cliamp status --json`).
 *
 * Mode:
 *   guest (default)  — join her Jam code, follow playback in cliamp
 *   host (--host)    — start a Jam from cliamp; she joins with the code
 *                      from her spicetify-jam extension as usual
 *
 * Usage:
 *   node jam-cliamp.mjs <CODE> [--name NAME] [--dry-run] [--daemon]
 *   node jam-cliamp.mjs --host [--code CODE] [--name NAME] [--gc] [--dry-run] [--daemon]
 *
 * --daemon starts a headless cliamp (`cliamp --daemon`) if none is running.
 */

import process from 'node:process';
import { execFile } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// Browser-global stubs + WebRTC polyfill, then load peerjs (see lib/)
const { Peer } = await import('./lib/peerjs-node.mjs');

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

// ---------------------------------------------------------------------------
// cliamp adapter — drives the running cliamp over its v2 IPC
// ---------------------------------------------------------------------------
const CLIAMP = process.env.CLIAMP_BIN || 'cliamp';
const SOCK = path.join(os.homedir(), '.config', 'cliamp', 'cliamp.sock');

function cli(...args) {
  return new Promise((resolve) => {
    execFile(CLIAMP, args, { timeout: 20000 }, (err, stdout) => {
      resolve(err ? { __error: err.message } : stdout);
    });
  });
}

const FIRE_AND_FORGET = new Set(['seek.absolute', 'runtime.play', 'runtime.pause', 'runtime.toggle']);

async function remoteCall(op, params = {}) {
  const out = await cli('remote', 'call', op, '--params', JSON.stringify(params));
  if (out.__error) return { error: out.__error };
  try {
    const j = JSON.parse(out);
    if (j.job?.id) {
      if (FIRE_AND_FORGET.has(op)) return { ok: true };
      // async op — poll the job so errors surface (light cadence; each poll is a subprocess)
      await sleep(200);
      for (let i = 0; i < 6; i++) {
        const jobOut = await cli('remote', 'job', j.job.id);
        try {
          const job = JSON.parse(jobOut)?.job;
          if (job?.state === 'succeeded') return { ok: true, result: job.result };
          if (job?.state === 'failed') return { error: job.error?.detail || job.error?.message || op + ' failed' };
        } catch { /* retry */ }
        await sleep(500);
      }
      return { ok: true };
    }
    return j;
  } catch {
    return { error: 'bad json from cliamp' };
  }
}

class CliampPlayer {
  constructor() { this.meta = {}; }

  get running() { return existsSync(SOCK); }

  async maybeStartDaemon() {
    if (this.running) return true;
    log('cliamp is not running — starting a headless daemon (cliamp --daemon)…');
    const { spawn } = await import('node:child_process');
    const child = spawn(CLIAMP, ['--daemon'], { detached: true, stdio: 'ignore' });
    child.unref();
    for (let i = 0; i < 40 && !existsSync(SOCK); i++) await sleep(250);
    if (!existsSync(SOCK)) { log('could not start the cliamp daemon'); return false; }
    await sleep(500);
    return true;
  }

  /** { playing, positionMs, durationMs, uri, title, artist, artUrl } */
  async state() {
    const out = await cli('status', '--json');
    try {
      const s = JSON.parse(out);
      const t = s.track ?? {};
      return {
        playing: s.state === 'playing',
        positionMs: (s.position ?? 0) * 1000,
        durationMs: (s.duration ?? 0) * 1000,
        uri: typeof t.path === 'string' && t.path.startsWith('spotify:track:') ? t.path : '',
        title: t.title ?? '',
        artist: t.artist ?? '',
        artUrl: '',
      };
    } catch {
      return null;
    }
  }

  /** Play a spotify track by URI. meta {title, artist} improves cliamp's display. */
  async openUri(uri, meta = {}) {
    const trackID = uri.replace('spotify:track:', '');
    const r = await remoteCall('track.play', {
      track: {
        path: uri,
        title: meta.title || trackID,
        artist: meta.artist || '',
        provider_meta: { kind: 'track', trackID },
      },
    });
    if (r.error) log('track.play:', r.error);
  }

  async seekTo(positionMs) {
    await remoteCall('seek.absolute', { value: Math.max(0, positionMs / 1000) });
  }

  play() { return remoteCall('runtime.play'); }
  pause() { return remoteCall('runtime.pause'); }
}

// ---------------------------------------------------------------------------
// Jam session — identical protocol to spicetify-jam
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
    this.guestControls = guestControls;
    this.members = [];
    this.queue = [];
    this.targetUri = null;
    this.targetStart = null;
    this.driftEma = 0;
    this.driftCount = 0;
    this.lastApplied = 0;
    this.lastSyncReq = 0;
    this.closed = false;
    this.reconnects = 0;
    this.timers = [];
  }

  targetNow() {
    if (!this.targetStart) return 0;
    const { posMs, atMs, playing } = this.targetStart;
    return playing ? posMs + (Date.now() - atMs) : posMs;
  }

  send(msg) { if (this.conn?.open) this.conn.send(msg); }
  broadcast(msg) { for (const c of this.guests?.values() ?? []) if (c.open) c.send(msg); }
  code() { return Math.random().toString(36).slice(2, 8).toUpperCase(); }

  async start({ host = false, code = null } = {}) {
    this.isHost = host;
    this.jamId = host ? (code || this.code()) : null;
    this.peer = new Peer(host ? this.jamId : undefined, { debug: 1 });
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

  clearTimers() { for (const t of this.timers) clearInterval(t); this.timers = []; }
  every(ms, fn) { this.timers.push(setInterval(fn, ms)); }

  // ---- guest side -------------------------------------------------------
  startGuestLoops() {
    this.every(5000, () => this.send({ type: 'PING', ts: Date.now() }));
    this.every(6000, () => {
      if (Date.now() - this.lastApplied < 3000 || Date.now() - this.lastSyncReq < 5000) return;
      this.lastSyncReq = Date.now();
      this.send({ type: 'SYNC' });
    });
    this.every(2000, () => this.enforceLock());
  }

  async applyPlayback({ uri, posMs, paused, np = null }) {
    const comp = this.ping > 0 ? this.ping / 2 : 0;
    this.targetUri = uri;
    this.targetStart = { posMs, atMs: Date.now(), playing: !paused };
    this.lastApplied = Date.now();

    const st = await this.player.state().catch(() => null);
    const sameTrack = st && st.uri === uri;

    if (this.dryRun) {
      log(`[dry-run] PLAY ${np?.title ?? uri} @${Math.round(posMs + comp)}ms ${paused ? '(paused)' : ''}${sameTrack ? ' (same track)' : ''}`);
      return;
    }

    if (!sameTrack) {
      await this.player.openUri(uri, { title: np?.title, artist: np?.artist });
      await sleep(700); // let cliamp connect & start
      await this.player.seekTo(posMs + comp + 250);
      if (paused) { await sleep(250); await this.player.pause(); }
    } else {
      const drift = Math.abs(st.positionMs - (posMs + comp));
      if (paused) {
        await this.player.pause();
      } else if (drift > 400) {
        await this.player.seekTo(posMs + comp);
        if (!st.playing) await this.player.play();
      } else if (!st.playing) {
        await this.player.play();
      }
    }
  }

  async enforceLock() {
    if (!this.targetUri || this.isHost) return;
    const st = await this.player.state().catch(() => null);
    if (!st || this.dryRun) return;
    if (st.uri !== this.targetUri) {
      log(`🔒 locked to Jam (was playing ${st.title || st.uri || 'something else'})`);
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
        await this.applyPlayback({ uri: n.uri, posMs: Number(n.pos || 0), paused: !!n.paused, np: n.np });
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
        this.dryRun ? log(`[dry-run] SEEK ${Math.round(n.pos + comp)}ms`) : await this.player.seekTo(n.pos + comp);
        break;
      }
      case 'SYNC_TICK': {
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
        await this.player.seekTo(want);
        log(`drift ${Math.round(drift)}ms — corrected`);
        break;
      }
      case 'PONG':
        this.ping = Math.max(0, Date.now() - n.ts);
        break;
      case 'MEMBERS': this.members = n.members ?? []; break;
      case 'GCTRL':
        this.guestControls = !!n.on;
        log(`guest controls ${this.guestControls ? 'enabled' : 'disabled'}`);
        break;
      case 'Q': this.queue = n.queue ?? []; break;
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
      ...[...this.guests.values()].map((g) => ({ id: g.conn.peer, name: g.name, image: g.image }))];
    this.members = members;
    this.broadcast({ type: 'MEMBERS', members });
  }

  hostNowPlaying(st) {
    return st && st.uri ? { uri: st.uri, title: st.title, artist: st.artist, artUrl: st.artUrl } : null;
  }

  startHostLoops() {
    let lastUri = undefined;
    this.every(1000, async () => {
      const st = await this.player.state().catch(() => null);
      if (!st) return;
      if (st.uri !== lastUri) {
        const first = lastUri === undefined;
        lastUri = st.uri;
        if (!first && st.uri) await this.hostBroadcastPlay(st);
        if (!first && !st.uri) this.broadcast({ type: 'PAUSE' });
      } else {
        if (this._lastPlaying !== undefined && this._lastPlaying !== st.playing) {
          if (st.playing) await this.hostBroadcastPlay(st);
          else this.broadcast({ type: 'PAUSE' });
        }
        this._lastPlaying = st.playing;
      }
    });
    this.every(5000, async () => {
      const st = await this.player.state().catch(() => null);
      if (st?.playing && st.uri) this.broadcast({ type: 'SYNC_TICK', pos: st.positionMs, ts: Date.now() });
    });
    this.pushMembers();
  }

  async hostBroadcastPlay(st) {
    this.targetUri = st.uri;
    this.broadcast({
      type: 'PLAY', uri: st.uri, pos: st.positionMs, ts: Date.now(),
      np: this.hostNowPlaying(st), paused: !st.playing, dur: st.durationMs,
    });
  }

  async playNextFromQueue() {
    const [next, ...rest] = this.queue;
    if (!next) return log('host: queue empty');
    this.queue = rest;
    this.dryRun ? log(`[dry-run] would play ${next.title ?? next.uri}`) : await this.player.openUri(next.uri, { title: next.title, artist: next.artist });
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
        if (st?.uri) guest.send({ type: 'PLAY', uri: st.uri, pos: st.positionMs, ts: Date.now(), np: this.hostNowPlaying(st), paused: !st.playing });
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
      case 'CMD': {
        if (!this.guestControls) break;
        const now = Date.now();
        if (now - (this._lastCmd ?? 0) < 500) break;
        this._lastCmd = now;
        log(`cmd from ${g?.name ?? 'guest'}: ${n.a}`);
        if (this.dryRun) break;
        if (n.a === 'play') await this.player.play();
        else if (n.a === 'pause') await this.player.pause();
        else if (n.a === 'seek') await this.player.seekTo(n.pos);
        else if (n.a === 'playuri') await this.player.openUri(n.uri, { title: n.np?.title, artist: n.np?.artist });
        else if (n.a === 'next') await this.playNextFromQueue();
        break;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : dflt;
};
const dryRun = args.includes('--dry-run');
const wantHost = args.includes('--host');
const wantDaemon = args.includes('--daemon');
const VALUE_FLAGS = new Set(['--name', '--code']);
const positional = args.filter((a, i) => i > 0 && VALUE_FLAGS.has(args[i - 1]) ? false : !a.startsWith('--'));
const codeArg = flag('code', positional[0]);

const name = flag('name', process.env.USER || 'cliamp');
const guestControls = args.includes('--gc') || args.includes('--guest-controls');

if (!wantHost && !codeArg) {
  console.log('usage: jam-cliamp.mjs <CODE>   |   jam-cliamp.mjs --host [--code CODE]');
  console.log('  --name NAME     display name (default: $USER)');
  console.log('  --gc            host mode: allow guest play/pause/next/seek');
  console.log('  --daemon        start a headless cliamp if none is running');
  console.log('  --dry-run       log actions without touching playback');
  process.exit(1);
}

const player = new CliampPlayer();
if (!player.running && wantDaemon) await player.maybeStartDaemon();
if (!player.running) {
  console.error('cliamp is not running (no socket). Start cliamp, or pass --daemon for headless.');
  process.exit(1);
}
const st = await player.state();
if (!st) {
  console.error('could not read cliamp status — is it responding? (try: cliamp status --json)');
  process.exit(1);
}
log(`attached to cliamp${st.title ? ` — currently: ${st.title}` : ''}`);

const session = new JamSession({ name, player, dryRun, guestControls });
process.on('SIGINT', () => session.stop());
await session.start({ host: wantHost, code: codeArg });
