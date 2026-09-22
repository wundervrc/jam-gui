// host-sim — pretends to be the spicetify-jam HOST (what her Windows client does)
// Sends the same messages the real extension sends. For testing jam-bridge guest mode.
import { createRequire } from 'node:module';
import process from 'node:process';

const { Peer } = await import('../lib/peerjs-node.mjs');

const CODE = process.argv[2] || 'TEST12';
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), '[host-sim]', ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SONGS = [
  { uri: 'spotify:track:4cOdK2wGLETKBW3PvgPWqT', title: 'Never Gonna Give You Up', artist: 'Rick Astley', dur: 213000 },
  { uri: 'spotify:track:5ChkMS8OtdzJeqyybCc9R5', title: 'Mr. Blue Sky', artist: 'Electric Light Orchestra', dur: 312000 },
  { uri: 'spotify:track:0VjIjW4GlUZAMYd2vXMi3b', title: 'Stay', artist: 'The Kid LAROI', dur: 141000 },
];

let songIdx = 0;
let posMs = 0;
let playing = true;
const guests = new Map();

const peer = new Peer(CODE, { debug: 0 });
peer.on('open', (id) => {
  log(`hosting as ${id} — song “${SONGS[0].title}”`);
  setInterval(() => { if (playing) posMs += 2000; }, 2000);
  // SYNC_TICK every 2s like the real host's drift ticker
  setInterval(() => {
    for (const g of guests.values()) if (g.open) g.send({ type: 'SYNC_TICK', pos: posMs, ts: Date.now() });
  }, 2000);
  // song change every 20s
  setInterval(() => {
    songIdx = (songIdx + 1) % SONGS.length;
    posMs = 0;
    const s = SONGS[songIdx];
    log(`songchange → ${s.title}`);
    for (const g of guests.values()) if (g.open) {
      g.send({ type: 'PLAY', uri: s.uri, pos: 0, ts: Date.now(), np: { uri: s.uri, title: s.title, artist: s.artist }, paused: !playing });
    }
  }, 20000);
  // pause toggle every 13s
  setInterval(() => {
    playing = !playing;
    log(playing ? 'resumed' : 'paused');
    const s = SONGS[songIdx];
    for (const g of guests.values()) if (g.open) {
      g.send(playing
        ? { type: 'PLAY', uri: s.uri, pos: posMs, ts: Date.now(), np: { uri: s.uri, title: s.title, artist: s.artist }, paused: false }
        : { type: 'PAUSE' });
    }
  }, 13000);
});

peer.on('connection', (conn) => {
  guests.set(conn.peer, conn);
  conn.on('data', (n) => {
    if (n.type === 'JOIN') {
      log(`JOIN from ${n.name}`);
      const s = SONGS[songIdx];
      conn.send({ type: 'INIT', np: { uri: s.uri, title: s.title, artist: s.artist }, queue: [], host: 'SimHost', gc: true, playing, members: [], progress: posMs, duration: s.dur });
      conn.send({ type: 'PLAY', uri: s.uri, pos: posMs, ts: Date.now(), np: { uri: s.uri, title: s.title, artist: s.artist }, paused: !playing });
      conn.send({ type: 'MEMBERS', members: [{ id: 'host', name: 'SimHost', isHost: true }, { id: conn.peer, name: n.name }] });
    } else if (n.type === 'PING') conn.send({ type: 'PONG', ts: n.ts });
    else if (n.type === 'SYNC') {
      const s = SONGS[songIdx];
      conn.send({ type: 'PLAY', uri: s.uri, pos: posMs, ts: Date.now(), np: { uri: s.uri, title: s.title, artist: s.artist }, paused: !playing, dur: s.dur });
    } else if (n.type === 'CMD') log(`guest cmd: ${n.a}`);
    else log('msg from guest:', n.type);
  });
  conn.on('close', () => guests.delete(conn.peer));
});
peer.on('error', (e) => log('peer error:', e.type));
