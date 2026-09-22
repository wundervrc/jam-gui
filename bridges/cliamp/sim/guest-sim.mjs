// guest-sim — pretends to be the spicetify-jam GUEST (what her Windows client
// does when she joins). For testing jam-bridge HOST mode.
import { createRequire } from 'node:module';
import process from 'node:process';

const { Peer } = await import('../lib/peerjs-node.mjs');

const CODE = process.argv[2] || 'TEST12';
const NAME = process.argv[3] || 'SimGirlfriend';
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), '[guest-sim]', ...a);

const peer = new Peer({ debug: 0 });
peer.on('open', () => {
  log(`my id ${peer.id}, joining ${CODE}…`);
  const conn = peer.connect(CODE, { reliable: true });
  conn.on('open', () => {
    log('connected, sending JOIN');
    conn.send({ type: 'JOIN', name: NAME, image: '' });
    setInterval(() => conn.open && conn.send({ type: 'PING', ts: Date.now() }), 2000);
    setInterval(() => conn.open && conn.send({ type: 'SYNC' }), 3000);
    // after 6s, test guest controls + queue add (host mode features)
    setTimeout(() => { if (conn.open) { log('sending CMD pause'); conn.send({ type: 'CMD', a: 'pause' }); } }, 6000);
    setTimeout(() => { if (conn.open) { log('sending CMD play'); conn.send({ type: 'CMD', a: 'play' }); } }, 9000);
    setTimeout(() => {
      if (conn.open) {
        log('sending ADD_Q (uri only — matches the real extension)');
        conn.send({ type: 'ADD_Q', uri: 'spotify:track:4cOdK2wGLETKBW3PvgPWqT', addedBy: { name: NAME } });
      }
    }, 12000);
    setTimeout(() => {
      if (conn.open) {
        log('sending CMD playuri (guest control: host should play this)');
        conn.send({ type: 'CMD', a: 'playuri', uri: 'spotify:track:5ChkMS8OtdzJeqyybCc9R5' });
      }
    }, 16000);
  });
  conn.on('data', (n) => {
    if (n.type === 'PING') conn.send({ type: 'PONG', ts: n.ts });
    else if (n.type === 'PLAY') log(`PLAY ${n.np?.title ?? n.uri} @${Math.round(n.pos ?? 0)}ms paused=${!!n.paused}`);
    else if (n.type === 'PONG') log(`pong rtt=${Date.now() - n.ts}ms`);
    else log(n.type, JSON.stringify(n).slice(0, 140));
  });
  conn.on('close', () => log('closed'));
});
peer.on('error', (e) => log('peer error:', e.type));
