# The Spicetify Jam wire protocol

Reverse-engineered from `spicetify-jam` v1.4.1 (bundled `spicetify-jam.js`), which is a
minified React app using **PeerJS 1.x** for transport. This is the reference the bridges
in this repo implement. All credit for the design: **Kyzenkms/spicetify-jam**.

## Transport

- **Signaling**: PeerJS public cloud — `wss://0.peerjs.com/peerjs` (default host, key `peerjs`).
  The host registers a peer with the 6-character Jam code as its ID; guests get server-assigned
  UUIDs and `connect()` to the code.
- **ICE servers** used by the extension:
  - `stun:stun.l.google.com:19302`
  - `turn:eu-0.turn.peerjs.com:3478`, `turn:us-0.turn.peerjs.com:3478` (user `peerjs`, pass `peerjsp`)
- **Data**: one reliable WebRTC DataChannel per guest (created by the guest, `reliable: true`),
  PeerJS default serialization (BinaryPack; all Jam messages are plain JSON objects).
- Host ↔ guest is star-shaped: the host relays state; guests never talk to each other except
  their avatars/metadata flow through the host's member list.

## Message shapes

All timestamps (`ts`) are `Date.now()` milliseconds. Positions are **milliseconds** into the track.

### Handshake

| dir | type | payload |
|---|---|---|
| guest → host | `JOIN` | `{name, image}` |
| host → guest | `INIT` | `{np, queue, host, gc, playing, progress, duration, members}` |
| host → guest | `MEMBERS` | `{members: [{id, name, image, isHost?}]}` |
| host → guest | `KICK` | *(none)* — guest closes |

On `JOIN` the host also immediately sends a `PLAY` for the current track.

### Playback sync

| dir | type | payload | meaning |
|---|---|---|---|
| host → * | `PLAY` | `{uri, pos, ts, np, paused, dur?}` | current track + position at `ts`. `np = {uri,title,artist,artUrl}` |
| host → * | `PAUSE` | *(none)* | host paused |
| host → * | `SEEK` | `{pos, ts}` | host seeked |
| host → * | `PS` | `{p, pos, dur, ts}` | play-state update (p = bool) |
| host → * | `SYNC_TICK` | `{pos, ts}` | periodic drift reference (every ~5s) |
| guest → host | `SYNC` | *(none)* | request a fresh `PLAY` (guest asks every ~6s) |
| either | `PING` | `{ts}` | RTT probe |
| either | `PONG` | `{ts}` | echo of `PING.ts`; RTT = now − ts |

Guest side logic (from the extension):
- target position = `pos + ping/2` (clamped 0–500ms when ping unknown)
- new track: `playUri(uri)`, wait ~350ms, `seek(pos + ping/2 + 350)`
- paused PLAY: `playUri` then `pause` after ~150ms (to position the track)
- drift: EMA smoothing (0.7 old / 0.3 new); ignore < 160ms; seek when > 650ms
  (or > 160ms on 2+ consecutive ticks); reset smoothing after seeking
- guests are locked: if the local track differs from `targetUri` without a host `PLAY`,
  the client force-plays the host's track ("🔒 Locked to Jam")

### Queue & controls

| dir | type | payload | meaning |
|---|---|---|---|
| guest → host | `ADD_Q` | `{uri, addedBy: {name, image}}` | guest "Add to Jam" context menu |
| guest → host | `RM_Q` | `{uri, uid}` | remove from queue |
| guest → host | `MOVE_Q` | `{from, to}` | reorder (rate-limited 800ms) |
| host → * | `Q` | `{queue: [...]}` | broadcast queue state |
| host → * | `GCTRL` | `{on: bool}` | guest controls toggled |
| guest → host | `CMD` | `{a, pos?, uri?}` | `a` ∈ `play\|pause\|next\|back\|seek\|playuri` (rate-limited 500ms, only when `gc` on) |

### Misc

- Guest reconnect: 3 attempts, backoff `1.5s × attempt`.
- The extension telemetry-beacons `session_start` / `session_join` / heartbeats to the
  author's VPS (`kyzen-vps-new.tail9c3971.ts.net/jam/ping`) with a device id in
  `localStorage["jam_did"]`. The bridges in this repo do **not** implement this.
- Update check fetches `raw.githubusercontent.com/Kyzenkms/spicetify-jam/main/manifest.json`
  and compares `version`/`patch`; failure is silently ignored.

## Host peer ID notes

The Jam code doubles as the PeerJS peer id: `Math.random().toString(36).slice(2,8).toUpperCase()`.
`id-taken` → the extension retries with a new code (up to 5 times).
