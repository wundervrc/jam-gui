# jam-cliamp

**Listen together, in cliamp.** Join or host a [Spicetify Jam](https://github.com/Kyzenkms/spicetify-jam)
session (the P2P listen-together extension for Spotify desktop) — but play your side in
cliamp, the retro terminal music player.

Your partner keeps using normal Spotify + Spicetify Jam on their machine. You run this bridge
in a second terminal. It speaks the exact same PeerJS/WebRTC protocol as the extension and
mirrors playback into cliamp through its **v2 IPC API** (`cliamp remote call` / `cliamp status --json`).

```
   her machine                         your machine
┌─────────────────────┐          ┌──────────────────────────┐
│ Spotify + spicetify │◀── P2P ──▶ jam-cliamp ── IPC ──▶ cliamp
│   (spicetify-jam)   │ WebRTC   (this repo, Node)        (TUI or --daemon)
└─────────────────────┘          └──────────────────────────┘
        either side can host the session
```

No audio is transmitted — only play/pause/seek/song messages. Each side streams from
their own Spotify account. Spotify playback through cliamp needs Premium.

## Install

```sh
cd jam-cliamp
npm install        # peerjs + node-datachannel
```

Requires: Node 18+ (tested on 24), a running cliamp (TUI anywhere, or headless).

## Use

**Follow her Jam** (she clicks "Start a new Jam" in her Spotify and gives you the code):

```sh
node jam-cliamp.mjs ABC123 --name wunder
```

**Host from cliamp** (she joins with the code from her extension as usual):

```sh
node jam-cliamp.mjs --host --gc --name wunder
```

**No TUI needed?** Add `--daemon` and the bridge starts a headless cliamp itself:

```sh
node jam-cliamp.mjs ABC123 --daemon
```

While hosting with `--gc`, her play/pause/next/seek buttons and "Add to Jam" context
menu work: added songs go into the bridge's queue and play in order via `track.play`.

Flags:

| flag | meaning |
|---|---|
| `--name NAME` | name shown to other listeners (default `$USER`) |
| `--host` | host a session instead of joining |
| `--code CODE` | pick your own code when hosting |
| `--gc` | (host) allow guest playback controls |
| `--daemon` | start headless `cliamp --daemon` if none is running |
| `--dry-run` | log what would happen without touching playback |

Ctrl+C leaves the Jam cleanly.

## How it works

- **Signaling**: PeerJS public cloud server (`0.peerjs.com`) — same as the extension
- **Connection**: WebRTC data channel via libdatachannel; STUN (Google) + TURN (PeerJS public relays)
- **Player control**: cliamp v2 IPC:
  - `track.play {track:{path,title,artist,provider_meta:{kind:"track",trackID}}}` to play a URI
  - `seek.absolute {value: seconds}`, `runtime.play` / `runtime.pause`
  - `cliamp status --json` polled for `{state, position, duration, track.path}`
- **Sync logic**: mirrors the extension — latency compensation (ping/2), drift-smoothed correction
  that only seeks when you're >650ms off (or >160ms twice), lock-back when your local track changes

The full message protocol is documented in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Testing without her

Two simulators are included (`sim/`) that impersonate each side of a real session:

```sh
node sim/host-sim.mjs TEST12        # fake host: song changes + pause toggles
node jam-cliamp.mjs TEST12 --dry-run
```

## Notes for cliamp development

- The adapter shells out to the `cliamp` binary (`remote call`/`status`), so it survives IPC
  schema changes only as well as those commands do. If you'd rather have a first-class
  integration, the clean hook points in cliamp are: a `track.play`-by-URI op that fetches
  metadata itself (so callers only pass a URI), and exposing `position` in `runtime.events`.
- `track.play` appends to the live playlist. The bridge does not clean up entries after a
  session; clear with `queue.clear` if you mind the history.

## Credits & license

- The Jam protocol, sync heuristics and ICE configuration are from
  **spicetify-jam by Kyzenkms** (v1.4.1, repo currently unavailable). Credit for the
  design belongs to them.
- This bridge: MIT.
