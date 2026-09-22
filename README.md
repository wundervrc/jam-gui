# Jam

**Listen to Spotify together — from lightweight players, not the heavy official Spotify app.**

A native **Rust + egui** client that joins or hosts [Spicetify Jam](https://github.com/Kyzenkms/spicetify-jam)
sessions (Kyzen's P2P listen-together extension for Spotify desktop) and plays your side through
[Spotifast](https://github.com/crmne/spotifast) or [cliamp](https://github.com/crmne/cliamp) — no
Spotify desktop, no Electron, no Node required for the GUI.

> **Compatible with Kyzen's Spicetify Jam** — your partner keeps using normal Spotify with the
> spicetify-jam extension; you run Jam. Either side can host.

```
┌─────────────────────────────┐         ┌──────────────────────────────────────┐
│        their machine        │         │             your machine             │
│  Spotify + spicetify-jam    │◀─ P2P ─▶│  Jam (this repo) ──▶ spotifast (MPRIS) │
│  (Windows, unchanged)       │ WebRTC  │                 └──▶ cliamp    (IPC)   │
└─────────────────────────────┘         └──────────────────────────────────────┘
```

No audio is transmitted — only play/pause/seek/song messages. Each side streams from their own
Spotify account (playback through spotifast/cliamp needs Spotify Premium).

## Build

```sh
cargo build --release
```

Requirements: Rust, cmake + a C++ compiler (libdatachannel is vendored and built statically).

## Use

Run the binary. Enter your display name, pick the player backend, then:

- **Join** — enter the 6-character code from the host's Jam
- **Host** — you get a code to share; toggle "let the guest control playback" if you want
  listeners to be able to play/pause/skip and add songs

Or headless, no GUI:

```sh
jam-gui --headless ABC123                      # spotifast (default MPRIS bus)
jam-gui --headless ABC123 --mpris spotify      # any MPRIS player by bus suffix
jam-gui --headless ABC123 --backend cliamp
jam-gui --headless --host --gc --name wunder   # host a Jam
jam-gui --headless ABC123 --dry-run            # log without touching playback
```

`Ctrl+C` or the Leave button exits the session cleanly.

## How it works

The GUI re-implements the spicetify-jam wire protocol natively in Rust — no JavaScript anywhere:

- **BinaryPack codec** (`src/binarypack.rs`) — byte-compatible with peerjs's js-binarypack
  (including its swapped string/bin marker ranges)
- **PeerJS signaling client** (`src/peerjs.rs`) — WebSocket to the public `0.peerjs.com` cloud,
  heartbeats, offer/answer/candidate flow, message chunking
- **WebRTC** — libdatachannel (vendored, statically linked)
- **Jam protocol** (`src/jam.rs`) — guest sync + host broadcast, ping/2 latency compensation,
  EMA-smoothed drift correction (seeks only past ~650 ms), lock-back when the local track changes,
  3 reconnect attempts with backoff
- **Player backends** (`src/player.rs`) — MPRIS for spotifast/anything (zbus, uncached property
  reads + position interpolation), cliamp v2 IPC (`track.play`, `seek.absolute`, `runtime.play`)

The full message protocol is documented in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Node bridges (optional fallbacks)

The [`bridges/`](bridges/) folder contains earlier **Node.js versions** of the same idea — one for
spotifast (MPRIS) and one for cliamp (IPC). They speak the identical protocol and were the
reference implementations the Rust GUI was ported from. Useful if you want a headless daemon
without building Rust, or want to hack on the protocol in JS:

```sh
cd bridges/spotifast && npm install
node jam-bridge.mjs ABC123            # same flags as the GUI's headless mode
```

Each bridge folder has its own README, and `bridges/sim/` contains simulators that impersonate
both sides of a real session for testing without a partner:

```sh
node bridges/sim/host-sim.mjs TEST12 &
node bridges/spotifast/jam-bridge.mjs TEST12 --dry-run
```

## Credits

- **[Spicetify Jam](https://github.com/Kyzenkms/spicetify-jam) by Kyzenkms** — the protocol, sync
  heuristics and session design are Kyzen's; this project is a compatible client, not a fork.
- [Spotifast](https://github.com/crmne/spotifast) by crmne (MIT) · [cliamp](https://github.com/bjarneo/cliamp) by bjarneo
- [libdatachannel](https://github.com/paullouisageneau/libdatachannel) · [peerjs](https://github.com/peers/peerjs)

## Licence

MIT — see [LICENSE](LICENSE).
