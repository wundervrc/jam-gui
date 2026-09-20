# jam-gui

**Listen together, natively.** A small, fast Rust + egui app that joins or hosts
[Spicetify Jam](https://github.com/Kyzenkms/spicetify-jam) sessions (the P2P
listen-together extension for Spotify desktop) and plays your side through
**spotifast** or **cliamp** — no Spotify desktop, no Electron, no Node.

Single binary, ~15 MB, one thread-pool, uses the official PeerJS cloud for
signaling and WebRTC data channels for transport (re-implemented natively in
Rust on top of libdatachannel — no JavaScript anywhere).

```
┌─────────────────────────────┐         ┌────────────────────────────────┐
│        her machine          │         │         your machine           │
│  Spotify + spicetify-jam    │◀─ P2P ─▶│  jam-gui ──▶ spotifast (MPRIS) │
│  (Windows, unchanged)       │ WebRTC  │          └─▶ cliamp    (IPC)   │
└─────────────────────────────┘         └────────────────────────────────┘
```

## Build

```sh
cargo build --release
# needs: rust, cmake + a C++ compiler (libdatachannel), system web-view deps
# for egui are already part of most desktops (X11/Wayland).
```

## Use

Run the binary. Type your display name, pick the player backend, then:

- **Join**: enter the 6-character code from her "Start a new Jam"
- **Host**: you get a code — she joins from her extension as usual

Or headless, no GUI:

```sh
jam-gui --headless ABC123                      # spotifast (default MPRIS bus)
jam-gui --headless ABC123 --mpris spotify      # any MPRIS player by bus suffix
jam-gui --headless ABC123 --backend cliamp
jam-gui --headless --host --gc --name wunder   # host a Jam
jam-gui --headless ABC123 --dry-run            # log without touching playback
```

## How it's built

| module | job |
|---|---|
| `src/binarypack.rs` | the msgpack-derived codec peerjs uses on data channels |
| `src/peerjs.rs` | signaling client (`wss://0.peerjs.com`), WebRTC glue (libdatachannel), chunking |
| `src/jam.rs` | the jam protocol state machine — guest sync, host broadcast, drift correction, lock |
| `src/player.rs` | player backends: spotifast via MPRIS (zbus), cliamp via its v2 IPC |
| `src/app.rs` | the egui front-end |

Sync logic mirrors the original extension: ping/2 latency compensation,
EMA-smoothed drift correction (seek only past ~650ms), lock-back when the local
track changes, 3 reconnect attempts with backoff.

### Differences vs the reference Node bridges (`jam-bridge`, `jam-cliamp`)

- No Node runtime — a single static-ish binary
- Host mode supports **one guest** in v1 (fine for two people; the protocol itself is star-shaped)
- PeerJS TURN relays are not configured in v1 (STUN only) — same NAT behavior
  as before on typical home internet; see `docs/PROTOCOL.md` if you want to add TURN
- The author's telemetry endpoints are not implemented (nor are they in the Node bridges)

## Testing without her

The Node bridges ship simulators that impersonate each side; use them against
the headless mode:

```sh
node ~/jam-bridge/sim/host-sim.mjs TEST12 &
jam-gui --headless TEST12 --dry-run
```

## Credits & license

- The Jam protocol, sync heuristics and session design come from
  **spicetify-jam by Kyzenkms** (v1.4.1, repo currently unavailable) — this
  project is a clean-room reimplementation of that wire protocol; no original
  code is included. Credit for the design belongs to them.
- [Spotifast](https://github.com/crmne/spotifast) by crmne (MIT), [cliamp](https://github.com/lennytkuchen/cliamp).
- MIT licensed.
