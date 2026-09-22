# jam-bridge

**Listen together, in spotifast.** Join or host a [Spicetify Jam](https://github.com/Kyzenkms/spicetify-jam)
session (the P2P listen-together extension for Spotify desktop) — but play your side in
[Spotifast](https://github.com/crmne/spotifast), the lightweight native Rust Spotify client.

Your partner keeps using normal Spotify + Spicetify Jam on their machine. You run this bridge
instead of the heavy official client. The bridge speaks the exact same PeerJS/WebRTC protocol
as the extension and mirrors playback into spotifast over **MPRIS**.

```
   her machine                         your machine
┌─────────────────────┐          ┌──────────────────────────┐
│ Spotify + spicetify │◀── P2P ──▶ jam-bridge ── MPRIS ──▶ spotifast
│   (spicetify-jam)   │ WebRTC   (this repo, Node)         (librespot)
└─────────────────────┘          └──────────────────────────┘
        either side can host the session
```

No audio is transmitted — only play/pause/seek/song messages (a few bytes). Each side
streams from their own Spotify account. Playback needs Premium on the spotifast side
(that's a spotifast/librespot constraint, not ours).

## Install

```sh
cd jam-bridge
npm install        # peerjs + node-datachannel + dbus-next
```

Requires: Node 18+ (tested on 24), a running spotifast, Linux (MPRIS).

## Use

**Follow her Jam** (she clicks "Start a new Jam" in her Spotify and gives you the code):

```sh
node jam-bridge.mjs ABC123 --name wunder
```

**Host from spotifast** (she joins with the code from her extension as usual):

```sh
node jam-bridge.mjs --host --gc --name wunder
# → prints a 6-char code; share it
```

While hosting with `--gc`, her play/pause/next/seek buttons and "Add to Jam" context
menu work: added songs go into the bridge's queue and play in order.

Flags:

| flag | meaning |
|---|---|
| `--name NAME` | name shown to other listeners (default `$USER`) |
| `--host` | host a session instead of joining |
| `--code CODE` | pick your own code when hosting |
| `--gc` | (host) allow guest playback controls |
| `--player BUS` | MPRIS bus suffix, default `fastpotify` |
| `--dry-run` | log what would happen without touching playback |

Ctrl+C leaves the Jam cleanly.

### Systemd user service (optional)

`~/.config/systemd/user/jam-bridge.service`:

```ini
[Unit]
Description=Spicetify Jam bridge for spotifast

[Service]
WorkingDirectory=%h/jam-bridge
ExecStart=/usr/bin/node %h/jam-bridge/jam-bridge.mjs ABC123 --name wunder
Restart=on-failure

[Install]
WantedBy=default.target
```

## How it works

- **Signaling**: PeerJS public cloud server (`0.peerjs.com`) — same as the extension
- **Connection**: WebRTC data channel via libdatachannel; STUN (Google) + TURN (PeerJS public relays)
- **Player control**: MPRIS on `org.mpris.MediaPlayer2.fastpotify` (`OpenUri`, `Play`, `Pause`, `SetPosition`)
- **Sync logic**: mirrors the extension — latency compensation (ping/2), drift-smoothed correction
  that only seeks when you're >650ms off (or >160ms twice), lock-back when your local track changes

The full message protocol is documented in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Testing without her

Two simulators are included (`sim/`) that impersonate each side of a real session:

```sh
node sim/host-sim.mjs TEST12        # fake host: song changes + pause toggles
node jam-bridge.mjs TEST12 --dry-run
```

## Troubleshooting

- **"could not reach MPRIS player"** — spotifast isn't running, or built with a different
  bus name; check with `dbus-send --session --print-reply --dest=org.freedesktop.DBus
  /org/freedesktop/DBus org.freedesktop.DBus.ListNames | grep mpris`
- **"Jam not found"** — wrong code, or her Jam ended; also possible if your network blocks
  WebRTC (try a hotspot/VPN, same as the extension advises)
- **~1s constant offset** — normal right after joining; the drift corrector eases you in

## Credits & license

- The Jam protocol, sync heuristics and ICE configuration are from
  **spicetify-jam by Kyzenkms** (v1.4.1, repo currently unavailable). Credit for the
  design belongs to them.
- Spotifast by [crmne](https://github.com/crmne/spotifast) (MIT).
- This bridge: MIT.
