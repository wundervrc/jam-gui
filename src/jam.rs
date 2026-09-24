//! JamCore — the spicetify-jam protocol state machine (guest + host),
//! driving a PlayerBackend. Ported 1:1 from the reference Node bridges.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::peerjs::{random_uuid, JamCodec, Peer2Peer, Signaling, CoreEvent};
use crate::player::{Backend, PlayerBackend, PlayerState};

// ---------------------------------------------------------------------------
// shared state with the UI
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Track {
    pub uri: String,
    pub title: String,
    pub artist: String,
    pub art_url: String,
}

impl Track {
    fn from_np(np: &Value) -> Option<Self> {
        np.get("uri").and_then(|u| u.as_str()).map(|uri| Self {
            uri: uri.into(),
            title: np.get("title").and_then(|t| t.as_str()).unwrap_or("").into(),
            artist: np.get("artist").and_then(|a| a.as_str()).unwrap_or("").into(),
            art_url: np.get("artUrl").and_then(|a| a.as_str()).unwrap_or("").into(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Member {
    pub id: String,
    pub name: String,
    pub is_host: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Idle,
    Joining,
    Guest,
    Hosting,
}

#[derive(Debug, Clone)]
pub struct SharedState {
    pub mode: Mode,
    pub jam_id: String,
    pub host_name: String,
    pub display_name: String,
    pub gc: bool,
    pub ping_ms: i64,
    pub connected: bool,
    pub members: Vec<Member>,
    pub now_playing: Option<Track>,
    pub playing: bool,
    pub progress_ms: f64,
    pub duration_ms: f64,
    pub queue: Vec<Track>,
    pub error: Option<String>,
    pub backend: &'static str,
    pub logs: VecDeque<String>,
    pub drift_enabled: bool,
    pub drift_deadband_ms: f64,
    pub drift_jump_ms: f64,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            mode: Mode::Idle,
            jam_id: String::new(),
            host_name: String::new(),
            display_name: String::new(),
            gc: false,
            ping_ms: -1,
            connected: false,
            members: vec![],
            now_playing: None,
            playing: false,
            progress_ms: 0.0,
            duration_ms: 0.0,
            queue: vec![],
            error: None,
            backend: "",
            logs: VecDeque::new(),
            drift_enabled: true,
            drift_deadband_ms: 160.0,
            drift_jump_ms: 650.0,
        }
    }
}

pub type Shared = Arc<Mutex<SharedState>>;

#[derive(Debug, Clone)]
pub enum UiCmd {
    Join { code: String, name: String },
    Host { name: String, gc: bool, code: Option<String> },
    SetBackend(Backend),
    Leave,
    Play,
    Pause,
    Next,
    SyncNow,
    /// guest mode: send ADD_Q to the host (paste-a-link)
    AddToQueue { uri: String },
    /// fine-tune drift correction on the fly (guest side)
    SetDrift { enabled: bool, deadband_ms: f64, jump_ms: f64 },
    /// host side: flip "let the guest control playback" live (broadcasts GCTRL)
    SetGc(bool),
}

fn make_player(backend: &Backend) -> Result<Box<dyn PlayerBackend>, String> {
    match backend {
        Backend::Spotifast { bus_suffix } => crate::player::MprisPlayer::new(bus_suffix)
            .map(|p| Box::new(p) as Box<dyn PlayerBackend>)
            .map_err(|e| e.to_string()),
        Backend::SpotifastWin => Ok(Box::new(crate::player::SpotifastWinPlayer::new())),
        Backend::Cliamp => Ok(Box::new(crate::player::CliampPlayer::new())),
    }
}

// ---------------------------------------------------------------------------
// core
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct Target {
    uri: String,
    title: String,
    artist: String,
    pos_ms: f64,
    at: Instant,
    playing: bool,
}

struct PendingSeek {
    uri: String,
    title: String,
    target_ms: f64,
    paused: bool,
    deadline: Instant,
    opened: bool,
}

/// Interpolates playback position between player reads — spotifast's MPRIS
/// `Position` property freezes at song start, so clients must predict
/// `last + elapsed` while playing and re-anchor when the reporter moves.
#[derive(Default)]
struct PosTracker {
    anchor_pos: f64,
    anchor_at: Option<Instant>,
    last_reported: f64,
    frozen: bool,
    /// during this window, far-off reported positions are treated as stale
    /// (some players report the previous track's position at song change)
    grace_until: Option<Instant>,
}

impl PosTracker {
    /// Force the anchor to a known position (after our own seek, or song start).
    fn anchor(&mut self, pos_ms: f64) {
        self.anchor_pos = pos_ms;
        self.anchor_at = Some(Instant::now());
        self.last_reported = pos_ms;
        self.frozen = false;
    }

    fn feed(&mut self, reported_ms: f64, playing: bool) -> f64 {
        let Some(at) = self.anchor_at else {
            self.anchor(reported_ms);
            return reported_ms;
        };
        if !playing {
            self.anchor_pos = reported_ms;
            self.anchor_at = Some(Instant::now());
            self.frozen = false;
            self.last_reported = reported_ms;
            return reported_ms;
        }
        if self.frozen {
            // reporter stalled — keep predicting, unless it starts moving again
            if (reported_ms - self.last_reported).abs() > 150.0 {
                self.frozen = false;
                self.anchor_pos = reported_ms;
                self.anchor_at = Some(Instant::now());
            }
        } else {
            let predicted = self.anchor_pos + at.elapsed().as_millis() as f64;
            let off = (reported_ms - predicted).abs();
            if off > 700.0 {
                if self.grace_until.map(|g| Instant::now() < g).unwrap_or(false) {
                    // stale report right after a song change — ignore
                } else {
                    // real seek or stall correction
                    self.anchor_pos = reported_ms;
                    self.anchor_at = Some(Instant::now());
                }
            } else if off < 50.0 {
                self.frozen = true;
            } else {
                // healthy advancing reporter — follow the truth
                self.anchor_pos = reported_ms;
                self.anchor_at = Some(Instant::now());
            }
        }
        self.last_reported = reported_ms;
        self.anchor_pos + self.anchor_at.unwrap().elapsed().as_millis() as f64
    }

    fn reset(&mut self) {
        self.anchor_at = None;
        self.frozen = false;
        self.grace_until = None;
    }
}

pub struct JamCore {
    pub shared: Shared,
    cmd_rx: Receiver<UiCmd>,
    ev_rx: Receiver<CoreEvent>,
    host_dc_rx: Receiver<Box<datachannel::RtcDataChannel<crate::peerjs::DcHandler>>>,
    host_dc_tx: Sender<Box<datachannel::RtcDataChannel<crate::peerjs::DcHandler>>>,
    ev_tx: Sender<CoreEvent>,
    signaling: Option<Signaling>,
    p2p: Option<Peer2Peer>,
    codec: JamCodec,
    player: Box<dyn PlayerBackend>,
    dry_run: bool,
    running: Arc<AtomicBool>,

    is_host: bool,
    jam_id: String,
    /// guest mode: code we're joining (kept for reconnects)
    target_code: Option<String>,
    /// host mode: peer id of the connected guest
    remote_id: String,
    guest_name: String,
    host_name: String,
    gc: bool,
    members: Vec<Member>,
    queue: Vec<Track>,
    target: Option<Target>,
    ping_ms: i64,
    drift_ema: f64,
    drift_count: u32,
    /// guest-side sync tuning (fine-tune on the fly):
    /// corrections fire when smoothed drift exceeds drift_jump_ms,
    /// ignoring everything under drift_deadband_ms
    drift_deadband_ms: f64,
    drift_jump_ms: f64,
    drift_enabled: bool,
    last_applied: Option<Instant>,
    last_sync_req: Option<Instant>,
    reconnects: u32,
    reconnect_at: Option<Instant>,
    joining_since: Option<Instant>,
    pending_seek: Option<PendingSeek>,
    pos: PosTracker,
    /// guest side: last local uri we asked the host to play (guest control)
    playuri_sent: Option<String>,
    playuri_sent_at: Option<Instant>,
    /// guest side: track playing locally when the session started — that song
    /// is not a "pick", so guest control must not push it to the host
    join_baseline: Option<String>,
    /// guest side: while set, guest-control playuri pushes are suppressed
    /// (post-join / post-player-switch settle window)
    settle_until: Option<Instant>,
    /// guest side: local track (key, pos, dur) at the previous lock tick —
    /// detecting a change + near-end of the old one = natural advance
    last_local: Option<(String, f64, f64)>,
    /// Some(true) = backend has its own queue (add_to_queue worked)
    own_queue: Option<bool>,
    // host-side watcher
    host_last_uri: Option<String>,
    host_last_playing: Option<bool>,
    t_host_pause: Instant,
    http: ureq::Agent,
    title_cache: std::collections::HashMap<String, (String, String)>,
    // timers
    t_ping: Instant,
    t_sync: Instant,
    t_lock: Instant,
    t_host_watch: Instant,
    t_tick: Instant,
}

impl JamCore {
    pub fn spawn(
        backend: Backend,
        dry_run: bool,
        running: Arc<AtomicBool>,
    ) -> (Shared, Sender<UiCmd>) {
        let shared: Shared = Arc::new(Mutex::new(SharedState::default()));
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<UiCmd>();
        let (ev_tx, ev_rx) = std::sync::mpsc::channel::<CoreEvent>();
        let (host_dc_tx, host_dc_rx) = std::sync::mpsc::channel();

        let shared2 = shared.clone();
        let s = shared.clone();
        std::thread::Builder::new()
            .name("jam-core".into())
            .spawn(move || {
                let player: Box<dyn PlayerBackend> = match make_player(&backend) {
                    Ok(p) => p,
                    Err(e) => {
                        s.lock().unwrap().error = Some(e);
                        return;
                    }
                };
                let mut core = JamCore {
                    shared: shared2,
                    cmd_rx,
                    ev_rx,
                    host_dc_rx,
                    host_dc_tx: host_dc_tx,
                    ev_tx,
                    signaling: None,
                    p2p: None,
                    codec: JamCodec::default(),
                    player,
                    dry_run,
                    running,
                    is_host: false,
                    jam_id: String::new(),
                    target_code: None,
                    remote_id: String::new(),
                    guest_name: String::new(),
                    host_name: String::new(),
                    gc: false,
                    members: vec![],
                    queue: vec![],
                    target: None,
                    ping_ms: -1,
                    drift_ema: 0.0,
                    drift_count: 0,
                    drift_deadband_ms: 160.0,
                    drift_jump_ms: 650.0,
                    drift_enabled: true,
                    last_applied: None,
                    last_sync_req: None,
                    reconnects: 0,
                    reconnect_at: None,
                    joining_since: None,
                    pending_seek: None,
                    pos: PosTracker::default(),
                    playuri_sent: None,
                    join_baseline: None,
                    last_local: None,
                    settle_until: None,
                    playuri_sent_at: None,
                    own_queue: None,
                    host_last_uri: None,
                    host_last_playing: None,
                    t_host_pause: Instant::now(),
                    http: ureq::AgentBuilder::new()
                        .timeout(std::time::Duration::from_secs(4))
                        .build(),
                    title_cache: std::collections::HashMap::new(),
                    t_ping: Instant::now(),
                    t_sync: Instant::now(),
                    t_lock: Instant::now(),
                    t_host_watch: Instant::now(),
                    t_tick: Instant::now(),
                };
                core.run();
            })
            .expect("spawn core");

        (shared, cmd_tx)
    }

    fn log(&self, msg: impl Into<String>) {
        let line = format!("{} {}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| {
                    let secs = d.as_secs() % 86400;
                    format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
                })
                .unwrap_or_default(),
            msg.into());
        // JAM_DEBUG: mirror everything to stderr so GUI users can capture it;
        // headless mode prints every line regardless.
        if std::env::var("JAM_DEBUG").is_ok() {
            eprintln!("{line}");
        }
        if let Ok(mut s) = self.shared.lock() {
            s.logs.push_back(line);
            while s.logs.len() > 400 {
                s.logs.pop_front();
            }
        }
    }

    fn sync_shared<F: FnOnce(&mut SharedState)>(&self, f: F) {
        if let Ok(mut s) = self.shared.lock() {
            f(&mut s);
        }
    }

    fn run(&mut self) {
        // initial player probe for the UI
        if let Some(st) = self.player.state() {
            if std::env::var("JAM_DEBUG").is_ok() {
                self.log(format!("player probe: {} — {} / {}ms playing={} uri={}",
                    st.title, st.position_ms, st.duration_ms, st.playing, st.uri));
            }
            self.sync_shared(|s| {
                s.now_playing = Some(Track {
                    uri: st.uri.clone(),
                    title: st.title.clone(),
                    artist: st.artist.clone(),
                    art_url: st.art_url.clone(),
                });
                s.playing = st.playing;
                s.progress_ms = st.position_ms;
                s.duration_ms = st.duration_ms;
                s.backend = self.player.name();
            });
        }

        while self.running.load(Ordering::Relaxed) {
            // ---- inbound events ----
            match self.ev_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(ev) => self.handle_event(ev),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
            // ---- UI commands ----
            while let Ok(cmd) = self.cmd_rx.try_recv() {
                self.handle_cmd(cmd);
            }
            // ---- periodic work ----
            self.tick();
        }
    }

    // ------------------------------------------------------------------
    fn handle_cmd(&mut self, cmd: UiCmd) {
        match cmd {
            UiCmd::Join { code, name } => self.start_session(false, code, name),
            UiCmd::Host { name, gc, code } => {
                self.gc = gc;
                self.start_session(true, code.unwrap_or_default(), name)
            }
            UiCmd::SetBackend(b) => {
                match make_player(&b) {
                    Ok(p) => {
                        self.player = p;
                        self.pos.reset();
                        self.host_last_uri = None;
                        self.host_last_playing = None;
                        self.pending_seek = None;
                        self.own_queue = None;
                        let nm = self.player.name();
                        self.sync_shared(|s| s.backend = nm);
                        self.log(format!("switched player → {nm}"));
                        // adopt the new player's current song + time
                        self.adopt_player_state();
                        // guest mid-session: the freshly adopted song is not a
                        // pick — same rule as joining. Never push it to the
                        // host; sync to the host's track instead, and give the
                        // new backend a moment before guest-control pushes.
                        if !self.is_host && self.target.is_some() {
                            self.join_baseline = self.player.state().map(|s| {
                                if s.uri.is_empty() { format!("\u{1}{}", s.title) } else { s.uri.clone() }
                            });
                            self.last_local = None;
                            self.settle_until = Some(Instant::now() + Duration::from_secs(5));
                            self.log("syncing the new player to the host's song…");
                        }
                        if self.is_host {
                            // tell guests what the new player is doing
                            if let Some(st) = self.player.state() {
                                if !st.uri.is_empty() {
                                    let pos = self.pos.feed(st.position_ms, st.playing);
                                    self.send_guest(json!({
                                        "type": "PLAY", "uri": st.uri, "pos": pos, "ts": now_ms(),
                                        "np": host_np(&st), "paused": !st.playing, "dur": st.duration_ms,
                                    }));
                                }
                            }
                        }
                        // guest mode: enforce_lock pulls the new player to the
                        // host's target within 2s, nothing else needed
                    }
                    Err(e) => self.log(format!("player switch failed: {e}")),
                }
            }
            UiCmd::SetDrift { enabled, deadband_ms, jump_ms } => {
                self.drift_enabled = enabled;
                self.drift_deadband_ms = deadband_ms.max(0.0);
                self.drift_jump_ms = jump_ms.max(self.drift_deadband_ms);
                self.drift_ema = 0.0;
                self.drift_count = 0;
                self.sync_shared(|s| {
                    s.drift_enabled = self.drift_enabled;
                    s.drift_deadband_ms = self.drift_deadband_ms;
                    s.drift_jump_ms = self.drift_jump_ms;
                });
                self.log(format!(
                    "drift correction: {} (deadband {}ms, jump {}ms)",
                    if enabled { "on" } else { "manual only" },
                    deadband_ms as u64,
                    jump_ms as u64
                ));
            }
            UiCmd::SetGc(v) => {
                if self.is_host {
                    self.gc = v;
                    self.broadcast(json!({"type": "GCTRL", "on": v}));
                    self.sync_shared(|s| s.gc = v);
                    self.log(format!("guest controls {} — broadcast to guests", if v { "enabled" } else { "disabled" }));
                }
            }
            UiCmd::Leave => self.leave(),
            UiCmd::Play => {
                if self.is_host {
                    self.player.play();
                } else if self.gc {
                    self.send(json!({"type": "CMD", "a": "play"}));
                }
            }
            UiCmd::Pause => {
                if self.is_host {
                    self.player.pause();
                } else if self.gc {
                    self.send(json!({"type": "CMD", "a": "pause"}));
                }
            }
            UiCmd::Next => {
                if self.is_host {
                    self.host_next();
                } else if self.gc {
                    self.send(json!({"type": "CMD", "a": "next"}));
                }
            }
            UiCmd::SyncNow => {
                if !self.is_host {
                    self.last_sync_req = Some(Instant::now());
                    self.send(json!({"type": "SYNC"}));
                    self.log("requesting sync…");
                }
            }
            UiCmd::AddToQueue { uri } => {
                if !self.is_host {
                    self.send(json!({
                        "type": "ADD_Q", "uri": uri,
                        "addedBy": { "name": self.display_name(), "image": "" }
                    }));
                    self.log("sent song to the host's queue ♪");
                }
            }
        }
    }

    /// Refresh the UI's now-playing/progress from the current player.
    /// In host mode, also adopts the player's track as the watched baseline.
    fn adopt_player_state(&mut self) {
        // guest mid-session: the display is driven by the host — adopting the
        // local player's song here would stomp it (e.g. on a backend swap)
        let guest_active = !self.is_host && self.target.is_some();
        if let Some(st) = self.player.state() {
            self.pos.anchor(st.position_ms);
            if self.is_host {
                self.host_last_uri = Some(st.uri.clone());
                self.host_last_playing = Some(st.playing);
            }
            // uri-less backends (spotifast's CLI) still show the name/artist
            let np = if st.uri.is_empty() && st.title.is_empty() {
                None
            } else {
                Some(Track {
                    uri: st.uri.clone(),
                    title: st.title.clone(),
                    artist: st.artist.clone(),
                    art_url: st.art_url.clone(),
                })
            };
            self.sync_shared(|s| {
                if !guest_active {
                    s.now_playing = np;
                    s.playing = st.playing;
                    s.progress_ms = st.position_ms;
                    s.duration_ms = st.duration_ms;
                }
            });
        }
    }

    fn start_session(&mut self, host: bool, code: String, name: String) {
        self.leave_quiet();
        self.is_host = host;
        self.reconnects = 0;
        self.members.clear();
        self.queue.clear();
        self.target = None;
        self.pending_seek = None;
        self.host_name.clear();
        self.guest_name.clear();
        self.ping_ms = -1;
        // guests: remember what was playing locally before the jam — asking
        // the host to play THAT would hijack the session on join
        self.join_baseline = (!host).then(|| {
            self.player.state().map(|s| {
                if s.uri.is_empty() { format!("\u{1}{}", s.title) } else { s.uri.clone() }
            })
        }).flatten();
        self.last_local = None;
        self.settle_until = if host { None } else { Some(Instant::now() + Duration::from_secs(5)) };

        let my_id = if host {
            let id = code
                .chars()
                .take(8)
                .filter(|c| c.is_ascii_alphanumeric())
                .collect::<String>();
            if id.is_empty() {
                crate::peerjs::random_token_upper(6)
            } else {
                id
            }
        } else {
            random_uuid()
        };
        self.jam_id = my_id.clone();

        self.sync_shared(|s| {
            s.mode = Mode::Joining;
            s.error = None;
            s.jam_id = if host { my_id.clone() } else { code.clone() };
            s.jam_id = if host { my_id.clone() } else { code.clone() };
            s.display_name = name.clone();
            s.gc = self.gc;
            s.backend = self.player.name();
            s.members.clear();
        });

        match Signaling::connect(&my_id, self.ev_tx.clone()) {
            Ok(sig) => {
                self.signaling = Some(sig);
                self.log(if host {
                    format!("starting Jam… (code {my_id})")
                } else {
                    format!("connecting to Jam {code}…")
                });
            }
            Err(e) => {
                self.sync_shared(|s| {
                    s.error = Some(format!("signaling failed: {e}"));
                    s.mode = Mode::Idle;
                });
            }
        }
        // guest: remember whose jam we're joining for reconnects
        if !host {
            self.target_code = Some(code);
            self.joining_since = Some(Instant::now());
        } else {
            self.target_code = None;
            self.joining_since = None;
        }
        // adopt whatever the player is doing right now (song + time)
        self.host_last_uri = None;
        self.host_last_playing = None;
        self.pos.reset();
        self.adopt_player_state();
    }

    fn leave_quiet(&mut self) {
        if let Some(dc) = self.p2p.as_mut().and_then(|p| p.dc.as_mut()) {
            let _ = dc.send(&crate::binarypack::pack(&crate::binarypack::Packed::Map(vec![(
                "__peerData".into(),
                crate::binarypack::Packed::Map(vec![("type".into(), crate::binarypack::Packed::Str("close".into()))]),
            )])));
        }
        self.p2p = None;
        if let Some(sig) = self.signaling.take() {
            sig.close();
        }
    }

    fn leave(&mut self) {
        self.leave_quiet();
        self.sync_shared(|s| {
            s.mode = Mode::Idle;
            s.connected = false;
            s.members.clear();
            s.queue.clear();
            s.now_playing = None;
            s.ping_ms = -1;
        });
        self.log("left the Jam");
    }

    // ------------------------------------------------------------------
    fn handle_event(&mut self, ev: CoreEvent) {
        match ev {
            CoreEvent::Log(m) => self.log(m),
            CoreEvent::PeerOpen(_id) => {
                self.log(format!("peer open as {}", self.jam_id));
                if self.is_host {
                    self.sync_shared(|s| {
                        s.mode = Mode::Hosting;
                        s.connected = true;
                        s.jam_id = self.jam_id.clone();
                        s.members = vec![Member {
                            id: "host".into(),
                            name: s.display_name.clone(),
                            is_host: true,
                        }];
                    });
                    self.log(format!("Jam code: {} — share it", self.jam_id));
                } else if let Some(code) = self.target_code.clone() {
                    self.connect_to_host(&code);
                }
            }
            CoreEvent::SignalClosed => {
                let mode = self.shared.lock().map(|s| s.mode).unwrap_or(Mode::Idle);
                if mode != Mode::Idle {
                    // signaling dropped but the session lives — reconnect the
                    // socket (existing data channels survive independently)
                    self.log("signaling server lost — reconnecting…");
                    self.signaling = None;
                    self.reconnect_at = Some(Instant::now() + Duration::from_secs(2));
                }
            }
            CoreEvent::SignalMessage(v) => self.handle_signal(v),
            CoreEvent::LocalDescription(sdp) => {
                // send OFFER/ANSWER over signaling
                let (kind, dst) = if self.is_host {
                    ("ANSWER", self.remote_id.clone())
                } else {
                    ("OFFER", self.target_code.clone().unwrap_or_default())
                };
                if std::env::var("JAM_DEBUG").is_ok() {
                    self.log(format!("local desc {kind} → {dst}: {}", sdp.to_string().chars().take(160).collect::<String>()));
                }
                let payload = if kind == "OFFER" {
                    json!({
                        "sdp": sdp,
                        "type": "data",
                        "connectionId": self.p2p.as_ref().map(|p| p.connection_id.clone()).unwrap_or_default(),
                        "metadata": {},
                        "label": self.p2p.as_ref().map(|p| p.connection_id.clone()).unwrap_or_default(),
                        "reliable": true,
                        "serialization": "binary",
                    })
                } else {
                    json!({
                        "sdp": sdp,
                        "type": "data",
                        "connectionId": self.p2p.as_ref().map(|p| p.connection_id.clone()).unwrap_or_default(),
                    })
                };
                if let Some(sig) = &self.signaling {
                    sig.send_json(&json!({"type": kind, "payload": payload, "dst": dst}));
                    self.log(format!("sent {kind}"));
                }
            }
            CoreEvent::LocalCandidate(cand, mid) => {
                let dst = if self.is_host {
                    self.remote_id.clone()
                } else {
                    self.target_code.clone().unwrap_or_default()
                };
                if let Some(sig) = &self.signaling {
                    sig.send_json(&json!({
                        "type": "CANDIDATE",
                        "payload": {
                            "candidate": { "candidate": cand, "sdpMid": mid, "sdpMLineIndex": 0 },
                            "type": "data",
                            "connectionId": self.p2p.as_ref().map(|p| p.connection_id.clone()).unwrap_or_default(),
                        },
                        "dst": dst,
                    }));
                }
            }
            CoreEvent::DcOpen => {
                self.reconnects = 0;
                if self.is_host {
                    self.log("guest connected");
                    self.sync_shared(|s| s.connected = true);
                } else {
                    self.log("connected! joining…");
                    self.send(json!({"type": "JOIN", "name": self.display_name(), "image": ""}));
                    self.sync_shared(|s| s.mode = Mode::Guest);
                }
            }
            CoreEvent::DcHostChannel => {
                while let Ok(dc) = self.host_dc_rx.try_recv() {
                    if let Some(p) = self.p2p.as_mut() {
                        p.dc = Some(dc);
                    }
                }
            }
            CoreEvent::DcRaw(bytes) => {
                if let Some(msg) = self.codec.decode(&bytes) {
                    self.handle_jam(msg);
                }
            }
            CoreEvent::DcClosed => {
                if !self.is_host {
                    self.p2p = None;
                    self.reconnects += 1;
                    if self.reconnects > 3 {
                        self.sync_shared(|s| {
                            s.error = Some("lost the host".into());
                            s.mode = Mode::Idle;
                            s.connected = false;
                        });
                        self.log("connection lost — retries exhausted");
                        self.leave_quiet();
                    } else {
                        self.log(format!("connection lost, reconnecting ({}/3)…", self.reconnects));
                        if let Some(sig) = self.signaling.take() {
                            sig.close();
                        }
                        self.reconnect_at =
                            Some(Instant::now() + Duration::from_millis(1500 * self.reconnects as u64));
                    }
                } else {
                    self.log("guest left");
                    self.guest_name.clear();
                    self.p2p = None;
                    self.remote_id.clear();
                    self.sync_shared(|s| {
                        s.connected = false;
                        s.members.retain(|m| m.is_host);
                    });
                }
            }
            CoreEvent::PeerError(e) => {
                self.log(format!("peer error: {e}"));
                let mode = self.shared.lock().map(|s| s.mode).unwrap_or(Mode::Idle);
                if mode == Mode::Joining {
                    self.sync_shared(|s| {
                        s.error = Some(e.clone());
                        s.mode = Mode::Idle;
                        s.connected = false;
                    });
                    // a failed join leaves the old signaling socket dangling
                    self.leave_quiet();
                }
            }
            CoreEvent::DcMessage(_) => unreachable!("decoding happens in core"),
        }
    }

    fn connect_to_host(&mut self, code: &str) {
        let p2p = match Peer2Peer::new(self.ev_tx.clone(), self.host_dc_tx.clone()) {
            Ok(p) => p,
            Err(e) => {
                self.log(format!("webrtc init failed: {e}"));
                return;
            }
        };
        self.p2p = Some(p2p);
        if let Err(e) = self.p2p.as_mut().unwrap().start_offer() {
            self.log(format!("offer failed: {e}"));
        }
    }

    fn display_name(&self) -> String {
        self.shared
            .lock()
            .map(|s| s.display_name.clone())
            .unwrap_or_default()
    }

    fn send(&mut self, msg: Value) {
        if std::env::var("JAM_DEBUG").is_ok() {
            let t = msg.get("type").and_then(|x| x.as_str()).unwrap_or("?");
            let brief = match t {
                "PLAY" | "SYNC_TICK" | "PS" | "SEEK" => format!(
                    "{} uri={} pos={:?} ts={:?}",
                    t,
                    msg.get("uri").and_then(|x| x.as_str()).unwrap_or(""),
                    msg.get("pos").and_then(|x| x.as_f64()),
                    msg.get("ts").and_then(|x| x.as_i64()),
                ),
                "PONG" => format!("PONG ts={:?}", msg.get("ts").and_then(|x| x.as_i64())),
                _ => t.to_string(),
            };
            self.log(format!("→ {brief}"));
        }
        let frames = self.codec.encode(&msg);
        if let Some(p) = self.p2p.as_mut() {
            p.send_frames(&frames);
        }
    }

    fn send_guest(&mut self, msg: Value) {
        // host → guest
        self.send(msg)
    }

    fn broadcast(&mut self, msg: Value) {
        self.send(msg);
    }

    // ------------------------------------------------------------------
    fn handle_signal(&mut self, v: Value) {
        let mtype = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match mtype {
            "OPEN" => {
                let _ = self.ev_tx.send(CoreEvent::PeerOpen(
                    v.get("payload").and_then(|p| p.get("peer")).and_then(|p| p.as_str()).unwrap_or("").into(),
                ));
            }
            "ID-TAKEN" => {
                if self.is_host {
                    self.log("jam code taken — regenerating");
                    let name = self.display_name();
                    let gc = self.gc;
                    self.handle_cmd(UiCmd::Host { name, gc, code: None });
                }
            }
            "EXPIRE" => self.log("offer expired"),
            "ERROR" => {
                let ptype = v.get("payload").and_then(|p| p.get("type")).and_then(|m| m.as_str()).unwrap_or("");
                let msg = v.get("payload").and_then(|p| p.get("msg")).and_then(|m| m.as_str()).unwrap_or("");
                let combined = if ptype.is_empty() { msg.to_string() } else { format!("{ptype}: {msg}") };
                let _ = self.ev_tx.send(CoreEvent::PeerError(combined));
            }
            "OFFER" => {
                // host mode: a guest wants in
                if self.is_host {
                    let src = v.get("src").and_then(|s| s.as_str()).unwrap_or("").to_string();
                    if std::env::var("JAM_DEBUG").is_ok() {
                        let sdp = v.get("payload").and_then(|p| p.get("sdp")).map(|s| s.to_string()).unwrap_or_default();
                        self.log(format!("OFFER from {src}: {}", sdp.chars().take(160).collect::<String>()));
                    }
                    if self.p2p.is_some() {
                        self.log(format!("another listener tried to join ({src}) — one guest supported in v1"));
                        return;
                    }
                    self.remote_id = src;
                    let sdp = v.get("payload").and_then(|p| p.get("sdp")).cloned().unwrap_or(Value::Null);
                    let conn_id = v
                        .get("payload")
                        .and_then(|p| p.get("connectionId"))
                        .and_then(|c| c.as_str())
                        .unwrap_or("dc_guest")
                        .to_string();
                    match Peer2Peer::new(self.ev_tx.clone(), self.host_dc_tx.clone()) {
                        Ok(mut p) => {
                            p.connection_id = conn_id;
                            match p.answer(sdp) {
                                Ok(()) => {
                                    self.p2p = Some(p);
                                    self.log("guest is connecting…");
                                }
                                Err(e) => self.log(format!("answer failed: {e}")),
                            }
                        }
                        Err(e) => self.log(format!("webrtc init failed: {e}")),
                    }
                }
            }
            "ANSWER" => {
                if !self.is_host {
                    let sdp = v.get("payload").and_then(|p| p.get("sdp")).cloned().unwrap_or(Value::Null);
                    if let Some(p) = self.p2p.as_mut() {
                        if let Err(e) = p.accept_answer(sdp) {
                            self.log(format!("accept answer failed: {e}"));
                        }
                    }
                }
            }
            "CANDIDATE" => {
                let payload = v.get("payload").unwrap_or(&Value::Null);
                let cand = payload
                    .get("candidate")
                    .and_then(|c| c.get("candidate"))
                    .and_then(|c| c.as_str())
                    .or_else(|| payload.get("candidate").and_then(|c| c.as_str()));
                let mid = payload
                    .get("candidate")
                    .and_then(|c| c.get("sdpMid"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("0");
                if let (Some(c), Some(p)) = (cand, self.p2p.as_mut()) {
                    p.add_candidate(c, mid);
                }
            }
            "LEAVE" => {
                let src = v.get("src").and_then(|s| s.as_str()).unwrap_or("");
                if self.is_host && self.remote_id == src {
                    self.log("guest left");
                    self.p2p = None;
                    self.guest_name.clear();
                    self.sync_shared(|s| {
                        s.connected = false;
                        s.members.retain(|m| m.is_host);
                    });
                }
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    fn handle_jam(&mut self, n: Value) {
        if std::env::var("JAM_DEBUG").is_ok() {
            let t = n.get("type").and_then(|x| x.as_str()).unwrap_or("?");
            let extra = match t {
                "PING" => format!(" ts={:?}", n.get("ts").and_then(|x| x.as_i64())),
                "SYNC_TICK" => format!(" pos={:?} ts={:?}", n.get("pos").and_then(|x| x.as_f64()), n.get("ts").and_then(|x| x.as_i64())),
                "PLAY" => format!(" uri={:?} pos={:?}", n.get("uri").and_then(|x| x.as_str()), n.get("pos").and_then(|x| x.as_f64())),
                _ => String::new(),
            };
            self.log(format!("← {t}{extra}"));
        }
        let mtype = n.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match mtype {
            "INIT" => {
                self.host_name = n.get("host").and_then(|h| h.as_str()).unwrap_or("Host").into();
                self.gc = n.get("gc").and_then(|g| g.as_bool()).unwrap_or(false);
                if let Some(q) = n.get("queue").and_then(|q| q.as_array()) {
                    self.queue = q.iter().filter_map(track_from_json).collect();
                }
                if let Some(np) = n.get("np") {
                    if let Some(t) = Track::from_np(np) {
                        // Kyzen parity: INIT's np becomes the sync target —
                        // the guest knows the host's song before the first
                        // PLAY arrives, and display state starts correct
                        self.target = Some(Target {
                            uri: t.uri.clone(),
                            title: t.title.clone(),
                            artist: t.artist.clone(),
                            pos_ms: n.get("progress").and_then(|p| p.as_f64()).unwrap_or(0.0),
                            at: Instant::now(),
                            playing: n.get("playing").and_then(|p| p.as_bool()).unwrap_or(false),
                        });
                        self.sync_shared(|s| s.now_playing = Some(t));
                    }
                }
                self.sync_shared(|s| {
                    if let Some(v) = n.get("progress").and_then(|p| p.as_f64()) {
                        s.progress_ms = v;
                    }
                    if let Some(v) = n.get("duration").and_then(|d| d.as_f64()) {
                        s.duration_ms = v;
                    }
                    if let Some(v) = n.get("playing").and_then(|p| p.as_bool()) {
                        s.playing = v;
                    }
                });
                self.sync_shared(|s| {
                    s.host_name = self.host_name.clone();
                    s.gc = self.gc;
                    s.queue = self.queue.clone();
                    s.connected = true;
                });
                self.log(format!("in a Jam with “{}”{}", self.host_name, if self.gc { " (guest controls on)" } else { "" }));
            }
            "PLAY" => {
                let uri = n.get("uri").and_then(|u| u.as_str()).unwrap_or("").to_string();
                let pos = n.get("pos").and_then(|p| p.as_f64()).unwrap_or(0.0);
                let paused = n.get("paused").and_then(|p| p.as_bool()).unwrap_or(false);
                if let Some(np) = n.get("np") {
                    if let Some(t) = Track::from_np(np) {
                        self.sync_shared(|s| s.now_playing = Some(t.clone()));
                        self.apply_playback(&uri, pos, paused, Some(&t));
                        return;
                    }
                }
                self.apply_playback(&uri, pos, paused, None);
            }
            "PAUSE" => {
                if let Some(t) = self.target.as_mut() {
                    t.playing = false;
                }
                if !self.dry_run {
                    self.player.pause();
                }
                self.sync_shared(|s| s.playing = false);
            }
            "PS" => {
                let p = n.get("p").and_then(|p| p.as_bool());
                let pos = n.get("pos").and_then(|p| p.as_f64());
                if let (Some(p), Some(pos)) = (p, pos) {
                    self.target = Some(Target {
                        uri: self.target.as_ref().map(|t| t.uri.clone()).unwrap_or_default(),
                        title: self.target.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
                        artist: self.target.as_ref().map(|t| t.artist.clone()).unwrap_or_default(),
                        pos_ms: pos,
                        at: Instant::now(),
                        playing: p,
                    });
                    self.sync_shared(|s| {
                        s.playing = p;
                        s.progress_ms = pos;
                    });
                }
            }
            "SEEK" => {
                let pos = n.get("pos").and_then(|p| p.as_f64()).unwrap_or(0.0);
                let comp = self.comp_ms();
                let target = pos + comp;
                if let Some(t) = self.target.as_mut() {
                    t.pos_ms = pos;
                    t.at = Instant::now();
                }
                self.last_applied = Some(Instant::now());
                if !self.dry_run {
                    self.player.seek_ms(target);
                }
                self.pos.anchor(target);
                self.log(format!("seek → {}ms", target as u64));
            }
            "SYNC_TICK" => self.drift_correct(&n),
            "PONG" => {
                if let Some(ts) = n.get("ts").and_then(|t| t.as_i64()) {
                    self.ping_ms = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0))
                        - ts;
                    self.sync_shared(|s| s.ping_ms = self.ping_ms);
                }
            }
            "MEMBERS" => {
                if let Some(m) = n.get("members").and_then(|m| m.as_array()) {
                    self.members = m
                        .iter()
                        .map(|x| Member {
                            id: x.get("id").and_then(|i| i.as_str()).unwrap_or("").into(),
                            name: x.get("name").and_then(|i| i.as_str()).unwrap_or("Listener").into(),
                            is_host: x.get("isHost").and_then(|i| i.as_bool()).unwrap_or(false),
                        })
                        .collect();
                    self.sync_shared(|s| s.members = self.members.clone());
                }
            }
            "GCTRL" => {
                self.gc = n.get("on").and_then(|o| o.as_bool()).unwrap_or(false);
                self.sync_shared(|s| s.gc = self.gc);
                self.log(format!("guest controls {}", if self.gc { "enabled" } else { "disabled" }));
            }
            "Q" => {
                if let Some(q) = n.get("queue").and_then(|q| q.as_array()) {
                    self.queue = q.iter().filter_map(track_from_json).collect();
                    self.sync_shared(|s| s.queue = self.queue.clone());
                }
            }
            "KICK" => {
                self.log("removed from the Jam by the host :c");
                self.leave();
            }
            // ---- host side ----
            "JOIN" if self.is_host => {
                let name = n.get("name").and_then(|n| n.as_str()).unwrap_or("Listener").to_string();
                self.guest_name = name.clone();
                let st = self.player.state();
                let np = st.as_ref().map(host_np);
                let (prog, playing) = match &st {
                    Some(s) => (self.pos.feed(s.position_ms, s.playing), s.playing),
                    None => (0.0, false),
                };
                let init = json!({
                    "np": np,
                    "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>(),
                    "host": self.display_name(),
                    "gc": self.gc,
                    "playing": playing,
                    "members": [json!({"id": "host", "name": self.display_name(), "isHost": true}),
                                 json!({"id": "guest", "name": name, "image": ""})],
                    "progress": prog,
                    "duration": st.as_ref().map(|s| s.duration_ms).unwrap_or(0.0),
                });
                self.send_guest(json!({"type": "INIT", "np": init["np"], "queue": init["queue"],
                    "host": init["host"], "gc": init["gc"], "playing": init["playing"],
                    "members": init["members"], "progress": init["progress"], "duration": init["duration"]}));
                if let Some(s) = &st {
                    // Kyzen parity: the join PLAY is sent whenever a track
                    // exists — uri-less hosts send uri:"" + np (guests follow
                    // play/pause; they have nothing to open, which is fine)
                    self.send_guest(json!({"type": "PLAY", "uri": s.uri, "pos": prog,
                        "ts": now_ms(), "np": host_np(s), "paused": !s.playing, "dur": s.duration_ms}));
                }
                self.send_guest(json!({"type": "MEMBERS", "members": init["members"]}));
                self.members = vec![
                    Member { id: "host".into(), name: self.display_name(), is_host: true },
                    Member { id: "guest".into(), name, is_host: false },
                ];
                self.sync_shared(|s| {
                    s.members = self.members.clone();
                    s.connected = true;
                });
                self.log("guest joined the Jam :3");
            }
            "PING" => {
                if let Some(ts) = n.get("ts").and_then(|t| t.as_i64()) {
                    self.send_guest(json!({"type": "PONG", "ts": ts}));
                }
            }
            "SYNC" if self.is_host => {
                if let Some(s) = self.player.state() {
                    if !s.uri.is_empty() || !s.title.is_empty() {
                        let pos = self.pos.feed(s.position_ms, s.playing);
                        self.send_guest(json!({"type": "PLAY", "uri": s.uri, "pos": pos,
                            "ts": now_ms(), "np": host_np(&s), "paused": !s.playing, "dur": s.duration_ms}));
                    }
                }
            }
            "ADD_Q" if self.is_host => {
                if let Some(mut t) = track_from_json(&n) {
                    let who = n.get("addedBy").and_then(|a| a.get("name")).and_then(|x| x.as_str()).unwrap_or("guest");
                    // the extension's ADD_Q carries only the URI — resolve the
                    // real title (and album art) via oEmbed when missing
                    if t.title.is_empty() {
                        self.resolve_track_meta(&mut t);
                    }
                    // prefer the player's own queue (cliamp supports it); fall
                    // back to the bridge queue otherwise
                    let mut real = false;
                    if self.own_queue != Some(false) {
                        real = self.player.add_to_queue(&t.uri, &t.title, &t.artist);
                        if real {
                            self.own_queue = Some(true);
                        }
                    }
                    self.queue.push(t.clone());
                    self.log(format!("queue += {} (from {who}) — {} up next{}", t.title, self.queue.len(),
                        if real { " (player queue)" } else { "" }));
                    self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
                    self.sync_shared(|s| s.queue = self.queue.clone());
                    if !real {
                        if let Some(s) = self.player.state() {
                            if s.uri.is_empty() {
                                self.host_play_next();
                            }
                        }
                    }
                }
            }
            "RM_Q" if self.is_host => {
                if let Some(uri) = n.get("uri").and_then(|u| u.as_str()) {
                    self.queue.retain(|t| t.uri != uri);
                    self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
                    self.sync_shared(|s| s.queue = self.queue.clone());
                }
            }
            "MOVE_Q" if self.is_host => {
                let from = n.get("from").and_then(|f| f.as_u64()).unwrap_or(0) as usize;
                let to = n.get("to").and_then(|t| t.as_u64()).unwrap_or(0) as usize;
                if from < self.queue.len() {
                    let item = self.queue.remove(from);
                    let to = to.min(self.queue.len());
                    self.queue.insert(to, item);
                    self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
                    self.sync_shared(|s| s.queue = self.queue.clone());
                }
            }
            "CMD" if self.is_host => {
                if !self.gc {
                    return;
                }
                let a = n.get("a").and_then(|a| a.as_str()).unwrap_or("");
                self.log(format!("cmd from {guest}: {a}", guest = self.guest_name));
                if self.dry_run {
                    return;
                }
                match a {
                    "play" => self.player.play(),
                    "pause" => self.player.pause(),
                    "seek" => {
                        if let Some(pos) = n.get("pos").and_then(|p| p.as_f64()) {
                            self.player.seek_ms(pos);
                        }
                    }
                    "playuri" => {
                        if let Some(uri) = n.get("uri").and_then(|u| u.as_str()) {
                            let mut t = Track {
                                uri: uri.to_string(),
                                title: String::new(),
                                artist: String::new(),
                                art_url: String::new(),
                            };
                            self.resolve_track_meta(&mut t);
                            self.log(format!("playing guest request: {}", t.title));
                            self.player.open_uri(uri, &t.title, &t.artist);
                        }
                    }
                    "next" => self.host_next(),
                    _ => {}
                }
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    fn comp_ms(&self) -> f64 {
        if self.ping_ms > 0 {
            self.ping_ms as f64 / 2.0
        } else {
            0.0
        }
    }

    fn apply_playback(&mut self, uri: &str, pos_ms: f64, paused: bool, np: Option<&Track>) {
        let comp = self.comp_ms();
        let msg_uri_empty = uri.is_empty();
        // uri-less PLAY (pre-resolution window on spotifast hosts): keep the
        // last real uri in the target so enforce_lock doesn't mistake the
        // guest's correctly-synced song for a hijack-worthy local pick
        let target_uri = if uri.is_empty() {
            self.target.as_ref().map(|t| t.uri.clone()).unwrap_or_default()
        } else {
            uri.to_string()
        };
        self.target = Some(Target {
            uri: target_uri,
            title: np.map(|t| t.title.clone()).unwrap_or_default(),
            artist: np.map(|t| t.artist.clone()).unwrap_or_default(),
            pos_ms,
            at: Instant::now(),
            playing: !paused,
        });
        self.last_applied = Some(Instant::now());
        self.sync_shared(|s| {
            s.playing = !paused;
            s.progress_ms = pos_ms;
            if let Some(t) = np {
                s.now_playing = Some(t.clone());
            }
        });
        if self.dry_run {
            self.log(format!("[dry-run] PLAY {} @{}ms {}", uri, (pos_ms + comp) as u64, if paused { "(paused)" } else { "" }));
            return;
        }
        // uri-less players (spotifast's Windows CLI has no uri) match by
        // title — a strict uri compare is always false and would reload
        // the track on every PLAY/SYNC reply
        let same = self.player.state().map(|s| {
            if s.uri.is_empty() || msg_uri_empty {
                match np {
                    Some(t) => !t.title.is_empty() && s.title == t.title,
                    None => true, // nothing to compare — assume in place
                }
            } else {
                s.uri == uri
            }
        }).unwrap_or(false);
        self.pos.reset();
        if !same {
            // uri-less host (e.g. spotifast's Windows CLI): the guest has no
            // way to open the host's track — follow play/pause/position only,
            // never open "" (it restarts tracks) and never re-seek a
            // different local song to the host's position
            if uri.is_empty() {
                return;
            }
            let title = np.map(|t| t.title.as_str()).unwrap_or("");
            let artist = np.map(|t| t.artist.as_str()).unwrap_or("");
            self.player.open_uri(uri, title, artist);
            self.pending_seek = Some(PendingSeek {
                uri: uri.to_string(),
                title: title.to_string(),
                target_ms: pos_ms + comp + 250.0,
                paused,
                deadline: Instant::now() + Duration::from_millis(4500),
                opened: false,
            });
        } else if let Some(st) = self.player.state() {
            let want = pos_ms + comp;
            if paused {
                self.player.pause();
                self.pos.anchor(want);
            } else if (st.position_ms - want).abs() > self.drift_jump_ms.max(400.0) {
                self.player.seek_ms(want);
                self.pos.anchor(want);
                if !st.playing {
                    self.player.play();
                }
            } else if !st.playing {
                self.player.play();
            }
        }
    }

    fn drift_correct(&mut self, n: &Value) {
        let Some(t) = self.target.as_ref() else { return };
        if !t.playing || self.dry_run {
            return;
        }
        if let Some(at) = self.last_applied {
            if at.elapsed() < Duration::from_secs(2) {
                return;
            }
        }
        let Some(st) = self.player.state() else { return };
        if !st.playing || (st.uri.is_empty() && st.title.is_empty()) {
            return;
        }
        // spotifast freezes its Position property — compare against the
        // interpolated position, not the raw read
        let local_pos = self.pos.feed(st.position_ms, true);
        let tick_pos = n.get("pos").and_then(|p| p.as_f64()).unwrap_or(0.0);
        let ts = n.get("ts").and_then(|t| t.as_i64()).unwrap_or(now_ms());
        let now = now_ms();
        let comp = if self.ping_ms > 0 {
            self.ping_ms as f64 / 2.0
        } else {
            (now - ts).clamp(0, 500) as f64
        };
        let want = tick_pos + comp;
        let drift = (local_pos - want).abs();
        self.drift_ema = if self.drift_ema == 0.0 {
            drift
        } else {
            0.7 * self.drift_ema + 0.3 * drift
        };
        if !self.drift_enabled {
            return;
        }
        if self.drift_ema < self.drift_deadband_ms {
            self.drift_count = 0;
            return;
        }
        if self.drift_ma_check() {
            return;
        }
        self.drift_count = 0;
        self.drift_ema = 0.0;
        self.last_applied = Some(Instant::now());
        if !self.dry_run {
            self.player.seek_ms(want);
        }
        self.log(format!("drift {}ms — corrected", drift as u64));
    }

    fn drift_ma_check(&mut self) -> bool {
        // seek when smoothed drift exceeds the jump threshold, or when it
        // exceeds the deadband twice in a row (matches the extension's
        // 650ms/160ms defaults — now tunable)
        if self.drift_ema >= self.drift_jump_ms {
            return false;
        }
        self.drift_count += 1;
        self.drift_count < 2
    }

    /// Skip: use the player's own queue when it has one (cliamp / TrackList),
    /// otherwise advance the bridge queue — and if the bridge queue is empty
    /// too, just skip in the player itself.
    fn host_next(&mut self) {        if self.own_queue == Some(true) || self.queue.is_empty() {
            self.player.next();
            return;
        }
        self.host_play_next();
    }

    fn guest_connected(&self) -> bool {
        self.p2p
            .as_ref()
            .map(|p| p.dc.is_some())
            .unwrap_or(false)
    }

    /// The extension's ADD_Q carries only the URI — resolve the real title
    /// (and album art) via Spotify's public oEmbed endpoint. Cached per URI.
    fn resolve_track_meta(&mut self, t: &mut Track) {
        if !t.title.is_empty() {
            return;
        }
        if let Some(cached) = self.title_cache.get(&t.uri) {
            t.title = cached.0.clone();
            if t.art_url.is_empty() {
                t.art_url = cached.1.clone();
            }
            return;
        }
        let Some(id) = t.uri.strip_prefix("spotify:track:") else { return };
        let oembed = format!("https://open.spotify.com/oembed?url=spotify%3Atrack%3A{id}");
        let mut fetched: Option<serde_json::Value> = None;
        match self.http.get(&oembed).call() {
            Ok(resp) => match resp.into_json::<serde_json::Value>() {
                Ok(v) => fetched = Some(v),
                Err(e) => self.log(format!("oembed: parse failed: {e}")),
            },
            Err(e) => self.log(format!("oembed: request failed: {e}")),
        }
        // Windows fallback: the OS ships curl.exe — use it if ureq failed
        if fetched.is_none() {
            if let Ok(out) = std::process::Command::new("curl.exe")
                .args(["-s", "--max-time", "6", &oembed])
                .output()
            {
                if out.status.success() {
                    if let Ok(v) = serde_json::from_slice(&out.stdout) {
                        self.log("oembed: resolved via curl fallback");
                        fetched = Some(v);
                    }
                }
            }
        }
        let Some(v) = fetched else {
            self.log(format!("oembed: could not resolve {}", t.uri));
            return;
        };
        let title = v.get("title").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let art = v.get("thumbnail_url").and_then(|x| x.as_str()).unwrap_or("").to_string();
        self.title_cache.insert(t.uri.clone(), (title.clone(), art.clone()));
        t.title = title;
        if t.art_url.is_empty() {
            t.art_url = art;
        }
    }

    fn host_play_next(&mut self) {
        if self.queue.is_empty() {
            self.log("queue empty");
            return;
        }
        let next = self.queue.remove(0);
        self.log(format!("host: playing {} (queue: {})", next.title, self.queue.len()));
        if !self.dry_run {
            self.player.open_uri(&next.uri, &next.title, &next.artist);
        }
        self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
        self.sync_shared(|s| s.queue = self.queue.clone());
    }

    fn host_broadcast_play(&mut self, st: &PlayerState) {
        // resume/current-state updates use the interpolated position —
        // raw reads can be stale on some players
        let pos = self.pos.feed(st.position_ms, st.playing);
        self.send_guest(json!({
            "type": "PLAY", "uri": st.uri, "pos": pos, "ts": now_ms(),
            "np": host_np(st), "paused": !st.playing, "dur": st.duration_ms,
        }));
    }

    // ------------------------------------------------------------------
    fn tick(&mut self) {
        let mode = self.shared.lock().map(|s| s.mode).unwrap_or(Mode::Idle);
        // stuck-join watchdog: a silent/hung join recovers the button
        if mode == Mode::Joining {
            if let Some(since) = self.joining_since {
                if since.elapsed() >= Duration::from_secs(6) {
                    self.joining_since = None;
                    self.leave_quiet();
                    self.sync_shared(|s| {
                        s.mode = Mode::Idle;
                        s.error = Some("Jam not found or unreachable — check the code and try again".into());
                    });
                    self.log("join timed out");
                }
            }
        }
        // guest reconnect backoff + signaling recovery (both modes)
        if let Some(at) = self.reconnect_at {
            if Instant::now() >= at {
                self.reconnect_at = None;
                let connect_id = if self.is_host {
                    self.jam_id.clone()
                } else {
                    random_uuid()
                };
                if let Ok(sig) = Signaling::connect(&connect_id, self.ev_tx.clone()) {
                    self.signaling = Some(sig);
                    if self.is_host {
                        self.log(format!("re-registered Jam {connect_id}"));
                    } else {
                        self.log(format!("reconnecting to Jam {}…", self.target_code.clone().unwrap_or_default()));
                    }
                }
            }
        }
        // resolve pending seek (waiting for the new track to load)
        if let Some(p) = self.pending_seek.as_mut() {
            if !p.opened {
                if let Some(st) = self.player.state() {
                    let matches = if p.uri.is_empty() || st.uri.is_empty() {
                        !p.title.is_empty() && st.title == p.title
                    } else {
                        st.uri == p.uri
                    };
                    if matches {
                        p.opened = true;
                        if !self.dry_run {
                            self.player.seek_ms(p.target_ms);
                            if p.paused {
                                self.player.pause();
                            }
                        }
                        self.pos.anchor(p.target_ms);
                    } else if Instant::now() > p.deadline {
                        if !self.dry_run {
                            self.player.seek_ms(p.target_ms);
                        }
                        self.pos.anchor(p.target_ms);
                        self.pending_seek = None;
                    }
                }
            } else {
                self.pending_seek = None;
            }
        }

        match mode {
            Mode::Guest => {
                if self.t_ping.elapsed() >= Duration::from_secs(5) {
                    self.t_ping = Instant::now();
                    self.send(json!({"type": "PING", "ts": now_ms()}));
                }
                if self.t_sync.elapsed() >= Duration::from_secs(6) {
                    self.t_sync = Instant::now();
                    let ok = match (self.last_applied, self.last_sync_req) {
                        (Some(a), _) => a.elapsed() >= Duration::from_secs(3),
                        (None, _) => true,
                    } && match self.last_sync_req {
                        Some(r) => r.elapsed() >= Duration::from_secs(5),
                        None => true,
                    };
                    if ok {
                        self.last_sync_req = Some(Instant::now());
                        self.send(json!({"type": "SYNC"}));
                    }
                }
                if self.t_lock.elapsed() >= Duration::from_secs(2) {
                    self.t_lock = Instant::now();
                    self.enforce_lock();
                }
                // progress estimate for the UI
                if let Some(t) = self.target.as_ref() {
                    let prog = if t.playing {
                        t.pos_ms + t.at.elapsed().as_millis() as f64
                    } else {
                        t.pos_ms
                    };
                    self.sync_shared(|s| s.progress_ms = prog);
                }
            }
            Mode::Hosting => {
                if self.t_host_watch.elapsed() >= Duration::from_millis(300) {
                    self.t_host_watch = Instant::now();
                    if let Some(st) = self.player.state() {
                        // track-change key: the uri when the player exposes one
                        // (mpris/cliamp), otherwise title+artist+dur — spotifast's
                        // CLI has no uri, and keying on "" would never change
                        let track_key = if st.uri.is_empty() {
                            format!("{}\u{1}{}\u{1}{}", st.title, st.artist, st.duration_ms)
                        } else {
                            st.uri.clone()
                        };
                        let uri_changed = self.host_last_uri.as_deref() != Some(track_key.as_str());
                        let first = self.host_last_uri.is_none();
                        // re-key: the uri JUST resolved for the same track —
                        // not a new song, don't reset the position
                        let rekey = !first && !st.uri.is_empty()
                            && self.host_last_uri.as_deref().map(|k| {
                                k.contains('\u{1}') && k.starts_with(&format!("{}\u{1}", st.title))
                            }).unwrap_or(false);
                        if uri_changed {
                            self.pos.reset();
                        }
                        self.host_last_uri = Some(track_key);
                        if uri_changed && !first {
                            if rekey {
                                // resolved uri for the current song — tell guests
                                // the real uri at the CURRENT position
                                self.host_broadcast_play(&st);
                                self.sync_shared(|s| {
                                    s.now_playing = Some(Track {
                                        uri: st.uri.clone(), title: st.title.clone(),
                                        artist: st.artist.clone(), art_url: st.art_url.clone(),
                                    });
                                    s.playing = st.playing;
                                });
                            } else if st.uri.is_empty() && st.title.is_empty() {
                                // player went silent
                                self.broadcast(json!({"type": "PAUSE"}));
                            } else if st.uri.is_empty() {
                                // spotifast-cli: no uri, but the track is known —
                                // np carries title/artist so guests still follow
                                self.pos.anchor(0.0);
                                self.pos.grace_until = Some(Instant::now() + Duration::from_secs(5));
                                self.broadcast(json!({
                                    "type": "PLAY", "uri": "", "pos": 0, "ts": now_ms(),
                                    "np": host_np(&st), "paused": !st.playing, "dur": st.duration_ms,
                                }));
                                self.sync_shared(|s| {
                                    s.now_playing = Some(Track {
                                        uri: st.uri.clone(), title: st.title.clone(),
                                        artist: st.artist.clone(), art_url: st.art_url.clone(),
                                    });
                                    s.playing = st.playing;
                                });
                            } else {
                                // drain display-queue entries that just played
                                if let Some(idx) = self.queue.iter().position(|t| t.uri == st.uri) {
                                    self.queue.drain(..=idx);
                                    self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
                                    self.sync_shared(|s| s.queue = self.queue.clone());
                                }
                                // backfill artist/art from the player for entries missing them
                                if !st.artist.is_empty() || !st.art_url.is_empty() {
                                    for q in self.queue.iter_mut() {
                                        if q.uri == st.uri {
                                            if q.artist.is_empty() { q.artist = st.artist.clone(); }
                                            if q.art_url.is_empty() { q.art_url = st.art_url.clone(); }
                                        }
                                    }
                                    self.broadcast(json!({"type": "Q", "queue": self.queue.iter().map(track_to_json).collect::<Vec<_>>()}));
                                    self.sync_shared(|s| s.queue = self.queue.clone());
                                }
                                // song changes start at 0 — never trust the player's
                                // position here (it can be stale, e.g. cached or the
                                // previous track's). Grace period ignores stale reads.
                                self.pos.anchor(0.0);
                                self.pos.grace_until = Some(Instant::now() + Duration::from_secs(5));
                                self.broadcast(json!({
                                    "type": "PLAY", "uri": st.uri, "pos": 0, "ts": now_ms(),
                                    "np": host_np(&st), "paused": !st.playing, "dur": st.duration_ms,
                                }));
                                self.sync_shared(|s| {
                                    s.now_playing = Some(Track {
                                        uri: st.uri.clone(), title: st.title.clone(),
                                        artist: st.artist.clone(), art_url: st.art_url.clone(),
                                    });
                                    s.playing = st.playing;
                                });
                            }
                        }
                        let play_changed = self.host_last_playing != Some(st.playing);
                        self.host_last_playing = Some(st.playing);
                        if play_changed && !uri_changed {
                            if st.playing {
                                self.host_broadcast_play(&st);
                            } else {
                                self.broadcast(json!({"type": "PAUSE"}));
                            }
                        }
                        // displayed progress: interpolated (spotifast freezes Position)
                        let disp = self.pos.feed(st.position_ms, st.playing);
                        if std::env::var("JAM_DEBUG").is_ok() {
                            self.log(format!("pos: reported={}ms interp={:.0}ms frozen={}", st.position_ms as u64, disp, self.pos.frozen));
                        }
                        let np_changed = uri_changed || first;
                        self.sync_shared(|s| {
                            s.progress_ms = disp;
                            s.duration_ms = st.duration_ms;
                            s.playing = st.playing;
                            if np_changed && !st.title.is_empty() {
                                s.now_playing = Some(Track {
                                    uri: st.uri.clone(), title: st.title.clone(),
                                    artist: st.artist.clone(), art_url: st.art_url.clone(),
                                });
                            }
                        });
                    }
                }
                if self.t_tick.elapsed() >= Duration::from_secs(5) {
                    self.t_tick = Instant::now();
                    if let Some(st) = self.player.state() {
                        if st.playing && (!st.uri.is_empty() || !st.title.is_empty()) {
                            let pos = self.pos.feed(st.position_ms, true);
                            self.broadcast(json!({"type": "SYNC_TICK", "pos": pos, "ts": now_ms()}));
                        }
                    }
                }
                // while paused, keep re-asserting the pause — the extension's
                // paused-join dance (playUri + pause after 150ms) can fail on
                // free clients, leaving the guest silently playing (+25s drift)
                if self.t_host_pause.elapsed() >= Duration::from_secs(2) {
                    self.t_host_pause = Instant::now();
                    if self.guest_connected() {
                        if let Some(st) = self.player.state() {
                            if !st.playing {
                                self.broadcast(json!({"type": "PAUSE"}));
                                self.broadcast(json!({"type": "PS", "p": false, "pos": st.position_ms,
                                    "dur": st.duration_ms, "ts": now_ms()}));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn enforce_lock(&mut self) {
        let Some(t) = self.target.clone() else { return };
        // uri-less targets (spotifast host) still enforce via title matching
        if t.uri.is_empty() && t.title.is_empty() {
            return;
        }
        if let Some(st) = self.player.state() {
            // track the local song between ticks: when it CHANGED since last
            // tick and the previous one was near its end, spotifast auto-
            // advanced naturally — ask for a sync, never hijack the host
            let cur_key = if st.uri.is_empty() { format!("\u{1}{}", st.title) } else { st.uri.clone() };
            let naturally_advanced = self.last_local.as_ref().map(|(k, pos, dur)| {
                *k != cur_key && *dur > 0.0 && *dur - *pos < 3500.0
            }).unwrap_or(false);
            self.last_local = Some((cur_key.clone(), st.position_ms, st.duration_ms));
            // spotifast-cli does not expose the uri — fall back to title matching
            let on_target = if st.uri.is_empty() && !st.title.is_empty() && !t.title.is_empty() {
                st.title == t.title
            } else {
                st.uri == t.uri
            };
            if on_target {
                // landed on the host's track — the pre-join song no longer
                // plays, so future local picks are real picks
                self.join_baseline = None;
            }
            if !on_target {
                // natural end-of-track: the guest auto-advanced into its own
                // junk context moments before the host broadcasts the real
                // next song — sync, never hijack (Kyzen's nearEnd guard)
                let near_end = st.duration_ms > 0.0 && st.duration_ms - st.position_ms < 3000.0;
                if self.gc && (naturally_advanced || near_end) {
                    self.send(json!({"type": "SYNC"}));
                    return;
                }
                // the pre-join song is not a pick — never push it to the host;
                // still snap back to the host's track below
                let cur_key = if st.uri.is_empty() { format!("\u{1}{}", st.title) } else { st.uri.clone() };
                let pre_join = self.join_baseline.as_deref() == Some(cur_key.as_str());
                // settle window (post-join / post-player-switch): sync to the
                // host first, no guest-control pushes yet
                let settling = self.settle_until.map(|t| Instant::now() < t).unwrap_or(false);
                // guest picked their own song with guest controls on — ask the
                // host to play it instead of snapping back (matches the
                // extension's songchange CMD playuri behavior)
                if self.gc && !pre_join && !settling && self.playuri_sent.as_deref() != Some(st.uri.as_str()) {
                    self.send(json!({"type": "CMD", "a": "playuri", "uri": st.uri}));
                    self.playuri_sent = Some(st.uri.clone());
                    self.playuri_sent_at = Some(Instant::now());
                    self.log(format!("guest control: asked the host to play {}", st.title));
                    return; // wait for the host's broadcast before touching playback
                }
                // give the host a moment to follow a playuri we already sent
                if self.gc {
                    if let Some(at) = self.playuri_sent_at {
                        if at.elapsed() < Duration::from_secs(3) {
                            return;
                        }
                    }
                }
                // uri-less target: there is no uri to open on the guest —
                // snapping back is impossible, so stand down instead of
                // looping the lock path every 2s (display/play-pause follow
                // via the host's broadcasts)
                if t.uri.is_empty() {
                    return;
                }
                self.log(format!("🔒 locked to Jam (was {})", if st.title.is_empty() { &st.uri } else { &st.title }));
                let pos = if t.playing {
                    t.pos_ms + t.at.elapsed().as_millis() as f64
                } else {
                    t.pos_ms
                };
                let np = if t.title.is_empty() {
                    None
                } else {
                    Some(Track { uri: t.uri.clone(), title: t.title.clone(), artist: t.artist.clone(), art_url: String::new() })
                };
                self.apply_playback(&t.uri, pos, !t.playing, np.as_ref());
            }
        }
    }
}

// helpers -------------------------------------------------------------------
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn host_np(s: &PlayerState) -> Value {
    json!({"uri": s.uri, "title": s.title, "artist": s.artist, "artUrl": s.art_url})
}

fn track_from_json(t: &Value) -> Option<Track> {
    t.get("uri").and_then(|u| u.as_str()).map(|uri| Track {
        uri: uri.into(),
        title: t.get("title").and_then(|x| x.as_str()).unwrap_or("").into(),
        artist: t.get("artist").and_then(|x| x.as_str()).unwrap_or("").into(),
        art_url: t.get("artUrl").and_then(|x| x.as_str()).unwrap_or("").into(),
    })
}

fn track_to_json(t: &Track) -> Value {
    json!({"uri": t.uri, "title": t.title, "artist": t.artist, "artUrl": t.art_url})
}
