//! Player backends: spotifast via MPRIS (D-Bus) and cliamp via its CLI/IPC.

use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct PlayerState {
    pub playing: bool,
    pub position_ms: f64,
    pub duration_ms: f64,
    pub uri: String,
    pub title: String,
    pub artist: String,
    pub art_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Backend {
    Spotifast { bus_suffix: String },
    SpotifastWin,
    Cliamp,
}

impl Backend {
    pub fn label(&self) -> &'static str {
        match self {
            Backend::Spotifast { .. } => "spotifast (MPRIS)",
            Backend::SpotifastWin => "spotifast (CLI)",
            Backend::Cliamp => "cliamp (IPC)",
        }
    }
}

pub trait PlayerBackend: Send {
    fn name(&self) -> &'static str;
    fn state(&mut self) -> Option<PlayerState>;
    fn open_uri(&mut self, uri: &str, title: &str, artist: &str);
    fn seek_ms(&mut self, ms: f64);
    fn play(&mut self);
    fn pause(&mut self);
    /// Skip within the player's own queue.
    fn next(&mut self) {}
    /// Insert into the player's own queue (plays after current). false = unsupported.
    fn add_to_queue(&mut self, _uri: &str, _title: &str, _artist: &str) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// spotifast — MPRIS on org.mpris.MediaPlayer2.fastpotify
// ---------------------------------------------------------------------------
pub struct MprisPlayer {
    bus_suffix: String,
    _conn: zbus::blocking::Connection,
    proxy: zbus::blocking::Proxy<'static>,
    tracklist: zbus::blocking::Proxy<'static>,
    tracklist_ok: Option<bool>,
    last_trackid: Option<String>,
    dbg_last: Option<(String, i64, String)>,
}

impl MprisPlayer {
    pub fn new(bus_suffix: &str) -> zbus::Result<Self> {
        let conn = zbus::blocking::Connection::session()?;
        let name = format!("org.mpris.MediaPlayer2.{bus_suffix}");
        // NOTE: properties must NOT be cached — MPRIS players don't emit
        // PropertiesChanged for Position (it's excluded from the spec), so a
        // cached proxy freezes the position at its first read.
        let proxy = zbus::blocking::proxy::Builder::<zbus::blocking::Proxy<'static>>::new(&conn)
            .destination(name.clone())?
            .path("/org/mpris/MediaPlayer2")?
            .interface("org.mpris.MediaPlayer2.Player")?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()?;
        let tracklist = zbus::blocking::proxy::Builder::<zbus::blocking::Proxy<'static>>::new(&conn)
            .destination(name)?
            .path("/org/mpris/MediaPlayer2")?
            .interface("org.mpris.MediaPlayer2.TrackList")?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()?;
        Ok(Self {
            bus_suffix: bus_suffix.to_string(),
            _conn: conn,
            proxy,
            tracklist,
            tracklist_ok: None,
            last_trackid: None,
            dbg_last: None,
        })
    }

    pub fn bus_suffix(&self) -> &str {
        &self.bus_suffix
    }
}

impl PlayerBackend for MprisPlayer {
    fn name(&self) -> &'static str {
        "spotifast"
    }

    fn state(&mut self) -> Option<PlayerState> {
        let status: String = match self.proxy.get_property("PlaybackStatus") {
            Ok(s) => s,
            Err(e) => {
                if std::env::var("JAM_DEBUG").is_ok() { eprintln!("mpris status err: {e}"); }
                return None;
            }
        };
        let pos: i64 = match self.proxy.get_property("Position") {
            Ok(p) => p,
            Err(e) => {
                if std::env::var("JAM_DEBUG").is_ok() { eprintln!("mpris pos err: {e}"); }
                return None;
            }
        };
        let meta: zbus::zvariant::OwnedValue = match self.proxy.get_property("Metadata") {
            Ok(m) => m,
            Err(e) => {
                if std::env::var("JAM_DEBUG").is_ok() { eprintln!("mpris meta err: {e}"); }
                return None;
            }
        };
        // zvariant -> JSON is far friendlier than matching dict types by hand.
        // OwnedValue serializes as a variant envelope {"signature","value"},
        // and each dict value is wrapped the same way — unwrap both.
        let m: serde_json::Value = serde_json::to_value(&meta).ok()?;
        let d = m.get("value").unwrap_or(&m);
        let inner = |k: &str| d.get(k).and_then(|v| v.get("value"));
        let s = |k: &str| inner(k).and_then(|v| v.as_str()).map(|x| x.to_string());
        let num = |k: &str| inner(k).and_then(|v| v.as_f64());
        let trackid = s("mpris:trackid");
        self.last_trackid.clone_from(&trackid);
        let uri = s("xesam:url")
            .filter(|u| u.starts_with("spotify:track:"))
            .or_else(|| {
                trackid
                    .as_ref()
                    .map(|t| {
                        t.rsplit(|c| c == '/' || c == '_')
                            .next()
                            .map(|id| format!("spotify:track:{id}"))
                            .unwrap_or_else(|| t.clone())
                    })
                    .filter(|u| u.starts_with("spotify:track:"))
            })
            .unwrap_or_default();
        let duration_us = num("mpris:length").unwrap_or(0.0);        let artist = inner("xesam:artist")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        Some(PlayerState {
            playing: status == "Playing",
            position_ms: pos as f64 / 1000.0,
            duration_ms: duration_us / 1000.0,
            uri,
            title: s("xesam:title").unwrap_or_default(),
            artist,
            art_url: s("mpris:artUrl").unwrap_or_default(),
        })
    }

    fn open_uri(&mut self, uri: &str, _title: &str, _artist: &str) {
        let _ = self.proxy.call_method("OpenUri", &(uri.to_string()));
    }

    fn seek_ms(&mut self, ms: f64) {
        if let Some(trackid) = self.last_trackid.clone() {
            if let Ok(path) = zbus::zvariant::ObjectPath::try_from(trackid) {
                let _ = self
                    .proxy
                    .call_method("SetPosition", &(path, (ms.max(0.0) as i64) * 1000));
            }
        }
    }

    fn play(&mut self) {
        let _ = self.proxy.call_method("Play", &());
    }

    fn pause(&mut self) {
        let _ = self.proxy.call_method("Pause", &());
    }

    fn next(&mut self) {
        let _ = self.proxy.call_method("Next", &());
    }

    fn add_to_queue(&mut self, uri: &str, _title: &str, _artist: &str) -> bool {
        // MPRIS TrackList is optional; the official Spotify client and older
        // spotifast builds don't implement it. Probe first (cached), then
        // AddTrack(uri, after=current, play=false) when available.
        if self.tracklist_ok != Some(true) {
            if self.tracklist_ok == Some(false) {
                return false;
            }
            let Ok(xml) = self.proxy.introspect() else {
                self.tracklist_ok = Some(false);
                return false;
            };
            self.tracklist_ok = Some(xml.contains("org.mpris.MediaPlayer2.TrackList"));
            if self.tracklist_ok != Some(true) {
                return false;
            }
        }
        let Ok(tl) = zbus::blocking::proxy::Builder::<zbus::blocking::Proxy<'static>>::new(&self._conn)
            .destination(format!("org.mpris.MediaPlayer2.{}", self.bus_suffix))
            .and_then(|b| b.path("/org/mpris/MediaPlayer2"))
            .and_then(|b| b.interface("org.mpris.MediaPlayer2.TrackList"))
            .and_then(|b| b.cache_properties(zbus::proxy::CacheProperties::No).build())
        else {
            self.tracklist_ok = Some(false);
            return false;
        };
        let after = self
            .last_trackid
            .clone()
            .unwrap_or_else(|| "/org/mpris/MediaPlayer2/TrackList/NoTrack".into());
        match zbus::zvariant::ObjectPath::try_from(after) {
            Ok(path) => tl
                .call_method("AddTrack", &(uri.to_string(), path, false))
                .is_ok(),
            Err(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// cliamp — v2 IPC via the `cliamp` binary
// ---------------------------------------------------------------------------
pub struct CliampPlayer {
    bin: String,
}

impl CliampPlayer {
    pub fn new() -> Self {
        Self {
            bin: std::env::var("CLIAMP_BIN").unwrap_or_else(|_| "cliamp".into()),
        }
    }

    fn run(&self, args: &[&str]) -> Option<String> {
        std::process::Command::new(&self.bin)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
    }

    fn remote(&self, op: &str, params: &str) -> bool {
        self.run(&["remote", "call", op, "--params", params]).is_some()
    }
}

impl PlayerBackend for CliampPlayer {
    fn name(&self) -> &'static str {
        "cliamp"
    }

    fn state(&mut self) -> Option<PlayerState> {
        let out = self.run(&["status", "--json"])?;
        let v: Value = serde_json::from_str(&out).ok()?;
        let track = v.get("track").cloned().unwrap_or(Value::Null);
        let path = track.get("path").and_then(|p| p.as_str()).unwrap_or("");
        Some(PlayerState {
            playing: v.get("state").and_then(|s| s.as_str()) == Some("playing"),
            position_ms: v.get("position").and_then(|p| p.as_f64()).unwrap_or(0.0) * 1000.0,
            duration_ms: {
                let d = v.get("duration").and_then(|d| d.as_f64()).unwrap_or(0.0);
                // sanitize: some setups emit denormal garbage durations
                if !d.is_finite() || !(0.0..=86400.0).contains(&d) { 0.0 } else { d * 1000.0 }
            },
            uri: if path.starts_with("spotify:track:") { path.into() } else { String::new() },
            title: track.get("title").and_then(|t| t.as_str()).unwrap_or("").into(),
            artist: track.get("artist").and_then(|a| a.as_str()).unwrap_or("").into(),
            art_url: String::new(),
        })
    }

    fn open_uri(&mut self, uri: &str, title: &str, artist: &str) {
        let id = uri.trim_start_matches("spotify:track:");
        let params = serde_json::json!({
            "track": {
                "path": uri,
                "title": if title.is_empty() { id } else { title },
                "artist": artist,
                "provider_meta": { "kind": "track", "trackID": id }
            }
        });
        self.remote("track.play", &params.to_string());
    }

    fn seek_ms(&mut self, ms: f64) {
        let params = format!(r#"{{"value":{}}}"#, (ms.max(0.0) / 1000.0) as u64);
        self.remote("seek.absolute", &params);
    }

    fn play(&mut self) {
        self.remote("runtime.play", "{}");
    }

    fn pause(&mut self) {
        self.remote("runtime.pause", "{}");
    }

    fn next(&mut self) {
        self.remote("runtime.next", "{}");
    }

    fn add_to_queue(&mut self, uri: &str, title: &str, artist: &str) -> bool {
        let id = uri.trim_start_matches("spotify:track:");
        let params = serde_json::json!({
            "track": {
                "path": uri,
                "title": if title.is_empty() { id } else { title },
                "artist": artist,
                "provider_meta": { "kind": "track", "trackID": id }
            }
        });
        self.remote("track.queue", &params.to_string())
    }
}

/// Append-only debug log next to the exe (only when JAM_DEBUG=1).
pub fn debug_log(msg: &str) {
    if std::env::var("JAM_DEBUG").is_err() {
        return;
    }
    let path = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("jam-gui-debug.log")))
        .unwrap_or_else(|| std::path::PathBuf::from("jam-gui-debug.log"));
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        use std::io::Write;
        let _ = writeln!(f, "{} {}", stamp, msg);
    }
}

/// Find the exe path of a running player process by name (Windows).
/// Note: sysinfo reports process names WITHOUT the .exe extension.
#[cfg(windows)]
pub fn find_running_process_exe(names: &[&str]) -> Option<std::path::PathBuf> {
    use sysinfo::{ProcessesToUpdate, System};
    let bases: Vec<String> = names
        .iter()
        .map(|n| n.to_lowercase().trim_end_matches(".exe").to_string())
        .collect();
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);
    for (_, proc_) in sys.processes() {
        let name = proc_.name().to_string_lossy().to_lowercase();
        let name = name.trim_end_matches(".exe");
        if bases.iter().any(|b| b == name) {
            if let Some(exe) = proc_.exe() {
                debug_log(&format!("process lookup: found {} at {}", name, exe.display()));
                return Some(exe.to_path_buf());
            }
        }
    }
    debug_log("process lookup: no running player process found");
    None
}

/// spotifast on Windows/macOS: control via its own CLI verbs (now-playing --raw,
/// play-uri, seek-to, play, pause, next). The binary talks to the running instance.
pub struct SpotifastWinPlayer {
    /// candidate binary names, tried in order; the first that spawns wins
    bin: String,
}

impl SpotifastWinPlayer {
    pub fn new() -> Self {
        Self {
            bin: std::env::var("SPOTIFAST_BIN").unwrap_or_else(|_| "spotifast".into()),
        }
    }

    fn run(&self, args: &[&str]) -> Option<String> {
        // try the configured binary first, then common alternate names,
        // the running player's exe path, and standard install locations
        for bin in self.candidate_paths() {
            if let Ok(out) = std::process::Command::new(&bin).args(args).output() {
                if out.status.success() {
                    return Some(String::from_utf8_lossy(&out.stdout).into_owned());
                }
            }
        }
        None
    }

    fn candidate_paths(&self) -> Vec<String> {
        let mut v = vec![self.bin.clone(), "spotifast".into(), "fastpotify".into(), "spotifast-cli".into()];
        #[cfg(windows)]
        if let Some(p) = crate::player::find_running_process_exe(&["spotifast.exe", "fastpotify.exe"]) {
            v.push(p.to_string_lossy().into_owned());
        }
        if let Ok(lad) = std::env::var("LOCALAPPDATA") {
            v.push(format!("{lad}\\Programs\\spotifast\\spotifast.exe"));
        }
        if let Ok(pf) = std::env::var("ProgramFiles") {
            v.push(format!("{pf}\\Spotifast\\spotifast.exe"));
        }
        if let Ok(h) = std::env::var("USERPROFILE") {
            v.push(format!("{h}\\go\\bin\\spotifast.exe"));
            v.push(format!("{h}\\scoop\\shims\\spotifast.exe"));
        }
        v
    }
}

impl PlayerBackend for SpotifastWinPlayer {
    fn name(&self) -> &'static str {
        "spotifast-cli"
    }

    fn state(&mut self) -> Option<PlayerState> {
        let out = self.run(&["now-playing", "--raw"])?;
        let line = out.lines().next()?;
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() < 10 {
            return None;
        }
        // fields: state, title, artists, album, position_ms, duration_ms,
        //         volume, shuffle, repeat, art_url, saved, device
        let position_ms: f64 = f[4].parse().unwrap_or(0.0);
        let duration_ms: f64 = f[5].parse().unwrap_or(0.0);
        Some(PlayerState {
            playing: f[0].eq_ignore_ascii_case("playing"),
            position_ms,
            duration_ms,
            uri: String::new(), // the CLI does not expose the uri; matching is title-based
            title: f[1].to_string(),
            artist: f[2].to_string(),
            art_url: f[9].to_string(),
        })
    }

    fn open_uri(&mut self, uri: &str, _title: &str, _artist: &str) {
        self.run(&["play-uri", uri]);
    }

    fn seek_ms(&mut self, ms: f64) {
        self.run(&["seek-to", &format!("{}", (ms.max(0.0) / 1000.0) as u64)]);
    }

    fn play(&mut self) {
        self.run(&["play"]);
    }

    fn pause(&mut self) {
        self.run(&["pause"]);
    }

    fn next(&mut self) {
        self.run(&["next"]);
    }
}
