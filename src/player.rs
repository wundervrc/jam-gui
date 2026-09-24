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
        // OpenUri("") makes players restart the current track — never call it
        // with nothing to open (uri-less hosts, e.g. the spotifast CLI backend)
        if uri.is_empty() {
            return;
        }
        if let Err(e) = self.proxy.call_method("OpenUri", &(uri.to_string())) {
            crate::player::debug_log(&format!("mpris OpenUri({uri}) failed: {e}"));
        }
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

    /// same discovery as the spotifast-cli backend: configured binary first,
    /// then PATH names, the running player's exe, and install locations
    fn candidate_paths(&self) -> Vec<String> {
        let mut v = vec![self.bin.clone(), "cliamp".into(), "cliampd".into()];
        #[cfg(windows)]
        if let Some(p) = crate::player::find_running_process_exe(&["cliamp.exe", "cliampd.exe"]) {
            v.push(p.to_string_lossy().into_owned());
        }
        if let Ok(lad) = std::env::var("LOCALAPPDATA") {
            v.push(format!("{lad}\\Programs\\cliamp\\cliamp.exe"));
        }
        if let Ok(pf) = std::env::var("ProgramFiles") {
            v.push(format!("{pf}\\cliamp\\cliamp.exe"));
        }
        if let Ok(h) = std::env::var("USERPROFILE") {
            v.push(format!("{h}\\scoop\\shims\\cliamp.exe"));
        }
        v
    }

    fn run(&self, args: &[&str]) -> Option<String> {
        for bin in self.candidate_paths() {
            if let Ok(out) = std::process::Command::new(&bin).args(args).output() {
                if out.status.success() {
                    return Some(String::from_utf8_lossy(&out.stdout).into_owned());
                }
            }
        }
        None
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
        if uri.is_empty() {
            return; // nothing to open — uri-less host
        }
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
    /// resolved uris by "title|artist" — the CLI has no uri, so we look it
    /// up (web api → deezer/musicbrainz) once per track
    uri_cache: std::collections::HashMap<String, Option<String>>,
    /// in-flight lookup: (key, receiver for the resolved uri)
    resolving: Option<(String, std::sync::mpsc::Receiver<Option<String>>)>,
}

#[cfg(windows)]
mod win_cred {
    use std::os::windows::ffi::EncodeWide;

    const CRED_TYPE_GENERIC: u32 = 1;

    #[repr(C)]
    struct FileTime {
        lo: u32,
        hi: u32,
    }
    #[repr(C)]
    struct Credential {
        flags: u32,
        cred_type: u32,
        target_name: *const u16,
        comment: *const u16,
        last_written: FileTime,
        blob_size: u32,
        blob: *const u8,
        persist: u32,
        attr_count: u32,
        attributes: *const std::ffi::c_void,
        target_alias: *const u16,
        user_name: *const u16,
    }
    #[link(name = "advapi32")]
    extern "system" {
        fn CredReadW(target: *const u16, cred_type: u32, flags: u32, cred: *mut *mut Credential) -> i32;
        fn CredEnumerateW(filter: *const u16, flags: u32, count: *mut u32, creds: *mut *mut *mut Credential) -> i32;
        fn CredFree(cred: *mut std::ffi::c_void);
    }

    fn wide(s: &str) -> Vec<u16> {
        s.encode_wide().chain(std::iter::once(0)).collect()
    }
    fn read_str(p: *const u16) -> String {
        if p.is_null() {
            return String::new();
        }
        unsafe {
            let mut len = 0usize;
            while *p.add(len) != 0 {
                len += 1;
            }
            String::from_utf16_lossy(std::slice::from_raw_parts(p, len))
        }
    }

    /// read a generic credential's blob as utf-8 by exact target name
    fn read_credential(target: &str) -> Option<Vec<u8>> {
        let wtarget = wide(target);
        let mut p: *mut Credential = std::ptr::null_mut();
        let ok = unsafe { CredReadW(wtarget.as_ptr(), CRED_TYPE_GENERIC, 0, &mut p) };
        if ok == 0 || p.is_null() {
            return None;
        }
        unsafe {
            let c = &*p;
            let blob = if c.blob_size > 0 && !c.blob.is_null() {
                Some(std::slice::from_raw_parts(c.blob, c.blob_size as usize).to_vec())
            } else {
                None
            };
            CredFree(p as *mut std::ffi::c_void);
            blob
        }
    }

    /// find the shared-web grant credential: spotifast names its web-token
    /// entry "<profile>:shared-web.rocks.fastpotify.Fastpotify" (or
    /// ".rocks.spotifast.Spotifast" in newer builds). The freshest wins.
    pub fn read_shared_web_grant() -> Option<(String, String, String, i64)> {
        let filter = wide("*");
        let mut count: u32 = 0;
        let mut list: *mut *mut Credential = std::ptr::null_mut();
        let ok = unsafe { CredEnumerateW(filter.as_ptr(), 0, &mut count, &mut list) };
        if ok == 0 || list.is_null() {
            return None;
        }
        let mut best: Option<(i64, Vec<u8>)> = None;
        unsafe {
            for i in 0..count as usize {
                let p = *list.add(i);
                if p.is_null() {
                    continue;
                }
                let c = &*p;
                let target = read_str(c.target_name);
                let suffix_ok = target.ends_with(":shared-web.rocks.fastpotify.Fastpotify")
                    || target.ends_with(":shared-web.rocks.spotifast.Spotifast");
                if !suffix_ok {
                    continue;
                }
                let blob = if c.blob_size > 0 && !c.blob.is_null() {
                    std::slice::from_raw_parts(c.blob, c.blob_size as usize).to_vec()
                } else {
                    continue;
                };
                let stamp = (c.last_written.hi as i64) << 32 | c.last_written.lo as i64;
                if best.as_ref().map(|(t, _)| stamp > *t).unwrap_or(true) {
                    best = Some((stamp, blob));
                }
            }
            CredFree(list as *mut std::ffi::c_void);
        }
        let (_, blob) = best?;
        String::from_utf8(blob).ok()
    }
}

/// the stored grant JSON: { grant: { Web: { client_id, access_token,
/// refresh_token, expires_at, scope } } }
#[cfg(windows)]
fn web_api_now_playing() -> Option<(String, String, String, String, f64)> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
    let grant_json = win_cred::read_shared_web_grant()?;
    let v: serde_json::Value = serde_json::from_str(&grant_json).ok()?;
    let web = v.pointer("/grant/Web")?;
    let cid = web.get("client_id").and_then(|x| x.as_str())?.to_string();
    let rtok = web.get("refresh_token").and_then(|x| x.as_str())?.to_string();
    let exp = web.get("expires_at").and_then(|x| x.as_i64()).unwrap_or(0);
    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(8))
        .build();
    // expired? refresh with the public PKCE client (no secret; spotify does
    // not rotate the app's stored refresh token, so the app keeps working)
    let tok = if exp - 60 > now {
        web.get("access_token").and_then(|x| x.as_str())?.to_string()
    } else {
        let resp = agent
            .post("https://accounts.spotify.com/api/token")
            .send_form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", rtok.as_str()),
                ("client_id", cid.as_str()),
            ])
            .ok()?
            .into_json::<serde_json::Value>()
            .ok()?;
        crate::player::debug_log("web api token refreshed");
        resp.get("access_token").and_then(|x| x.as_str())?.to_string()
    };
    let v = agent
        .get("https://api.spotify.com/v1/me/player")
        .set("Authorization", &format!("Bearer {tok}"))
        .call()
        .ok()?
        .into_json()
        .ok()?;
    let uri = v.pointer("/item/uri").and_then(|x| x.as_str())?;
    if !uri.starts_with("spotify:track:") {
        return None;
    }
    let title = v.pointer("/item/name").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let artist = v
        .pointer("/item/artists")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|a| a.get("name"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let art = v
        .pointer("/item/album/images")
        .and_then(|i| i.as_array())
        .and_then(|i| i.first())
        .and_then(|i| i.get("url"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let dur = v.pointer("/item/duration_ms").and_then(|x| x.as_f64()).unwrap_or(0.0);
    Some((uri.to_string(), title, artist, art, dur))
}

/// uri resolution for the spotifast-cli backend: web api on windows (exact,
/// real-time), deezer/musicbrainz search elsewhere (best effort)
fn resolve_any_uri(title: &str, artist: &str, duration_ms: f64) -> Option<String> {
    #[cfg(windows)]
    {
        if let Some((uri, api_title, _, _, _)) = web_api_now_playing() {
            // guard against a race where the app skipped during resolution
            let a = title.to_lowercase();
            let b = api_title.to_lowercase();
            if a == b || a.contains(&b) || b.contains(&a) {
                return Some(uri);
            }
            crate::player::debug_log("web api title mismatch — falling back to search");
        }
    }
    resolve_track_uri(title, artist, duration_ms)
}

/// Look up a spotify track uri from title + artist + duration via public
/// no-auth services: deezer search (→ isrc) → musicbrainz (→ spotify link).
fn resolve_track_uri(title: &str, artist: &str, duration_ms: f64) -> Option<String> {
    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(8))
        .build();
    // 1) deezer: plain-text search, filter client-side by artist + duration
    let q = format!("{title} {artist}");
    let url = format!("https://api.deezer.com/search?q={}", urlencode(&q));
    let v: serde_json::Value = agent.get(&url).call().ok()?.into_json().ok()?;
    let tracks = v.get("data")?.as_array()?;
    let mut isrc: Option<String> = None;
    for t in tracks {
        let t_artist = t.pointer("/artist/name").and_then(|a| a.as_str()).unwrap_or("");
        let dur = t.get("duration").and_then(|d| d.as_f64()).unwrap_or(0.0);
        let artist_ok = artist.is_empty()
            || t_artist.to_lowercase().split(',').any(|p| {
                let p = p.trim();
                !p.is_empty() && (artist.to_lowercase().contains(p) || p.contains(&artist.to_lowercase()))
            });
        let dur_ok = duration_ms <= 0.0 || dur <= 0.0 || (dur * 1000.0 - duration_ms).abs() < 5000.0;
        if artist_ok && dur_ok {
            isrc = t.get("isrc").and_then(|i| i.as_str()).map(String::from);
            break;
        }
    }
    let isrc = isrc?;
    // 2) musicbrainz: isrc → recordings (1 req/s limit is fine, one per track)
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let mb = format!("https://musicbrainz.org/ws/2/isrc/{isrc}?fmt=json");
    let v: serde_json::Value = agent.get(&mb)
        .set("User-Agent", "jam-gui/0.1 (listen-together client)")
        .call().ok()?.into_json().ok()?;
    let recordings: Vec<String> = v.get("recordings")?.as_array()?
        .iter().filter_map(|r| r.get("id").and_then(|i| i.as_str()).map(String::from)).collect();
    // 3) each recording → url-rels → open.spotify.com/track link
    for mbid in recordings.iter().take(3) {
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let url = format!("https://musicbrainz.org/ws/2/recording/{mbid}?inc=url-rels&fmt=json");
        let Ok(resp) = agent.get(&url)
            .set("User-Agent", "jam-gui/0.1 (listen-together client)")
            .call() else { continue };
        let Ok(v) = resp.into_json::<serde_json::Value>() else { continue };
        if let Some(rels) = v.get("relations").and_then(|r| r.as_array()) {
            for rel in rels {
                if rel.get("type").and_then(|t| t.as_str()) != Some("free streaming")
                    && rel.get("type").and_then(|t| t.as_str()) != Some("streaming") {
                    continue;
                }
                if let Some(res) = rel.pointer("/url/resource").and_then(|u| u.as_str()) {
                    if let Some(id) = res.strip_prefix("https://open.spotify.com/track/") {
                        let id = id.trim_end_matches('/');
                        if id.len() == 22 {
                            return Some(format!("spotify:track:{id}"));
                        }
                    }
                }
            }
        }
    }
    None
}

/// percent-encode for query strings
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl SpotifastWinPlayer {
    pub fn new() -> Self {
        Self {
            bin: std::env::var("SPOTIFAST_BIN").unwrap_or_else(|_| "spotifast".into()),
            uri_cache: std::collections::HashMap::new(),
            resolving: None,
        }
    }

    /// the CLI has no uri — resolve one in the background (deezer → isrc →
    /// musicbrainz → spotify link); cached, one lookup per track change
    fn resolve_uri(&mut self, title: &str, artist: &str, duration_ms: f64) -> Option<String> {
        let key = format!("{title}|{artist}");
        // collect finished lookups
        if let Some((pending_key, rx)) = &self.resolving {
            if let Ok(result) = rx.try_recv() {
                let pending_key = pending_key.clone();
                let (pk, _) = self.resolving.take().unwrap();
                self.uri_cache.insert(pk, result.clone());
                if pending_key == key {
                    if result.is_some() {
                        crate::player::debug_log(&format!("uri resolved for {key:?}"));
                    } else {
                        crate::player::debug_log(&format!("no uri found for {key:?}"));
                    }
                }
            }
        }
        match self.uri_cache.get(&key) {
            Some(Some(uri)) => return Some(uri.clone()),
            Some(None) => return None, // cached negative
            None => {}
        }
        // kick off a lookup if none in flight for this key
        let in_flight = self.resolving.as_ref().map(|(k, _)| k.clone());
        if in_flight.as_deref() == Some(key.as_str()) {
            return None; // still working on it
        }
        let title = title.to_string();
        let artist = artist.to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        crate::player::debug_log(&format!("uri lookup started: {key:?}"));
        std::thread::spawn(move || {
            crate::player::debug_log("uri lookup thread running");
            let result = resolve_any_uri(&title, &artist, duration_ms);
            crate::player::debug_log(&format!("uri lookup finished: {result:?}"));
            let _ = tx.send(result);
        });
        self.resolving = Some((key, rx));
        None
    }

    fn run(&self, args: &[&str]) -> Option<String> {
        // try the configured binary first, then common alternate names,
        // the running player's exe path, and standard install locations
        for bin in self.candidate_paths() {
            crate::player::debug_log(&format!("spotifast-cli spawn: {} {:?}", bin, args));
            if let Ok(out) = std::process::Command::new(&bin).args(args).output() {
                let ok = out.status.success();
                crate::player::debug_log(&format!(
                    "spotifast-cli spawn result: ok={} out_len={} out_head={}",
                    ok,
                    out.stdout.len(),
                    String::from_utf8_lossy(&out.stdout[..out.stdout.len().min(120)])
                ));
                if ok {
                    return Some(String::from_utf8_lossy(&out.stdout).into_owned());
                }
            } else {
                crate::player::debug_log("spotifast-cli spawn failed to launch");
            }
        }
        crate::player::debug_log("spotifast-cli: all candidates exhausted");
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
            crate::player::debug_log(&format!("spotifast-cli state parse: fields={} line={:?}", f.len(), line));
            return None;
        }
        // fields: state, title, artists, album, position_ms, duration_ms,
        //         volume, shuffle, repeat, art_url, saved, device
        let position_ms: f64 = f[4].parse().unwrap_or(0.0);
        let duration_ms: f64 = f[5].parse().unwrap_or(0.0);
        let title = f[1].to_string();
        let artist = f[2].to_string();
        // the CLI never reports a uri — resolve one in the background so the
        // host can hand guests something to play (cliamp/mpris guests)
        let uri = if title.is_empty() {
            String::new()
        } else {
            self.resolve_uri(&title, &artist, duration_ms).unwrap_or_default()
        };
        Some(PlayerState {
            playing: f[0].eq_ignore_ascii_case("playing"),
            position_ms,
            duration_ms,
            uri,
            title,
            artist,
            art_url: f[9].to_string(),
        })
    }

    fn open_uri(&mut self, uri: &str, _title: &str, _artist: &str) {
        if uri.is_empty() {
            return; // nothing to open — uri-less host
        }
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

#[cfg(test)]
mod uri_tests {
    #[test]
    fn resolves_known_track() {
        let uri = super::resolve_track_uri("DAMN", "JOYRYDE, Freddie Gibbs", 220839.0);
        println!("resolved: {uri:?}");
        assert!(uri.is_some());
    }
}
