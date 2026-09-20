//! PeerJS client in pure Rust: signaling over WebSocket to the public cloud
//! server + WebRTC data channels via libdatachannel. Wire-compatible with
//! peerjs 1.5.x (see docs/PROTOCOL.md).

use std::io::ErrorKind;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use datachannel::{
    DataChannelHandler, DataChannelInfo, IceCandidate, PeerConnectionHandler, RtcConfig,
    RtcDataChannel, RtcPeerConnection, SessionDescription, SdpType,
};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{client::IntoClientRequest, Message};

use crate::binarypack::{self, Packed};

pub const CLOUD_HOST: &str = "0.peerjs.com";
pub const PEERJS_VERSION: &str = "1.5.5";
const HEARTBEAT_MS: u64 = 5000;
/// peerjs chunks payloads larger than this (binaryPackChunker.chunkedMTU)
const CHUNKED_MTU: usize = 16300;

pub const ICE_SERVERS: &[&str] = &[
    "stun:stun.l.google.com:19302",
    // peerjs public TURN relays (username/credential not settable via the
    // simple RtcConfig API; kept for documentation — see PROTOCOL.md)
    // "turn:eu-0.turn.peerjs.com:3478", "turn:us-0.turn.peerjs.com:3478",
];

// ---------------------------------------------------------------------------
// events into the core loop
// ---------------------------------------------------------------------------
#[derive(Debug)]
pub enum CoreEvent {
    Log(String),
    PeerOpen(String),                 // signaling ready; our id
    SignalMessage(serde_json::Value), // raw server message {type, src, dst, payload}
    SignalClosed,
    DcOpen,
    DcRaw(Vec<u8>),              // raw data-channel frame (decode in core)
    DcMessage(serde_json::Value), // decoded jam message
    DcHostChannel,                // host mode: guest opened a data channel
    DcClosed,
    LocalDescription(serde_json::Value), // {type:"offer"|"answer", sdp}
    LocalCandidate(String, String),      // candidate, sdpMid
    PeerError(String),
}

enum WsCmd {
    Send(String),
    Close,
}

pub fn random_token(n: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..n).map(|_| CHARS[rng.random_range(0..CHARS.len())] as char).collect()
}

pub fn random_token_upper(n: usize) -> String {
    random_token(n).to_uppercase()
}

pub fn random_uuid() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut b = [0u8; 16];
    rng.fill(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

// ---------------------------------------------------------------------------
// signaling socket
// ---------------------------------------------------------------------------
pub struct Signaling {
    cmd_tx: Sender<WsCmd>,
}

impl Signaling {
    pub fn connect(id: &str, events: Sender<CoreEvent>) -> std::io::Result<Self> {
        let token = random_token(24);
        let url = format!(
            "wss://{}/peerjs?key=peerjs&id={}&token={}&version={}",
            CLOUD_HOST, id, token, PEERJS_VERSION
        );
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<WsCmd>();
        let events2 = events.clone();

        std::thread::Builder::new()
            .name("jam-signal".into())
            .spawn(move || {
                let mut req = match url.into_client_request() {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = events2.send(CoreEvent::PeerError(format!("bad ws url: {e}")));
                        return;
                    }
                };
                req.headers_mut()
                    .insert("Origin", "https://0.peerjs.com".parse().unwrap());
                let (mut ws, _resp) = match tungstenite::connect(req) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = events2.send(CoreEvent::PeerError(format!(
                            "signaling connect failed: {e}"
                        )));
                        return;
                    }
                };
                // allow the loop to interleave writes with reads
                {
                    let s = ws.get_ref();
                    let _ = match s {
                        MaybeTlsStream::Rustls(t) => t
                            .get_ref()
                            .set_read_timeout(Some(Duration::from_millis(250))),
                        MaybeTlsStream::Plain(t) => {
                            t.set_read_timeout(Some(Duration::from_millis(250)))
                        }
                        _ => Ok(()),
                    };
                }
                let _ = events2.send(CoreEvent::Log("signaling: connected".into()));

                let mut last_hb = Instant::now();
                loop {
                    match cmd_rx.try_recv() {
                        Ok(WsCmd::Send(s)) => {
                            if ws.send(Message::Text(s.into())).is_err() {
                                break;
                            }
                        }
                        Ok(WsCmd::Close) => {
                            let _ = ws.close(None);
                            break;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                    }
                    if last_hb.elapsed() >= Duration::from_millis(HEARTBEAT_MS) {
                        if ws
                            .send(Message::Text(r#"{"type":"HEARTBEAT"}"#.into()))
                            .is_err()
                        {
                            break;
                        }
                        last_hb = Instant::now();
                    }
                    match ws.read() {
                        Ok(Message::Text(txt)) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                let _ = events2.send(CoreEvent::SignalMessage(v));
                            }
                        }
                        Ok(Message::Ping(p)) => {
                            let _ = ws.send(Message::Pong(p));
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(tungstenite::Error::Io(ref e))
                            if e.kind() == ErrorKind::WouldBlock
                                || e.kind() == ErrorKind::TimedOut =>
                        {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        Err(_) => break,
                    }
                }
                let _ = events2.send(CoreEvent::SignalClosed);
            })?;

        Ok(Self { cmd_tx })
    }

    pub fn send_json(&self, v: &serde_json::Value) {
        let _ = self.cmd_tx.send(WsCmd::Send(v.to_string()));
    }

    pub fn close(&self) {
        let _ = self.cmd_tx.send(WsCmd::Close);
    }
}

// ---------------------------------------------------------------------------
// WebRTC peer
// ---------------------------------------------------------------------------
pub struct DcHandler {
    events: Sender<CoreEvent>,
}

impl DataChannelHandler for DcHandler {
    fn on_open(&mut self) {
        let _ = self.events.send(CoreEvent::DcOpen);
    }
    fn on_message(&mut self, msg: &[u8]) {
        let _ = self.events.send(CoreEvent::DcRaw(msg.to_vec()));
    }
    fn on_closed(&mut self) {
        let _ = self.events.send(CoreEvent::DcClosed);
    }
}

pub struct PcHandler {
    events: Sender<CoreEvent>,
    /// host mode: channel the core can use to grab the incoming data channel
    host_dc: Sender<Box<RtcDataChannel<DcHandler>>>,
}

impl PeerConnectionHandler for PcHandler {
    type DCH = DcHandler;

    fn data_channel_handler(&mut self, _info: DataChannelInfo) -> Self::DCH {
        DcHandler {
            events: self.events.clone(),
        }
    }

    fn on_data_channel(&mut self, data_channel: Box<RtcDataChannel<Self::DCH>>) {
        let _ = self.events.send(CoreEvent::DcHostChannel);
        let _ = self.host_dc.send(data_channel);
    }

    fn on_description(&mut self, desc: SessionDescription) {
        // convert to the peerjs wire shape {type, sdp}
        let json = serde_json::json!({
            "type": match desc.sdp_type { SdpType::Offer => "offer", SdpType::Answer => "answer", _ => return },
            "sdp": desc.sdp.to_string(),
        });
        let _ = self.events.send(CoreEvent::LocalDescription(json));
    }

    fn on_candidate(&mut self, cand: IceCandidate) {
        let _ = self.events.send(CoreEvent::LocalCandidate(
            cand.candidate.clone(),
            cand.mid.clone(),
        ));
    }
}

/// Everything the core loop needs to drive one peer connection.
pub struct Peer2Peer {
    pub pc: Box<RtcPeerConnection<PcHandler>>,
    /// guest mode: the channel we created; host mode: None until guest connects
    pub dc: Option<Box<RtcDataChannel<DcHandler>>>,
    pub connection_id: String,
    dc_events: Sender<CoreEvent>,
}

impl Peer2Peer {
    pub fn new(
        events: Sender<CoreEvent>,
        host_dc: Sender<Box<RtcDataChannel<DcHandler>>>,
    ) -> Result<Self, String> {
        let config = RtcConfig::new(ICE_SERVERS);
        let handler = PcHandler {
            events: events.clone(),
            host_dc,
        };
        let pc = RtcPeerConnection::new(&config, handler).map_err(|e| e.to_string())?;
        Ok(Self {
            pc,
            dc: None,
            connection_id: format!("dc_{}", random_token(16)),
            dc_events: events,
        })
    }

    /// Guest side: create the channel and start offering.
    pub fn start_offer(&mut self) -> Result<(), String> {
        let handler = DcHandler {
            events: self.dc_events.clone(),
        };
        let dc = self
            .pc
            .create_data_channel(&self.connection_id, handler)
            .map_err(|e| e.to_string())?;
        self.dc = Some(dc);
        self.pc
            .set_local_description(SdpType::Offer)
            .map_err(|e| e.to_string())
    }

    /// Host side: we received an OFFER; answer it. The guest's data channel
    /// arrives via `on_data_channel` once connected. libdatachannel generates
    /// the answer itself when the remote (offer) description is set — do NOT
    /// call set_local_description again (invalid state).
    pub fn answer(&mut self, offer: serde_json::Value) -> Result<(), String> {
        let desc: SessionDescription =
            serde_json::from_value(offer).map_err(|e| format!("bad sdp: {e}"))?;
        self.pc
            .set_remote_description(&desc)
            .map_err(|e| e.to_string())
    }

    pub fn accept_answer(&mut self, answer: serde_json::Value) -> Result<(), String> {
        let desc: SessionDescription =
            serde_json::from_value(answer).map_err(|e| format!("bad sdp: {e}"))?;
        self.pc
            .set_remote_description(&desc)
            .map_err(|e| e.to_string())
    }

    pub fn add_candidate(&mut self, candidate: &str, mid: &str) {
        let cand = IceCandidate {
            candidate: candidate.to_string(),
            mid: mid.to_string(),
        };
        let _ = self.pc.add_remote_candidate(&cand);
    }

    pub fn send_frames(&mut self, frames: &[Vec<u8>]) {
        if let Some(dc) = self.dc.as_mut() {
            for f in frames {
                let _ = dc.send(f);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Jam wire codec: json <-> binarypack frames (with peerjs chunking support)
// ---------------------------------------------------------------------------
#[derive(Default)]
pub struct JamCodec {
    chunks: std::collections::HashMap<u64, (usize, Vec<Option<Vec<u8>>>)>, // id -> (total, parts)
    chunk_counter: u64,
}

impl JamCodec {
    pub fn encode(&mut self, msg: &serde_json::Value) -> Vec<Vec<u8>> {
        let packed = Packed::from_json(msg);
        let blob = binarypack::pack(&packed);
        if blob.len() <= CHUNKED_MTU {
            return vec![blob];
        }
        let total = blob.len().div_ceil(CHUNKED_MTU);
        self.chunk_counter += 1;
        let id = self.chunk_counter;
        let mut frames = Vec::new();
        for (n, part) in blob.chunks(CHUNKED_MTU).enumerate() {
            let chunk = Packed::Map(vec![
                ("__peerData".into(), Packed::UInt(id)),
                ("n".into(), Packed::UInt(n as u64)),
                ("total".into(), Packed::UInt(total as u64)),
                ("data".into(), Packed::Bytes(part.to_vec())),
            ]);
            frames.push(binarypack::pack(&chunk));
        }
        frames
    }

    /// Decode incoming bytes; returns Some(json) when a full message arrived.
    pub fn decode(&mut self, bytes: &[u8]) -> Option<serde_json::Value> {
        let (v, _) = binarypack::unpack(bytes)?;
        let Packed::Map(m) = &v else { return Some(v.to_json()) };
        let Some((_, Packed::UInt(id))) = m.iter().find(|(k, _)| k == "__peerData") else {
            return Some(v.to_json());
        };
        let id = *id;
        let get = |key: &str| -> Option<u64> {
            m.iter().find(|(k, _)| k == key).and_then(|(_, v)| match v {
                Packed::UInt(u) => Some(*u),
                _ => None,
            })
        };
        // {__peerData:{type:"close"}} — graceful close from a peerjs client
        if get("n").is_none() {
            return None;
        }
        let n = get("n")? as usize;
        let total = get("total")?;
        let data = m
            .iter()
            .find(|(k, _)| k == "data")
            .and_then(|(_, v)| match v {
                Packed::Bytes(b) => Some(b.clone()),
                _ => None,
            })?;
        let entry = self
            .chunks
            .entry(id)
            .or_insert_with(|| (total as usize, Vec::new()));
        if entry.1.len() <= n {
            entry.1.resize(n + 1, None);
        }
        entry.1[n] = Some(data);
        if entry.1.len() == entry.0 && entry.1.iter().all(|p| p.is_some()) {
            let mut blob = Vec::new();
            for p in entry.1.iter().flatten() {
                blob.extend_from_slice(p);
            }
            self.chunks.remove(&id);
            return self.decode(&blob);
        }
        None
    }
}
