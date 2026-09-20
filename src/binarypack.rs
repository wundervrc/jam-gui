//! BinaryPack — the msgpack-derived serialization peerjs uses on data channels.
//! Implements exactly the marker set accepted/produced by peers/js-binarypack.

#[derive(Debug, Clone, PartialEq)]
pub enum Packed {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<Packed>),
    Map(Vec<(String, Packed)>),
}

impl Packed {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Packed::Null => serde_json::Value::Null,
            Packed::Bool(b) => (*b).into(),
            Packed::Int(i) => (*i).into(),
            Packed::UInt(u) => (*u).into(),
            Packed::Float(f) => (*f).into(),
            Packed::Str(s) => s.as_str().into(),
            Packed::Bytes(b) => {
                // transport-internal (chunk data); surface as lossless marker
                serde_json::json!({ "__bytes_len": b.len() })
            }
            Packed::Array(a) => serde_json::Value::Array(a.iter().map(|v| v.to_json()).collect()),
            Packed::Map(m) => {
                let mut o = serde_json::Map::new();
                for (k, v) in m {
                    o.insert(k.clone(), v.to_json());
                }
                serde_json::Value::Object(o)
            }
        }
    }

    pub fn from_json(v: &serde_json::Value) -> Packed {
        match v {
            serde_json::Value::Null => Packed::Null,
            serde_json::Value::Bool(b) => Packed::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Packed::UInt(u)
                } else if let Some(i) = n.as_i64() {
                    Packed::Int(i)
                } else {
                    Packed::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Packed::Str(s.clone()),
            serde_json::Value::Array(a) => Packed::Array(a.iter().map(Packed::from_json).collect()),
            serde_json::Value::Object(o) => {
                Packed::Map(o.iter().map(|(k, v)| (k.clone(), Packed::from_json(v))).collect())
            }
        }
    }
}

fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn push_str(out: &mut Vec<u8>, s: &str) {
    // NOTE: js-binarypack is NOT standard msgpack — string markers are
    // 0xb0+len (fix), 0xd8 (u16), 0xd9 (u32); the 0xa0/0xda/0xdb range is
    // *binary* instead. Mirrored exactly from peers/js-binarypack lib.
    let b = s.as_bytes();
    match b.len() {
        0..=0x0f => out.push(0xb0 | b.len() as u8),
        0x10..=0xffff => {
            out.push(0xd8);
            push_u16(out, b.len() as u16);
        }
        _ => {
            out.push(0xd9);
            push_u32(out, b.len() as u32);
        }
    }
    out.extend_from_slice(b);
}
fn push_len(out: &mut Vec<u8>, len: usize, fix: u8, sixteen: u8, thirtytwo: u8) {
    match len {
        0..=15 => out.push(fix | len as u8),
        16..=65535 => {
            out.push(sixteen);
            push_u16(out, len as u16);
        }
        _ => {
            out.push(thirtytwo);
            push_u32(out, len as u32);
        }
    }
}

pub fn pack(v: &Packed) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    pack_into(v, &mut out);
    out
}

fn pack_bytes_payload(b: &[u8], out: &mut Vec<u8>) {
    // binary markers per js-binarypack: 0xa0+len (fix), 0xda (u16), 0xdb (u32)
    match b.len() {
        0..=0x0f => out.push(0xa0 | b.len() as u8),
        0x10..=0xffff => {
            out.push(0xda);
            push_u16(out, b.len() as u16);
        }
        _ => {
            out.push(0xdb);
            push_u32(out, b.len() as u32);
        }
    }
    out.extend_from_slice(b);
}

fn pack_into(v: &Packed, out: &mut Vec<u8>) {
    match v {
        Packed::Null => out.push(0xc0),
        Packed::Bool(false) => out.push(0xc2),
        Packed::Bool(true) => out.push(0xc3),
        Packed::UInt(u) => match *u {
            0..=127 => out.push(*u as u8),
            128..=255 => {
                out.push(0xcc);
                out.push(*u as u8);
            }
            256..=65535 => {
                out.push(0xcd);
                push_u16(out, *u as u16);
            }
            65536..=4294967295 => {
                out.push(0xce);
                push_u32(out, *u as u32);
            }
            _ => {
                out.push(0xcf);
                push_u64(out, *u);
            }
        },
        Packed::Int(i) => match *i {
            -32..=-1 => out.push(0xe0 | (*i as i8) as u8),
            -128..=-33 => {
                out.push(0xd0);
                out.push(*i as i8 as u8);
            }
            -32768..=-129 => {
                out.push(0xd1);
                push_u16(out, *i as i16 as u16);
            }
            -2147483648..=-32769 => {
                out.push(0xd2);
                push_u32(out, *i as i32 as u32);
            }
            _ => {
                out.push(0xd3);
                push_u64(out, *i as u64);
            }
        },
        Packed::Float(f) => {
            out.push(0xcb);
            out.extend_from_slice(&f.to_be_bytes());
        }
        Packed::Str(s) => push_str(out, s),
        Packed::Bytes(b) => pack_bytes_payload(b, out),
        Packed::Array(a) => {
            push_len(out, a.len(), 0x90, 0xdc, 0xdd);
            for item in a {
                pack_into(item, out);
            }
        }
        Packed::Map(m) => {
            push_len(out, m.len(), 0x80, 0xde, 0xdf);
            for (k, v) in m {
                push_str(out, k);
                pack_into(v, out);
            }
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let v = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.buf.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(s)
    }
    fn u16(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        let b = self.take(4)?;
        Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Option<u64> {
        let b = self.take(8)?;
        Some(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    fn value(&mut self) -> Option<Packed> {
        let marker = self.u8()?;
        // js-binarypack swaps msgpack's string/bin marker ranges:
        // 0xa0..=0xaf / 0xda / 0xdb are BINARY; 0xb0..=0xbf / 0xd8 / 0xd9 are STRINGS
        Some(match marker {
            0x00..=0x7f => Packed::UInt(marker as u64),
            0xe0..=0xff => Packed::Int(marker as i8 as i64),
            0xc0 | 0xc1 => Packed::Null,
            0xc2 => Packed::Bool(false),
            0xc3 => Packed::Bool(true),
            0xa0..=0xaf => Packed::Bytes(self.take((marker & 0x0f) as usize)?.to_vec()),
            0xda => {
                let n = self.u16()? as usize;
                Packed::Bytes(self.take(n)?.to_vec())
            }
            0xdb => {
                let n = self.u32()? as usize;
                Packed::Bytes(self.take(n)?.to_vec())
            }
            0xb0..=0xbf => Packed::Str(String::from_utf8_lossy(self.take((marker & 0x0f) as usize)?).into_owned()),
            0xd8 => {
                let n = self.u16()? as usize;
                Packed::Str(String::from_utf8_lossy(self.take(n)?).into_owned())
            }
            0xd9 => {
                let n = self.u32()? as usize;
                Packed::Str(String::from_utf8_lossy(self.take(n)?).into_owned())
            }
            0xca => Packed::Float(f32::from_be_bytes(self.take(4)?.try_into().ok()?) as f64),
            0xcb => Packed::Float(f64::from_be_bytes(self.take(8)?.try_into().ok()?)),
            0xcc => Packed::UInt(self.u8()? as u64),
            0xcd => Packed::UInt(self.u16()? as u64),
            0xce => Packed::UInt(self.u32()? as u64),
            0xcf => Packed::UInt(self.u64()?),
            0xd0 => Packed::Int(self.u8()? as i8 as i64),
            0xd1 => Packed::Int(self.u16()? as i16 as i64),
            0xd2 => Packed::Int(self.u32()? as i32 as i64),
            0xd3 => Packed::Int(self.u64()? as i64),
            0xd4..=0xd7 => Packed::Null, // js-binarypack decodes ext markers as undefined
            0xdc => {
                let n = self.u16()?;
                Packed::Array((0..n).map(|_| self.value()).collect::<Option<Vec<_>>>()?)
            }
            0xdd => {
                let n = self.u32()?;
                Packed::Array((0..n).map(|_| self.value()).collect::<Option<Vec<_>>>()?)
            }
            0x80..=0x8f | 0xde | 0xdf => {
                let n = match marker {
                    0x80..=0x8f => (marker & 0x0f) as usize,
                    0xde => self.u16()? as usize,
                    _ => self.u32()? as usize,
                };
                let mut m = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = self.value()?;
                    let v = self.value()?;
                    let key = match k {
                        Packed::Str(s) => s,
                        other => format!("{:?}", other),
                    };
                    m.push((key, v));
                }
                Packed::Map(m)
            }
            0x90..=0x9f => {
                let n = (marker & 0x0f) as usize;
                Packed::Array((0..n).map(|_| self.value()).collect::<Option<Vec<_>>>()?)
            }
            _ => return None,
        })
    }
}

/// Unpack one value from the front of `buf`. Returns the value and bytes consumed.
pub fn unpack(buf: &[u8]) -> Option<(Packed, usize)> {
    let mut r = Reader { buf, pos: 0 };
    let v = r.value()?;
    Some((v, r.pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let v = Packed::Map(vec![
            ("type".into(), Packed::Str("PLAY".into())),
            ("uri".into(), Packed::Str("spotify:track:abc".into())),
            ("pos".into(), Packed::UInt(123456)),
            ("paused".into(), Packed::Bool(false)),
            ("list".into(), Packed::Array(vec![Packed::UInt(1), Packed::Null])),
        ]);
        let bytes = pack(&v);
        let (back, used) = unpack(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(v, back);
    }

    #[test]
    fn negative_and_float() {
        for v in [Packed::Int(-1), Packed::Int(-40), Packed::Int(-300), Packed::Int(-70000), Packed::Float(1.5)] {
            let bytes = pack(&v);
            let (back, _) = unpack(&bytes).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn json_bridge() {
        let j: serde_json::Value = serde_json::json!({
            "type": "JOIN", "name": "wunder", "ts": 1726800000000u64, "ok": true, "x": null
        });
        let p = Packed::from_json(&j);
        let bytes = pack(&p);
        let (back, _) = unpack(&bytes).unwrap();
        assert_eq!(back.to_json(), j);
    }
}
