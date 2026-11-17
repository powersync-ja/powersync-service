//! Experimental BSON-to-SQLite conversion. This follows the direct TypeScript converter,
//! including its raw regex flags, lossy UTF-8, and nested binary omission semantics.
use std::fmt::Write;

#[derive(Clone, Copy)]
pub enum DateRenderMode {
    LegacyMilliseconds,
    IsoMilliseconds,
    IsoSeconds,
}

#[derive(Clone, Copy)]
pub struct ConversionOptions {
    pub date_render_mode: DateRenderMode,
    pub fixed_booleans_in_json: bool,
}

pub enum SqliteValue {
    Null,
    Integer(i64),
    Timestamp(u64),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub struct ConvertedRow {
    pub row: Vec<(String, SqliteValue)>,
    /// A BSON document containing only the first _id element. The Node adapter uses
    /// the upstream BSON decoder so replica identities retain their original types.
    pub id_document: Vec<u8>,
}

type Result<T> = std::result::Result<T, String>;
const DEPTH_LIMIT: usize = 20;

pub fn raw_to_sqlite_row(bytes: &[u8], options: ConversionOptions) -> Result<ConvertedRow> {
    let mut reader = Reader { bytes, offset: 0 };
    let end = reader.document_end()?;
    let mut row = Vec::new();
    let mut id_document = None;
    while reader.offset < end - 1 {
        let start = reader.offset;
        let kind = reader.byte()?;
        let key = reader.cstring()?;
        let value = reader.value(kind, options, None)?;
        if reader.offset > end - 1 {
            return Err("Invalid BSON document length".into());
        }
        if key == "_id" && id_document.is_none() {
            let mut id = Vec::with_capacity(reader.offset - start + 5);
            id.extend_from_slice(&((reader.offset - start + 5) as i32).to_le_bytes());
            id.extend_from_slice(&bytes[start..reader.offset]);
            id.push(0);
            id_document = Some(id);
        }
        row.push((key, value));
    }
    Ok(ConvertedRow {
        row,
        id_document: id_document.ok_or("Attempt to parse document without _id")?,
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or("Invalid BSON length")?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or("Truncated BSON value")?;
        self.offset = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn int32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn int64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn length(&mut self, minimum: usize, message: &str) -> Result<usize> {
        let length = self.int32()?;
        if length < 0 || (length as usize) < minimum {
            return Err(message.into());
        }
        Ok(length as usize)
    }

    fn document_end(&mut self) -> Result<usize> {
        let start = self.offset;
        let length = self.length(5, "Invalid BSON document length")?;
        let end = start
            .checked_add(length)
            .ok_or("Invalid BSON document length")?;
        if end > self.bytes.len() {
            return Err("Invalid BSON document length".into());
        }
        if self.bytes[end - 1] != 0 {
            return Err("Invalid BSON document terminator".into());
        }
        Ok(end)
    }

    fn cstring(&mut self) -> Result<String> {
        let length = self.bytes[self.offset..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("Invalid BSON: missing cstring terminator")?;
        let value = String::from_utf8_lossy(self.take(length)?).into_owned();
        self.byte()?;
        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        let length = self.length(1, "Invalid BSON string length")?;
        let bytes = self.take(length)?;
        if bytes[length - 1] != 0 {
            return Err("Invalid BSON string terminator".into());
        }
        Ok(String::from_utf8_lossy(&bytes[..length - 1]).into_owned())
    }

    /// `depth` is present for nested JSON values. Undefined values and non-UUID
    /// binary are handled by `json_value`, since their omission depends on placement.
    fn value(
        &mut self,
        kind: u8,
        options: ConversionOptions,
        depth: Option<usize>,
    ) -> Result<SqliteValue> {
        Ok(match kind {
            0x01 => SqliteValue::Double(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            0x02 | 0x0e => SqliteValue::Text(self.string()?),
            0x03 | 0x04 => SqliteValue::Text(self.document_json(
                kind == 0x04,
                depth.map_or(0, |d| d + 1),
                options,
            )?),
            0x05 => {
                let (subtype, bytes) = self.binary()?;
                if subtype == 4 && bytes.len() == 16 {
                    SqliteValue::Text(uuid(bytes))
                } else {
                    SqliteValue::Blob(bytes.to_vec())
                }
            }
            0x06 | 0x0a | 0x7f | 0xff => SqliteValue::Null,
            0x07 => SqliteValue::Text(hex(self.take(12)?)),
            0x08 => SqliteValue::Integer(i64::from(self.byte()? != 0)),
            0x09 => SqliteValue::Text(date_string(self.int64()?, options.date_render_mode)?),
            0x0b => {
                let pattern = self.cstring()?;
                let flags = self.cstring()?;
                SqliteValue::Text(format!(
                    "{{\"pattern\":{},\"options\":{}}}",
                    quote(&pattern),
                    quote(&flags)
                ))
            }
            0x0c => {
                let namespace = self.string()?;
                let oid = hex(self.take(12)?);
                let (db, collection) = namespace
                    .split_once('.')
                    .map_or((None, namespace.as_str()), |(db, collection)| {
                        (Some(db), collection)
                    });
                let mut json = format!(
                    "{{\"collection\":{},\"oid\":{}",
                    quote(collection),
                    quote(&oid)
                );
                if let Some(db) = db {
                    write!(json, ",\"db\":{}", quote(db)).unwrap();
                }
                json.push_str(",\"fields\":{}}");
                SqliteValue::Text(json)
            }
            0x0d => SqliteValue::Text(format!(
                "{{\"code\":{},\"scope\":null}}",
                quote(&self.string()?)
            )),
            0x0f => {
                let start = self.offset;
                let length = self.length(14, "Invalid BSON code with scope length")?;
                let code = self.string()?;
                let scope = self.document_json(false, depth.unwrap_or(0) + 1, options)?;
                if self.offset != start + length {
                    return Err("Invalid BSON code with scope length".into());
                }
                SqliteValue::Text(format!("{{\"code\":{},\"scope\":{scope}}}", quote(&code)))
            }
            0x10 => SqliteValue::Integer(self.int32()? as i64),
            0x11 => SqliteValue::Timestamp(self.int64()? as u64),
            0x12 => SqliteValue::Integer(self.int64()?),
            // Delegate Decimal128 formatting to the upstream implementation, as
            // the TypeScript converter does. Never round it through an f64.
            0x13 => SqliteValue::Text(
                bson::Decimal128::from_bytes(self.take(16)?.try_into().unwrap()).to_string(),
            ),
            _ => return Err(format!("Unsupported BSON type: 0x{kind:x}")),
        })
    }

    fn binary(&mut self) -> Result<(u8, &'a [u8])> {
        let length = self.length(0, "Invalid BSON binary length")?;
        let subtype = self.byte()?;
        let mut bytes = self.take(length)?;
        if subtype == 2 {
            let raw_length = bytes.get(..4).ok_or("Invalid BSON legacy binary length")?;
            let length = i32::from_le_bytes(raw_length.try_into().unwrap());
            if length < 0 {
                return Err("Invalid BSON legacy binary length".into());
            }
            bytes = bytes
                .get(4..4 + length as usize)
                .ok_or("Invalid BSON legacy binary length")?;
        }
        Ok((subtype, bytes))
    }

    fn document_json(
        &mut self,
        array: bool,
        depth: usize,
        options: ConversionOptions,
    ) -> Result<String> {
        if depth > DEPTH_LIMIT {
            return Err(format!(
                "json nested object depth exceeds the limit of {DEPTH_LIMIT}"
            ));
        }
        let end = self.document_end()?;
        let mut json = String::from(if array { "[" } else { "{" });
        let mut first = true;
        while self.offset < end - 1 {
            let kind = self.byte()?;
            let key = self.cstring()?;
            let value = self.json_value(kind, depth, options)?;
            if self.offset > end - 1 {
                return Err("Invalid BSON document length".into());
            }
            if value.is_none() && !array {
                continue;
            }
            if !first {
                json.push(',');
            }
            first = false;
            if !array {
                json.push_str(&quote(&key));
                json.push(':');
            }
            json.push_str(value.as_deref().unwrap_or("null"));
        }
        self.byte()?;
        json.push(if array { ']' } else { '}' });
        Ok(json)
    }

    fn json_value(
        &mut self,
        kind: u8,
        depth: usize,
        options: ConversionOptions,
    ) -> Result<Option<String>> {
        if kind == 0x06 {
            return Ok(None);
        }
        if kind == 0x05 {
            let (subtype, bytes) = self.binary()?;
            return Ok((subtype == 4 && bytes.len() == 16).then(|| quote(&uuid(bytes))));
        }
        if kind == 0x08 && options.fixed_booleans_in_json {
            return Ok(Some(
                if self.byte()? != 0 { "true" } else { "false" }.into(),
            ));
        }
        let value = self.value(kind, options, Some(depth))?;
        Ok(Some(match value {
            SqliteValue::Null => "null".into(),
            SqliteValue::Integer(value) => value.to_string(),
            SqliteValue::Timestamp(value) => value.to_string(),
            SqliteValue::Double(value) => {
                if value.is_finite() {
                    ryu_js::Buffer::new().format(value).to_owned()
                } else {
                    "null".into()
                }
            }
            SqliteValue::Text(value) => {
                if matches!(kind, 0x03 | 0x04 | 0x0b | 0x0c | 0x0d | 0x0f) {
                    value
                } else {
                    quote(&value)
                }
            }
            SqliteValue::Blob(_) => unreachable!("binary handled above"),
        }))
    }
}

fn quote(value: &str) -> String {
    // Rust strings are valid UTF-8; serde_json handles all control characters.
    serde_json::to_string(value).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

fn uuid(bytes: &[u8]) -> String {
    let h = hex(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

fn date_string(millis: i64, mode: DateRenderMode) -> Result<String> {
    // Match JavaScript Date's TimeClip range, including expanded ISO years.
    if !(-8_640_000_000_000_000..=8_640_000_000_000_000).contains(&millis) {
        return Err("Invalid time value".into());
    }
    let days = millis.div_euclid(86_400_000);
    let time = millis.rem_euclid(86_400_000);
    // Proleptic Gregorian civil date from days since 1970-01-01 (400-year eras).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else {
        format!("{year:+07}")
    };
    let separator = if matches!(mode, DateRenderMode::LegacyMilliseconds) {
        ' '
    } else {
        'T'
    };
    let mut output = format!(
        "{year}-{month:02}-{day:02}{separator}{:02}:{:02}:{:02}",
        time / 3_600_000,
        time / 60_000 % 60,
        time / 1000 % 60
    );
    if !matches!(mode, DateRenderMode::IsoSeconds) {
        write!(output, ".{:03}", time % 1000).unwrap();
    }
    output.push('Z');
    Ok(output)
}
