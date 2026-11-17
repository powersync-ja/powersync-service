//! Experimental BSON-to-SQLite conversion. This follows the direct TypeScript converter,
//! including its raw regex flags, lossy UTF-8, and nested binary omission semantics.
use std::{borrow::Cow, fmt::Write, io::Write as _};

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

pub enum SqliteValue<'a> {
    Null,
    Integer(i64),
    Timestamp(u64),
    Double(f64),
    Text(Cow<'a, str>),
    Blob(&'a [u8]),
}

pub struct ConvertedRow<'a> {
    pub row: Vec<(Cow<'a, str>, SqliteValue<'a>)>,
    /// A BSON document containing only the first _id element. The Node adapter uses
    /// the upstream BSON decoder so replica identities retain their original types.
    pub id_document: Vec<u8>,
}

type Result<T> = std::result::Result<T, String>;
const DEPTH_LIMIT: usize = 20;

pub fn raw_to_sqlite_row(bytes: &[u8], options: ConversionOptions) -> Result<ConvertedRow<'_>> {
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

    fn cstring(&mut self) -> Result<Cow<'a, str>> {
        let length = self.bytes[self.offset..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("Invalid BSON: missing cstring terminator")?;
        let value = text(self.take(length)?);
        self.byte()?;
        Ok(value)
    }

    fn string(&mut self) -> Result<Cow<'a, str>> {
        let length = self.length(1, "Invalid BSON string length")?;
        let bytes = self.take(length)?;
        if bytes[length - 1] != 0 {
            return Err("Invalid BSON string terminator".into());
        }
        Ok(text(&bytes[..length - 1]))
    }

    /// `depth` is present for nested JSON values. Undefined values and non-UUID
    /// binary are handled by `json_value_into`, since their omission depends on placement.
    fn value(
        &mut self,
        kind: u8,
        options: ConversionOptions,
        depth: Option<usize>,
    ) -> Result<SqliteValue<'a>> {
        Ok(match kind {
            0x01 => SqliteValue::Double(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            0x02 | 0x0e => SqliteValue::Text(self.string()?),
            0x03 | 0x04 => SqliteValue::Text(
                self.document_json(kind == 0x04, depth.map_or(0, |d| d + 1), options)?
                    .into(),
            ),
            0x05 => {
                let (subtype, bytes) = self.binary()?;
                if subtype == 4 && bytes.len() == 16 {
                    SqliteValue::Text(uuid(bytes).into())
                } else {
                    SqliteValue::Blob(bytes)
                }
            }
            0x06 | 0x0a | 0x7f | 0xff => SqliteValue::Null,
            0x07 => SqliteValue::Text(hex(self.take(12)?).into()),
            0x08 => SqliteValue::Integer(i64::from(self.byte()? != 0)),
            0x09 => SqliteValue::Text(date_string(self.int64()?, options.date_render_mode)?.into()),
            0x0b => {
                let pattern = self.cstring()?;
                let flags = self.cstring()?;
                SqliteValue::Text(
                    format!(
                        "{{\"pattern\":{},\"options\":{}}}",
                        quote(&pattern),
                        quote(&flags)
                    )
                    .into(),
                )
            }
            0x0c => {
                let namespace = self.string()?;
                let oid = hex(self.take(12)?);
                let (db, collection) = namespace
                    .split_once('.')
                    .map_or((None, namespace.as_ref()), |(db, collection)| {
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
                SqliteValue::Text(json.into())
            }
            0x0d => SqliteValue::Text(
                format!("{{\"code\":{},\"scope\":null}}", quote(&self.string()?)).into(),
            ),
            0x0f => {
                let start = self.offset;
                let length = self.length(14, "Invalid BSON code with scope length")?;
                let code = self.string()?;
                let scope = self.document_json(false, depth.unwrap_or(0) + 1, options)?;
                if self.offset != start + length {
                    return Err("Invalid BSON code with scope length".into());
                }
                SqliteValue::Text(format!("{{\"code\":{},\"scope\":{scope}}}", quote(&code)).into())
            }
            0x10 => SqliteValue::Integer(self.int32()? as i64),
            0x11 => SqliteValue::Timestamp(self.int64()? as u64),
            0x12 => SqliteValue::Integer(self.int64()?),
            // Delegate Decimal128 formatting to the upstream implementation, as
            // the TypeScript converter does. Never round it through an f64.
            0x13 => SqliteValue::Text(
                bson::Decimal128::from_bytes(self.take(16)?.try_into().unwrap())
                    .to_string()
                    .into(),
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
        let mut json = Vec::new();
        self.document_json_into(array, depth, options, &mut json)?;
        // Every fragment below is ASCII or serialized from a valid Rust string.
        Ok(String::from_utf8(json).unwrap())
    }

    fn document_json_into(
        &mut self,
        array: bool,
        depth: usize,
        options: ConversionOptions,
        json: &mut Vec<u8>,
    ) -> Result<()> {
        if depth > DEPTH_LIMIT {
            return Err(format!(
                "json nested object depth exceeds the limit of {DEPTH_LIMIT}"
            ));
        }
        let end = self.document_end()?;
        json.push(if array { b'[' } else { b'{' });
        let mut first = true;
        while self.offset < end - 1 {
            let kind = self.byte()?;
            let key = self.cstring()?;
            let before = json.len();
            if !first {
                json.push(b',');
            }
            if !array {
                quote_into(json, &key);
                json.push(b':');
            }
            let defined = self.json_value_into(kind, depth, options, json)?;
            if self.offset > end - 1 {
                return Err("Invalid BSON document length".into());
            }
            if !defined {
                if array {
                    json.extend_from_slice(b"null");
                } else {
                    json.truncate(before);
                    continue;
                }
            }
            first = false;
        }
        self.byte()?;
        json.push(if array { b']' } else { b'}' });
        Ok(())
    }

    fn json_value_into(
        &mut self,
        kind: u8,
        depth: usize,
        options: ConversionOptions,
        json: &mut Vec<u8>,
    ) -> Result<bool> {
        if kind == 0x06 {
            return Ok(false);
        }
        if kind == 0x05 {
            let (subtype, bytes) = self.binary()?;
            if subtype == 4 && bytes.len() == 16 {
                quote_into(json, &uuid(bytes));
                return Ok(true);
            }
            return Ok(false);
        }
        if kind == 0x08 && options.fixed_booleans_in_json {
            json.extend_from_slice(if self.byte()? != 0 { b"true" } else { b"false" });
            return Ok(true);
        }
        if kind == 0x03 || kind == 0x04 {
            self.document_json_into(kind == 0x04, depth + 1, options, json)?;
            return Ok(true);
        }
        if kind == 0x0f {
            let start = self.offset;
            let length = self.length(14, "Invalid BSON code with scope length")?;
            json.extend_from_slice(b"{\"code\":");
            quote_into(json, &self.string()?);
            json.extend_from_slice(b",\"scope\":");
            self.document_json_into(false, depth + 1, options, json)?;
            json.push(b'}');
            if self.offset != start + length {
                return Err("Invalid BSON code with scope length".into());
            }
            return Ok(true);
        }
        let value = self.value(kind, options, Some(depth))?;
        match value {
            SqliteValue::Null => json.extend_from_slice(b"null"),
            SqliteValue::Integer(value) => write!(json, "{value}").unwrap(),
            SqliteValue::Timestamp(value) => write!(json, "{value}").unwrap(),
            SqliteValue::Double(value) => {
                if value.is_finite() {
                    json.extend_from_slice(ryu_js::Buffer::new().format(value).as_bytes());
                } else {
                    json.extend_from_slice(b"null");
                }
            }
            SqliteValue::Text(value) => {
                if matches!(kind, 0x03 | 0x04 | 0x0b | 0x0c | 0x0d | 0x0f) {
                    json.extend_from_slice(value.as_bytes());
                } else {
                    quote_into(json, &value);
                }
            }
            SqliteValue::Blob(_) => unreachable!("binary handled above"),
        }
        Ok(true)
    }
}

fn text(bytes: &[u8]) -> Cow<'_, str> {
    // Validate without allocating on valid input. The lossy decoder scans even
    // valid strings byte by byte, so reserve it for the invalid UTF-8 fallback.
    if bytes.is_ascii() {
        // SAFETY: Every ASCII byte is a valid, single-byte UTF-8 code point.
        return Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(bytes) });
    }
    match std::str::from_utf8(bytes) {
        Ok(value) => Cow::Borrowed(value),
        Err(_) => String::from_utf8_lossy(bytes),
    }
}

fn quote(value: &str) -> String {
    // Rust strings are valid UTF-8; serde_json handles all control characters.
    serde_json::to_string(value).unwrap()
}

fn quote_into(json: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(json, value).unwrap();
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
    if (0..=9999).contains(&year) {
        // Common dates have a fixed layout. Fill it directly instead of allocating
        // separate strings for the year and fractional seconds with format!.
        let mut output = *b"0000-00-00T00:00:00.000Z";
        two_digits(&mut output[0..2], year / 100);
        two_digits(&mut output[2..4], year % 100);
        two_digits(&mut output[5..7], month);
        two_digits(&mut output[8..10], day);
        if matches!(mode, DateRenderMode::LegacyMilliseconds) {
            output[10] = b' ';
        }
        two_digits(&mut output[11..13], time / 3_600_000);
        two_digits(&mut output[14..16], time / 60_000 % 60);
        two_digits(&mut output[17..19], time / 1000 % 60);
        if matches!(mode, DateRenderMode::IsoSeconds) {
            output[19] = b'Z';
            return Ok(String::from_utf8(output[..20].to_vec()).unwrap());
        }
        output[20] = b'0' + (time % 1000 / 100) as u8;
        two_digits(&mut output[21..23], time % 100);
        return Ok(String::from_utf8(output.to_vec()).unwrap());
    }
    let year = format!("{year:+07}");
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

fn two_digits(output: &mut [u8], value: i64) {
    output[0] = b'0' + (value / 10) as u8;
    output[1] = b'0' + (value % 10) as u8;
}
