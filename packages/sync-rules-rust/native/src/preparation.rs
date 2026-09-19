//! MongoDB identity and checksum preparation, performed before returning to JavaScript.
use bson::{doc, oid::ObjectId, raw::RawDocument, spec::BinarySubtype, Bson, Document};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const ID_NAMESPACE: Uuid = Uuid::from_u128(0xa396dd9109fc4017a28d3df722f651e9);

pub struct Identity {
    pub bson: Vec<u8>,
    pub subkey: String,
    pub delete_checksum: u32,
}

pub fn hash_data(table: &str, id: &str, data: &str) -> u32 {
    checksum(&[
        b"put.",
        table.as_bytes(),
        b".",
        id.as_bytes(),
        b".",
        data.as_bytes(),
    ])
}

fn checksum(parts: &[&[u8]]) -> u32 {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part);
    }
    let digest = hash.finalize();
    u32::from_le_bytes(digest[..4].try_into().expect("SHA-256 has 32 bytes"))
}

pub fn identity(bytes: &[u8], table: ObjectId) -> Result<Identity, String> {
    let document = RawDocument::from_bytes(bytes).map_err(|e| e.to_string())?;
    let mut offset = 4;
    for element in document.iter_elements() {
        let element = element.map_err(|e| e.to_string())?;
        let end = offset + 1 + element.key().len() + 1 + element.len();
        if element.key() != "_id" {
            offset = end;
            continue;
        }
        // Preserve the original BSON type/bytes for storage, independently of the
        // deserialize/reserialize normalization used by the historical subkey hash.
        let mut bson = Vec::with_capacity(end - offset + 5);
        bson.extend_from_slice(&((end - offset + 5) as i32).to_le_bytes());
        bson.extend_from_slice(&bytes[offset..end]);
        bson.push(0);
        let id: Bson = element
            .value()
            .map_err(|e| e.to_string())?
            .try_into()
            .map_err(|e: bson::raw::Error| e.to_string())?;
        let subkey = if let Bson::Binary(binary) = &id {
            if binary.subtype == BinarySubtype::Uuid {
                let uuid = Uuid::from_slice(&binary.bytes).map_err(|e| e.to_string())?;
                format!("{}/{}", table.to_hex(), uuid.hyphenated())
            } else {
                hashed_subkey(table, id)?
            }
        } else {
            hashed_subkey(table, id)?
        };
        return Ok(Identity {
            bson,
            delete_checksum: checksum(&[b"delete.", subkey.as_bytes()]),
            subkey,
        });
    }
    Err("Attempt to parse document without _id".into())
}

fn hashed_subkey(table: ObjectId, id: Bson) -> Result<String, String> {
    let repr = bson::to_vec(&doc! { "table": table, "id": normalize_id(id)? })
        .map_err(|e| e.to_string())?;
    Ok(Uuid::new_v5(&ID_NAMESPACE, &repr).to_string())
}

// JS BSON.deserialize({ useBigInt64: true }) promotes doubles/int32 to Number,
// then BSON.serialize writes integral Numbers in int32 range as int32 (except -0).
// Embedded object keys follow JS enumeration order. Int64 remains an Int64 bigint.
fn normalize_id(value: Bson) -> Result<Bson, String> {
    Ok(match value {
        Bson::Double(v)
            if v.fract() == 0.0
                && v >= i32::MIN as f64
                && v <= i32::MAX as f64
                && !(v == 0.0 && v.is_sign_negative()) =>
        {
            Bson::Int32(v as i32)
        }
        Bson::Symbol(v) => Bson::String(v),
        // JS drops undefined object fields when reserializing BSON. Reject this
        // deprecated identity type until context-sensitive omission is supported.
        Bson::Undefined => return Err("Undefined BSON replica IDs are not supported".into()),
        Bson::Document(doc) => {
            let mut fields: Vec<_> = doc.into_iter().collect();
            fields.sort_by_key(|(key, _)| {
                key.parse::<u32>()
                    .ok()
                    .filter(|i| *i != u32::MAX && i.to_string() == *key)
                    .map_or((1, 0), |i| (0, i))
            });
            let mut result = Document::new();
            for (key, value) in fields {
                if key == "__proto__" {
                    return Err("Prototype-sensitive replica ID keys are not supported".into());
                }
                result.insert(key, normalize_id(value)?);
            }
            Bson::Document(result)
        }
        Bson::Array(values) => Bson::Array(
            values
                .into_iter()
                .map(normalize_id)
                .collect::<Result<_, _>>()?,
        ),
        Bson::RegularExpression(_)
        | Bson::JavaScriptCode(_)
        | Bson::JavaScriptCodeWithScope(_)
        | Bson::DbPointer(_) => {
            return Err("Unsupported BSON replica ID type".into());
        }
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_protocol_vectors() {
        // Golden values from service-core's existing Node crypto implementation.
        assert_eq!(hash_data("docs", "x", "{}"), 2269995260);
        assert_eq!(
            checksum(&[b"delete.a396dd91-09fc-4017-a28d-3df722f651e9"]),
            3707386091
        );
    }

    #[test]
    fn identity_validation_and_normalization() {
        let table = ObjectId::parse_str("66e834cc91d805df11fa0ecb").unwrap();
        assert!(identity(&[], table).is_err());
        assert!(identity(&bson::to_vec(&doc! { "x": 1 }).unwrap(), table).is_err());
        let mut invalid_element = bson::to_vec(&doc! { "_id": 1 }).unwrap();
        invalid_element[4] = 0x77;
        assert!(identity(&invalid_element, table).is_err());
        let invalid_uuid = bson::to_vec(&doc! { "_id": Bson::Binary(bson::Binary { subtype: BinarySubtype::Uuid, bytes: vec![1] }) }).unwrap();
        assert!(identity(&invalid_uuid, table).is_err());
        let mut fields = Document::new();
        fields.insert("10", Bson::Double(2.0));
        fields.insert("2", Bson::Null);
        fields.insert("a", Bson::Symbol("x".into()));
        let normalized = normalize_id(Bson::Document(fields)).unwrap();
        assert_eq!(
            bson::to_vec(&doc! { "id": normalized }).unwrap(),
            bson::to_vec(&doc! { "id": { "2": Bson::Null, "10": 2, "a": "x" } }).unwrap()
        );
        assert_eq!(
            normalize_id(Bson::Double(2147483648.0)).unwrap(),
            Bson::Double(2147483648.0)
        );
        assert!(normalize_id(Bson::JavaScriptCode("x".into())).is_err());
        assert!(normalize_id(Bson::Undefined).is_err());
    }
}
