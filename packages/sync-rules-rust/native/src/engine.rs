use crate::functions;
use rusqlite::{types::Value, Connection};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Processor {
    pub kind: Kind,
    pub source: u32,
    pub sql: String,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Projection>,
    pub output_count: usize,
    pub parameter_count: usize,
    pub table: String,
    pub bucket_prefix: String,
}

#[derive(Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Data,
    Parameters,
}

#[derive(Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Input {
    Column { column: String },
    Constant { constant: String },
}

#[derive(Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Projection {
    Star(String),
    Expression { index: usize, alias: String },
}

pub type Fields = Vec<(String, Value)>;
#[derive(Default)]
pub struct RowResult {
    pub data: Vec<DataResult>,
    pub parameters: Vec<ParameterResult>,
    pub errors: Vec<(Kind, String)>,
}
pub struct DataResult {
    pub source: u32,
    pub bucket: String,
    pub id: String,
    pub table: String,
    pub data: String,
}
pub struct ParameterResult {
    pub source: u32,
    pub values: Vec<Value>,
    pub rows: Vec<Fields>,
}

pub struct Engine {
    db: Connection,
    processors: Vec<Processor>,
}

impl Engine {
    pub fn new(processors: Vec<Processor>) -> Result<Self, String> {
        let db = Connection::open_in_memory().map_err(|e| e.to_string())?;
        functions::register(&db).map_err(|e| e.to_string())?;
        db.execute_batch("PRAGMA query_only=ON;")
            .map_err(|e| e.to_string())?;
        db.set_prepared_statement_cache_capacity(processors.len().max(1));
        // Validate all metadata before accepting the handle. The native module is an internal API,
        // but malformed plans must report errors rather than cause indexing panics.
        for p in &processors {
            let stmt = db.prepare_cached(&p.sql).map_err(|e| e.to_string())?;
            if !stmt.readonly() {
                return Err("Evaluator statements must be read-only".into());
            }
            if stmt.parameter_count() != p.inputs.len() {
                return Err("SQL parameter count does not match input bindings".into());
            }
            if p.output_count
                .checked_add(p.parameter_count)
                .is_none_or(|n| n > stmt.column_count())
            {
                return Err("Output layout exceeds SQL result columns".into());
            }
            for output in &p.outputs {
                match output {
                    Projection::Star(s) if s != "star" => {
                        return Err("Invalid star projection".into())
                    }
                    Projection::Expression { index, .. } if *index >= p.output_count => {
                        return Err("Invalid projection index".into())
                    }
                    _ => {}
                }
            }
        }
        Ok(Self { db, processors })
    }

    pub fn evaluate(&self, rows: &[Fields]) -> Vec<RowResult> {
        rows.iter().map(|row| self.evaluate_row(row)).collect()
    }

    pub fn evaluate_row(&self, row: &Fields) -> RowResult {
        let mut result = RowResult::default();
        for processor in &self.processors {
            if let Err(error) = self.apply(processor, row, &mut result) {
                result.errors.push((processor.kind, error));
            }
        }
        result
    }

    fn apply(&self, p: &Processor, input: &Fields, result: &mut RowResult) -> Result<(), String> {
        let mut stmt = self.db.prepare_cached(&p.sql).map_err(|e| e.to_string())?;
        let bindings: Vec<Value> = p
            .inputs
            .iter()
            .map(|value| match value {
                Input::Constant { constant } => Ok(Value::Text(constant.clone())),
                Input::Column { column } => input
                    .iter()
                    .find(|(k, _)| k == column)
                    .map(|(_, v)| v.clone())
                    .ok_or_else(|| format!("Missing input column: {column}")),
            })
            .collect::<Result<_, _>>()?;
        // Like node:sqlite Statement.all(), collect all SQL results before projecting. A SQL
        // failure does not expose an earlier partial expansion from this statement.
        let columns = stmt.column_count();
        let evaluated: Vec<Vec<Value>> = stmt
            .query_map(rusqlite::params_from_iter(&bindings), |row| {
                (0..columns).map(|i| row.get(i)).collect()
            })
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<_>>()
            .map_err(|e| e.to_string())?;

        let mut lookups: Vec<ParameterResult> = Vec::new();
        for values in evaluated {
            let parameters = &values[p.output_count..p.output_count + p.parameter_count];
            match p.kind {
                Kind::Data => {
                    if parameters.iter().any(|v| !valid_parameter(v)) {
                        continue;
                    }
                    let mut fields = Fields::new();
                    for projection in &p.outputs {
                        match projection {
                            Projection::Star(_) => {
                                for (name, value) in input {
                                    if json_value(value) {
                                        set_field(&mut fields, name, value.clone());
                                    }
                                }
                            }
                            Projection::Expression { index, alias } => {
                                let value = &values[*index];
                                if json_value(value) {
                                    set_field(&mut fields, alias, value.clone());
                                }
                            }
                        }
                    }
                    let id = fields
                        .iter()
                        .find(|(k, _)| k == "id")
                        .and_then(|(_, v)| functions::text(v))
                        .unwrap_or_default();
                    let bucket = format!("{}{}", p.bucket_prefix, bucket_parameters(parameters));
                    result.data.push(DataResult {
                        source: p.source,
                        bucket,
                        id,
                        table: p.table.clone(),
                        data: serialize_fields(&fields),
                    });
                }
                Kind::Parameters => {
                    if values.iter().any(|v| !valid_parameter(v)) {
                        continue;
                    }
                    let fields: Fields = values[..p.output_count]
                        .iter()
                        .enumerate()
                        .map(|(i, v)| (i.to_string(), v.clone()))
                        .collect();
                    // Preserve JS strict equality while grouping, before lookup-number normalization.
                    if let Some(existing) = lookups.iter_mut().find(|l| l.values == parameters) {
                        existing.rows.push(fields);
                    } else {
                        lookups.push(ParameterResult {
                            source: p.source,
                            values: parameters.to_vec(),
                            rows: vec![fields],
                        });
                    }
                }
            }
        }
        result.parameters.extend(lookups);
        Ok(())
    }
}

fn json_value(v: &Value) -> bool {
    !matches!(v, Value::Blob(_))
}

// Match JavaScript object enumeration: array-index keys precede other keys, which
// retain insertion order. This matters for the checksum of the serialized payload.
fn array_index(name: &str) -> Option<u32> {
    let index = name.parse::<u32>().ok()?;
    (index != u32::MAX && index.to_string() == name).then_some(index)
}

fn serialize_fields(fields: &Fields) -> String {
    let mut ordered: Vec<_> = fields.iter().collect();
    ordered.sort_by_key(|(name, _)| array_index(name).map_or((1, 0), |index| (0, index)));
    let mut output = String::from("{");
    for (i, (name, value)) in ordered.into_iter().enumerate() {
        if i != 0 {
            output.push(',');
        }
        output.push_str(&serde_json::to_string(name).expect("String serialization is infallible"));
        output.push(':');
        match value {
            Value::Integer(value) => output.push_str(&value.to_string()),
            Value::Real(value) => {
                let text = functions::js_number(*value);
                output.push_str(&text);
                // JSONBig preserves SQLite REAL vs INTEGER using a decimal suffix.
                if value.is_finite() && !text.contains(['.', 'e', 'E']) {
                    output.push_str(".0");
                }
            }
            Value::Text(value) => output.push_str(
                &serde_json::to_string(value).expect("String serialization is infallible"),
            ),
            _ => output.push_str("null"),
        }
    }
    output.push('}');
    output
}
fn valid_parameter(v: &Value) -> bool {
    !matches!(v, Value::Blob(_) | Value::Null)
}

fn set_field(fields: &mut Fields, name: &str, value: Value) {
    if let Some((_, existing)) = fields.iter_mut().find(|(k, _)| k == name) {
        *existing = value;
    } else {
        fields.push((name.to_owned(), value));
    }
}

fn bucket_parameters(values: &[Value]) -> String {
    let parts: Vec<_> = values
        .iter()
        .map(|v| match v {
            Value::Integer(i) => i.to_string(),
            Value::Real(f) if f.is_finite() => functions::js_number(*f),
            Value::Text(s) => serde_json::to_string(s).expect("String serialization is infallible"),
            _ => "null".into(),
        })
        .collect();
    format!("[{}]", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan(sql: &str) -> Processor {
        Processor {
            kind: Kind::Data,
            source: 0,
            sql: sql.into(),
            inputs: vec![],
            outputs: vec![],
            output_count: 0,
            parameter_count: 0,
            table: "test".into(),
            bucket_prefix: "b".into(),
        }
    }
    #[test]
    fn rejects_malformed_plans() {
        assert!(Engine::new(vec![plan("invalid")]).is_err());
        assert!(Engine::new(vec![plan("CREATE TABLE x(a)")]).is_err());
        assert!(Engine::new(vec![plan("SELECT ?1")]).is_err());
        let mut p = plan("SELECT 1");
        p.output_count = usize::MAX;
        p.parameter_count = 1;
        assert!(Engine::new(vec![p]).is_err());
    }
    #[test]
    fn bucket_identity_normalizes_numbers() {
        assert_eq!(
            bucket_parameters(&[Value::Integer(1)]),
            bucket_parameters(&[Value::Real(1.0)])
        );
        assert_eq!(bucket_parameters(&[Value::Real(-0.0)]), "[0]");
        assert_eq!(
            bucket_parameters(&[Value::Integer(i64::MAX)]),
            "[9223372036854775807]"
        );
    }
}
