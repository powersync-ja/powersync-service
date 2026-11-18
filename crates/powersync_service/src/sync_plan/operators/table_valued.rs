use crate::sync_plan::{EvaluationError, SqliteValue, evaluator::Frame, processor_group::Slot};
use bumpalo::{
    Bump,
    collections::{String as ArenaString, Vec as ArenaVec},
};
use serde_json::Value as JsonValue;

pub struct TableValuedFunctionEvaluation<'a> {
    outputs: &'a [Slot],
    state: FunctionState<'a>,
}

enum FunctionState<'a> {
    JsonEach(JsonEachState<'a>),
}

impl<'a> TableValuedFunctionEvaluation<'a> {
    pub fn json_each(
        frame: &Frame<'a>,
        inputs: &'a [Slot],
        outputs: &'a [Slot],
    ) -> Result<Self, EvaluationError<'a>> {
        assert!(outputs.len() == 2); // key, value

        if inputs.len() != 1 {
            return Err(EvaluationError::ParameterCount {
                name: "json_each",
                expected: 1,
                actual: inputs.len(),
            });
        }

        let value = frame[inputs[0]];
        let state = match value {
            SqliteValue::Null => FunctionState::JsonEach(Default::default()),
            SqliteValue::Text(src) => {
                let mut state = JsonEachState::default();
                // serde_json::from_str allocates outside of the frame. We could avoid most
                // allocations by using a visitor, but that is substantially more code.
                state.add_from_json(serde_json::from_str(src)?, frame);

                FunctionState::JsonEach(state)
            }
            _ => {
                return Err(EvaluationError::WrongType {
                    name: "json_each",
                    value,
                });
            }
        };

        Ok(Self { outputs, state })
    }

    /// If the table-valued function has another row, returns true and writes output into the slots
    /// buffer.
    pub fn next(&mut self, context: &mut Frame<'a>) -> bool {
        match self.state {
            FunctionState::JsonEach(ref mut json) => {
                if json.index < json.entries.len() {
                    let (k, v) = json.entries[json.index];

                    context[self.outputs[0]] = k;
                    context[self.outputs[1]] = v;
                    json.index += 1;
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Default)]
struct JsonEachState<'a> {
    index: usize,
    /// `(key, value)` pairs. Keys are indices for arrays, property names for objects and `NULL`
    /// for scalars.
    entries: &'a [(SqliteValue<'a>, SqliteValue<'a>)],
}

impl<'a> JsonEachState<'a> {
    fn add_from_json(&mut self, value: JsonValue, frame: &Frame<'a>) {
        let arena = frame.arena();
        let fixed_json_extract = frame.compatibility().fixed_json_extract;
        let convert = |value| json_value_to_sqlite(arena, fixed_json_extract, value);

        self.entries = match value {
            JsonValue::Array(values) => arena.alloc_slice_fill_iter(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| (SqliteValue::Integer(index as i64), convert(value))),
            ),
            JsonValue::Object(entries) => arena.alloc_slice_fill_iter(
                entries
                    .into_iter()
                    .map(|(key, value)| (SqliteValue::Text(arena.alloc_str(&key)), convert(value))),
            ),
            scalar => std::slice::from_ref(arena.alloc((SqliteValue::Null, convert(scalar)))),
        };
    }
}

/// Converts a JSON value into a SQLite value, like `jsonValueToSqlite` in the TypeScript
/// implementation.
///
/// Strings are copied into the arena, nested arrays and objects are encoded as JSON text.
fn json_value_to_sqlite<'a>(
    arena: &'a Bump,
    fixed_json_extract: bool,
    value: JsonValue,
) -> SqliteValue<'a> {
    match value {
        JsonValue::Null if fixed_json_extract => SqliteValue::Null,
        JsonValue::Bool(value) => SqliteValue::Integer(value as i64),
        JsonValue::Number(number) => match number.as_i64() {
            Some(value) => SqliteValue::Integer(value),
            None => SqliteValue::Float(number.as_f64().unwrap_or(f64::NAN)),
        },
        JsonValue::String(value) => SqliteValue::Text(arena.alloc_str(&value)),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => {
            let mut buffer = ArenaVec::new_in(arena);
            serde_json::to_writer(&mut buffer, &value).expect("writing to a buffer can't fail");
            let text = ArenaString::from_utf8(buffer).expect("serde_json emits valid UTF-8");
            SqliteValue::Text(text.into_bump_str())
        }
    }
}

#[cfg(test)]
mod test {
    use bumpalo::Bump;

    use super::TableValuedFunctionEvaluation;
    use crate::sync_plan::{
        CompatibilityContext, SqliteValue, evaluator::Frame, processor_group::Slot,
    };

    const INPUTS: [Slot; 1] = [Slot::Input(0)];
    const OUTPUTS: [Slot; 2] = [Slot::Internal(0), Slot::Internal(1)];

    fn json_each_with<'a>(
        arena: &'a Bump,
        compatibility: &'a CompatibilityContext,
        source: SqliteValue<'a>,
    ) -> Vec<(SqliteValue<'a>, SqliteValue<'a>)> {
        let mut frame = Frame::new(arena, compatibility, 1, 2);
        frame[INPUTS[0]] = source;

        let mut evaluation =
            TableValuedFunctionEvaluation::json_each(&frame, &INPUTS, &OUTPUTS).unwrap();
        let mut rows = Vec::new();
        while evaluation.next(&mut frame) {
            rows.push((frame[OUTPUTS[0]], frame[OUTPUTS[1]]));
        }
        rows
    }

    fn json_each(source: &str) -> Vec<(String, String)> {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default_for_tests();
        json_each_with(&arena, &compatibility, SqliteValue::Text(source))
            .into_iter()
            .map(|(k, v)| (format!("{k:?}"), format!("{v:?}")))
            .collect()
    }

    fn rows(rows: &[(SqliteValue, SqliteValue)]) -> Vec<(String, String)> {
        rows.iter()
            .map(|(k, v)| (format!("{k:?}"), format!("{v:?}")))
            .collect()
    }

    #[test]
    fn null_input() {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default_for_tests();
        assert!(json_each_with(&arena, &compatibility, SqliteValue::Null).is_empty());
    }

    #[test]
    fn array() {
        use SqliteValue::*;

        assert_eq!(
            json_each(r#"[1, 2.5, "a", "\"escaped\"", true, false, null, 18446744073709551615]"#),
            rows(&[
                (Integer(0), Integer(1)),
                (Integer(1), Float(2.5)),
                (Integer(2), Text("a")),
                (Integer(3), Text("\"escaped\"")),
                (Integer(4), Integer(1)),
                (Integer(5), Integer(0)),
                (Integer(6), Null),
                (Integer(7), Float(18446744073709551615.0)),
            ])
        );
    }

    #[test]
    fn object() {
        use SqliteValue::*;

        assert_eq!(
            json_each(r#"{"b": 1, "a\n": "x"}"#),
            rows(&[(Text("b"), Integer(1)), (Text("a\n"), Text("x"))])
        );
    }

    #[test]
    fn scalar() {
        use SqliteValue::*;

        assert_eq!(json_each("42"), rows(&[(Null, Integer(42))]));
        assert_eq!(json_each(r#""str""#), rows(&[(Null, Text("str"))]));
    }

    #[test]
    fn nested_values_are_stringified() {
        use SqliteValue::*;

        assert_eq!(
            json_each(
                r#"[ {"z": [1, 2.0, 1e300, 1e-7, "\u0001\"\n"], "a": {} }, [], [null, true] ]"#
            ),
            rows(&[
                (
                    Integer(0),
                    Text(r#"{"z":[1,2.0,1e+300,1e-7,"\u0001\"\n"],"a":{}}"#)
                ),
                (Integer(1), Text("[]")),
                (Integer(2), Text("[null,true]")),
            ])
        );
    }

    #[test]
    fn legacy_null_behavior() {
        let arena = Bump::new();
        let mut compatibility = CompatibilityContext::default_for_tests();
        compatibility.fixed_json_extract = false;

        let rows = json_each_with(&arena, &compatibility, SqliteValue::Text("[null]"));
        assert_eq!(rows, [(SqliteValue::Integer(0), SqliteValue::Text("null"))]);
    }

    #[test]
    fn invalid_json() {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default_for_tests();
        let mut frame = Frame::new(&arena, &compatibility, 1, 2);

        for source in ["[1,", "{} trailing", ""] {
            frame[INPUTS[0]] = SqliteValue::Text(source);
            assert!(TableValuedFunctionEvaluation::json_each(&frame, &INPUTS, &OUTPUTS).is_err());
        }
    }
}
