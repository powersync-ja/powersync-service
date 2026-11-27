use crate::sync_plan::{
    EvaluationError, SqliteValue,
    evaluation::Frame,
    processor_group::Slot,
    sources::json::{ScalarSqliteValueOrFail, ScalarSqliteValueOrText},
};
use bumpalo::collections::Vec;
use serde::{
    Deserializer,
    de::{DeserializeSeed, Visitor},
};
use serde_json::Deserializer as JsonDeserializer;

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
            SqliteValue::Null { is_undefined: _ } => FunctionState::JsonEach(Default::default()),
            SqliteValue::Text(src) => {
                let src: &'a str = src;
                let state = JsonEachState::parse_json(src, frame)?;

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
    fn parse_json(json: &'a str, frame: &Frame<'a>) -> Result<Self, EvaluationError<'a>> {
        // First, is this a scalar value?
        let mut deserializer = JsonDeserializer::from_str(json);
        if let Ok(value) = ScalarSqliteValueOrFail(frame.into()).deserialize(&mut deserializer) {
            deserializer.end()?;

            // It is, we report that as a single (null, scalar value) entry.
            let entries = frame.arena.alloc_slice_copy(&[(
                SqliteValue::Null {
                    is_undefined: false,
                },
                value,
            )]);

            return Ok(Self { index: 0, entries });
        }

        // Ok, it isn't. Handle maps and sequences then.
        let mut deserializer = JsonDeserializer::from_str(json);

        struct DeserializeToEntries<'a, 'b> {
            frame: &'b Frame<'a>,
        }

        /// Parses sequences and maps for `json_each`. Scalar values are parsed, nested JSON
        /// elements are preserved as strings.
        impl<'a, 'b> Visitor<'a> for DeserializeToEntries<'a, 'b> {
            type Value = &'a [(SqliteValue<'a>, SqliteValue<'a>)];

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "an array or object")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'a>,
            {
                let mut vec = Vec::new_in(self.frame.arena);
                let mut i = 0;
                while let Some(value) =
                    seq.next_element_seed(ScalarSqliteValueOrText(self.frame.into()))?
                {
                    vec.push((SqliteValue::Integer(i), value));
                    i += 1;
                }

                Ok(vec.into_bump_slice())
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'a>,
            {
                let mut vec = Vec::new_in(self.frame.arena);
                while let Some((key, value)) = map.next_entry_seed(
                    // The key must be a string, values can be nested (in which case we don't parse
                    // them).
                    ScalarSqliteValueOrFail(self.frame.into()),
                    ScalarSqliteValueOrText(self.frame.into()),
                )? {
                    vec.push((key, value));
                }

                // Remove duplicate keys, with the last entry winning.
                vec.sort_by_key(|(k, _)| match k {
                    SqliteValue::Text(k) => *k,
                    _ => "", // Unreachable for valid json
                });
                vec.dedup_by(|later, earlier| {
                    let is_duplicate = later.0 == earlier.0;
                    if is_duplicate {
                        earlier.1 = later.1;
                    }
                    is_duplicate
                });

                Ok(vec.into_bump_slice())
            }
        }

        let entries = deserializer.deserialize_any(DeserializeToEntries { frame })?;
        deserializer.end()?;

        Ok(Self { index: 0, entries })
    }
}

#[cfg(test)]
mod test {
    use bumpalo::Bump;

    use super::TableValuedFunctionEvaluation;
    use crate::sync_plan::{
        CompatibilityContext, SqliteValue, evaluation::Frame, processor_group::Slot,
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
        let compatibility = CompatibilityContext::default();
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
        let compatibility = CompatibilityContext::default();
        assert!(
            json_each_with(
                &arena,
                &compatibility,
                SqliteValue::Null {
                    is_undefined: false
                }
            )
            .is_empty()
        );
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
                (
                    Integer(6),
                    Null {
                        is_undefined: false
                    }
                ),
                (Integer(7), Float(18446744073709551615.0)),
            ])
        );
    }

    #[test]
    fn object() {
        use SqliteValue::*;

        assert_eq!(
            json_each(r#"{"b": 1, "a\n": "x"}"#),
            rows(&[(Text("a\n"), Text("x")), (Text("b"), Integer(1))])
        );
    }

    #[test]
    fn object_duplicate_keys() {
        use SqliteValue::*;

        assert_eq!(
            json_each(r#"{"b": 1, "a": 2, "b": 3, "c": 4, "b": [5], "a": null}"#),
            rows(&[
                (
                    Text("a"),
                    Null {
                        is_undefined: false
                    }
                ),
                (Text("b"), Text("[5]")),
                (Text("c"), Integer(4))
            ])
        );
        // Escaped keys are compared after unescaping.
        assert_eq!(
            json_each(r#"{"\u0061": 1, "a": 2}"#),
            rows(&[(Text("a"), Integer(2))])
        );
    }

    #[test]
    fn scalar() {
        use SqliteValue::*;

        assert_eq!(
            json_each("42"),
            rows(&[(
                Null {
                    is_undefined: false
                },
                Integer(42)
            )])
        );
        assert_eq!(
            json_each(r#""str""#),
            rows(&[(
                Null {
                    is_undefined: false
                },
                Text("str")
            )])
        );
    }

    #[test]
    fn nested_values_are_preserved() {
        use SqliteValue::*;

        assert_eq!(
            json_each(
                r#"[ {"z": [1, 2.0, 1e300, 1e-7, "\u0001\"\n"], "a": {} }, [], [null, true] ]"#
            ),
            rows(&[
                (
                    Integer(0),
                    Text(r#"{"z": [1, 2.0, 1e300, 1e-7, "\u0001\"\n"], "a": {} }"#)
                ),
                (Integer(1), Text("[]")),
                (Integer(2), Text("[null, true]")),
            ])
        );
    }

    #[test]
    fn legacy_null_behavior() {
        let arena = Bump::new();
        let compatibility = CompatibilityContext {
            fixed_json_extract: false,
            ..Default::default()
        };

        let rows = json_each_with(&arena, &compatibility, SqliteValue::Text("[null]"));
        assert_eq!(rows, [(SqliteValue::Integer(0), SqliteValue::Text("null"))]);
    }

    #[test]
    fn invalid_json() {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default();
        let mut frame = Frame::new(&arena, &compatibility, 1, 2);

        for source in ["[1,", "{} trailing", ""] {
            frame[INPUTS[0]] = SqliteValue::Text(source);
            assert!(TableValuedFunctionEvaluation::json_each(&frame, &INPUTS, &OUTPUTS).is_err());
        }
    }
}
