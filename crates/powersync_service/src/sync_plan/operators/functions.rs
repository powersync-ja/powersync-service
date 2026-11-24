use bumpalo::collections::String;
use serde::{
    Deserializer,
    de::{SeqAccess, Visitor},
};

use crate::sync_plan::{
    EvaluationError,
    SqliteValue::{self, Blob, Float, Integer, Null, Text},
    cast::{cast_as_integer, cast_as_text},
    evaluation::Frame,
    processor_group::Slot,
    sources::json::{ScalarSqliteValueOrText, SqliteValueFromJson},
};

/// The implementation of a scalar function, evaluating it with arguments stored in the given slots.
pub type ScalarFunctionImpl = for<'a> fn(&[Slot], &Frame<'a>) -> ScalarFunctionResult<'a>;

pub type ScalarFunctionResult<'a> = Result<SqliteValue<'a>, EvaluationError<'a>>;

pub fn upper<'a>(slots: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    let [value] = arguments("upper", slots, frame)?;
    let Some(text) = cast_as_text(value, frame.arena) else {
        return Ok(SqliteValue::null());
    };

    let mut result = String::new_in(frame.arena);
    for c in text.chars() {
        for c in c.to_uppercase() {
            result.push(c);
        }
    }

    Ok(Text(result.into_bump_str()))
}

pub fn lower<'a>(slots: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    let [value] = arguments("lower", slots, frame)?;
    let Some(text) = cast_as_text(value, frame.arena) else {
        return Ok(SqliteValue::null());
    };

    let mut result = String::new_in(frame.arena);
    for c in text.chars() {
        for c in c.to_lowercase() {
            result.push(c);
        }
    }

    Ok(Text(result.into_bump_str()))
}

pub fn length<'a>(slots: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    let [value] = arguments("length", slots, frame)?;

    Ok(match value {
        Blob(bytes) => Integer(bytes.len() as i64),
        _ => cast_as_text(value, frame.arena)
            .map(|s| Integer(s.chars().count() as i64))
            .unwrap_or(SqliteValue::null()),
    })
}

pub fn fn_typeof<'a>(slots: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    let [value] = arguments("typeof", slots, frame)?;

    Ok(Text(match value {
        Null { is_undefined: _ } => "null",
        Float(_) => "real",
        Integer(_) => "integer",
        Blob(_) => "blob",
        Text(_) => "text",
    }))
}

pub fn ps_json_contains<'a>(slots: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    let [needle, array] = arguments("ps_json_contains", slots, frame)?;
    if needle.is_null() || array.is_null() {
        return Ok(SqliteValue::null());
    }

    let Text(json) = array else {
        return Err(EvaluationError::WrongType {
            name: "ps_json_contains",
            value: array,
        });
    };

    struct Contains<'a, 'f> {
        frame: &'f Frame<'a>,
        needle: SqliteValue<'a>,
    }

    impl<'a> Visitor<'a> for Contains<'a, '_> {
        type Value = bool;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a JSON array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'a>,
        {
            let mut found = false;
            // Keep parsing after a match, so that invalid JSON is always reported.
            while let Some(element) = seq.next_element_seed(ScalarSqliteValueOrText(
                SqliteValueFromJson::for_source_row(self.frame),
            ))? {
                found = found || element.compare(&self.needle).is_eq();
            }

            Ok(found)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_str(json);
    let found = deserializer.deserialize_seq(Contains { frame, needle })?;
    deserializer.end()?;
    Ok(found.into())
}

/// `substring(value, start, length?)`.
///
/// Like in SQLite, indices refer to Unicode code points. Note that this differs from the
/// JavaScript implementation in `sql_functions.ts`, which uses UTF-16 code units.
pub fn substring<'a>(inputs: &[Slot], frame: &Frame<'a>) -> ScalarFunctionResult<'a> {
    if inputs.len() < 2 || inputs.len() > 3 {
        return Err(EvaluationError::ParameterCount {
            name: "substring",
            expected: 3, // not exactly, but close enough for the error message
            actual: inputs.len(),
        });
    }

    let arena = frame.arena;
    let Some(input) = cast_as_text(frame[inputs[0]], arena) else {
        return Ok(SqliteValue::null());
    };
    let Some(start) = cast_as_integer(frame[inputs[1]]) else {
        return Ok(SqliteValue::null());
    };
    let length = match inputs.get(2).map(|s| frame[*s]) {
        // An explicit NULL length is different from an omitted one, matching SQLite.
        Some(SqliteValue::Null { is_undefined: _ }) => return Ok(SqliteValue::null()),
        Some(length) => cast_as_integer(length),
        None => None,
    };

    let is_ascii = input.is_ascii();
    let text_length = if is_ascii {
        input.len()
    } else {
        input.chars().count()
    } as i64;

    // Without a length, take the remainder (+1 to account for the start = 0 special case).
    let mut length = length.unwrap_or(text_length + 1);

    let real_start = if start < 0 {
        (text_length + start).max(0)
    } else if start == 0 {
        // Weird special case. Saturating since length might be i64::MIN.
        length = length.saturating_sub(1);
        0
    } else {
        start - 1
    };

    // Saturating since both values might be large (or length might be i64::MIN after the
    // decrement above). That's fine because the indices are clamped to the string afterwards.
    let end = real_start.saturating_add(length);
    let (from, to) = if length < 0 {
        // Negative length means we return that many characters _before_ the start index.
        (end, real_start)
    } else {
        (real_start, end)
    };

    // Clamp both indices to the string and swap them if necessary (like String.prototype.substring
    // in the JavaScript implementation).
    let from = from.clamp(0, text_length) as usize;
    let to = to.clamp(0, text_length) as usize;
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    if is_ascii {
        // Code points and bytes are the same for ASCII strings.
        return Ok(Text(&input[from..to]));
    }

    // Translate code point indices into byte offsets.
    let mut byte_offsets = input
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(input.len()));
    let from_byte = byte_offsets.nth(from).unwrap();
    let to_byte = if to == from {
        from_byte
    } else {
        byte_offsets.nth(to - from - 1).unwrap()
    };

    Ok(Text(&input[from_byte..to_byte]))
}

/// Reads the values of `N` argument slots, or fails if the function was called with a different
/// amount of arguments.
fn arguments<'a, const N: usize>(
    name: &'static str,
    slots: &[Slot],
    frame: &Frame<'a>,
) -> Result<[SqliteValue<'a>; N], EvaluationError<'a>> {
    let slots: &[Slot; N] = slots
        .try_into()
        .map_err(|_| EvaluationError::ParameterCount {
            name,
            expected: N,
            actual: slots.len(),
        })?;

    Ok(slots.map(|slot| frame[slot]))
}

#[cfg(test)]
mod test {
    use bumpalo::Bump;

    use super::{ScalarFunctionImpl, ps_json_contains, upper};
    use crate::sync_plan::{
        CompatibilityContext, EvaluationError, SqliteValue, evaluation::Frame,
        processor_group::Slot,
    };

    /// Calls a function with the given arguments, returning the result formatted with [Debug] so
    /// that it doesn't borrow from the frame.
    fn call(function: ScalarFunctionImpl, args: &[SqliteValue]) -> String {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default();
        let mut frame = Frame::new(&arena, &compatibility, args.len() as u32, 0);

        let slots: Vec<Slot> = (0..args.len() as u32).map(Slot::Input).collect();
        for (slot, arg) in slots.iter().zip(args) {
            frame[*slot] = match *arg {
                SqliteValue::Text(text) => SqliteValue::Text(arena.alloc_str(text)),
                other => other,
            };
        }

        format!("{:?}", function(&slots, &frame))
    }

    #[test]
    fn checks_argument_count() {
        let arena = Bump::new();
        let compatibility = CompatibilityContext::default();
        let frame = Frame::new(&arena, &compatibility, 2, 0);

        let error = upper(&[Slot::Input(0), Slot::Input(1)], &frame).unwrap_err();
        assert!(matches!(
            error,
            EvaluationError::ParameterCount {
                name: "upper",
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn ps_json_contains_values() {
        use SqliteValue::*;
        let t = "Ok(Integer(1))";
        let f = "Ok(Integer(0))";

        assert_eq!(call(ps_json_contains, &[Integer(1), Text("[1, 2]")]), t);
        assert_eq!(call(ps_json_contains, &[Integer(3), Text("[1, 2]")]), f);
        assert_eq!(call(ps_json_contains, &[Float(1.0), Text("[1, 2]")]), t);
        assert_eq!(call(ps_json_contains, &[Integer(2), Text("[1.5, 2.0]")]), t);
        assert_eq!(
            call(ps_json_contains, &[Text("a"), Text(r#"["a", "b"]"#)]),
            t
        );
        assert_eq!(call(ps_json_contains, &[Text("1"), Text("[1]")]), f);
        assert_eq!(call(ps_json_contains, &[Integer(1), Text("[true]")]), t);
        assert_eq!(call(ps_json_contains, &[Integer(0), Text("[false]")]), t);
        assert_eq!(call(ps_json_contains, &[Text("[1]"), Text("[[1]]")]), t);
        assert_eq!(call(ps_json_contains, &[Integer(1), Text("[null]")]), f);
        assert_eq!(call(ps_json_contains, &[Integer(1), Text("[]")]), f);
        assert_eq!(
            call(
                ps_json_contains,
                &[Integer(9223372036854775807), Text("[9223372036854775807]")]
            ),
            t
        );
        assert_eq!(
            call(ps_json_contains, &[Text("a\n"), Text(r#"["a\n"]"#)]),
            t
        );

        assert_eq!(
            call(ps_json_contains, &[SqliteValue::null(), Text("[null]")]),
            "Ok(Null)"
        );
        assert_eq!(
            call(ps_json_contains, &[Integer(1), SqliteValue::null()]),
            "Ok(Null)"
        );
    }

    #[test]
    fn ps_json_contains_errors() {
        use SqliteValue::*;

        for array in [
            Text("{}"),
            Text("1"),
            Text("[1,"),
            Text("[1] x"),
            Integer(1),
        ] {
            assert!(
                call(ps_json_contains, &[Integer(1), array]).starts_with("Err("),
                "{array:?}"
            );
        }

        // Invalid JSON is reported even after a match.
        assert!(call(ps_json_contains, &[Integer(1), Text("[1, ]")]).starts_with("Err("));
    }
}
