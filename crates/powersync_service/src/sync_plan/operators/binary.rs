//! Implementation for binary SQL operators.
use crate::sync_plan::{
    EvaluationError,
    SqliteValue::{self, Float, Integer, Text},
    cast::{Numeric, cast_as_integer, cast_as_numeric, cast_as_text},
    evaluation::Frame,
    expression::BinaryOperator,
};

// Usage: sqlite_cmp!(a, <, b)
macro_rules! sqlite_cmp {
    ($a:expr, $op:tt, $b:expr) => {{
        let (a, b): (SqliteValue, SqliteValue) = ($a, $b);
        if a.is_null() || b.is_null() {
            SqliteValue::null()
        } else {
            // Orderings are ordered too (Less < Equal < Greater), so comparing the result with
            // Equal using the operator applies the operator to the values.
            (a.compare(&b) $op std::cmp::Ordering::Equal).into()
        }
    }};
}

// For operations that only work on integers. Usage: integer_op!(a, %, b)
macro_rules! integer_op {
    ($a:expr, $op:tt, $b:expr) => {{
        if let (Some(a), Some(b)) = (cast_as_integer($a), cast_as_integer($b)) {
            Integer(a $op b)
        } else {
            SqliteValue::null()
        }
    }};
}

/// Usage: `math_op!(a, checked_add, +, b)`.
///
/// Evaluates math similar to SQLite, using integer math if possible and falling back to doubles if
/// needed.
macro_rules! math_op {
    ($a:expr, $checked:ident, $op:tt, $b:expr) => {{
        match (cast_as_numeric($a), cast_as_numeric($b)) {
            (None, _) | (_, None) => SqliteValue::null(),
            (Some(Numeric::Integer(a)), Some(Numeric::Integer(b))) => match a.$checked(b) {
                Some(result) => Integer(result),
                None => Float(a as f64 $op b as f64),
            },
            (Some(a), Some(b)) => Float(a.as_real() $op b.as_real()),
        }
    }};
}

pub fn evaluate_binary_operator<'a>(
    frame: &Frame<'a>,
    a: SqliteValue<'a>,
    operator: BinaryOperator,
    b: SqliteValue<'a>,
) -> Result<SqliteValue<'a>, EvaluationError<'a>> {
    Ok(match operator {
        BinaryOperator::Equals => sqlite_cmp!(a, ==, b),
        BinaryOperator::Less => sqlite_cmp!(a, <, b),
        BinaryOperator::LessOrEqual => sqlite_cmp!(a, <=, b),
        BinaryOperator::Greater => sqlite_cmp!(a, >, b),
        BinaryOperator::GreaterOrEqual => sqlite_cmp!(a, >=, b),
        BinaryOperator::Is => a.compare(&b).is_eq().into(),

        BinaryOperator::Plus => math_op!(a, checked_add, +, b),
        BinaryOperator::Minus => math_op!(a, checked_sub, -, b),
        BinaryOperator::Multiply => math_op!(a, checked_mul, *, b),
        BinaryOperator::Divide => {
            match cast_as_numeric(b) {
                // Dividing by zero evaluates to NULL.
                None | Some(Numeric::Integer(0)) | Some(Numeric::Float(0.0)) => SqliteValue::null(),
                Some(b) => math_op!(a, checked_div, /, b.into()),
            }
        }
        BinaryOperator::Concatenate => {
            let bump = frame.arena;
            let (Some(a), Some(b)) = (cast_as_text(a, bump), cast_as_text(b, bump)) else {
                return Ok(SqliteValue::null());
            };

            Text(bumpalo::format!(in bump, "{}{}", a, b).into_bump_str())
        }

        BinaryOperator::Or => {
            if a.is_null() || b.is_null() {
                SqliteValue::null()
            } else {
                (a.is_truthy() || b.is_truthy()).into()
            }
        }
        BinaryOperator::And => {
            if a.is_null() || b.is_null() {
                SqliteValue::null()
            } else {
                (a.is_truthy() && b.is_truthy()).into()
            }
        }

        BinaryOperator::Modulo => integer_op!(a, %, b),
        BinaryOperator::BinaryAnd => integer_op!(a, &, b),
        BinaryOperator::BinaryOr => integer_op!(a, |, b),
        BinaryOperator::ShiftLeft => integer_op!(a, <<, b),
        BinaryOperator::ShiftRight => integer_op!(a, >>, b),
    })
}
