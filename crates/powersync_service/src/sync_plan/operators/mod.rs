use std::cmp::Ordering;

use crate::sync_plan::{
    SqliteValue::{self, Null},
    evaluation::Frame,
    operators::bool::{SQLITE_FALSE, SQLITE_TRUE},
    processor_group::Slot,
};

pub mod binary;
pub mod bool;
pub mod cast;
pub mod functions;
pub mod table_valued;

pub fn scalar_in<'a>(frame: &Frame<'a>, target: Slot, compare: &[Slot]) -> SqliteValue<'a> {
    let target = frame[target];
    if target.is_null() {
        return Null {
            is_undefined: false,
        };
    }

    let mut has_null_comparison = false;
    for slot in compare {
        let evaluated = frame[*slot];
        if evaluated.is_null() {
            has_null_comparison = true;
            continue;
        }

        if target.compare(&evaluated) == Ordering::Equal {
            return SQLITE_TRUE;
        }
    }

    if has_null_comparison {
        Null {
            is_undefined: false,
        }
    } else {
        SQLITE_FALSE
    }
}

pub fn between<'a>(frame: &Frame<'a>, value: Slot, low: Slot, high: Slot) -> SqliteValue<'a> {
    let (value, low, high) = (frame[value], frame[low], frame[high]);
    if value.is_null() || low.is_null() || high.is_null() {
        return Null {
            is_undefined: false,
        };
    }

    (value.compare(&low).is_ge() && value.compare(&high).is_le()).into()
}

pub fn case_when<'a>(
    frame: &Frame<'a>,
    operand: Option<Slot>,
    whens: &[(Slot, Slot)],
    r#else: Option<Slot>,
) -> SqliteValue<'a> {
    let operand = operand.map(|slot| frame[slot]);

    for (when, then) in whens {
        let when = frame[*when];
        let selected = match operand {
            Some(operand) => {
                !operand.is_null() && !when.is_null() && operand.compare(&when).is_eq()
            }
            None => when.is_truthy(),
        };

        if selected {
            return frame[*then];
        }
    }

    r#else.map_or(
        Null {
            is_undefined: false,
        },
        |slot| frame[slot],
    )
}
