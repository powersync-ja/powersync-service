use crate::sync_plan::SqliteValue::{self};

pub const SQLITE_TRUE: SqliteValue<'static> = SqliteValue::Integer(1);
pub const SQLITE_FALSE: SqliteValue<'static> = SqliteValue::Integer(0);

pub fn sqlite_not(value: SqliteValue) -> SqliteValue<'static> {
    if value.is_null() {
        return SqliteValue::null();
    }

    (!value.is_truthy()).into()
}
