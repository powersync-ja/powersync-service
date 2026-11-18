mod compatibility;
mod error;
mod evaluator;
mod expression;
mod hydrated;
mod operators;
mod plan;
mod processor_group;
mod sources;
mod table_pattern;
mod value;

pub use compatibility::*;
pub use error::EvaluationError;
pub use evaluator::{ErrorSource, EvaluationSink, Evaluator};
pub use hydrated::*;
pub use value::*;

/// Interns column names and aliases referenced by processors in a [HydratedSyncConfig].
///
/// Since a processor can be part of multiple processor groups, this avoids storing the same names
/// once per group. It also allows looking up symbols by `&str` without allocating, which is used to
/// resolve field names of source rows.
pub type StringInterner = string_interner::DefaultStringInterner;
pub type Symbol = string_interner::DefaultSymbol;
