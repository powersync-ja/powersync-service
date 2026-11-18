use std::sync::Arc;

use crate::sync_plan::SqliteValue;

/// An enumeration of all scalar SQL expressions supported by the sync service.
///
/// The `Data` type parameter encodes what external data references an [SqlExpression::ExternalData] node may
/// reference: Bucket data sources can only reference row data (e.g. to evaluate filters), while queriers can only
/// reference connection data.
#[derive(Debug)]
pub enum SqlExpression<Data> {
    /// External data injected into the expression during evaluation.
    ExternalData(Data),
    Unary(UnaryExpression<Data>),
    Binary(BinaryExpression<Data>),
    Between(BetweenExpression<Data>),
    ScalarIn(ScalarInExpression<Data>),
    CaseWhen(CaseWhenExpression<Data>),
    Cast(CastExpression<Data>),
    FunctionCall(ScalarFunctionCallExpression<Data>),
    Literal(LiteralExpression),
}

#[derive(Clone, Copy, Debug)]
pub enum UnaryOperator {
    Not,
    Plus,
}

#[derive(Debug)]
pub struct UnaryExpression<Data> {
    pub operand: Box<SqlExpression<Data>>,
    pub operator: UnaryOperator,
}

#[derive(Clone, Copy, Debug)]
pub enum BinaryOperator {
    Or,
    And,
    Equals,
    Is,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    BinaryAnd,
    BinaryOr,
    ShiftLeft,
    ShiftRight,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Concatenate,
}

/// A binary expression in SQLite, `$left $op $right`.
///
/// `LIKE`, `GLOB`, `REGEXP`, `MATCH`, `->` and `->>` are represented as function calls instead. Negated operators
/// (e.g. `!=` or `NOT IN`) are represented by wrapping the expression in a unary `NOT`.
#[derive(Debug)]
pub struct BinaryExpression<Data> {
    pub left: Box<SqlExpression<Data>>,
    pub operator: BinaryOperator,
    pub right: Box<SqlExpression<Data>>,
}

/// A `$value BETWEEN $low AND $high` expression.
#[derive(Debug)]
pub struct BetweenExpression<Data> {
    pub value: Box<SqlExpression<Data>>,
    pub low: Box<SqlExpression<Data>>,
    pub high: Box<SqlExpression<Data>>,
}

/// An `$target IN ($in0, $in1, ..., $inN)` expression.
#[derive(Debug)]
pub struct ScalarInExpression<Data> {
    pub target: Box<SqlExpression<Data>>,
    pub r#in: Vec<SqlExpression<Data>>,
}

/// A `CASE WHEN` expression as supported by SQLite.
#[derive(Debug)]
pub struct CaseWhenExpression<Data> {
    pub operand: Option<Box<SqlExpression<Data>>>,
    pub whens: Vec<WhenThen<Data>>,
    pub r#else: Option<Box<SqlExpression<Data>>>,
}

/// A `WHEN $when THEN $then` clause in a [CaseWhenExpression].
#[derive(Debug)]
pub struct WhenThen<Data> {
    pub when: SqlExpression<Data>,
    pub then: SqlExpression<Data>,
}

#[derive(Clone, Copy, Debug)]
pub enum CastType {
    Text,
    Numeric,
    Real,
    Integer,
    Blob,
}

/// A `CAST($operand AS $cast_as)` expression.
#[derive(Debug)]
pub struct CastExpression<Data> {
    pub operand: Box<SqlExpression<Data>>,
    pub cast_as: CastType,
}

/// A constant literal in SQL.
#[derive(Clone, Debug)]
pub enum LiteralExpression {
    Null,
    Double(f64),
    Integer(i64),
    String(Arc<String>),
}

/// A scalar (non-aggregate, non-window, non-table-valued) function call in SQL.
#[derive(Debug)]
pub struct ScalarFunctionCallExpression<Data> {
    pub function: ScalarFunction,
    pub parameters: Vec<SqlExpression<Data>>,
}

/// Scalar functions supported in sync plans.
#[derive(Clone, Copy, Debug)]
pub enum ScalarFunction {
    // Core SQLite functions, https://sqlite.org/lang_corefunc.html
    Hex,
    IfNull,
    Iif,
    Instr,
    Length,
    Lower,
    Substring,
    TypeOf,
    Upper,
    JsonArrayLength,
    JsonExtract,
    UnixEpoch,
    DateTime,
    // PowerSync-specific functions
    Base64,
    JsonKeys,
    UuidBlob,
    StAsGeoJson,
    StAsText,
    StX,
    StY,
}

#[derive(Debug, Clone, Copy)]
pub enum TableValuedFunction {
    JsonEach,
}

impl<Data> SqlExpression<Data> {
    pub fn map<Other>(&self, mut map_data: impl FnMut(&Data) -> Other) -> SqlExpression<Other> {
        self.map_with(&mut map_data)
    }

    fn map_with<Other, F: FnMut(&Data) -> Other>(&self, map_data: &mut F) -> SqlExpression<Other> {
        match self {
            Self::ExternalData(original) => SqlExpression::ExternalData(map_data(original)),
            Self::Unary(unary) => SqlExpression::Unary(UnaryExpression {
                operand: Box::new(unary.operand.map_with(map_data)),
                operator: unary.operator,
            }),
            Self::Binary(binary) => SqlExpression::Binary(BinaryExpression {
                left: Box::new(binary.left.map_with(map_data)),
                operator: binary.operator,
                right: Box::new(binary.right.map_with(map_data)),
            }),
            Self::Between(between) => SqlExpression::Between(BetweenExpression {
                value: Box::new(between.value.map_with(map_data)),
                low: Box::new(between.low.map_with(map_data)),
                high: Box::new(between.high.map_with(map_data)),
            }),
            Self::ScalarIn(scalar_in) => SqlExpression::ScalarIn(ScalarInExpression {
                target: Box::new(scalar_in.target.map_with(map_data)),
                r#in: scalar_in
                    .r#in
                    .iter()
                    .map(|e| e.map_with(map_data))
                    .collect(),
            }),
            Self::CaseWhen(case_when) => SqlExpression::CaseWhen(CaseWhenExpression {
                operand: case_when
                    .operand
                    .as_ref()
                    .map(|e| Box::new(e.map_with(map_data))),
                whens: case_when
                    .whens
                    .iter()
                    .map(|when_then| WhenThen {
                        when: when_then.when.map_with(map_data),
                        then: when_then.then.map_with(map_data),
                    })
                    .collect(),
                r#else: case_when
                    .r#else
                    .as_ref()
                    .map(|e| Box::new(e.map_with(map_data))),
            }),
            Self::Cast(cast) => SqlExpression::Cast(CastExpression {
                operand: Box::new(cast.operand.map_with(map_data)),
                cast_as: cast.cast_as,
            }),
            Self::FunctionCall(call) => SqlExpression::FunctionCall(ScalarFunctionCallExpression {
                function: call.function,
                parameters: call
                    .parameters
                    .iter()
                    .map(|e| e.map_with(map_data))
                    .collect(),
            }),
            Self::Literal(l) => SqlExpression::Literal(l.clone()),
        }
    }
}

impl LiteralExpression {
    pub fn evaluate<'a>(&'a self) -> SqliteValue<'a> {
        match self {
            LiteralExpression::Null => SqliteValue::Null,
            LiteralExpression::Double(value) => SqliteValue::Float(*value),
            LiteralExpression::Integer(value) => SqliteValue::Integer(*value),
            LiteralExpression::String(value) => SqliteValue::Text(value),
        }
    }
}

impl TableValuedFunction {
    pub fn output_columns(&self) -> &'static [&'static str] {
        match self {
            TableValuedFunction::JsonEach => &["key", "value"],
        }
    }
}
