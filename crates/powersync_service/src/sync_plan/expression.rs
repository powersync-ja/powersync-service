use std::{borrow::Cow, sync::Arc};

use serde::{Deserialize, de::Error};

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

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum UnaryOperator {
    #[serde(rename = "not")]
    Not,
    #[serde(rename = "+")]
    Plus,
}

#[derive(Debug, Deserialize)]
pub struct UnaryExpression<Data> {
    pub operand: Box<SqlExpression<Data>>,
    pub operator: UnaryOperator,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum BinaryOperator {
    #[serde(rename = "or")]
    Or,
    #[serde(rename = "and")]
    And,
    #[serde(rename = "=")]
    Equals,
    #[serde(rename = "is")]
    Is,
    #[serde(rename = "<")]
    Less,
    #[serde(rename = "<=")]
    LessOrEqual,
    #[serde(rename = ">")]
    Greater,
    #[serde(rename = ">=")]
    GreaterOrEqual,
    #[serde(rename = "&")]
    BinaryAnd,
    #[serde(rename = "|")]
    BinaryOr,
    #[serde(rename = "<<")]
    ShiftLeft,
    #[serde(rename = ">>")]
    ShiftRight,
    #[serde(rename = "+")]
    Plus,
    #[serde(rename = "-")]
    Minus,
    #[serde(rename = "*")]
    Multiply,
    #[serde(rename = "/")]
    Divide,
    #[serde(rename = "%")]
    Modulo,
    #[serde(rename = "||")]
    Concatenate,
}

/// A binary expression in SQLite, `$left $op $right`.
///
/// `LIKE`, `GLOB`, `REGEXP`, `MATCH`, `->` and `->>` are represented as function calls instead. Negated operators
/// (e.g. `!=` or `NOT IN`) are represented by wrapping the expression in a unary `NOT`.
#[derive(Debug, Deserialize)]
pub struct BinaryExpression<Data> {
    pub left: Box<SqlExpression<Data>>,
    pub operator: BinaryOperator,
    pub right: Box<SqlExpression<Data>>,
}

/// A `$value BETWEEN $low AND $high` expression.
#[derive(Debug, Deserialize)]
pub struct BetweenExpression<Data> {
    pub value: Box<SqlExpression<Data>>,
    pub low: Box<SqlExpression<Data>>,
    pub high: Box<SqlExpression<Data>>,
}

/// An `$target IN ($in0, $in1, ..., $inN)` expression.
#[derive(Debug, Deserialize)]
pub struct ScalarInExpression<Data> {
    pub target: Box<SqlExpression<Data>>,
    pub r#in: Vec<SqlExpression<Data>>,
}

/// A `CASE WHEN` expression as supported by SQLite.
#[derive(Debug, Deserialize)]
pub struct CaseWhenExpression<Data> {
    pub operand: Option<Box<SqlExpression<Data>>>,
    pub whens: Vec<WhenThen<Data>>,
    pub r#else: Option<Box<SqlExpression<Data>>>,
}

/// A `WHEN $when THEN $then` clause in a [CaseWhenExpression].
#[derive(Debug, Deserialize)]
pub struct WhenThen<Data> {
    pub when: SqlExpression<Data>,
    pub then: SqlExpression<Data>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CastType {
    Text,
    Numeric,
    Real,
    Integer,
    Blob,
}

/// A `CAST($operand AS $cast_as)` expression.
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
pub struct ScalarFunctionCallExpression<Data> {
    pub function: ScalarFunction,
    pub parameters: Vec<SqlExpression<Data>>,
}

/// Scalar functions supported in sync plans.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarFunction {
    // Core SQLite functions, https://sqlite.org/lang_corefunc.html
    Hex,
    #[serde(rename = "ifnull")]
    IfNull,
    Iif,
    Instr,
    Length,
    Lower,
    Substring,
    #[serde(rename = "typeof")]
    TypeOf,
    Upper,
    JsonArrayLength,
    JsonExtract,
    #[serde(rename = "unixepoch")]
    UnixEpoch,
    #[serde(rename = "datetime")]
    DateTime,
    // PowerSync-specific functions
    Base64,
    JsonKeys,
    UuidBlob,
    #[serde(rename = "st_asgeojson")]
    StAsGeoJson,
    #[serde(rename = "st_astext")]
    StAsText,
    StX,
    StY,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableValuedFunction {
    JsonEach,
}

impl<Data> SqlExpression<Data> {
    /// Transforms all [SqlExpression::ExternalData] nodes in this expression, failing if `map_data` fails for any of
    /// them.
    pub fn try_map<Other, E, F>(self, map_data: &mut F) -> Result<SqlExpression<Other>, E>
    where
        F: FnMut(Data) -> Result<Other, E>,
    {
        fn map_box<Data, Other, E, F: FnMut(Data) -> Result<Other, E>>(
            expr: SqlExpression<Data>,
            map_data: &mut F,
        ) -> Result<Box<SqlExpression<Other>>, E> {
            Ok(Box::new(expr.try_map(map_data)?))
        }

        fn map_all<Data, Other, E, F: FnMut(Data) -> Result<Other, E>>(
            exprs: Vec<SqlExpression<Data>>,
            map_data: &mut F,
        ) -> Result<Vec<SqlExpression<Other>>, E> {
            exprs.into_iter().map(|e| e.try_map(map_data)).collect()
        }

        Ok(match self {
            Self::ExternalData(data) => SqlExpression::ExternalData(map_data(data)?),
            Self::Unary(unary) => SqlExpression::Unary(UnaryExpression {
                operand: map_box(*unary.operand, map_data)?,
                operator: unary.operator,
            }),
            Self::Binary(binary) => SqlExpression::Binary(BinaryExpression {
                left: map_box(*binary.left, map_data)?,
                operator: binary.operator,
                right: map_box(*binary.right, map_data)?,
            }),
            Self::Between(between) => SqlExpression::Between(BetweenExpression {
                value: map_box(*between.value, map_data)?,
                low: map_box(*between.low, map_data)?,
                high: map_box(*between.high, map_data)?,
            }),
            Self::ScalarIn(scalar_in) => SqlExpression::ScalarIn(ScalarInExpression {
                target: map_box(*scalar_in.target, map_data)?,
                r#in: map_all(scalar_in.r#in, map_data)?,
            }),
            Self::CaseWhen(case_when) => SqlExpression::CaseWhen(CaseWhenExpression {
                operand: case_when
                    .operand
                    .map(|e| map_box(*e, map_data))
                    .transpose()?,
                whens: case_when
                    .whens
                    .into_iter()
                    .map(|when_then| {
                        Ok(WhenThen {
                            when: when_then.when.try_map(map_data)?,
                            then: when_then.then.try_map(map_data)?,
                        })
                    })
                    .collect::<Result<_, E>>()?,
                r#else: case_when
                    .r#else
                    .map(|e| map_box(*e, map_data))
                    .transpose()?,
            }),
            Self::Cast(cast) => SqlExpression::Cast(CastExpression {
                operand: map_box(*cast.operand, map_data)?,
                cast_as: cast.cast_as,
            }),
            Self::FunctionCall(call) => SqlExpression::FunctionCall(ScalarFunctionCallExpression {
                function: call.function,
                parameters: map_all(call.parameters, map_data)?,
            }),
            Self::Literal(literal) => SqlExpression::Literal(literal),
        })
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

// Deserializes expression from the format given by packages/sync-rules/src/sync_plan/expression.ts.
impl<'de, Data: Deserialize<'de>> Deserialize<'de> for SqlExpression<Data> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        pub enum TypeScriptSqlExpression<'a, Data> {
            #[serde(rename = "data")]
            ExternalData { source: Data },
            #[serde(rename = "unary")]
            Unary(UnaryExpression<Data>),
            #[serde(rename = "binary")]
            Binary(BinaryExpression<Data>),
            #[serde(rename = "between")]
            Between(BetweenExpression<Data>),
            #[serde(rename = "scalar_in")]
            ScalarIn(ScalarInExpression<Data>),
            #[serde(rename = "case_when")]
            CaseWhen(CaseWhenExpression<Data>),
            #[serde(rename = "cast")]
            Cast(CastExpression<Data>),
            #[serde(rename = "function")]
            FunctionCall(ScalarFunctionCallExpression<Data>),
            #[serde(rename = "lit_null")]
            Null,
            #[serde(rename = "lit_double")]
            Double { value: f64 },
            #[serde(rename = "lit_int")]
            Integer {
                #[serde(borrow)]
                base10: Cow<'a, str>,
            },
            #[serde(rename = "lit_string")]
            String { value: String },
        }

        let ts = TypeScriptSqlExpression::deserialize(deserializer)?;

        Ok(match ts {
            TypeScriptSqlExpression::ExternalData { source } => Self::ExternalData(source),
            TypeScriptSqlExpression::Unary(e) => Self::Unary(e),
            TypeScriptSqlExpression::Binary(e) => Self::Binary(e),
            TypeScriptSqlExpression::Between(e) => Self::Between(e),
            TypeScriptSqlExpression::ScalarIn(e) => Self::ScalarIn(e),
            TypeScriptSqlExpression::CaseWhen(e) => Self::CaseWhen(e),
            TypeScriptSqlExpression::Cast(e) => Self::Cast(e),
            TypeScriptSqlExpression::FunctionCall(e) => Self::FunctionCall(e),
            TypeScriptSqlExpression::Null => Self::Literal(LiteralExpression::Null),
            TypeScriptSqlExpression::Double { value } => {
                Self::Literal(LiteralExpression::Double(value))
            }
            TypeScriptSqlExpression::Integer { base10 } => {
                Self::Literal(LiteralExpression::Integer(
                    base10
                        .parse()
                        .map_err(|_| D::Error::custom("invalid integer literal"))?,
                ))
            }
            TypeScriptSqlExpression::String { value } => {
                Self::Literal(LiteralExpression::String(Arc::new(value)))
            }
        })
    }
}
