#[derive(Debug, Clone)]
struct SelectQueryAst {
    tables: Vec<TableAst>,
    columns: Vec<SelectItemAst>,
    output_alias: String,
    where_expr: Option<ExprAst>,
    join_equalities: Vec<JoinEqualityAst>,
}

#[derive(Debug, Clone)]
struct TableAst {
    table: String,
    schema: Option<String>,
    alias: String,
    explicit_alias: bool,
    derived_query: Option<Box<SelectQueryAst>>,
    derived_columns: Option<Vec<SelectItemAst>>,
}

#[derive(Debug, Clone)]
struct JoinEqualityAst {
    left: QualifiedColumnAst,
    right: QualifiedColumnAst,
}

#[derive(Debug, Clone)]
struct QualifiedColumnAst {
    table_alias: String,
    column: String,
}

#[derive(Debug, Clone)]
struct LookupStageBuilder {
    table_alias: String,
    table: String,
    schema: Option<String>,
    partition_column: String,
    output_column: String,
    instantiation: SerializedParameterValue,
    filters: Vec<SqlExpression>,
}

#[derive(Debug, Clone)]
struct CompiledQueryBindings {
    data_filters: Vec<SqlExpression>,
    request_filters: Vec<SqlExpression>,
    partition_by: Vec<PartitionKey>,
    source_instantiation: Vec<SerializedParameterValue>,
    lookup_stages: Vec<Vec<SerializedExpandingLookup>>,
}

#[derive(Debug, Clone)]
enum SelectItemAst {
    Star { qualifier: Option<String> },
    Expr {
        expr: ExprAst,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone)]
enum ExprAst {
    Identifier(String),
    String(String),
    Int(String),
    Double(f64),
    Bool(bool),
    Null,
    Function {
        name: String,
        args: Vec<ExprAst>,
    },
    Unary {
        op: String,
        operand: Box<ExprAst>,
    },
    Binary {
        left: Box<ExprAst>,
        op: String,
        right: Box<ExprAst>,
    },
    ScalarIn {
        target: Box<ExprAst>,
        values: Vec<ExprAst>,
    },
    Between {
        value: Box<ExprAst>,
        low: Box<ExprAst>,
        high: Box<ExprAst>,
    },
    Cast {
        expr: Box<ExprAst>,
        cast_as: String,
    },
    CaseWhen {
        operand: Option<Box<ExprAst>>,
        whens: Vec<(ExprAst, ExprAst)>,
        else_expr: Option<Box<ExprAst>>,
    },
    SubqueryIn {
        target: Box<ExprAst>,
        subquery: Box<SelectQueryAst>,
    },
}
