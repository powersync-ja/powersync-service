use std::sync::Arc;

use crate::sync_plan::{
    expression::{SqlExpression, TableValuedFunction},
    table_pattern::TablePattern,
};

/// A description for a data source processing rows to replicate.
///
/// A single data source might be used in multiple buckets. In those cases, we store duplicate
/// bucket data but share the processing logic.
#[derive(Debug)]
pub struct StreamDataSource {
    pub processor: TableProcessor,
    pub projection: RowProjection<ColumnSource>,
    /// The name of the output table for evaluated rows.
    ///
    /// When [None], the name of the table being evaluated should be used instead.
    pub output_table_name: Option<Arc<String>>,
}

/// A data source creating parameter lookup values that can be queried when fetching buckets
/// for a connection.
#[derive(Debug)]
pub struct StreamParameterIndexLookupCreator {
    pub processor: TableProcessor,
    /// Outputs to persist in the lookup.
    pub outputs: Vec<TableProcessorExpression>,
}

/// Something that processes a source row if it matches filters.
#[derive(Debug)]
pub struct TableProcessor {
    /// The source table to process into buckets or parameter lookups.
    pub source_table: TablePattern,
    /// Filters that must all evaluate to a truthy value (according to SQL definitions) for the
    /// row to be processed.
    pub filters: Vec<TableProcessorExpression>,
    /// How to partition rows.
    ///
    /// For data sources, these describe parameters of the bucket being created. For parameter
    /// lookups, these form input values for the lookup.
    pub parameters: Vec<TableProcessorExpression>,
    /// Table-valued functions joined to the source row.
    ///
    /// Buckets for a row are derived from the cartesian product of all partition keys, so a
    /// function evaluating to zero rows means the source row is not put into any bucket.
    pub table_valued_functions: Vec<Arc<TableProcessorTableValuedFunction>>,
}

pub type TableProcessorExpression = SqlExpression<TableProcessorData>;

/// An external reference in expressions for replication processors.
#[derive(Debug)]
pub enum TableProcessorData {
    /// Evaluates to the value of a column in the row to process.
    ColumnInSourceRow(Arc<String>),
    /// Evaluates to a string representing the schema name of the row to process.
    SchemaName,
    /// Evaluates to a string representing the table suffix (where the prefix is the wildcard
    /// [TablePattern]).
    TableSuffix,
    TableValuedFunction(TableProcessorTableValuedFunctionOutput),
}

/// Output columns produced when a matching source row is evaluated.
#[derive(Debug)]
pub enum RowProjection<Column> {
    /// A projection without `*`, emitting only the given columns.
    Columns(Vec<Column>),
    /// A projection of the form `before, *, after`.
    ///
    /// Since later columns win, `after` columns shadow fields of the source row with the same
    /// name, while fields of the source row shadow `before` columns.
    Star {
        before: Vec<Column>,
        after: Vec<Column>,
    },
}

#[derive(Debug)]
pub struct TableProcessorTableValuedFunction {
    pub function: TableValuedFunction,
    pub inputs: Vec<TableProcessorExpression>,
}

#[derive(Debug)]
pub struct TableProcessorTableValuedFunctionOutput {
    pub function: Arc<TableProcessorTableValuedFunction>,
    pub output_slot: usize,
}

/// A non-`*` output in a replicated row.
#[derive(Debug)]
pub struct ColumnSource {
    /// The name of the output column in the generated object.
    pub alias: Arc<String>,
    /// The expression to evaluate for this.
    pub expr: TableProcessorExpression,
}

impl<Column> RowProjection<Column> {
    pub fn has_star(&self) -> bool {
        matches!(self, Self::Star { .. })
    }

    /// Columns before the `*`, or all columns if there is no `*`.
    pub fn before(&self) -> &[Column] {
        match self {
            Self::Columns(columns) => columns,
            Self::Star { before, .. } => before,
        }
    }

    /// Columns after the `*`, or an empty slice if there is no `*`.
    pub fn after(&self) -> &[Column] {
        match self {
            Self::Columns(_) => &[],
            Self::Star { after, .. } => after,
        }
    }

    pub fn map<Other>(&self, mut map: impl FnMut(&Column) -> Other) -> RowProjection<Other> {
        match self {
            Self::Columns(columns) => RowProjection::Columns(columns.iter().map(map).collect()),
            Self::Star { before, after } => RowProjection::Star {
                before: before.iter().map(&mut map).collect(),
                after: after.iter().map(map).collect(),
            },
        }
    }
}
