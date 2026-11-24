use std::sync::Arc;

use napi::{Error, Result, Status, bindgen_prelude::External};
use napi_derive::napi;
use powersync_service::{
    block::{BlockEmbedder, BlockSourceTable, BucketElement, ParameterElement},
    sync_plan::{self, StreamDataSource, StreamParameterIndexLookupCreator},
};

/// Data sources and parameter index lookup creators to evaluate for rows of a source table.
///
/// JavaScript is responsible for matching source tables against table patterns, so a group should
/// contain exactly the elements matching the tables it's used for. Elements are identified by an
/// index chosen by JavaScript, which is reported in outputs of [crate::evaluator::SyncStreamEvaluator].
///
/// Groups can't be changed after they've been used to create a [SourceTable].
#[napi]
pub struct ProcessorGroup {
    inner: Arc<sync_plan::ProcessorGroup<BlockEmbedder>>,
}

#[napi]
impl ProcessorGroup {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Adds a data source (from `deserializeStreamDataSource`).
    ///
    /// Rows are emitted once per entry in `prefixes`.
    #[napi(catch_unwind)]
    pub fn include_data(
        &mut self,
        element: u32,
        prefixes: Vec<String>,
        source: &External<StreamDataSource>,
    ) -> Result<()> {
        self.group_mut()?.include_data(
            Arc::new(BucketElement {
                index: element,
                prefixes,
            }),
            source,
        );
        Ok(())
    }

    /// Adds a parameter index lookup creator (from `deserializeStreamParameterLookupCreator`).
    #[napi(catch_unwind)]
    pub fn include_parameter(
        &mut self,
        element: u32,
        lookup_name: String,
        query_id: String,
        source: &External<StreamParameterIndexLookupCreator>,
    ) -> Result<()> {
        self.group_mut()?.include_parameter(
            Arc::new(ParameterElement {
                index: element,
                lookup_name,
                query_id,
            }),
            source,
        );
        Ok(())
    }

    fn group_mut(&mut self) -> Result<&mut sync_plan::ProcessorGroup<BlockEmbedder>> {
        Arc::get_mut(&mut self.inner).ok_or_else(|| {
            Error::new(
                Status::InvalidArg,
                "Processor group can't be changed after it has been used in a source table",
            )
        })
    }
}

/// A source table along with the [ProcessorGroup] to evaluate for its rows.
#[napi]
pub struct SourceTable {
    pub(crate) inner: Arc<BlockSourceTable>,
}

#[napi]
impl SourceTable {
    #[napi(constructor)]
    pub fn new(
        connection_tag: String,
        schema: String,
        name: String,
        group: &ProcessorGroup,
    ) -> Self {
        Self {
            inner: Arc::new(BlockSourceTable {
                connection_tag,
                schema,
                name,
                group: group.inner.clone(),
            }),
        }
    }
}
