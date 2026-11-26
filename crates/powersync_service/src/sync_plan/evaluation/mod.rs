mod frame;
mod input_row;
mod program;
mod projected_row;
#[cfg(test)]
mod test;

use std::sync::Arc;

use bumpalo::Bump;
pub use frame::Frame;
pub use input_row::{InputRow, PreparedField};
pub use projected_row::ProjectedRow;

use crate::sync_plan::{
    CompatibilityContext, EvaluationError, Parameters, ReplicationEmbedder, SourceTableRef,
    SqliteValue, StringInterner, Symbol,
    evaluation::program::Program,
    processor_group::{
        ProcessedDataSource, ProcessedGroupMember, ProcessedParameterSource, ProcessorGroup, Slot,
    },
};

/// Evaluates rows to buckets and parameter results.
///
/// Evaluators are meant to be thread-local, while [ProcessorGroup]s contain prepared state and can
/// be used across threads.
pub struct Evaluator {
    compatibility: Arc<CompatibilityContext>,
    /// Storage for values computed while evaluating a row, reset for each row.
    arena: Bump,
}

impl Evaluator {
    pub fn new(compatibility: Arc<CompatibilityContext>) -> Self {
        Self {
            compatibility,
            arena: Bump::new(),
        }
    }

    /// Starts evaluating a row in the given source table with the processors in `group`.
    ///
    /// The caller is responsible for passing a group containing the processors matching `table`.
    /// If no processor matches the table, callers should skip the row instead of decoding it.
    ///
    /// Call [InputRow::prepare_field] for each field in the source row and then [InputRow::finish]
    /// (or use [InputRow::push_bson] / [InputRow::push_json]).
    pub fn evaluate<'a, E: ReplicationEmbedder>(
        &'a mut self,
        table: SourceTableRef<'a>,
        group: &'a ProcessorGroup<E>,
    ) -> InputRow<'a, E> {
        // Values of the previous row are no longer referenced, since the buffers are empty.
        self.arena.reset();

        let arena = &self.arena;

        let frame = Frame::new(
            arena,
            &self.compatibility,
            group.num_input_slots,
            group.num_internal_slots,
        );
        InputRow::new(group, table, frame)
    }
}

/// A decoded input row being evaluated.
///
/// Iterate over [Self::members] to evaluate the row. For data and parameter sources, call
/// [Self::process_data] and [Self::process_parameters] to evaluate results.
///
/// `'a` refers both to values allocated in the scoped bumpalo allocator and to values borrowed from
/// the processed row.
pub struct RowEvaluation<'a, E: ReplicationEmbedder> {
    group: &'a ProcessorGroup<E>,
    table: SourceTableRef<'a>,

    program: Program<'a>,
    /// A buffer for parameters of data and parameter sources, large enough for all members of the
    /// group.
    parameters: &'a mut [SqliteValue<'a>],
    /// A buffer for outputs of parameter sources, large enough for all members of the group.
    parameter_outputs: &'a mut [SqliteValue<'a>],
}

impl<'a, E: ReplicationEmbedder> RowEvaluation<'a, E> {
    pub(super) fn new(
        group: &'a ProcessorGroup<E>,
        table: SourceTableRef<'a>,
        program: Program<'a>,
        parameters: &'a mut [SqliteValue<'a>],
        parameter_outputs: &'a mut [SqliteValue<'a>],
    ) -> Self {
        Self {
            group,
            table,
            program,
            parameters,
            parameter_outputs,
        }
    }

    /// All processors that need to evaluate this row.
    pub fn members(&self) -> &'a [ProcessedGroupMember<E>] {
        &self.group.processors
    }

    /// Evaluates a data source of this row's group, returning an iterator over the buckets the
    /// row is put into.
    pub fn process_data<'b>(
        &'b mut self,
        source: &'a ProcessedDataSource<E>,
    ) -> BucketDataIterator<'a, 'b, E> {
        self.program.start(&source.common.instructions);
        let table = source
            .output_table_name
            .map(|s| resolve(&self.group.interner, s))
            .unwrap_or(self.table.name);

        BucketDataIterator {
            source,
            table,
            done: false,
            eval: self,
        }
    }

    /// Evaluates a parameter source of this row's group, returning an iterator over the
    /// parameter lookups created from the row.
    pub fn process_parameters<'b>(
        &'b mut self,
        source: &'a ProcessedParameterSource<E>,
    ) -> ParameterLookupIterator<'a, 'b, E> {
        self.program.start(&source.common.instructions);

        ParameterLookupIterator {
            source,
            done: false,
            eval: self,
        }
    }

    /// Runs the program to the next match for which `read_outputs` returns `true`.
    ///
    /// Returns `false` once there are no further matches.
    fn next_match(
        &mut self,
        read_outputs: impl Fn(&mut Self) -> bool,
    ) -> Result<bool, EvaluationError<'a>> {
        while self.program.next_match()? {
            if read_outputs(self) {
                return Ok(true);
            }
            // Invalid parameters, skip this match.
        }

        Ok(false)
    }
}

/// Copies values from `slots` into `buffer`, returning whether they are valid parameters.
fn read_values<'a>(frame: &Frame<'a>, slots: &[Slot], buffer: &mut [SqliteValue<'a>]) -> bool {
    let mut valid = true;
    for (slot, value) in slots.iter().zip(buffer.iter_mut()) {
        *value = frame[*slot];
        valid &= value.is_valid_parameter();
    }

    valid
}

/// Views the first `len` values of a buffer filled by [read_values].
fn as_parameters<'b, 'a>(buffer: &'b [SqliteValue<'a>], len: usize) -> Parameters<'b, 'a> {
    Parameters(&buffer[..len])
}

pub struct BucketDataIterator<'a, 'b, E: ReplicationEmbedder> {
    source: &'a ProcessedDataSource<E>,
    table: &'a str,
    done: bool,
    eval: &'b mut RowEvaluation<'a, E>,
}

/// An evaluated output row.
pub struct BucketData<'a, 'r, E: ReplicationEmbedder> {
    pub bucket: &'a E::BucketId,
    pub parameters: Parameters<'r, 'a>,
    pub table: &'a str,
    pub row: ProjectedRow<'r, 'a>,
}

impl<'a, E: ReplicationEmbedder> BucketDataIterator<'a, '_, E> {
    /// Returns the next bucket for the row.
    ///
    /// After an error, the data source is abandoned and this returns [None].
    // This is a lending iterator, can't implement the trait
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<BucketData<'a, '_, E>, EvaluationError<'a>>> {
        if self.done {
            return None;
        }

        let source = self.source;
        let has_match = self.eval.next_match(|eval| {
            read_values(
                &eval.program.frame,
                &source.common.parameters,
                eval.parameters,
            )
        });

        match has_match {
            Ok(true) => {}
            Ok(false) => {
                self.done = true;
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        }

        let eval = &*self.eval;
        let frame = &eval.program.frame;
        let projection = &source.projection;
        Some(Ok(BucketData {
            bucket: &source.id,
            parameters: as_parameters(eval.parameters, source.common.parameters.len()),
            table: self.table,
            row: ProjectedRow {
                interner: &eval.group.interner,
                projection,
                frame,
                star_columns: if projection.columns.has_star() {
                    &frame.star_columns
                } else {
                    &[]
                },
            },
        }))
    }
}

pub struct ParameterLookupIterator<'a, 'b, E: ReplicationEmbedder> {
    source: &'a ProcessedParameterSource<E>,
    done: bool,
    eval: &'b mut RowEvaluation<'a, E>,
}

pub struct EvaluatedParameter<'a, 'r, E: ReplicationEmbedder> {
    pub id: &'a E::ParameterId,
    pub lookup: Parameters<'r, 'a>,
    pub outputs: Parameters<'r, 'a>,
}

impl<'a, E: ReplicationEmbedder> ParameterLookupIterator<'a, '_, E> {
    /// Returns the next parameter lookup for the row.
    ///
    /// After an error, the parameter source is abandoned and this returns [None].
    #[allow(clippy::should_implement_trait)] // This is a lending iterator.
    pub fn next(&mut self) -> Option<Result<EvaluatedParameter<'a, '_, E>, EvaluationError<'a>>> {
        if self.done {
            return None;
        }

        let source = self.source;
        let has_match = self.eval.next_match(|eval| {
            let frame = &eval.program.frame;
            // Not short-circuiting since both buffers need to be filled for a valid match.
            read_values(frame, &source.common.parameters, eval.parameters)
                & read_values(frame, &source.outputs, eval.parameter_outputs)
        });

        match has_match {
            Ok(true) => {}
            Ok(false) => {
                self.done = true;
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        }

        let eval = &*self.eval;
        Some(Ok(EvaluatedParameter {
            id: &source.id,
            lookup: as_parameters(eval.parameters, source.common.parameters.len()),
            outputs: as_parameters(eval.parameter_outputs, source.outputs.len()),
        }))
    }
}

fn resolve(interner: &StringInterner, symbol: Symbol) -> &str {
    interner
        .resolve(symbol)
        .expect("aliases are interned in the config's interner")
}
