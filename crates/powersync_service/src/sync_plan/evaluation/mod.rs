mod frame;
mod input_row;
mod program;
mod projected_row;
#[cfg(test)]
mod test;

use std::{mem::MaybeUninit, sync::Arc};

use bumpalo::Bump;
pub use frame::Frame;
pub use input_row::{InputRow, PreparedField};
pub use projected_row::ProjectedRow;

use crate::sync_plan::{
    CompatibilityContext, EvaluationError, ReplicationEmbedder, SourceTableRef, StringInterner,
    Symbol,
    evaluation::program::Program,
    processor_group::{
        ProcessedDataSource, ProcessedGroupMember, ProcessedParameterSource, ProcessorGroup, Slot,
    },
    value::Parameter,
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
    parameters: &'a mut [MaybeUninit<Parameter<'a>>],
}

impl<'a, E: ReplicationEmbedder> RowEvaluation<'a, E> {
    pub(super) fn new(
        group: &'a ProcessorGroup<E>,
        table: SourceTableRef<'a>,
        program: Program<'a>,
        parameters: &'a mut [MaybeUninit<Parameter<'a>>],
    ) -> Self {
        Self {
            group,
            table,
            program,
            parameters,
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
}

/// Runs the program to the next match for which all `slots` contain valid parameters.
///
/// Returns the parameters read from `slots`, or [None] once there are no further valid matches.
fn next_match<'r, 'a>(
    program: &mut Program<'a>,
    slots: impl Iterator<Item = Slot> + Clone,
    buffer: &'r mut [MaybeUninit<Parameter<'a>>],
) -> Result<Option<&'r [Parameter<'a>]>, EvaluationError<'a>> {
    'matches: while program.next_match()? {
        let frame = &program.frame;
        let mut len = 0;
        for slot in slots.clone() {
            let Some(parameter) = frame[slot].as_parameter() else {
                // Not valid parameters, skip this match.
                continue 'matches;
            };

            buffer[len].write(parameter);
            len += 1;
        }

        return Ok(Some(unsafe {
            // SAFETY: The loop above initialized the first `len` entries of the buffer.
            buffer[..len].assume_init_ref()
        }));
    }

    Ok(None)
}

pub struct BucketDataIterator<'a, 'b, E: ReplicationEmbedder> {
    pub source: &'a ProcessedDataSource<E>,
    pub table: &'a str,
    done: bool,
    eval: &'b mut RowEvaluation<'a, E>,
}

/// An evaluated output row.
pub struct BucketData<'a, 'r, E: ReplicationEmbedder> {
    pub bucket: &'a E::BucketId,
    pub parameters: &'r [Parameter<'a>],
    pub table: &'a str,
    pub row: ProjectedRow<'r, 'a>,
}

impl<'a, E: ReplicationEmbedder> BucketDataIterator<'a, '_, E> {
    /// The allocator for values computed while evaluating the row.
    pub fn arena(&self) -> &'a Bump {
        self.eval.program.frame.arena
    }

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
        let next_match = next_match(
            &mut self.eval.program,
            source.common.parameters.iter().copied(),
            self.eval.parameters,
        );
        let parameters = match next_match {
            Ok(Some(parameters)) => parameters,
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        };

        let frame = &self.eval.program.frame;
        let projection = &source.projection;
        Some(Ok(BucketData {
            bucket: &source.id,
            parameters,
            table: self.table,
            row: ProjectedRow {
                interner: &self.eval.group.interner,
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
    pub source: &'a ProcessedParameterSource<E>,
    done: bool,
    eval: &'b mut RowEvaluation<'a, E>,
}

pub struct EvaluatedParameter<'a, 'r, E: ReplicationEmbedder> {
    pub id: &'a E::ParameterId,
    pub lookup: &'r [Parameter<'a>],
    pub outputs: &'r [Parameter<'a>],
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
        let next_match = next_match(
            &mut self.eval.program,
            source
                .common
                .parameters
                .iter()
                .chain(&source.outputs)
                .copied(),
            self.eval.parameters,
        );

        let (lookup, outputs) = match next_match {
            Ok(Some(values)) => values.split_at(source.common.parameters.len()),
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        };

        Some(Ok(EvaluatedParameter {
            id: &source.id,
            lookup,
            outputs,
        }))
    }
}

fn resolve(interner: &StringInterner, symbol: Symbol) -> &str {
    interner
        .resolve(symbol)
        .expect("aliases are interned in the config's interner")
}
