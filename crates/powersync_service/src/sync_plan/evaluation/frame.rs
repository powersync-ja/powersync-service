use std::ops::{Index, IndexMut};

use bumpalo::{Bump, collections::Vec};

use crate::sync_plan::{CompatibilityContext, SqliteValue, Symbol, processor_group::Slot};

/// State for evaluating a single source row.
pub struct Frame<'a> {
    pub arena: &'a Bump,
    pub compatibility: &'a CompatibilityContext,
    /// Values for [Slot::Input], written before evaluating processors.
    inputs: &'a mut [SqliteValue<'a>],
    /// Values for [Slot::Internal], written by instructions of the processor being evaluated.
    internals: &'a mut [SqliteValue<'a>],
    pub star_columns: Vec<'a, StarColumn<'a>>,
}

impl<'a> Frame<'a> {
    pub fn new(
        arena: &'a Bump,
        compatibility: &'a CompatibilityContext,
        num_inputs: u32,
        num_internals: u32,
    ) -> Self {
        Self {
            arena,
            compatibility,
            inputs: arena.alloc_slice_fill_default(num_inputs as usize),
            internals: arena.alloc_slice_fill_default(num_internals as usize),
            star_columns: Vec::new_in(arena),
        }
    }

    pub fn clear_internals(&mut self) {
        self.internals.fill(SqliteValue::default());
    }
}

impl<'a> Index<Slot> for Frame<'a> {
    type Output = SqliteValue<'a>;

    fn index(&self, index: Slot) -> &Self::Output {
        match index {
            Slot::Input(index) => &self.inputs[index as usize],
            Slot::Internal(index) => &self.internals[index as usize],
        }
    }
}

impl<'a> IndexMut<Slot> for Frame<'a> {
    fn index_mut(&mut self, index: Slot) -> &mut Self::Output {
        match index {
            Slot::Input(index) => &mut self.inputs[index as usize],
            Slot::Internal(index) => &mut self.internals[index as usize],
        }
    }
}

/// A field of the source row included in `*` projections.
#[derive(Clone, Copy, Debug)]
pub struct StarColumn<'a> {
    pub name: &'a str,
    /// The interned name, or [None] if no processor references a column with that name.
    pub symbol: Option<Symbol>,
    pub value: SqliteValue<'a>,
}
