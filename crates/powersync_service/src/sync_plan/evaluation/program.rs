use std::mem;

use bumpalo::collections::Vec;

use crate::sync_plan::{
    EvaluationError,
    cast::cast,
    evaluation::Frame,
    expression::{TableValuedFunction, UnaryOperator},
    operators::{
        between, binary::evaluate_binary_operator, bool::sqlite_not, case_when, scalar_in,
        table_valued::TableValuedFunctionEvaluation,
    },
    processor_group::{EvaluatorInstruction, ScalarOperation},
};

/// Runs [EvaluatorInstruction]s of a processor as a state machine, stopping at each match.
///
/// A match is a combination of table-valued function rows for which all filters are truthy.
/// Expanding a table-valued function conceptually runs the remaining instructions in a loop for
/// each of its tows. To do that without recursion, we keep a stack of active expansions to
/// backtrack into once the remaining instructions have been evaluated for a row.
pub struct Program<'a> {
    pub frame: Frame<'a>,
    instructions: &'a [EvaluatorInstruction],
    /// The index of the next instruction to run.
    pc: usize,
    /// Whether the last [Self::next_match] call returned a match, meaning that the next call needs
    /// to backtrack before running instructions.
    did_yield: bool,
    /// A stack of table-valued functions being expanded, with the innermost function last.
    table_valued_stack: Vec<'a, Expansion<'a>>,
}

struct Expansion<'a> {
    /// The index of the instruction to resume at for each row of the function.
    loop_start: usize,
    function: TableValuedFunctionEvaluation<'a>,
}

impl<'a> Program<'a> {
    /// Creates a program evaluating instructions on the `frame`.
    ///
    /// `max_table_valued_functions` is the maximum amount of table-valued functions of any processor
    /// to run, used to allocate the stack of expansions ahead of time.
    pub fn new(frame: Frame<'a>, max_table_valued_functions: usize) -> Self {
        Self {
            instructions: &[],
            pc: 0,
            did_yield: false,
            table_valued_stack: Vec::with_capacity_in(max_table_valued_functions, frame.arena),
            frame,
        }
    }

    pub fn start(&mut self, instructions: &'a [EvaluatorInstruction]) {
        self.frame.clear_internals();

        self.instructions = instructions;
        self.pc = 0;
        self.did_yield = false;
        self.table_valued_stack.clear();
    }

    /// Runs instructions until the next match, returning `false` once all matches have been
    /// visited.
    ///
    /// After this returns `false` or an error, [Self::start] must be called before calling this
    /// again.
    pub fn next_match(&mut self) -> Result<bool, EvaluationError<'a>> {
        if mem::take(&mut self.did_yield) && !self.backtrack() {
            return Ok(false);
        }

        loop {
            let Some(instruction) = self.instructions.get(self.pc) else {
                self.did_yield = true;
                return Ok(true);
            };
            self.pc += 1;

            match instruction {
                EvaluatorInstruction::Evaluate { op, output_slot } => {
                    let scalar = match op {
                        ScalarOperation::Literal(literal) => literal.evaluate(),
                        ScalarOperation::Unary { operator, operand } => {
                            let operand = self.frame[*operand];

                            match operator {
                                UnaryOperator::Not => sqlite_not(operand),
                                UnaryOperator::Plus => operand,
                            }
                        }
                        ScalarOperation::Binary {
                            left,
                            operator,
                            right,
                        } => evaluate_binary_operator(
                            &self.frame,
                            self.frame[*left],
                            *operator,
                            self.frame[*right],
                        )?,
                        ScalarOperation::FunctionCall { function, inputs } => {
                            function(inputs, &self.frame)?
                        }
                        ScalarOperation::Cast { operand, cast_as } => {
                            let operand = self.frame[*operand];
                            cast(operand, *cast_as, self.frame.arena)
                        }
                        ScalarOperation::ScalarIn { target, r#in } => {
                            scalar_in(&self.frame, *target, r#in)
                        }
                        ScalarOperation::Between { value, low, high } => {
                            between(&self.frame, *value, *low, *high)
                        }
                        ScalarOperation::CaseWhen {
                            operand,
                            whens,
                            r#else,
                        } => case_when(&self.frame, *operand, whens, *r#else),
                    };

                    self.frame[*output_slot] = scalar;
                }
                EvaluatorInstruction::CheckFilter(slot) => {
                    if !self.frame[*slot].is_truthy() && !self.backtrack() {
                        return Ok(false);
                    }
                }
                EvaluatorInstruction::ExpandTableValuedFunction(function) => {
                    let function = match function.function {
                        TableValuedFunction::JsonEach => TableValuedFunctionEvaluation::json_each(
                            &self.frame,
                            &function.inputs,
                            &function.outputs,
                        ),
                    }?;
                    self.table_valued_stack.push(Expansion {
                        loop_start: self.pc,
                        function,
                    });

                    // Moves to the first row of the function, if there is one.
                    if !self.backtrack() {
                        return Ok(false);
                    }
                }
            }
        }
    }

    /// Moves to the next row of the innermost table-valued function that has remaining rows,
    /// returning `false` if there is none.
    fn backtrack(&mut self) -> bool {
        while let Some(expansion) = self.table_valued_stack.last_mut() {
            if expansion.function.next(&mut self.frame) {
                self.pc = expansion.loop_start;
                return true;
            }

            self.table_valued_stack.pop();
        }

        false
    }
}
