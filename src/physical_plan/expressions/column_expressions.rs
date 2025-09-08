use std::fmt;

use crate::datatypes::{column_vector::ColumnVector, record_batch::RecordBatch};

#[derive(Debug, Clone, Copy)]
pub struct ColumnExpression {
    pub i: usize,
}

impl ColumnExpression {
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        return input.field(self.i);
    }
}

impl fmt::Display for ColumnExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.i)
    }
}
