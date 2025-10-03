use arrow::datatypes::DataType;

use crate::{
    datatypes::{
        arrow_vector_builder::{TypeVector, build_vector},
        column_vector::{ColumnVector, ColumnVectorTrait},
        record_batch::RecordBatch,
        value::ArrowValue,
    },
    physical_plan::expressions::Expression,
};

pub struct UnaryExpr {
    _inner: Box<dyn UnaryPlan>,
    pub expr: Expression,
}

impl UnaryExpr {
    fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        let n = self.expr.evaluate(input);

        unimplemented!()
    }
}
pub trait UnaryPlan {
    fn apply(&self, value: ColumnVector) -> ColumnVector;
}

struct Sqrt;
impl UnaryPlan for Sqrt {
    fn apply(&self, value: ColumnVector) -> ColumnVector {
        unimplemented!()
    }
}

struct Log;
impl UnaryPlan for Log {
    fn apply(&self, value: ColumnVector) -> ColumnVector {
        unimplemented!()
    }
}
