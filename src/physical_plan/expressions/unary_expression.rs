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
    inner: Box<dyn UnaryPlan>,
    pub expr: Expression,
}

impl UnaryExpr {
    fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        let n = self.expr.evaluate(input);
        let mut vec: Vec<f32> = vec![];

        for i in 0..n.size() {
            let nv = n.get_value_inner(i);

            if let None = nv {
                vec.push(0.0);
            } else {
                if let Some(ArrowValue::FloatType(nv)) = nv {
                    vec.push(self.inner.apply(nv));
                }
            }
        }

        let a_value = TypeVector::Float(vec);

        let a = build_vector(DataType::Float32, &a_value);

        a
    }
}
pub trait UnaryPlan {
    fn apply(&self, value: f32) -> f32;
}

struct Sqrt;
impl UnaryPlan for Sqrt {
    fn apply(&self, value: f32) -> f32 {
        value.sqrt() as f32
    }
}

struct Log;
impl UnaryPlan for Log {
    fn apply(&self, value: f32) -> f32 {
        value.ln() as f32
    }
}
