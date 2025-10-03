use std::sync::Arc;

use arrow::{
    array::BooleanArray,
    compute:: kernels::filter,
};

use crate::{
    datatypes::{
        arrow_field_vector::ArrowFieldVector, column_vector::ColumnVector,
        record_batch::RecordBatch,
    },
    physical_plan::{PhysPlanTrait, PhysicaPlan, expressions::Expression},
};

pub struct SelectionExec {
    input: Arc<PhysicaPlan>,
    expr: Expression,
}

impl PhysPlanTrait for SelectionExec {
    fn schema(&self) -> crate::datatypes::schema::Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<std::sync::Arc<PhysicaPlan>> {
        vec![self.input.clone()]
    }
    fn execute(&self) -> impl Iterator<Item = crate::datatypes::record_batch::RecordBatch> {
        let input = self.input.execute();

        let iter = input.map(|batch| {
            if let ColumnVector::ArrowVector(result) = self.expr.evaluate(batch.clone()) {
                let array_ref = result.field;

                let predicate = array_ref
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Boolean Array");

                let filtered_fields: ColumnVector = ColumnVector::ArrowVector(ArrowFieldVector {
                    field: filter::filter(predicate, predicate).unwrap(),
                    
                });

                RecordBatch {
                    schema: self.schema(),
                    fields: vec![filtered_fields],
                }
            } else {
                panic!("Cannot execute on a literal value vector")
            }
        });

        iter
    }
}
