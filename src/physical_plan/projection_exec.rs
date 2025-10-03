use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    datatypes::{
        column_vector::ColumnVector, record_batch::RecordBatch, schema::schema_from_arrow_schema,
    },
    physical_plan::{PhysPlanTrait, PhysicaPlan, expressions::Expression},
};

pub struct ProjectionExec {
    input: Arc<PhysicaPlan>,
    schema: Arc<Schema>,
    expr: Vec<Expression>,
}

impl PhysPlanTrait for ProjectionExec {
    fn schema(&self) -> crate::datatypes::schema::Schema {
        schema_from_arrow_schema(self.schema.clone())
    }

    fn children(&self) -> Vec<Arc<PhysicaPlan>> {
        vec![self.input.clone()]
    }

    fn execute(&self) -> impl Iterator<Item = RecordBatch> {
        let return_vec = self.input.execute().map( move |batch| {
            let columns: Vec<ColumnVector> =
                self.expr.iter().map(|it| it.evaluate(batch.clone())).collect();

            RecordBatch {
                fields: columns,
                schema: self.schema(),
            }
        });

        return return_vec
    }
}
