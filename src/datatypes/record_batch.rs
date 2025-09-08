

use crate::datatypes::{
    column_vector::{ColumnVector, ColumnVectorTrait},
    schema::Schema,
};

#[derive(Debug,Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub fields: Vec<ColumnVector>,
}

impl RecordBatch {
    pub fn row_count(&self) -> usize {
        self.fields.first().unwrap().size()
    }

    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    /** Access one column by index */
    pub fn field(&self, i: usize) -> ColumnVector {
        let field = self.fields.get(i).unwrap().clone();
        field
    }
}
