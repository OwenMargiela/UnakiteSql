use crate::datatypes::{column_vector::ColumnVector, schema::Schema};

struct RecordBatch {
    schema: Schema,
    fields: Vec<Box<dyn ColumnVector>>,
}

impl RecordBatch {
    pub fn row_count(&self) -> usize {
        self.fields.first().unwrap().size()
    }

    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    /** Access one column by index */
    pub fn field(&self, i: usize) -> &Box<dyn ColumnVector> {
        self.fields.get(i).unwrap()
    }
}
