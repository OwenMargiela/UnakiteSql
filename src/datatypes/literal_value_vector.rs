use crate::datatypes::{column_vector::ColumnVector, value::ArrowValue};
use arrow::datatypes::DataType;

#[allow(dead_code)]
struct LiteralValueVector {
    arrow_type: DataType,
    value: Option<ArrowValue>,
    size: usize,
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> DataType {
        self.arrow_type.clone()
    }

    fn get_value(&self, i: usize) -> Option<ArrowValue> {
        if i >= self.size {
            let literal = format!("Index {} out of bounds", i);
            panic!("{literal}");
        }

        self.value.clone()
    }

    fn size(&self) -> usize {
        self.size
    }
}
