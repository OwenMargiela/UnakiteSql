use crate::datatypes::value::ArrowValue;
use arrow::datatypes::DataType;

#[allow(dead_code)]

#[derive(Clone, Debug)]
pub struct LiteralValueVector {
    pub arrow_type: DataType,
    pub value: Option<ArrowValue>,
    pub size: usize,
}

impl LiteralValueVector {
    pub fn get_type(&self) -> DataType {
        self.arrow_type.clone()
    }

    pub fn get_value(&self, i: usize) -> Option<ArrowValue> {
        if i >= self.size {
            let literal = format!("Index {} out of bounds", i);
            panic!("{literal}");
        }

        self.value.clone()
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
