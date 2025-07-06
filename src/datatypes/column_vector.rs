use arrow::datatypes::DataType;

use crate::datatypes::value::ArrowValue;

pub trait ColumnVector {
    fn get_type(&self) -> DataType;
    fn get_value(&self, i: usize) -> Option<ArrowValue>;
    fn size(&self) -> usize;
}
