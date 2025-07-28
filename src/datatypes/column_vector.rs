use arrow::datatypes::DataType;

use crate::datatypes::{
    arrow_field_vector::ArrowFieldVector, literal_value_vector::LiteralValueVector,
    value::ArrowValue,
};

pub trait ColumnVectorTrait {
    fn get_type(&self) -> DataType;
    fn get_value(&self, i: usize) -> Option<ArrowValue>;
    fn size(&self) -> usize;
}

#[derive(Clone,Debug)]
pub enum ColumnVector {
    Literal(LiteralValueVector),
    ArrowVector(ArrowFieldVector),
}

impl ColumnVectorTrait for ColumnVector {
    fn get_type(&self) -> DataType {
        match self {
            ColumnVector::Literal(literal_value_vector) => literal_value_vector.get_type(),
            ColumnVector::ArrowVector(arrow_field_vector) => arrow_field_vector.get_type(),
        }
    }

    fn get_value(&self, i: usize) -> Option<ArrowValue> {
        match self {
            ColumnVector::Literal(literal_value_vector) => literal_value_vector.get_value(i),
            ColumnVector::ArrowVector(arrow_field_vector) => arrow_field_vector.get_value(i),
        }
    }

    fn size(&self) -> usize {
        match self {
            ColumnVector::Literal(literal_value_vector) => literal_value_vector.size(),
            ColumnVector::ArrowVector(arrow_field_vector) => arrow_field_vector.size(),
        }
    }
}
