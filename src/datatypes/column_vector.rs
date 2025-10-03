use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::datatypes::{
    arrow_field_vector::ArrowFieldVector, literal_value_vector::LiteralValueVector,
    value::ArrowValue,
};

pub trait ColumnVectorTrait {
    fn get_type(&self) -> DataType;
    fn get_value_inner(&self, i: usize) -> Option<ArrowValue>;
    fn size(&self) -> usize;
}

#[derive(Clone, Debug)]
pub enum ColumnVector {
    Literal(Arc<LiteralValueVector>),
    ArrowVector(ArrowFieldVector),
}

impl ColumnVector {
    pub fn get_value(&self, i: usize) -> ArrowValue {
        if let Some(inner) = self.get_value_inner(i) {
            return inner;
        }

        panic!("Unwarp on a null value")
    }

    pub fn get_vector(&self) -> &ArrowFieldVector {
        if let ColumnVector::ArrowVector(vec) = self {
            return vec;
        } else {
            panic!("Fall getting Field Vector")
        }
    }

     pub fn get_mut_vector(self) -> ArrowFieldVector {
        if let ColumnVector::ArrowVector(vec) = self {
            return vec;
        } else {
            panic!("Fall getting Field Vector")
        }
    }
}
impl ColumnVectorTrait for ColumnVector {
    fn get_type(&self) -> DataType {
        match self {
            ColumnVector::Literal(literal_value_vector) => literal_value_vector.get_type(),
            ColumnVector::ArrowVector(arrow_field_vector) => arrow_field_vector.get_type(),
        }
    }

    fn get_value_inner(&self, i: usize) -> Option<ArrowValue> {
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
