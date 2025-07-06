use crate::{
    build_array,
    datatypes::{column_vector::ColumnVector, value::ArrowValue},
    downcast_arry, match_and,
};
use anyhow::{ Ok};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
        Float64Builder, Int8Array, Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder,
        Int64Array, Int64Builder, StringArray, StringBuilder, UInt8Array, UInt8Builder,
        UInt16Array, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::DataType,
};

/*Experiment with more macros */
struct FieldVectorFactory;
impl FieldVectorFactory {
    pub fn create(arrow_type: DataType, initital_capacity: usize) -> anyhow::Result<ArrayRef> {
        let field_vector = match_and!(arrow_type, build_array, initital_capacity);

        Ok(field_vector)
    }
}

/** Wrapper around Arrow ArrayRef */
pub struct ArrowFieldVector {
    pub field: ArrayRef,
}
impl ColumnVector for ArrowFieldVector {
    fn get_type(&self) -> DataType {
        return self.field.data_type().clone();
    }

    fn get_value(&self, i: usize) -> Option<ArrowValue> {
        if self.field.is_null(i) {
            panic!("Null Field");
        }

        let field = self.field.clone();
        let value = match_and!(self.field.data_type(), downcast_arry, field, i);

        Some(value)
    }

    fn size(&self) -> usize {
        self.field.len()
    }
}
