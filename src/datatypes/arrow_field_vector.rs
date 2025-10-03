use crate::{datatypes::value::ArrowValue, downcast_arry, match_and};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, PrimitiveArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::DataType,
};

pub struct ArrowPVector<T: arrow::array::ArrowPrimitiveType> {
    pub field: PrimitiveArray<T>,
}

/** Wrapper around Arrow ArrayRef */
#[derive(Clone, Debug)]
pub struct ArrowFieldVector {
    pub field: ArrayRef,
}
impl ArrowFieldVector {
    pub fn get_type(&self) -> DataType {
        return self.field.data_type().clone();
    }

    pub fn get_value(&self, i: usize) -> Option<ArrowValue> {
        if self.field.is_null(i) {
            return None;
        } else {
            let field = self.field.clone();
            let value = match_and!(self.field.data_type(), downcast_arry, field, i);

            return Some(value);
        }
    }

    pub fn size(&self) -> usize {
        self.field.len()
    }
}
