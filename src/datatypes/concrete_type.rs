use arrow::datatypes::DataType;

use crate::datatypes::{arrow_vector_builder::VectorBuilder, value::ArrowValue};

pub trait ConcreteType {
    fn get_conc_type(&self) -> DataType;
}

impl ConcreteType for ArrowValue {
    fn get_conc_type(&self) -> arrow::datatypes::DataType {
        match self {
            ArrowValue::BooleanType(_) => DataType::Boolean,

            ArrowValue::DoubleType(_) => DataType::Float64,
            ArrowValue::FloatType(_) => DataType::Float32,

            ArrowValue::Int16Type(_) => DataType::Int16,
            ArrowValue::Int32Type(_) => DataType::Int32,
            ArrowValue::Int64Type(_) => DataType::Int64,
            ArrowValue::Int8Type(_) => DataType::Int8,

            ArrowValue::StringType(_) => DataType::Utf8,

            ArrowValue::UInt16Type(_) => DataType::UInt16,
            ArrowValue::UInt32Type(_) => DataType::UInt32,
            ArrowValue::UInt64Type(_) => DataType::UInt64,
            ArrowValue::UInt8Type(_) => DataType::UInt8,
        }
    }
}

impl ConcreteType for VectorBuilder {
    fn get_conc_type(&self) -> DataType {
        match self {
            VectorBuilder::Boolean(_) => DataType::Boolean,

            VectorBuilder::Double(_) => DataType::Float64,
            VectorBuilder::Float(_) => DataType::Float32,

            VectorBuilder::Int16(_) => DataType::Int16,
            VectorBuilder::Int32(_) => DataType::Int32,
            VectorBuilder::Int64(_) => DataType::Int64,
            VectorBuilder::Int8(_) => DataType::Int8,

            VectorBuilder::String(_) => DataType::Utf8,

            VectorBuilder::UInt16(_) => DataType::UInt16,
            VectorBuilder::UInt32(_) => DataType::UInt32,
            VectorBuilder::UInt64(_) => DataType::UInt64,
            VectorBuilder::UInt8(_) => DataType::UInt8,
        }
    }
}
