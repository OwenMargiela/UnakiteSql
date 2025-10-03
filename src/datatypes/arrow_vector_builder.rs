use std::{sync::Arc, vec};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
        UInt64Builder,
    },
    datatypes::DataType,
};

use crate::{
    append_dispatch,
    datatypes::{
        arrow_field_vector::ArrowFieldVector, column_vector::ColumnVector,
        concrete_type::ConcreteType, value::ArrowValue,
    },
    dispatch_builder_array_ref, init_builder, match_and,
};

pub enum VectorBuilder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float(Float32Builder),
    Double(Float64Builder),
    String(StringBuilder),
}

struct Stager {
    buffer: Vec<Option<ArrowValue>>,
}

impl Stager {
    pub fn new() -> Self {
        Self {
            buffer: vec![None; 10],
        }
    }

    pub fn set(&mut self, index: usize, value: Option<ArrowValue>) {
        if index >= self.buffer.len() {
            panic!("Index out of bounds");
        }

        self.buffer[index] = value;
    }
}

pub struct ArrowVectorBuilder {
    builder: VectorBuilder,
    stager: Stager,
}

impl ArrowVectorBuilder {
    pub fn new(datatype: &DataType) -> Self {
        let builder = match_and!(init_builder, datatype);
        let stager = Stager::new();

        Self { builder, stager }
    }
    pub fn set(&mut self, i: usize, value: Option<ArrowValue>) {
        if value.is_none() {
            self.stager.set(i, value);
            return;
        }

        if let Some(ref arrow_value) = value {
            if arrow_value.get_conc_type() == self.builder.get_conc_type() {
                self.stager.set(i, value);
            }
        }
    }

    pub fn set_all(&mut self, values: &mut Vec<Option<ArrowValue>>) {
        if values.is_empty() {
            return;
        }

        for (i, value) in values.iter_mut().enumerate() {
            if let Some(inner) = value.take() {
                if inner.get_conc_type() == self.builder.get_conc_type() {
                    self.stager.set(i, Some(inner));
                } else {
                    self.stager.set(i, None);
                }
            } else {
                self.stager.set(i, None);
            }
        }
    }

    fn append(&mut self, value: Option<ArrowValue>) {
        append_dispatch!(
            &mut self.builder,
            value,
            (Boolean, BooleanType),
            (Int8, Int8Type),
            (Int16, Int16Type),
            (Int32, Int32Type),
            (Int64, Int64Type),
            (UInt8, UInt8Type),
            (UInt16, UInt16Type),
            (UInt32, UInt32Type),
            (UInt64, UInt64Type),
            (Float, FloatType),
            (Double, DoubleType),
        );
    }

    pub fn build(mut self) -> ColumnVector {
        for arrow_value in self.stager.buffer.clone().into_iter() {
            self.append(arrow_value);
        }

        let array_ref = dispatch_builder_array_ref!(
            &mut self.builder,
            Boolean,
            Int8,
            Int16,
            Int32,
            Int64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Float,
            Double,
            String,
        );

        ColumnVector::ArrowVector(ArrowFieldVector { field: array_ref })
    }
}

pub enum TypeVector {
    Boolean(Vec<bool>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    String(Vec<String>),
}

use std::any::Any;

impl TypeVector {
    pub fn as_any(&self) -> &dyn Any {
        match self {
            TypeVector::Boolean(v) => v,
            TypeVector::Int8(v) => v,
            TypeVector::Int16(v) => v,
            TypeVector::Int32(v) => v,
            TypeVector::Int64(v) => v,
            TypeVector::UInt8(v) => v,
            TypeVector::UInt16(v) => v,
            TypeVector::UInt32(v) => v,
            TypeVector::UInt64(v) => v,
            TypeVector::Float(v) => v,
            TypeVector::Double(v) => v,
            TypeVector::String(v) => v,
        }
    }
}

// Helper to generate vectors
#[macro_export]
macro_rules! generate_match_arms {
    ($builder:expr, $vector:expr, $( ($variant:ident, $arrow_type:ident) ),* $(,)? ) => {
        match $vector {
            $(
                TypeVector::$variant(items) => {
                    for (i, value) in items.iter().enumerate() {
                        $builder.set(i, Some(ArrowValue::$arrow_type(*value)));
                    }
                }
            )*
            // Special case for String
            TypeVector::String(items) => {
                for (i, value) in items.iter().enumerate() {
                    $builder.set(i, Some(ArrowValue::StringType(value.to_string())));
                }
            }
        }
    };
}

pub fn build_vector(data_type: DataType, vector: &TypeVector) -> ColumnVector {
    let mut builder = ArrowVectorBuilder::new(&data_type);

    generate_match_arms!(
        builder,
        vector,
        (Boolean, BooleanType),
        (Int8, Int8Type),
        (Int16, Int16Type),
        (Int32, Int32Type),
        (Int64, Int64Type),
        (UInt8, UInt8Type),
        (UInt16, UInt16Type),
        (UInt32, UInt32Type),
        (UInt64, UInt64Type),
        (Float, FloatType),
        (Double, DoubleType),
    );

    builder.build()
}
