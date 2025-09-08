use arrow::datatypes::DataType;

use crate::datatypes::value::ArrowValue;

pub trait CastAcross {
    fn cast_to(&self, data_type: DataType) -> ArrowValue;
}

// Make seggsy macro
macro_rules! match_arrow_value {
    ( $data_type:expr, $( ($variant:ident, $arrow_value:ident)),* $(,)? ) => {
        // Seggsy macro here

        match data_type {

            $(
                DataType::$variant => {
                    $arrow_value
                }
            )*

        }

    };
}

impl CastAcross for ArrowValue {
    fn cast_to(&self, data_type: DataType) -> ArrowValue {
        let value = match self {
            // --- Boolean -> other types ---
            ArrowValue::BooleanType(val) =>
            //
            {
                match data_type {
                    DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                    DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                    DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                    DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                    DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                    DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                    DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                    DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                    DataType::Float32 => ArrowValue::FloatType(*val as i8 as f32),
                    DataType::Float64 => ArrowValue::DoubleType(*val as i8 as f64),
                    DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                    DataType::Boolean => ArrowValue::BooleanType(*val),
                    _ => todo!(),
                }
            }

            // --- Int8 -> other types ---
            ArrowValue::Int8Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Int8 => ArrowValue::Int8Type(*val),
                _ => todo!(),
            },

            // --- Int16 -> other types ---
            ArrowValue::Int16Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Int16 => ArrowValue::Int16Type(*val),
                _ => todo!(),
            },

            // --- Int32 -> other types ---
            ArrowValue::Int32Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Int32 => ArrowValue::Int32Type(*val),
                _ => todo!(),
            },

            // --- Int64 -> other types ---
            ArrowValue::Int64Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Int64 => ArrowValue::Int64Type(*val),
                _ => todo!(),
            },

            // --- Unsigned ints ---
            ArrowValue::UInt8Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::UInt8 => ArrowValue::UInt8Type(*val),
                _ => todo!(),
            },

            ArrowValue::UInt16Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::UInt16 => ArrowValue::UInt16Type(*val),
                _ => todo!(),
            },

            ArrowValue::UInt32Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt64 => ArrowValue::UInt64Type(*val as u64),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::UInt32 => ArrowValue::UInt32Type(*val),
                _ => todo!(),
            },

            ArrowValue::UInt64Type(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0),
                DataType::Int8 => ArrowValue::Int8Type(*val as i8),
                DataType::Int16 => ArrowValue::Int16Type(*val as i16),
                DataType::Int32 => ArrowValue::Int32Type(*val as i32),
                DataType::Int64 => ArrowValue::Int64Type(*val as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(*val as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(*val as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(*val as u32),
                DataType::Float32 => ArrowValue::FloatType(*val as f32),
                DataType::Float64 => ArrowValue::DoubleType(*val as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::UInt64 => ArrowValue::UInt64Type(*val),
                _ => todo!(),
            },

            // --- Float32 ---
            ArrowValue::FloatType(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0.0),
                DataType::Int8 => ArrowValue::Int8Type(val.round() as i8),
                DataType::Int16 => ArrowValue::Int16Type(val.round() as i16),
                DataType::Int32 => ArrowValue::Int32Type(val.round() as i32),
                DataType::Int64 => ArrowValue::Int64Type(val.round() as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(val.round() as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(val.round() as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(val.round() as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(val.round() as u64),
                DataType::Float64 => ArrowValue::DoubleType(val.round() as f64),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Float32 => ArrowValue::FloatType(*val),
                _ => todo!(),
            },

            // --- Float64 ---
            ArrowValue::DoubleType(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(*val != 0.0),
                DataType::Int8 => ArrowValue::Int8Type(val.round() as i8),
                DataType::Int16 => ArrowValue::Int16Type(val.round() as i16),
                DataType::Int32 => ArrowValue::Int32Type(val.round() as i32),
                DataType::Int64 => ArrowValue::Int64Type(val.round() as i64),
                DataType::UInt8 => ArrowValue::UInt8Type(val.round() as u8),
                DataType::UInt16 => ArrowValue::UInt16Type(val.round() as u16),
                DataType::UInt32 => ArrowValue::UInt32Type(val.round() as u32),
                DataType::UInt64 => ArrowValue::UInt64Type(val.round() as u64),
                DataType::Float32 => ArrowValue::FloatType(val.round() as f32),
                DataType::Utf8 => ArrowValue::StringType(val.to_string()),
                DataType::Float64 => ArrowValue::DoubleType(*val),
                _ => todo!(),
            },

            // --- Strings ---
            ArrowValue::StringType(val) => match data_type {
                DataType::Boolean => ArrowValue::BooleanType(val.to_lowercase() == "true"),
                DataType::Int8 => ArrowValue::Int8Type(val.parse::<i8>().unwrap_or(0)),
                DataType::Int16 => ArrowValue::Int16Type(val.parse::<i16>().unwrap_or(0)),
                DataType::Int32 => ArrowValue::Int32Type(val.parse::<i32>().unwrap_or(0)),
                DataType::Int64 => ArrowValue::Int64Type(val.parse::<i64>().unwrap_or(0)),
                DataType::UInt8 => ArrowValue::UInt8Type(val.parse::<u8>().unwrap_or(0)),
                DataType::UInt16 => ArrowValue::UInt16Type(val.parse::<u16>().unwrap_or(0)),
                DataType::UInt32 => ArrowValue::UInt32Type(val.parse::<u32>().unwrap_or(0)),
                DataType::UInt64 => ArrowValue::UInt64Type(val.parse::<u64>().unwrap_or(0)),
                DataType::Float32 => ArrowValue::FloatType(val.parse::<f32>().unwrap_or(0.0)),
                DataType::Float64 => ArrowValue::DoubleType(val.parse::<f64>().unwrap_or(0.0)),
                DataType::Utf8 => ArrowValue::StringType(val.clone()),
                _ => todo!(),
            },
        };

        value
    }
}
