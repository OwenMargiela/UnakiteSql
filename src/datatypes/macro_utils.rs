/*Simple array initializer. Does not specifcy unique values at unqiue indexes */

// Innit with Capacity

#[macro_export]
macro_rules! build_array {
    // Special case: StringBuilder requires 2 capacities
    (StringBuilder, $capacity:expr) => {{
        let mut builder = StringBuilder::with_capacity($capacity, $capacity * 8);
        std::sync::Arc::new(builder.finish()) as std::sync::Arc<dyn arrow::array::Array>
    }};

    // Generic case
    ($builder_type:ident, $capacity:expr) => {{
        let mut builder = $builder_type::with_capacity($capacity);
        std::sync::Arc::new(builder.finish()) as std::sync::Arc<dyn arrow::array::Array>
    }};
}

// Finishes an ArrowBuilder
#[macro_export]
macro_rules! finish_builder {
    ($builder:ident) => {{ ::std::sync::Arc::new(builder.finish()) }};
}

#[macro_export]
macro_rules! downcast_arry {
    ($variant:ident, StringArray, $field:ident, $index:expr) => {{
        let array = $field
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Downcast failed");
        if array.is_null($index) {
            panic!("Illegal Type");
        }
        super::value::ArrowValue::$variant(array.value($index).to_string())
    }};

    ($variant:ident, $array_type:ident, $field:ident, $index:expr) => {{
        let array = $field
            .as_any()
            .downcast_ref::<$array_type>()
            .expect("Downcast failed");
        super::value::ArrowValue::$variant(array.value($index))
    }};
}

#[macro_export]
macro_rules! init_builder {
    ($builder_variant: ident, $dt:ident) => {{
        let builder = $builder_variant::new();
        let vector_builder = VectorBuilder::$dt(builder);
        vector_builder
    }};
}

#[macro_export]
macro_rules! match_and {
      ($macro:ident, $dt:expr) => {{
        match $dt {
            DataType::Boolean => $macro!(BooleanBuilder, Boolean),
            DataType::Int8 => $macro!(Int8Builder, Int8),
            DataType::Int16 => $macro!(Int16Builder, Int16),
            DataType::Int32 => $macro!(Int32Builder, Int32),
            DataType::Int64 => $macro!(Int64Builder, Int64),
            DataType::UInt8 => $macro!(UInt8Builder, UInt8),
            DataType::UInt16 => $macro!(UInt16Builder, UInt16),
            DataType::UInt32 => $macro!(UInt32Builder, UInt32),
            DataType::UInt64 => $macro!(UInt64Builder, UInt64),
            DataType::Float32 => $macro!(Float32Builder, Float),
            DataType::Float64 => $macro!(Float64Builder, Double),
            DataType::Utf8 => $macro!(StringBuilder, String),
            _ => panic!("Unsupported data type: {:?}", $dt),
        }
    }};

    // Match and get arrow data
    ($dt:expr, $macro:ident, $field:ident, $index:expr) => {{
        match $dt {
            DataType::Boolean => $macro!(BooleanType, BooleanArray, $field, $index),
            DataType::Int8 => $macro!(Int8Type, Int8Array, $field, $index),
            DataType::Int16 => $macro!(Int16Type, Int16Array, $field, $index),
            DataType::Int32 => $macro!(Int32Type, Int32Array, $field, $index),
            DataType::Int64 => $macro!(Int64Type, Int64Array, $field, $index),
            DataType::UInt8 => $macro!(UInt8Type, UInt8Array, $field, $index),
            DataType::UInt16 => $macro!(UInt16Type, UInt16Array, $field, $index),
            DataType::UInt32 => $macro!(UInt32Type, UInt32Array, $field, $index),
            DataType::UInt64 => $macro!(UInt64Type, UInt64Array, $field, $index),
            DataType::Float32 => $macro!(FloatType, Float32Array, $field, $index),
            DataType::Float64 => $macro!(DoubleType, Float64Array, $field, $index),
            DataType::Utf8 => $macro!(StringType, StringArray, $field, $index),
            _ => panic!("Unsupported data type: {:?}", $dt),
        }
    }};

    // Builder with max capacity
    ($dt:expr, $macro:ident, $($args:tt)*) => {
        match $dt {
            DataType::Boolean => $macro!(BooleanBuilder, $($args)*),
            DataType::Int8 => $macro!(Int8Builder, $($args)*),
            DataType::Int16 => $macro!(Int16Builder, $($args)*),
            DataType::Int32 => $macro!(Int32Builder, $($args)*),
            DataType::Int64 => $macro!(Int64Builder, $($args)*),
            DataType::UInt8 => $macro!(UInt8Builder, $($args)*),
            DataType::UInt16 => $macro!(UInt16Builder, $($args)*),
            DataType::UInt32 => $macro!(UInt32Builder, $($args)*),
            DataType::UInt64 => $macro!(UInt64Builder, $($args)*),
            DataType::Float32 => $macro!(Float32Builder, $($args)*),
            DataType::Float64 => $macro!(Float64Builder, $($args)*),
            DataType::Utf8 => $macro!(StringBuilder, $($args)*),
            _ => panic!("Unsupported data type: {:?}", $dt),
        }
    };

}

#[macro_export]
macro_rules! dispatch_builder_array_ref {
    ($builder:expr, $( $builder_variant:ident ),* $(,)?) => {
        match $builder {
            $(
                VectorBuilder::$builder_variant(b) => {
                    let finish = Arc::new(b.finish()) as ArrayRef;
                    finish

                }
            )*
        }
    };
}

#[macro_export]
macro_rules! append_dispatch {
    ($builder:expr, $value:expr, $( ($builder_variant:ident, $value_variant:ident) ),* $(,)?) => {
        match ($builder, $value) {
            $(
                (VectorBuilder::$builder_variant(b), Some(ArrowValue::$value_variant(v))) => b.append_value(v),
                (VectorBuilder::$builder_variant(b), None) => b.append_null(),
            )*
            // Special case for String which needs reference
            (VectorBuilder::String(b), Some(ArrowValue::StringType(s))) => b.append_value(&s),
            (VectorBuilder::String(b), None) => b.append_null(),
            _ => panic!("Type mismatch"),
        }
    };
}
