#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ArrowValue {
    BooleanType(bool),
    Int8Type(i8),
    Int16Type(i16),
    Int32Type(i32),
    Int64Type(i64),
    UInt8Type(u8),
    UInt16Type(u16),
    UInt32Type(u32),
    UInt64Type(u64),
    FloatType(f32),
    DoubleType(f64),
    StringType(String),
}
