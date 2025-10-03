use core::fmt;

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

impl fmt::Display for ArrowValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowValue::BooleanType(val) => write!(f, "BooleanType({})", val),
            ArrowValue::Int8Type(val) => write!(f, "Int8TypeType({})", val),
            ArrowValue::Int16Type(val) => write!(f, "Int16Type({})", val),
            ArrowValue::Int32Type(val) => write!(f, "Int32Type({})", val),
            ArrowValue::Int64Type(val) => write!(f, "Int64Type({})", val),
            ArrowValue::UInt8Type(val) => write!(f, "UInt8Type({})", val),
            ArrowValue::UInt16Type(val) => write!(f, "UInt16Type({})", val),
            ArrowValue::UInt32Type(val) => write!(f, "UInt32Type({})", val),
            ArrowValue::UInt64Type(val) => write!(f, "UInt64Type({})", val),
            ArrowValue::FloatType(val) => write!(f, "FloatType({})", val),
            ArrowValue::DoubleType(val) => write!(f, "DoubleType({})", val),
            ArrowValue::StringType(val) => write!(f, "StringType({})", val),
        }
    }
}
// Used for Arrow Value to rust native type conversions
macro_rules! impl_from {
    ($(($data_type: ty, $variant:ident)),* $(,)?) => {
        $(

            impl From<$data_type> for ArrowValue {
                fn from(value: $data_type) -> Self {
                    ArrowValue::$variant(value)
                }
            }

            impl From<ArrowValue> for $data_type {
                fn from(value: ArrowValue) -> Self {
                    if let ArrowValue::$variant(value) = value {
                        return value;
                    } else {
                        panic!("Can not cast to target type")
                    }
                }
            }

        )*
    };
}

impl_from!(
    (bool, BooleanType),
    (i8, Int8Type),
    (i16, Int16Type),
    (i32, Int32Type),
    (i64, Int64Type),
    (u8, UInt8Type),
    (u16, UInt16Type),
    (u32, UInt32Type),
    (u64, UInt64Type),
    (f32, FloatType),
    (f64, DoubleType),
    (String, StringType)
);

#[cfg(test)]
pub mod test {
    use crate::datatypes:: value::ArrowValue;

    #[test]
    fn test() {
        let b: ArrowValue = false.into();
        let bool = ArrowValue::from(false);

        assert_eq!(b, bool);
        let lit_bool: bool = b.into();

        assert_eq!(false, lit_bool);

        let int: ArrowValue = 12u8.into();
        let integer: ArrowValue = ArrowValue::from(12u8);

        if let ArrowValue::UInt8Type(_) = int {
            println!("{:?}", int)
        } else {
            panic!("Incorrect Conversion")
        }

        assert_eq!(int, integer);

        let lit_int: u8 = int.into();

        assert_eq!(lit_int, 12u8);

        let from_u8 = u8::from(integer);

        assert_eq!(from_u8, lit_int);

        let b: ArrowValue = "String".to_string().into();

        println!("{:?}", b);
    }

}
