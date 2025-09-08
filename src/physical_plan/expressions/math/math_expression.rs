// macro_rules! impl_binary_math_op {
//     ($(($struct:ident, $infix:tt)),* $(,)?) => {
//         $(
//             pub struct $struct;

//             impl crate::physical_plan::expressions::math::MathExpr for $struct {
//                 fn evaluate_pair(
//                     &self,
//                     _l: Option<crate::physical_plan::expressions::math::ArrowValue>,
//                     _r: Option<crate::physical_plan::expressions::math::ArrowValue>,
//                     _arrow_type: arrow::datatypes::DataType,
//                 ) -> Option<crate::physical_plan::expressions::math::ArrowValue> {
//                     let (l, r) = match (_l, _r) {
//                         (Some(l), Some(r)) => (l, r),
//                         _ => panic!("Both operands must be Some"),
//                     };

//                     use crate::datatypes::concrete_type::ConcreteType;
//                     use crate::physical_plan::expressions::math::ArrowValue;

//                     if l.get_conc_type() == r.get_conc_type() && r.get_conc_type() == _arrow_type {
//                         match _arrow_type {
//                             arrow::datatypes::DataType::Int8 => Some(ArrowValue::Int8Type(i8::from(l) $infix i8::from(r))),
//                             arrow::datatypes::DataType::Int16 => Some(ArrowValue::Int16Type(i16::from(l) $infix i16::from(r))),
//                             arrow::datatypes::DataType::Int32 => Some(ArrowValue::Int32Type( i32::from(l) $infix i32::from(r))),
//                             arrow::datatypes::DataType::Int64 => Some(ArrowValue::Int64Type( i64::from(l) $infix i64::from(r))),
//                             arrow::datatypes::DataType::UInt8 => Some(ArrowValue::UInt8Type (u8::from(l) $infix u8::from(r))),
//                             arrow::datatypes::DataType::UInt16 => Some(ArrowValue::UInt16Type (u16::from(l) $infix u16::from(r))),
//                             arrow::datatypes::DataType::UInt32 => Some(ArrowValue::UInt32Type(u32::from(l) $infix u32::from(r))),
//                             arrow::datatypes::DataType::UInt64 => Some(ArrowValue::UInt64Type(u64::from(l) $infix u64::from(r))),
//                             arrow::datatypes::DataType::Float32 => Some(ArrowValue::FloatType(f32::from(l) $infix f32::from(r))),
//                             arrow::datatypes::DataType::Float64 => Some(ArrowValue::DoubleType(f64::from(l) $infix f64::from(r))),


//                             _ => panic!("Invalid Arrow datatype for this operation"),
//                         }
//                     } else {
//                         panic!("Invalid type combination or mismatched Arrow types")
//                     }
//                 }
//             }
            
//             impl std::fmt::Display for $struct {
//             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//                 write!(f, "{} ", stringify!($infix))

//             }
//             }


//                 impl std::fmt::Debug for $struct {
//             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//                 write!(f, "{} ", stringify!($infix))

//             }
//             }
//         )*
//     };
// }

// impl_binary_math_op!(
//     (AddExpression, +),
//     (SubtractExpression, -),
//     (MultiplyExpression, *),
//     (DivideExpression, /)
// );
