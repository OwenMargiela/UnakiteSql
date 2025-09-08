// pub mod math_expression;

// use core::fmt;
// use std::sync::Arc;

// use arrow::datatypes::DataType;

// use crate::{
//     datatypes::{
//         arrow_vector_builder::ArrowVectorBuilder,
//         column_vector::{ColumnVector, ColumnVectorTrait},
//         value::ArrowValue,
//     },
//     physical_plan::expressions::{
//         Expression,
//         math::math_expression::{
//             AddExpression, DivideExpression, MultiplyExpression, SubtractExpression,
//         },
//     },
// };

// pub struct BinaryExpression {
//     inner: Arc<dyn MathExpr>,
//     l: Arc<Expression>,
//     r: Arc<Expression>,
// }

// impl BinaryExpression {

// }

// impl BinaryExpression {
//     pub fn get_l(&self) -> Arc<Expression> {
//         self.l.clone()
//     }
//     pub fn get_r(&self) -> Arc<Expression> {
//         self.r.clone()
//     }
// }

// impl fmt::Display for BinaryExpression {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?} {} {:?}", self.l, self.inner, self.r)
//     }
// }

// pub trait MathExpr: fmt::Display + fmt::Debug {
//     fn evaluate(&self, l: ColumnVector, r: ColumnVector) -> ColumnVector {
//         let mut builder = ArrowVectorBuilder::new(l.get_type());

//         for i in 0..l.size() {
//             let value = self.evaluate_pair(l.get_value(i), r.get_value(i), l.get_type());
//             builder.set(i, value);
//         }

//         builder.build()
//     }

//     fn evaluate_pair(
//         &self,
//         _l: Option<ArrowValue>,
//         _r: Option<ArrowValue>,
//         arrow_type: DataType,
//     ) -> Option<ArrowValue>;
// }

// // Helpers to create physical math expressions
// macro_rules! impl_phys_math_expr {
//     ($ (($func_name:ident,$variant:ident)),* $(,)?  ) => {

//         $(
//             pub fn $func_name(l: Expression, r: Expression) -> BinaryExpression {
//                 BinaryExpression {
//                     inner: Arc::new($variant),
//                     l: Arc::new(l),
//                     r: Arc::new(r),
//                 }
//             }
//         )*
//     };
// }

// impl_phys_math_expr!(
//     (add_expr, AddExpression),
//     (sub_expr, SubtractExpression),
//     (mul_expr, MultiplyExpression),
//     (div_expr, DivideExpression),
// );
