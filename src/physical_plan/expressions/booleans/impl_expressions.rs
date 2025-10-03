use std::sync::Arc;

use crate::datatypes::arrow_field_vector::ArrowFieldVector;
use crate::physical_plan::expressions::booleans::BooleanPair;

use crate::physical_plan::expressions::booleans::ColumnVector;
use arrow::array::AsArray;
use arrow::compute::and;
// use arrow::compute::kernels::cmp::*;
use arrow::compute::or;

pub struct AndPlan;

impl BooleanPair for AndPlan {
    fn evaluate_pair(&self, l: ColumnVector, r: ColumnVector) -> ColumnVector {
        let vec = and(
            &l.get_vector().field.as_boolean(),
            &r.get_vector().field.as_boolean(),
        )
        .unwrap();

        let coulumn_vec = ColumnVector::ArrowVector(ArrowFieldVector {
            field: Arc::new(vec),
        });

        coulumn_vec
    }
}

impl std::fmt::Display for AndPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "&&")
    }
}

impl std::fmt::Debug for AndPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "&& ")
    }
}

pub struct OrPlan;

impl BooleanPair for OrPlan {
    fn evaluate_pair(&self, l: ColumnVector, r: ColumnVector) -> ColumnVector {
        let vec = or(
            &l.get_vector().field.as_boolean(),
            &r.get_vector().field.as_boolean(),
        )
        .unwrap();

        let coulumn_vec = ColumnVector::ArrowVector(ArrowFieldVector {
            field: Arc::new(vec),
        });

        coulumn_vec
    }
}

impl std::fmt::Display for OrPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "||")
    }
}

impl std::fmt::Debug for OrPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|| ")
    }
}

macro_rules! impl_binary_infix_op {
    ($(($struct:ident, $infix:tt, $cmp_function:ident)),* $(,)?) => {
        $(
            pub struct $struct ;

            impl BooleanPair for $struct {

                fn evaluate_pair(
                    &self,
                    l: ColumnVector,
                    r: ColumnVector,

                ) -> ColumnVector {
                    let coulumn_vec = ColumnVector::ArrowVector(ArrowFieldVector {
                        field: Arc::new(arrow::compute::kernels::cmp::$cmp_function(&l.get_vector().field, &r.get_vector().field).unwrap())
                    });
                    coulumn_vec
                }

            }


            impl std::fmt::Display for $struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} ", stringify!($infix))

            }
            }


                impl std::fmt::Debug for $struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} ", stringify!($infix))

            }
            }
        )*
    };
}

// Implements binary infix operations

impl_binary_infix_op!(
    (EqPlan, ==,eq),
    (NeqPlan, !=,neq),

    (LtPlan, <,lt),
    (LteqPlan, <=,lt_eq),
    (GtPlan, >,gt),
    (GteqPlan, >=,gt_eq),

);
