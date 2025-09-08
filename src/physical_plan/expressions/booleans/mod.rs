pub mod impl_expressions;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use arrow::datatypes::DataType;

use crate::{
    datatypes::{
        arrow_vector_builder::ArrowVectorBuilder,
        column_vector::{ColumnVector, ColumnVectorTrait},
        record_batch::RecordBatch,
        value::ArrowValue,
    },
    physical_plan::expressions::{
        Expression,
        booleans::impl_expressions::{
            AndPlan, EqPlan, GtPlan, GteqPlan, LtPlan, LteqPlan, NeqPlan, OrPlan,
        },
        column_expressions::ColumnExpression,
    },
};

#[derive(Debug)]
pub struct BooleanExpression {
    pub inner: Arc<dyn BooleanPair>,
    pub l: Arc<Expression>,
    pub r: Arc<Expression>,
}

impl BooleanExpression {
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        let (l, r) = { (self.get_l(), self.get_r()) };

        let ll = l.evaluate(input.clone());
        let rr = r.evaluate(input);
        assert_eq!(ll.size(), rr.size());

        if ll.get_type() != rr.get_type() {
            let error_msg = format!(
                "Boolean expression operands do not have the same type: {} != {}",
                ll.get_type(),
                rr.get_type()
            );
            panic!("{}", error_msg)
        }

        self.compare(ll, rr)
    }

    pub fn compare(&self, l: ColumnVector, r: ColumnVector) -> ColumnVector {
        let expr = { self.inner.clone() };

        let mut b = ArrowVectorBuilder::new(&DataType::Boolean);
        for i in 0..l.size() {
            // println!("got {:?}", l.get_value(i));
            // println!("got {:?}", r.get_value(i));
            // println!("set {}",i);

            let value = expr.evaluate_pair(l.get_value_inner(i), r.get_value_inner(i), l.get_type());

            // I should make My like a lot easier and implement make some into and from functions
            b.set(i, Some(ArrowValue::BooleanType(value)));
        }

        b.build()
    }

    pub fn get_l(&self) -> Arc<Expression> {
        self.l.clone()
    }
    pub fn get_r(&self) -> Arc<Expression> {
        self.r.clone()
    }
}

pub trait BooleanPair: Debug + Display {
    fn evaluate_pair(
        &self,
        _l: Option<ArrowValue>,
        _r: Option<ArrowValue>,
        _arrow_type: DataType,
    ) -> bool;
}

// Helpers

macro_rules! make_bool_expr_fn {
    ( $( ($fname:ident, $plan:ident) ),* $(,)? ) => {
        $(
            pub fn $fname() -> BooleanExpression {
                BooleanExpression {
                    inner: Arc::new($plan),
                    l: Arc::new(Expression::Column(ColumnExpression { i: 0 })),
                    r: Arc::new(Expression::Column(ColumnExpression { i: 1 })),
                }
            }
        )*
    };
}

// expand into all your functions
make_bool_expr_fn!(
    (gteq, GteqPlan),
    (gt, GtPlan),
    (lt, LtPlan),
    (lteq, LteqPlan),
    (eq, EqPlan),
    (neq, NeqPlan),
    (or, OrPlan),
    (and, AndPlan),
);
