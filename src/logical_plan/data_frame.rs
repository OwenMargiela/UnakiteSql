use std::sync::Arc;

use crate::{
    datatypes::schema::Schema,
    logical_plan::{
        AggregateExpr, LogicalPlan, aggregate::Aggregate, expr::ExprRef, projection::Projection,
        selection::Selection,
    },
};

pub trait DataFrame {
    /** Apply a projection */
    fn project(&self, expr: Vec<ExprRef>) -> Frame
    where
        Self: Sized;

    /** Apply a filter */
    fn filter(&self, expr: ExprRef) -> Frame
    where
        Self: Sized;

    /** Aggregate */
    fn aggregate(&self, group_by: Vec<ExprRef>, aggregate_expr: Vec<AggregateExpr>) -> Frame
    where
        Self: Sized;

    /** Returns the schema of the data that will be produced by this DataFrame. */
    fn schema(&self) -> Arc<Schema>
    where
        Self: Sized;

    /** Get the logical plan */
    fn logical_plan(&self) -> Arc<LogicalPlan>
    where
        Self: Sized;
}

pub struct Frame {
    pub plan: Arc<LogicalPlan>,
}

impl DataFrame for Frame {
    fn project(&self, expr: Vec<ExprRef>) -> Frame
    where
        Self: Sized,
    {
        Frame {
            plan: Arc::new(LogicalPlan::ProjectionPlan(Projection {
                expr,
                input: self.plan.clone(),
            })),
        }
    }

    fn filter(&self, expr: ExprRef) -> Frame
    where
        Self: Sized,
    {
        Frame {
            plan: Arc::new(LogicalPlan::SelectionPlan(Selection {
                input: self.plan.clone(),
                expr,
            })),
        }
    }

    fn aggregate(&self, group_by: Vec<ExprRef>, aggregate_expr: Vec<AggregateExpr>) -> Frame
    where
        Self: Sized,
    {
        Frame {
            plan: Arc::new(LogicalPlan::AggregatePlan(Aggregate {
                input: self.plan.clone(),
                group_expr: group_by,
                aggregate_expr: aggregate_expr,
            })),
        }
    }

    fn schema(&self) -> Arc<Schema>
    where
        Self: Sized,
    {
        self.plan.schema()
    }

    fn logical_plan(&self) -> Arc<LogicalPlan>
    where
        Self: Sized,
    {
        self.plan.clone()
    }
}
