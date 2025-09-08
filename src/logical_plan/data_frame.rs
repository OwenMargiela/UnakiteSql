use std::sync::Arc;

use crate::{
    datatypes::schema::Schema,
    logical_plan::{
        AggregateExpr, LogicalPlan,
        aggregate::Aggregate,
        expr::ExprRef,
        helper::numeric_lit_expr_to_usize,
        join::{Join, JoinType},
        limit::Limit,
        projection::Projection,
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

    /** Apply a limit */
    fn limit(&self, expr: ExprRef) -> Frame
    where
        Self: Sized;

    /** Aggregate */
    fn aggregate(&self, group_by: Vec<ExprRef>, aggregate_expr: Vec<AggregateExpr>) -> Frame
    where
        Self: Sized;

    /** Apply a join */
    fn join(&self, plan: Frame, join_type: JoinType, on: Vec<(String, String)>) -> Frame
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

    fn limit(&self, expr: ExprRef) -> Frame
    where
        Self: Sized,
    {
        let state = expr.state.as_ref();
        let limit = numeric_lit_expr_to_usize(state);
        Frame {
            plan: Arc::new(LogicalPlan::LimitPlan(Limit {
                input: self.plan.clone(),
                limit,
            })),
        }
    }

    fn join(&self, plan: Frame, join_type: JoinType, on: Vec<(String, String)>) -> Frame
    where
        Self: Sized,
    {
        let join_type = match join_type {
            JoinType::Inner => JoinType::Inner,
            JoinType::Left => JoinType::Left,
            JoinType::Right => JoinType::Right,
        };
        Frame {
            plan: Arc::new(LogicalPlan::JoinPlan(Join {
                left: self.plan.clone(),
                right: plan.plan.clone(),
                join_type,
                on,
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
