pub mod aggregate;
pub mod data_frame;
pub mod expr;
pub mod expression;
pub mod join;
pub mod limit;
pub mod macro_utils;
pub mod projection;
pub mod scan;
pub mod selection;
pub mod test;
pub mod helper;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use crate::{
    datatypes::schema::{Field, Schema},
    logical_plan::{
        aggregate::Aggregate,
        join::Join,
        limit::Limit,
        macro_utils::{
            AggregateAvg, AggregateCount, AggregateCountDistinct, AggregateMax, AggregateMin,
            AggregateSum,
        },
        projection::Projection,
        scan::Scan,
        selection::Selection,
    },
};

/**
 * An enum representing all aggregate variants
 */

#[derive(Debug)]
pub enum AggregateExpr {
    Sum(AggregateSum),
    Min(AggregateMin),
    Max(AggregateMax),
    Avg(AggregateAvg),
    Count(AggregateCount),
    CountDistinct(AggregateCountDistinct),
}

/**
 * A logical plan represents a data transformation or action that returns a relation (a set of
 * tuples).
 */

pub enum LogicalPlan {
    JoinPlan(Join),
    LimitPlan(Limit),
    ProjectionPlan(Projection),
    ScanPlan(Scan),
    SelectionPlan(Selection),
    AggregatePlan(Aggregate),
}

/// This enum likely makes all the dyn traits null and void
/// TODO
/// Delete all traces of the Logical plan trait
/// Macros! Macros! Macros!
/// Replace schema and children function with implementation macros
impl LogicalPlan {
    /** Returns the schema of the data that will be produced by this logical plan. */
    fn schema(&self) -> Arc<Schema> {
        match self {
            LogicalPlan::JoinPlan(join) => join.schema(),
            LogicalPlan::LimitPlan(limit) => limit.schema(),
            LogicalPlan::ProjectionPlan(projection) => projection.schema(),
            LogicalPlan::ScanPlan(scan) => scan.schema(),
            LogicalPlan::SelectionPlan(selection) => selection.schema(),
            LogicalPlan::AggregatePlan(aggregate) => aggregate.schema(),
        }
    }

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */

    fn children(&self) -> Vec<Arc<LogicalPlan>> {
        match self {
            LogicalPlan::JoinPlan(join) => join.children(),
            LogicalPlan::LimitPlan(limit) => limit.children(),
            LogicalPlan::ProjectionPlan(projection) => projection.children(),
            LogicalPlan::ScanPlan(scan) => scan.children(),
            LogicalPlan::SelectionPlan(selection) => selection.children(),
            LogicalPlan::AggregatePlan(aggregate) => aggregate.children(),
        }
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalPlan::JoinPlan(join) => {
                write!(f, "{}", join.to_string())
            }
            LogicalPlan::LimitPlan(limit) => {
                write!(f, "{}", limit.to_string())
            }
            LogicalPlan::ProjectionPlan(projection) => {
                write!(f, "{}", projection.to_string())
            }
            LogicalPlan::ScanPlan(scan) => {
                write!(f, "{}", scan.to_string())
            }
            LogicalPlan::SelectionPlan(selection) => {
                write!(f, "{}", selection.to_string())
            }
            LogicalPlan::AggregatePlan(aggregate) => {
                write!(f, "{}", aggregate.to_string())
            }
        }
    }
}

/**
 * Logical Expression for use in logical query plans. The logical expression provides information
 * needed during the planning phase such as the name and data type of the expression.
 */
pub trait LogicalExpr: Display + Debug {
    /**
     * Return meta-data about the value that will be produced by this expression when evaluated
     * against a particular input.
     */
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field;
}

pub fn format_plan(plan: &LogicalPlan) -> String {
    pretty_format(plan, 0)
}
pub fn pretty_format(plan: &LogicalPlan, indent: usize) -> String {
    let mut b = String::new();

    for _ in 0..indent {
        b.push('\t');
    }

    b.push_str(&plan.to_string());
    b.push('\n');

    for child in plan.children() {
        b.push_str(&pretty_format(&*child, indent + 1));
    }

    b
}
