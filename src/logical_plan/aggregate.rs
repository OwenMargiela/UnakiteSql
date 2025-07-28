use std::{fmt, sync::Arc};

use crate::{
    datatypes::schema::{Field, Schema},
    logical_plan::{expr::ExprRef, AggregateExpr, LogicalExpr, LogicalPlan},
};

pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<ExprRef>,
    pub aggregate_expr: Vec<AggregateExpr>,
}

impl Aggregate {
    pub fn schema(&self) -> Arc<Schema> {
        let mut groups: Vec<Field> = self
            .group_expr
            .iter()
            .map(|it| it.state.to_field(self.input.clone()))
            .collect();

        let mut aggregates: Vec<Field> = self
            .group_expr
            .iter()
            .map(|it| it.state.to_field(self.input.clone()))
            .collect();

        let mut fields = Vec::<Field>::new();
        fields.append(&mut groups);
        fields.append(&mut aggregates);

        Arc::new(Schema { fields })
    }

    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![self.input.clone()]
    }
}

impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Aggregate: groupExpr=#{:?}, aggregateExpr={:?}",
            self.group_expr, self.aggregate_expr
        )
    }
}
