use std::sync::Arc;

use crate::{
    datatypes::schema::Schema,
    logical_plan::{LogicalPlan, expr::ExprRef},
};

pub struct Selection {
    pub input: Arc<LogicalPlan>,
    pub expr: ExprRef,
}

impl Selection {
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![self.input.clone()]
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }
}

impl std::fmt::Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Selection {}", self.expr.state)
    }
}
