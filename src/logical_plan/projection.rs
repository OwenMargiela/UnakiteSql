use std::sync::Arc;

use crate::{
    datatypes::schema::{Field, Schema},
    logical_plan::{LogicalExpr, LogicalPlan, expr::ExprRef},
};

pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub expr: Vec<ExprRef>,
}

impl Projection {
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![self.input.clone()]
    }

    pub fn schema(&self) -> Arc<Schema> {
        let fields: Vec<Field> = self
            .expr
            .iter()
            .map(|it| it.state.to_field(self.input.clone()))
            .collect();

        Arc::new(Schema { fields })
    }
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields: Vec<String> = self.expr.iter().map(|it| it.state.to_string()).collect();
        write!(f, "Projection: {:?}", fields)
    }
}
