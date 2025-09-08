use std::sync::Arc;

use crate::{datatypes::schema::Schema, logical_plan::LogicalPlan};

pub struct Limit {
    pub input: Arc<LogicalPlan>,
    pub limit: usize,
}

impl Limit {
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![self.input.clone()]
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit: {}", self.limit)
    }
}
