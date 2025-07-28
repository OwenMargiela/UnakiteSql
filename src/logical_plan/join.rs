use std::sync::Arc;

use crate::{
    datatypes::schema::{Field, Schema},
    logical_plan::LogicalPlan,
};

#[derive(Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub join_type: JoinType,
    pub on: Vec<(String, String)>,
}

impl Join {
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        let l = self.left.clone();
        let r = self.right.clone();

        let children: Vec<Arc<LogicalPlan>> = vec![l, r];
        children
    }
    pub fn schema(&self) -> Arc<Schema> {
        let duplicate_keys: Vec<String> = self
            .on
            .iter()
            .filter(|&it| it.0 == it.1)
            .map(|it| it.0.clone())
            .collect();

        let fields = match self.join_type {
            JoinType::Inner | JoinType::Right => {
                let mut left_fields = self.left.schema().fields.clone();
                let mut right_fields: Vec<Field> = self
                    .right
                    .schema()
                    .fields
                    .iter()
                    .filter(|it| duplicate_keys.contains(&it.name))
                    .cloned()
                    .collect();

                left_fields.append(&mut right_fields);
                left_fields
            }
            JoinType::Left => {
                let mut left_fields: Vec<Field> = self
                    .left
                    .schema()
                    .fields
                    .iter()
                    .filter(|it| duplicate_keys.contains(&it.name))
                    .cloned()
                    .collect();
                let mut right_fields = self.right.schema().fields.clone();

                right_fields.append(&mut left_fields);
                right_fields
            }
        };

        Arc::new(Schema { fields })
    }
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"Join: Left = {} Right= {}\n  On: {:?}", self.left,self.right,self.on)
    }
}
