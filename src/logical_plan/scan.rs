use std::{ sync::Arc};

use crate::{
    datasource::{DataSource, DataSourceTrait},
    datatypes::schema::Schema,
    logical_plan::LogicalPlan,
};

pub struct Scan {
    data_source: DataSource,
    path: String,
    projection: Arc<Vec<String>>,
}

impl Scan {
    pub fn new(path: String, data_source: DataSource, projection: Arc<Vec<String>>) -> Scan {
        Scan {
            data_source,
            projection,
            path,
        }
    }
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![]
    }

    pub fn schema(&self) -> Arc<Schema> {
        let schema = self.derive_schema();
        schema
    }
    pub fn derive_schema(&self) -> Arc<Schema> {
        let schema = self.data_source.schema();
        if self.projection.is_empty() {
            return schema;
        } else {
            return Arc::new(schema.select(self.projection.clone()).unwrap());
        }
    }
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.projection.is_empty() {
            write!(f, "Scan: {}; projection=None;", self.path)
        } else {
            write!(f, "Scan: {}; projection={:?};", self.path, self.projection)
        }
    }
}
