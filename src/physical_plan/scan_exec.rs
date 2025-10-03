use std::{fmt, sync::Arc};

use crate::{
    datasource::{DataSource, DataSourceTrait},
    datatypes::schema::Schema,
    physical_plan::{PhysPlanTrait, PhysicaPlan},
};

pub struct ScanExec {
    ds: DataSource,
    projection: Arc<Vec<String>>,
}

impl PhysPlanTrait for ScanExec {
    fn schema(&self) -> Schema {
        self.ds.schema().select(self.projection.clone()).unwrap()
    }

    fn children(&self) -> Vec<Arc<PhysicaPlan>> {
        vec![]
    }

    fn execute(&self) -> impl Iterator<Item = crate::datatypes::record_batch::RecordBatch> {
        self.ds.scan(self.projection.to_vec())
    }
}

impl fmt::Display for ScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScanExec: schema={:?}, projection={:?}",
            self.ds.schema(),
            self.projection
        )
    }
}
