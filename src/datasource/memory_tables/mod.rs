use arrow::datatypes::Schema as ArrowSchema;
use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::Schema;

use crate::datatypes::{column_vector::ColumnVector, record_batch::RecordBatch};

macro_rules! define_datasource_enum {
    (
        $(
            $variant:ident({
                $($field:ident : $ty:ty),* $(,)?
            })
        ),* $(,)?
    ) => {
        pub enum InMemoryDataSource {
            $(
                $variant {
                    $(
                        $field: $ty,
                    )*
                },
            )*
        }
    };
}

define_datasource_enum! {
    BTree({
        data: BTreeMap<i32, ColumnVector>,
        schema: Schema
    }),
    DynList({
        data: Vec<ColumnVector>,
        schema: Schema
    }),
}

impl InMemoryDataSource {
    pub fn schema(&self) -> Schema {
        todo!()
    }

    pub fn scan(&self, _projection: Vec<String>) {
        match self {
            InMemoryDataSource::BTree { data: _, schema: _ } => todo!(),
            InMemoryDataSource::DynList { data: _, schema: _ } => todo!(),
        }
    }
}

pub struct InMemoryDataSourceIterator {
    _reader: InMemoryDataSource,

    _projected_schema: Arc<ArrowSchema>,
}

impl Iterator for InMemoryDataSourceIterator {
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
