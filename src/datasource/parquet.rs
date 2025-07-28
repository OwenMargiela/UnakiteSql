use std::{
    fs::File,
    sync::{Arc, Mutex},
};

use arrow::datatypes::{Field, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    datasource::Iterators,
    datatypes::{
        arrow_field_vector::ArrowFieldVector,
        column_vector::ColumnVector,
        record_batch::RecordBatch,
        schema::{Schema, schema_from_arrow_schema},
    },
};

pub struct ParquetDataSource {
    pub path: String,
    schema: Arc<ArrowSchema>,
    data: Arc<Mutex<ParquetRecordBatchReader>>,
}

impl ParquetDataSource {
    pub fn new(path: String) -> Self {
        let file = File::open(path.clone()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema().clone();
        let reader = builder.build().unwrap();

        Self {
            path: path,
            schema,
            data: Arc::new(Mutex::new(reader)),
        }
    }
    pub fn schema(&self) -> Arc<Schema> {
        Arc::new(schema_from_arrow_schema(self.schema.clone()))
    }

    pub fn scan(&self, projection: Vec<String>) -> Iterators {
        match File::open(self.path.clone()) {
            Err(_) => panic!("File Not found {}", self.path),
            Ok(_) => {}
        };

        let iter = ParquetIterator::new(self.schema.clone(), self.data.clone(), projection);

        Iterators::Parquet(iter)
    }
}

pub struct ParquetIterator {
    reader: Arc<Mutex<ParquetRecordBatchReader>>,

    projected_schema: Arc<ArrowSchema>,
}

impl ParquetIterator {
    pub fn new(
        schema: Arc<ArrowSchema>,
        reader: Arc<Mutex<ParquetRecordBatchReader>>,
        projected_columns: Vec<String>,
    ) -> Self {
        let projected_schema: Arc<ArrowSchema>;

        if !projected_columns.is_empty() {
            let fields: Vec<Arc<Field>> = projected_columns
                .iter()
                .filter_map(|name| schema.fields.iter().find(|it| it.name() == name).cloned())
                .collect();

            projected_schema = Arc::new(ArrowSchema::new(fields));
        } else {
            projected_schema = schema.clone();
        }

        Self {
            reader,
            projected_schema,
        }
    }
}
impl Iterator for ParquetIterator {
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        let local_schema = schema_from_arrow_schema(self.projected_schema.clone());
        let mut reader = self.reader.lock().unwrap();

        match reader.next() {
            Some(reader_batch) => {
                let mut fields: Vec<Box<ColumnVector>> = vec![];

                let batches = reader_batch.unwrap();

                for col in batches.columns() {
                    fields.push(Box::new(ColumnVector::ArrowVector(ArrowFieldVector {
                        field: col.clone(),
                    })));
                }

                return Some(RecordBatch {
                    schema: local_schema,
                    fields,
                });
            }
            None => None,
        }
    }
}
