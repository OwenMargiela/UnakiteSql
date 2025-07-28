use std::{fs::File, sync::Arc};

use arrow::csv::{Reader as CsvArrowReader, ReaderBuilder};
use arrow::datatypes::{Field, Schema as ArrowSchema};

use crate::datasource::Iterators;
use crate::datatypes::arrow_field_vector::ArrowFieldVector;
use crate::datatypes::column_vector::ColumnVector;
use crate::datatypes::schema::schema_from_arrow_schema;
use crate::datatypes::{record_batch::RecordBatch, schema::Schema};

pub struct CsvDataSource {
    pub file_path: String,

    has_headers: bool,
    schema: Arc<ArrowSchema>,
}

impl CsvDataSource {
    pub fn new(file_path: String, has_headers: bool, schema: ArrowSchema) -> Self {
        Self {
            file_path,
            has_headers,
            schema: Arc::new(schema),
        }
    }
    pub fn schema(&self) -> Arc<Schema> {
        Arc::new(schema_from_arrow_schema(self.schema.clone()))
    }

    pub fn scan(&self, projection: Vec<String>) -> Iterators {
        let file = match File::open(self.file_path.clone()) {
            Err(_) => panic!("File Not found {}", self.file_path),
            Ok(file) => file,
        };

        let iter = CsvIterator::new(projection, self.has_headers, file, self.schema.clone());

        Iterators::Csv(iter)
    }
}

pub struct CsvIterator {
    reader: CsvArrowReader<File>,
    schema: Arc<ArrowSchema>,
}

impl CsvIterator {
    pub fn infer_rows(batch_size: usize) -> usize {
        let percent = (batch_size as f64 * 0.01).ceil() as usize;
        percent.clamp(3, 100)
    }

    pub fn new(
        projected_columns: Vec<String>,
        has_header: bool,
        file: File,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        let fields: Vec<Arc<Field>> = projected_columns
            .iter()
            .filter_map(|name| schema.fields.iter().find(|it| it.name() == name).cloned())
            .collect();

        let projected_schema = Arc::new(ArrowSchema::new(fields));

        let csv = ReaderBuilder::new(schema.clone())
            .with_header(has_header)
            .build(file)
            .unwrap();

        Self {
            reader: csv,
            schema: projected_schema,
        }
    }
}
impl Iterator for CsvIterator {
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        let local_schema = schema_from_arrow_schema(self.schema.clone());
        match self.reader.next() {
            Some(batch) => {
                let mut fields: Vec<Box<ColumnVector>> = vec![];

                let batches = batch.unwrap();

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
            None => return None,
        }
    }
}
