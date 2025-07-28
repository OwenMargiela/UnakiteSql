pub mod csv;

pub mod memory_tables;
pub mod parquet;
pub mod test;

use std::sync::Arc;

use crate::{
    datasource::{
        csv::{CsvDataSource, CsvIterator},
        parquet::{ParquetDataSource, ParquetIterator},
    },
    datatypes::{record_batch::RecordBatch, schema::Schema},
};

pub trait DataSourceTrait {
    /** Return the schema for the underlying data source */
    fn schema(&self) -> Arc<Schema>;

    /** Scan the data source, selecting the specified columns */
    fn scan(&self, projection: Vec<String>) -> Iterators;
}
pub enum DataSource {
    CSV(CsvDataSource),
    Parquet(ParquetDataSource),
}
impl DataSourceTrait for DataSource {
    /** Return the schema for the underlying data source */
    fn schema(&self) -> Arc<Schema> {
        match self {
            DataSource::CSV(csv) => csv.schema(),
            DataSource::Parquet(parquet) => parquet.schema(),
        }
    }

    /** Scan the data source, selecting the specified columns */
    fn scan(&self, projection: Vec<String>) -> Iterators {
        let iter = match self {
            DataSource::CSV(csv) => csv.scan(projection),
            DataSource::Parquet(parquet) => parquet.scan(projection),
        };
        iter
    }
}

pub enum Iterators {
    Csv(CsvIterator),
    Parquet(ParquetIterator),
}

impl Iterator for Iterators {
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iterators::Csv(csv_iterator) => csv_iterator.next(),
            Iterators::Parquet(parquet_iterator) => parquet_iterator.next(),
        }
    }
}
