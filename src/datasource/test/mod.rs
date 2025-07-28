#[cfg(test)]
pub mod test {

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::datasource::{csv::CsvDataSource, parquet::ParquetDataSource};

    #[test]
    fn parquet_test() {
        let file_path = String::from("/home/spaceriot/unakitesql/src/test_data/mtcars.parquet");

        let data = ParquetDataSource::new(file_path);
        let batch_iter = data.scan(vec![]);

        for batch in batch_iter {
            for field in batch.fields {
                println!("{:?}", field)
            }
        }
    }

    #[test]
    fn csv_test() {
        let has_headers = false;
        let file_path = String::from("/home/spaceriot/unakitesql/src/test_data/uk_cities.csv");

        let data = CsvDataSource::new(
            file_path,
            has_headers,
            Schema::new(vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ]),
        );

        let mut batch_iter = data.scan(vec![]);

        while let Some(batch) = batch_iter.next() {
            for col in batch.fields {
                println!("Col: {:?}", col)
            }
        }
    }
}
