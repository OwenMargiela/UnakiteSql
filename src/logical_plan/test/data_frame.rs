#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::{
        datasource::{DataSource, csv::CsvDataSource},
        logical_plan::{
            LogicalPlan,
            data_frame::{DataFrame, Frame},
            expr::AsAlias,
            format_plan,
            helper::{column, count, max, min},
            macro_utils::{eq, literal_float, literal_string},
            scan::Scan,
        },
    };

    #[test]
    fn build_data_frame() {


        let df = csv()
            .filter(eq(column("city"), literal_string("Uk")))
            .project(vec![column("city"), column("lat"), column("lng")]);
        println!("{}", format_plan(&df.plan));
    }

    #[test]
    fn aggregate_data_frame() {
        let df = csv().aggregate(
            vec![column("state")],
            vec![min("city"), count("lat"), max("lng")],
        );

        println!("{}", format_plan(&df.plan));
    }

    #[test]
    fn multiply_and_alias() {
        // column("city").eq(literal_string("London")) and eq(column("city"), literal_string("London"))
        // are logically equivalent
        
        let df = csv()
            .filter(column("city").eq(literal_string("London")))
            .project(vec![column("city"), column("lat")])
            .filter((column("lat") * literal_float(1000.75)).alias("alias"));

        println!("{}", format_plan(&df.plan));
    }

    fn csv() -> Frame {
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

        let scan = Scan::new(
            "uk_cities".to_string(),
            DataSource::CSV(data),
            Arc::new(vec![]),
        );

        Frame {
            plan: Arc::new(LogicalPlan::ScanPlan(scan)),
        }
    }
}
