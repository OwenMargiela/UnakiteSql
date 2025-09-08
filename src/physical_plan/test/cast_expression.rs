#[cfg(test)]

pub mod test {
    use arrow::datatypes::DataType;

    use crate::{
        datatypes::{
            arrow_vector_builder::{TypeVector, build_vector},
            record_batch::RecordBatch,
            schema::{Field, Schema},
        },
        physical_plan::expressions::{
            cast_expression::CastExpression, column_expressions::ColumnExpression,
        },
    };

    #[test]
    fn cast_float_to_int32() {
        let schema = Schema {
            fields: vec![Field::new("a", arrow::datatypes::DataType::Float32)],
        };

        let a_value = TypeVector::Float(vec![
            36.54f32,
            28.31f32,
            45.15f32,
            43.4f32,
            36.26f32,
            12.74f32,
            17.64f32,
            44.16f32,
            f32::MAX,
            f32::MIN,
        ]);

        let batch = RecordBatch {
            schema,
            fields: vec![build_vector(DataType::Float32, &a_value)],
        };

        let expr = CastExpression::new(ColumnExpression { i: 0 }, DataType::Int32);

        let result = expr.evaluate(batch);

        // Fix issues surrounding null values
        println!("{:?}", result);
    }

    #[test]
    fn cast_string_to_float() {
        let schema = Schema {
            fields: vec![Field::new("a", arrow::datatypes::DataType::Utf8)],
        };

        let a_value = TypeVector::String(vec![
            36.54f32.to_string(),
            28.31f32.to_string(),
            45.15f32.to_string(),
            43.4f32.to_string(),
            36.26f32.to_string(),
            12.74f32.to_string(),
            17.64f32.to_string(),
            44.16f32.to_string(),
            f32::MAX.to_string(),
            f32::MIN.to_string(),
        ]);

        let batch = RecordBatch {
            schema,
            fields: vec![build_vector(DataType::Utf8, &a_value)],
        };

        let expr = CastExpression::new(ColumnExpression { i: 0 }, DataType::Float32);

        let result = expr.evaluate(batch);

        // Fix issues surrounding null values
        println!("{:?}", result);
    }

    #[test]
    fn cast_byte_to_string() {
        let schema = Schema {
            fields: vec![Field::new("a", arrow::datatypes::DataType::Int8)],
        };

        let a_value = TypeVector::Int8(vec![
            36i8,
            28i8,
            45i8,
            4i8,
            36i8,
            12i8,
            17i8,
            44i8,
            i8::MAX,
            i8::MIN,
        ]);

        let batch = RecordBatch {
            schema,
            fields: vec![build_vector(DataType::Int8, &a_value)],
        };

        let expr = CastExpression::new(ColumnExpression { i: 0 }, DataType::Utf8);

        let result = expr.evaluate(batch);

        // Fix issues surrounding null values
        println!("{:?}", result);
    }
}
