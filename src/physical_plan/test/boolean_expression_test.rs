#[cfg(test)]
pub mod test {

    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::{
        datatypes::{
            arrow_vector_builder::{ArrowVectorBuilder, TypeVector, build_vector},
            column_vector::ColumnVectorTrait,
            record_batch::RecordBatch,
            schema::{Field, Schema},
            value::ArrowValue,
        },
        physical_plan::expressions::{
            Expression,
            booleans::{BooleanExpression, gteq, impl_expressions::GteqPlan},
            column_expressions::ColumnExpression,
        },
    };

    // Fix null entry bug

    #[test]
    fn gteq_bytes() {
        let a_value = TypeVector::Int8(vec![10, 20, 30, 40, 50, 60, 70, 80, i8::MAX, i8::MIN]);

        let a_v = a_value.as_any().downcast_ref::<Vec<i8>>().unwrap();

        let b_value = TypeVector::Int8(vec![10, 30, 20, 40, 50, 70, 60, 80, i8::MIN, i8::MAX]);

        let b_v = b_value.as_any().downcast_ref::<Vec<i8>>().unwrap();

        let schema = Schema {
            fields: vec![
                Field::new("a", arrow::datatypes::DataType::Int8),
                Field::new("b", arrow::datatypes::DataType::Int8),
            ],
        };

        let batch = RecordBatch {
            schema,
            fields: vec![
                build_vector(DataType::Int8, &a_value),
                build_vector(DataType::Int8, &b_value),
            ],
        };

        let expr = gteq();

        let res = expr.evaluate(batch);

        for i in 0..res.size() {
            let value = bool::from(res.get_value(i));
            assert_eq!(a_v[i] >= b_v[i], value);
        }
    }

    #[test]
    fn gteq_shorts() {
        let mut a = ArrowVectorBuilder::new(&DataType::Int16);
        let a_value: Vec<i16> = vec![
            1000,
            2000,
            3000,
            4000,
            5000,
            6000,
            7000,
            8000,
            i16::MAX,
            i16::MIN,
        ];

        for (i, value) in a_value.iter().enumerate() {
            a.set(i, Some(ArrowValue::Int16Type(*value)));
        }

        let mut b = ArrowVectorBuilder::new(&DataType::Int16);
        let b_value: Vec<i16> = vec![
            1000,
            3000,
            2000,
            4000,
            5000,
            7000,
            6000,
            8000,
            i16::MIN,
            i16::MAX,
        ];

        for (i, value) in b_value.iter().enumerate() {
            b.set(i, Some(ArrowValue::Int16Type(*value)));
        }

        let a_v = a.build();
        let b_v = b.build();

        let vec_field = vec![
            Field::new("a", arrow::datatypes::DataType::Int16),
            Field::new("b", arrow::datatypes::DataType::Int16),
        ];
        let schema = Schema { fields: vec_field };

        let batch = RecordBatch {
            schema,
            fields: vec![a_v, b_v],
        };

        let expr = BooleanExpression {
            inner: Arc::new(GteqPlan),
            l: Arc::new(Expression::Column(ColumnExpression { i: 0 })),
            r: Arc::new(Expression::Column(ColumnExpression { i: 1 })),
        };

        let res = expr.evaluate(batch);

        for i in 0..res.size() {
            let value: bool = res.get_value(i).into();
            assert_eq!(a_value[i] >= b_value[i], value);
        }
    }

    #[test]
    fn gteq_string() {
        let mut a = ArrowVectorBuilder::new(&DataType::Utf8);
        let a_value = vec![
            "aac", "bbb", "bbc", "ccc", "ddd", "eee", "fff", "yya", "yyb", "zzz",
        ];

        for (i, value) in a_value.iter().enumerate() {
            a.set(i, Some(ArrowValue::StringType(value.to_string())));
        }

        let mut b = ArrowVectorBuilder::new(&DataType::Utf8);
        let b_value = vec![
            "aab", "bbc", "bbb", "ccc", "ddd", "eee", "fff", "yya", "yyb", "aaa",
        ];

        for (i, value) in b_value.iter().enumerate() {
            b.set(i, Some(ArrowValue::StringType(value.to_string())));
        }

        let a_v = a.build();
        let b_v = b.build();

        let vec_field = vec![
            Field::new("a", arrow::datatypes::DataType::Utf8),
            Field::new("b", arrow::datatypes::DataType::Utf8),
        ];
        let schema = Schema { fields: vec_field };

        let batch = RecordBatch {
            schema,
            fields: vec![a_v, b_v],
        };

        let expr = BooleanExpression {
            inner: Arc::new(GteqPlan),
            l: Arc::new(Expression::Column(ColumnExpression { i: 0 })),
            r: Arc::new(Expression::Column(ColumnExpression { i: 1 })),
        };

        let res = expr.evaluate(batch);

        for i in 0..res.size() {
            let value: bool = res.get_value(i).into();
            assert_eq!(a_value[i] >= b_value[i], value);
        }
    }
}
