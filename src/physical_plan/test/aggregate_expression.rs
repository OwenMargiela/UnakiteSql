#[cfg(test)]
pub mod test {
    use arrow::datatypes::DataType;

    use crate::{
        datatypes::{arrow_vector_builder::ArrowVectorBuilder, value::ArrowValue},
        physical_plan::expressions::aggregates::{
            AggregateExpression, max_expression, min_expression, sum_expression,
        },
    };

    #[test]
    fn accumulator() {
        let mut a_accumulator = sum_expression().create_accumulator();
        let mut b_accumulator = min_expression().create_accumulator();
        let mut c_accumulator = max_expression().create_accumulator();

        let mut b = ArrowVectorBuilder::new(&DataType::Int64);

        b.set_all(&mut vec![
            Some(ArrowValue::Int64Type(0 as i64)),
            Some(ArrowValue::Int64Type(1 as i64)),
            Some(ArrowValue::Int64Type(2 as i64)),
            Some(ArrowValue::Int64Type(3 as i64)),
            Some(ArrowValue::Int64Type(4 as i64)),
            Some(ArrowValue::Int64Type(5 as i64)),
            Some(ArrowValue::Int64Type(6 as i64)),
            Some(ArrowValue::Int64Type(7 as i64)),
            Some(ArrowValue::Int64Type(8 as i64)),
            Some(ArrowValue::Int64Type(9 as i64)),
        ]);

        let v = b.build();

        let _ = a_accumulator.update(&v);
        let _ = b_accumulator.update(&v);
        let _ = c_accumulator.update(&v);

        let f = a_accumulator.final_value();
        let g = b_accumulator.final_value();
        let h = c_accumulator.final_value();

        println!("{:?}", f);
        println!("{:?}", g);
        println!("{:?}", h);
    }
}
