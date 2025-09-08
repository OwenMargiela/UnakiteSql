#[cfg(test)]
pub mod test {
    use crate::{
        datatypes::value::ArrowValue,
        physical_plan::expressions::aggregates::{
            AggregateExpression, max_expression, min_expression, sum_expression,
        },
    };

    #[test]
    fn min_accumulator() {
        let mut a = min_expression().create_accumulator();

        let values: Vec<ArrowValue> = vec![10.into(), 14.into(), 4.into()];

        for v in values {
            a.accumulate(Some(v));
        }

        let f = a.final_value().unwrap();

        println!("{:?}", f);
    }

    #[test]
    fn max_accumulator() {
        let mut a = max_expression().create_accumulator();

        let values: Vec<ArrowValue> = vec![10.into(), 14.into(), 4.into()];

        for v in values {
            a.accumulate(Some(v));
        }

        let f = a.final_value().unwrap();

        println!("{:?}", f);
    }

    #[test]
    fn sum_accumulator() {
        let mut a = sum_expression().create_accumulator();

        let values: Vec<ArrowValue> = vec![10.into(), 14.into(), 4.into()];

        for v in values {
            a.accumulate(Some(v));
        }

        let f = a.final_value().unwrap();

        println!("{:?}", f);
    }
}
