#[cfg(test)]

pub mod test {
    use arrow::{
        array::{AsArray, Float64Builder, Int32Array},
        compute::{self, kernels::cast},
        datatypes::{DataType, Float64Type, Int64Type},
    };

    use crate::datatypes::{arrow_vector_builder::ArrowVectorBuilder, value::ArrowValue};

    #[test]
    fn cast_to() {
        let a = Int32Array::from(vec![1, 2, 3]);
        let b = cast(&a, &arrow::datatypes::DataType::Float64).unwrap();
        let c = b.as_primitive::<Float64Type>();

        assert_eq!(1.0, c.value(0));
        assert_eq!(2.0, c.value(1));
        assert_eq!(3.0, c.value(2));

        let mut aa = Float64Builder::new();
        aa.append_value(1.0);
        aa.append_value(2.0);
        aa.append_value(3.0);

        let _cc = aa.finish();
    }

    #[test]
    fn compute_aggregate() {
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

        let a = v.get_vector();

        let min = compute::min(a.field.as_primitive::<Int64Type>()).unwrap();
        println!("{}", min)
    }
}
