#[cfg(test)]
pub mod test {
    use arrow::datatypes::DataType;

    use crate::datatypes::{
        arrow_vector_builder::ArrowVectorBuilder, column_vector::ColumnVector, value::ArrowValue,
    };

    #[test]
    fn build_int64_vector() {
        let size = 10;
        let mut b = ArrowVectorBuilder::new(DataType::Int64);
        for i in 0..size {
            b.set(i, Some(ArrowValue::Int64Type(i as i64)));
        }
        let v = b.build();

        let v_size = v.size();

        assert_eq!(size, v_size);

        for i  in 0..v_size {
            let v_value = v.get_value(i).unwrap();

            if let ArrowValue::Int64Type(int_value) = v_value {
                assert_eq!(i as i64, int_value);
            }
        }
    }
}
