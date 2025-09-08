use std::sync::Arc;

use anyhow::Error;
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};

pub fn schema_from_arrow_schema(arrow_schema: Arc<ArrowSchema>) -> Schema {
    let fields: Vec<Field> = arrow_schema
        .fields
        .iter()
        .map(|f| Field {
            name: f.name().to_string(),
            data_type: f.data_type().clone(),
        })
        .collect();

    Schema { fields }
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn to_arrow(&self) -> ArrowSchema {
        let arrow_fields: Vec<ArrowField> =
            self.fields.iter().cloned().map(|f| f.to_arrow()).collect();

        ArrowSchema::new(arrow_fields)
    }

    pub fn project(&self, indices: Vec<usize>) -> Schema {
        let projection_fields: Vec<Field> =
            indices.iter().map(|it| self.fields[*it].clone()).collect();

        Schema {
            fields: projection_fields,
        }
    }

    pub fn select(&self, names: Arc<Vec<String>>) -> anyhow::Result<Schema> {
        let mut f: Vec<Field> = Vec::new();

        for name in names.iter() {
            let m: Vec<Field> = self
                .fields
                .iter()
                .cloned()
                .filter(|it| it.name == *name)
                .collect();
            if m.len() == 1 {
                f.push(m[0].clone());
            } else {
                return anyhow::Result::Err(Error::msg("Multiple Fields with the same name"));
            }
        }

        Ok(Schema { fields: f })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

impl Field {
    pub fn new(string: &str, data_type: DataType) -> Self {
        Field {
            name: string.to_string(),
            data_type,
        }
    }
    pub fn to_arrow(self) -> ArrowField {
        let field = ArrowField::new(self.name, self.data_type, true);
        field
    }
}
