pub mod expressions;
pub mod test;
use crate::{datasource::Iterators, datatypes::schema::Schema};

pub enum PhysicaPlan {}

impl PhysicaPlan {
    pub fn children(&self) -> Vec<PhysicaPlan> {
        unimplemented!()
    }

    pub fn execute(&self) -> Iterators {
        unimplemented!()
    }

    pub fn schema(&self) -> Schema {
        unimplemented!()
    }

    pub fn format_plan(&self) -> String {
        pretty_format(self.get_plan(), 0)
    }

    fn get_plan(&self) -> &PhysicaPlan {
        unimplemented!()
    }
}

impl std::fmt::Display for PhysicaPlan {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

pub fn pretty_format(plan: &PhysicaPlan, indent: usize) -> String {
    let mut b = String::new();

    for _ in 0..indent {
        b.push('\t');
    }

    b.push_str(&plan.to_string());
    b.push('\n');

    for child in plan.children() {
        b.push_str(&pretty_format(&child, indent + 1));
    }

    b
}
