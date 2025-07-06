use crate::datatypes::schema::Schema;

/**
 * A logical plan represents a data transformation or action that returns a relation (a set of
 * tuples).
 */

pub trait LogicalPLan {
    /** Returns the schema of the data that will be produced by this logical plan. */
    fn schema(&self) -> Schema;

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */

    fn children(&self) -> Vec<Box<dyn LogicalPLan>>;
}



pub fn pretty_format(plan: Box<dyn LogicalPLan>, indent: i32) -> String {
    unimplemented!()
}
