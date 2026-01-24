pub mod ast;
pub mod eval;
pub mod parser;

pub use ast::*;
pub use eval::{CompiledExpr, EvalContext};

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Select(SelectQuery),
    ListTopics,
}

pub use parser::{parse_command, parse_query};
