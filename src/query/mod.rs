pub mod ast;
pub mod parser;

pub use ast::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Select(SelectQuery),
    ListTopics,
}

pub use parser::{parse_command, parse_query};
