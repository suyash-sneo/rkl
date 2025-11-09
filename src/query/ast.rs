#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Key,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootPath {
    Key,
    Value,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPath {
    pub root: RootPath,
    pub segments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    // Future: Neq, Lt, Gt, Le, Ge, Like, In, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Comparison like: value->payload->method = 'PUT'
    Cmp { left: JsonPath, op: CmpOp, right: Literal },
    // Future: And(Box<Expr>, Box<Expr>), Or(...), Not(...)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderField {
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSpec {
    pub field: OrderField,
    pub dir: OrderDir,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub select: Vec<SelectItem>,
    pub from: String, // Kafka topic (raw string for now)
    pub r#where: Option<Expr>,
    pub order: Option<OrderSpec>,
    pub limit: Option<usize>,
}

impl Expr {
    /// Evaluate this expression against a message triple `(key, value_json, timestamp_ms)`.
    pub fn matches(&self, key: &str, value: &Value, timestamp_ms: i64) -> bool {
        match self {
            Expr::Cmp { left, op, right } => {
                let lv = resolve_path(left, key, value, timestamp_ms);
                match op {
                    CmpOp::Eq => cmp_eq(&lv, right),
                }
            }
        }
    }
}

fn resolve_path(path: &JsonPath, key: &str, value: &Value, timestamp_ms: i64) -> Value {
    match path.root {
        RootPath::Key => Value::String(key.to_string()),
        RootPath::Timestamp => Value::Number(serde_json::Number::from(timestamp_ms)),
        RootPath::Value => {
            let mut cur = value;
            for seg in &path.segments {
                match cur {
                    Value::Object(map) => {
                        if let Some(v) = map.get(seg) { cur = v; } else { return Value::Null; }
                    }
                    _ => return Value::Null,
                }
            }
            cur.clone()
        }
    }
}

fn cmp_eq(left: &Value, right: &Literal) -> bool {
    match right {
        Literal::String(s) => left.as_str().map(|x| x == s).unwrap_or(false),
        Literal::Number(n) => left.as_f64().map(|x| (x - *n).abs() < f64::EPSILON).unwrap_or_else(|| {
            // try integer equality if left is i64
            if let Some(i) = left.as_i64() { (*n - i as f64).abs() < f64::EPSILON } else { false }
        }),
        Literal::Bool(b) => left.as_bool().map(|x| x == *b).unwrap_or(false),
        Literal::Null => left.is_null(),
    }
}
use serde_json::Value;
