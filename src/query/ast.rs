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
    Neq,
    Contains,
    // Future: Lt, Gt, Le, Ge, Like, In, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Comparison like: value->payload->method = 'PUT'
    Cmp {
        left: JsonPath,
        op: CmpOp,
        right: Literal,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    // Future: Not(...)
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
    pub fn matches(
        &self,
        key: &str,
        value: &Value,
        value_str: Option<&str>,
        timestamp_ms: i64,
    ) -> bool {
        match self {
            Expr::And(lhs, rhs) => {
                lhs.matches(key, value, value_str, timestamp_ms)
                    && rhs.matches(key, value, value_str, timestamp_ms)
            }
            Expr::Or(lhs, rhs) => {
                lhs.matches(key, value, value_str, timestamp_ms)
                    || rhs.matches(key, value, value_str, timestamp_ms)
            }
            Expr::Cmp { left, op, right } => match op {
                CmpOp::Eq => {
                    cmp_eq_with_value_str(left, right, key, value, value_str, timestamp_ms)
                }
                CmpOp::Neq => {
                    !cmp_eq_with_value_str(left, right, key, value, value_str, timestamp_ms)
                }
                CmpOp::Contains => {
                    let left_str = path_to_string(left, key, value, value_str, timestamp_ms);
                    cmp_contains(&left_str, right)
                }
            },
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
                        if let Some(v) = map.get(seg) {
                            cur = v;
                        } else {
                            return Value::Null;
                        }
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
        Literal::Number(n) => left
            .as_f64()
            .map(|x| (x - *n).abs() < f64::EPSILON)
            .unwrap_or_else(|| {
                // try integer equality if left is i64
                if let Some(i) = left.as_i64() {
                    (*n - i as f64).abs() < f64::EPSILON
                } else {
                    false
                }
            }),
        Literal::Bool(b) => left.as_bool().map(|x| x == *b).unwrap_or(false),
        Literal::Null => left.is_null(),
    }
}

fn cmp_eq_with_value_str(
    left: &JsonPath,
    right: &Literal,
    key: &str,
    value: &Value,
    value_str: Option<&str>,
    timestamp_ms: i64,
) -> bool {
    if matches!(left.root, RootPath::Value) && left.segments.is_empty() {
        if let Literal::String(expected) = right {
            return as_full_value_string(value, value_str) == *expected;
        }
    }
    let lv = resolve_path(left, key, value, timestamp_ms);
    cmp_eq(&lv, right)
}

fn cmp_contains(left: &str, right: &Literal) -> bool {
    let needle = literal_to_string(right);
    left.contains(&needle)
}

fn literal_to_string(lit: &Literal) -> String {
    match lit {
        Literal::String(s) => s.clone(),
        Literal::Number(n) => n.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => "null".to_string(),
    }
}

fn path_to_string(
    left: &JsonPath,
    key: &str,
    value: &Value,
    value_str: Option<&str>,
    timestamp_ms: i64,
) -> String {
    if matches!(left.root, RootPath::Value) && left.segments.is_empty() {
        as_full_value_string(value, value_str)
    } else {
        let resolved = resolve_path(left, key, value, timestamp_ms);
        value_to_string(&resolved)
    }
}

fn as_full_value_string(value: &Value, value_str: Option<&str>) -> String {
    if let Some(s) = value_str {
        s.to_string()
    } else {
        serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}
use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;

    fn path(root: RootPath, segments: &[&str]) -> JsonPath {
        JsonPath {
            root,
            segments: segments.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn matches_equality_and_inequality() {
        let key = "user-123";
        let raw = r#"{"payload":{"method":"PUT","msg":"hello error world","code":42,"flag":true,"none":null}}"#;
        let value_json: Value = serde_json::from_str(raw).unwrap();
        let ts = 1_700_000_000i64;

        let method_eq = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "method"]),
            op: CmpOp::Eq,
            right: Literal::String("PUT".to_string()),
        };
        assert!(method_eq.matches(key, &value_json, Some(raw), ts));

        let method_neq = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "method"]),
            op: CmpOp::Neq,
            right: Literal::String("GET".to_string()),
        };
        assert!(method_neq.matches(key, &value_json, Some(raw), ts));

        let method_neq_false = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "method"]),
            op: CmpOp::Neq,
            right: Literal::String("PUT".to_string()),
        };
        assert!(!method_neq_false.matches(key, &value_json, Some(raw), ts));

        let code_eq = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "code"]),
            op: CmpOp::Eq,
            right: Literal::Number(42.0),
        };
        assert!(code_eq.matches(key, &value_json, Some(raw), ts));

        let flag_eq = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "flag"]),
            op: CmpOp::Eq,
            right: Literal::Bool(true),
        };
        assert!(flag_eq.matches(key, &value_json, Some(raw), ts));

        let none_eq = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "none"]),
            op: CmpOp::Eq,
            right: Literal::Null,
        };
        assert!(none_eq.matches(key, &value_json, Some(raw), ts));

        let full_value_eq = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Eq,
            right: Literal::String(raw.to_string()),
        };
        assert!(full_value_eq.matches(key, &value_json, Some(raw), ts));

        let full_value_neq = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Neq,
            right: Literal::String("other".to_string()),
        };
        assert!(full_value_neq.matches(key, &value_json, Some(raw), ts));
    }

    #[test]
    fn matches_contains_and_boolean_logic() {
        let key = "user-123";
        let raw = r#"{"payload":{"method":"PUT","msg":"hello error world","code":42}}"#;
        let value_json: Value = serde_json::from_str(raw).unwrap();
        let ts = 1_700_000_100i64;

        let key_contains = Expr::Cmp {
            left: path(RootPath::Key, &[]),
            op: CmpOp::Contains,
            right: Literal::String("123".to_string()),
        };
        assert!(key_contains.matches(key, &value_json, Some(raw), ts));

        let value_contains = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Contains,
            right: Literal::String("error".to_string()),
        };
        assert!(value_contains.matches(key, &value_json, Some(raw), ts));

        let nested_contains = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "msg"]),
            op: CmpOp::Contains,
            right: Literal::String("error".to_string()),
        };
        assert!(nested_contains.matches(key, &value_json, Some(raw), ts));

        let contains_number = Expr::Cmp {
            left: path(RootPath::Value, &["payload", "code"]),
            op: CmpOp::Contains,
            right: Literal::Number(42.0),
        };
        assert!(contains_number.matches(key, &value_json, Some(raw), ts));

        let timestamp_contains = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Contains,
            right: Literal::String("100".to_string()),
        };
        assert!(timestamp_contains.matches(key, &value_json, Some(raw), ts));

        let bool_expr = Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::Cmp {
                    left: path(RootPath::Key, &[]),
                    op: CmpOp::Eq,
                    right: Literal::String("x".to_string()),
                }),
                Box::new(Expr::Cmp {
                    left: path(RootPath::Key, &[]),
                    op: CmpOp::Eq,
                    right: Literal::String("user-123".to_string()),
                }),
            )),
            Box::new(Expr::Cmp {
                left: path(RootPath::Value, &["payload", "method"]),
                op: CmpOp::Neq,
                right: Literal::String("GET".to_string()),
            }),
        );
        assert!(bool_expr.matches(key, &value_json, Some(raw), ts));
    }

    #[test]
    fn matches_value_string_fallbacks() {
        let key = "plain-key";
        let raw_plain = "plain text";
        let value_json = Value::Null; // invalid JSON fallback
        let ts = 0i64;

        let contains_plain = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Contains,
            right: Literal::String("plain".to_string()),
        };
        assert!(contains_plain.matches(key, &value_json, Some(raw_plain), ts));

        let nested_contains = Expr::Cmp {
            left: path(RootPath::Value, &["foo"]),
            op: CmpOp::Contains,
            right: Literal::String("x".to_string()),
        };
        assert!(!nested_contains.matches(key, &value_json, Some(raw_plain), ts));

        let full_value_eq = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Eq,
            right: Literal::String(raw_plain.to_string()),
        };
        assert!(full_value_eq.matches(key, &value_json, Some(raw_plain), ts));

        let fallback_value = Expr::Cmp {
            left: path(RootPath::Value, &[]),
            op: CmpOp::Contains,
            right: Literal::String("hello".to_string()),
        };
        let json_value = serde_json::json!({"msg":"hello"});
        assert!(fallback_value.matches(key, &json_value, None, ts));
    }
}
