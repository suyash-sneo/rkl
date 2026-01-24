#[cfg(test)]
use serde_json::Value;
use std::{cmp::Ordering, sync::OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectItem {
    Partition,
    Offset,
    Timestamp,
    Key,
    Value,
}

impl SelectItem {
    pub fn standard(include_value: bool) -> Vec<SelectItem> {
        let mut cols = vec![
            SelectItem::Partition,
            SelectItem::Offset,
            SelectItem::Timestamp,
            SelectItem::Key,
        ];
        if include_value {
            cols.push(SelectItem::Value);
        }
        cols
    }
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
    Lt,
    Le,
    Gt,
    Ge,
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
    Poffset,
    PoffsetTs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSpec {
    pub field: OrderField,
    pub dir: OrderDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryExecutionPlan {
    pub order_field: OrderField,
    pub order_dir: OrderDir,
    pub order_desc: bool,
    pub n_global: Option<usize>,
    pub per_partition_limit: Option<usize>,
    pub global_sort_by_timestamp: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub select: Vec<SelectItem>,
    pub from: String, // Kafka topic (raw string for now)
    pub r#where: Option<Expr>,
    pub order: Option<OrderSpec>,
    pub limit: Option<usize>,
}

impl SelectQuery {
    /// Returns (field, dir, is_default) where default means ORDER BY was omitted.
    pub fn effective_order(&self) -> (OrderField, OrderDir, bool) {
        match self.order.as_ref() {
            Some(spec) => (spec.field, spec.dir, false),
            None => (OrderField::Poffset, OrderDir::Desc, true),
        }
    }

    pub fn execution_plan(
        &self,
        partition_count: usize,
        base_limit: Option<usize>,
    ) -> QueryExecutionPlan {
        let partitions = partition_count.max(1);
        let (order_field, order_dir, _) = self.effective_order();
        let order_desc = matches!(order_dir, OrderDir::Desc);
        let n_global = base_limit;
        let per_partition_limit = match (order_field, n_global) {
            (OrderField::Poffset | OrderField::PoffsetTs, Some(n)) => {
                Some(n.div_ceil(partitions))
            }
            (OrderField::Timestamp, Some(n)) => Some(n),
            _ => None,
        };
        let global_sort_by_timestamp =
            matches!(order_field, OrderField::Timestamp | OrderField::PoffsetTs);
        QueryExecutionPlan {
            order_field,
            order_dir,
            order_desc,
            n_global,
            per_partition_limit,
            global_sort_by_timestamp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampBound {
    pub millis: i64,
    pub inclusive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimestampBounds {
    pub lower: Option<TimestampBound>,
    pub upper: Option<TimestampBound>,
}

impl Expr {
    /// Evaluate this expression against a message triple `(key, value_json, timestamp_ms)`.
    #[cfg(test)]
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
                CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                    if matches!(left.root, RootPath::Timestamp) && left.segments.is_empty() {
                        let rhs_ms = match right {
                            Literal::String(s) => parse_timestamp_literal_ms(s),
                            Literal::Number(n) => Some(*n as i64),
                            _ => None,
                        };
                        if let Some(rhs_ms) = rhs_ms {
                            match op {
                                CmpOp::Lt => timestamp_ms < rhs_ms,
                                CmpOp::Le => timestamp_ms <= rhs_ms,
                                CmpOp::Gt => timestamp_ms > rhs_ms,
                                CmpOp::Ge => timestamp_ms >= rhs_ms,
                                _ => unreachable!(),
                            }
                        } else {
                            false
                        }
                    } else {
                        let lv = resolve_path(left, key, value, timestamp_ms);
                        cmp_ordering(&lv, right, *op)
                    }
                }
            },
        }
    }

    /// Extract intersected timestamp bounds from an expression tree (AND only).
    pub fn timestamp_bounds(&self) -> Option<TimestampBounds> {
        fn merge_bounds(
            acc: Option<TimestampBounds>,
            next: Option<TimestampBounds>,
        ) -> Option<TimestampBounds> {
            match (acc, next) {
                (None, b) | (b, None) => b,
                (Some(a), Some(b)) => {
                    let lower = match (a.lower, b.lower) {
                        (None, x) | (x, None) => x,
                        (Some(l1), Some(l2)) => {
                            let (millis, inclusive) = match l1.millis.cmp(&l2.millis) {
                                Ordering::Greater => (l1.millis, l1.inclusive),
                                Ordering::Less => (l2.millis, l2.inclusive),
                                Ordering::Equal => (l1.millis, l1.inclusive && l2.inclusive),
                            };
                            Some(TimestampBound { millis, inclusive })
                        }
                    };

                    let upper = match (a.upper, b.upper) {
                        (None, x) | (x, None) => x,
                        (Some(u1), Some(u2)) => {
                            let (millis, inclusive) = match u1.millis.cmp(&u2.millis) {
                                Ordering::Less => (u1.millis, u1.inclusive),
                                Ordering::Greater => (u2.millis, u2.inclusive),
                                Ordering::Equal => (u1.millis, u1.inclusive && u2.inclusive),
                            };
                            Some(TimestampBound { millis, inclusive })
                        }
                    };

                    Some(TimestampBounds { lower, upper })
                }
            }
        }

        fn walk(expr: &Expr) -> Option<TimestampBounds> {
            match expr {
                Expr::And(lhs, rhs) => {
                    let lb = walk(lhs);
                    let rb = walk(rhs);
                    merge_bounds(lb, rb)
                }
                Expr::Or(_, _) => None,
                Expr::Cmp { left, op, right } => {
                    if !matches!(left.root, RootPath::Timestamp) || !left.segments.is_empty() {
                        return Some(TimestampBounds::default());
                    }
                    let millis = match right {
                        Literal::String(s) => parse_timestamp_literal_ms(s),
                        Literal::Number(n) => Some(*n as i64),
                        _ => None,
                    }?;
                    let bound = match op {
                        CmpOp::Gt => Some(TimestampBound {
                            millis,
                            inclusive: false,
                        }),
                        CmpOp::Ge => Some(TimestampBound {
                            millis,
                            inclusive: true,
                        }),
                        CmpOp::Lt => Some(TimestampBound {
                            millis,
                            inclusive: false,
                        }),
                        CmpOp::Le => Some(TimestampBound {
                            millis,
                            inclusive: true,
                        }),
                        _ => None,
                    };
                    if let Some(b) = bound {
                        if matches!(op, CmpOp::Gt | CmpOp::Ge) {
                            Some(TimestampBounds {
                                lower: Some(b),
                                upper: None,
                            })
                        } else {
                            Some(TimestampBounds {
                                lower: None,
                                upper: Some(b),
                            })
                        }
                    } else {
                        Some(TimestampBounds::default())
                    }
                }
            }
        }

        let res = walk(self)?;
        if res.lower.is_none() && res.upper.is_none() {
            None
        } else {
            Some(res)
        }
    }
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn cmp_contains(left: &str, right: &Literal) -> bool {
    let needle = literal_to_string(right);
    left.contains(&needle)
}

#[cfg(test)]
fn literal_to_string(lit: &Literal) -> String {
    match lit {
        Literal::String(s) => s.clone(),
        Literal::Number(n) => n.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => "null".to_string(),
    }
}

#[cfg(test)]
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

#[cfg(test)]
fn as_full_value_string(value: &Value, value_str: Option<&str>) -> String {
    if let Some(s) = value_str {
        s.to_string()
    } else {
        serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
    }
}

#[cfg(test)]
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

#[cfg(test)]
fn cmp_ordering(left: &Value, right: &Literal, op: CmpOp) -> bool {
    let ordering = match right {
        Literal::Number(n) => left.as_f64().and_then(|x| x.partial_cmp(n)),
        Literal::String(s) => left.as_str().map(|x| x.cmp(s)),
        Literal::Bool(b) => left.as_bool().map(|x| x.cmp(b)),
        Literal::Null => {
            if left.is_null() {
                Some(Ordering::Equal)
            } else {
                Some(Ordering::Greater)
            }
        }
    };
    matches!(
        (ordering, op),
        (Some(Ordering::Less), CmpOp::Lt)
            | (Some(Ordering::Less), CmpOp::Le)
            | (Some(Ordering::Equal), CmpOp::Le)
            | (Some(Ordering::Equal), CmpOp::Ge)
            | (Some(Ordering::Greater), CmpOp::Gt)
            | (Some(Ordering::Greater), CmpOp::Ge)
    )
}

pub(crate) fn parse_timestamp_literal_ms(s: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;
    use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(dt) = OffsetDateTime::parse(trimmed, &Rfc3339) {
        let millis = (dt.unix_timestamp_nanos() / 1_000_000) as i64;
        return Some(millis);
    }

    fn local_format() -> &'static [time::format_description::FormatItem<'static>] {
        static FMT: OnceLock<Vec<time::format_description::FormatItem<'static>>> = OnceLock::new();
        FMT.get_or_init(|| {
            time::format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]")
                .expect("valid timestamp format")
        })
    }

    if let Ok(pdt) = PrimitiveDateTime::parse(trimmed, local_format()) {
        if let Ok(offset) = UtcOffset::current_local_offset() {
            let odt = pdt.assume_offset(offset);
            let millis = (odt.unix_timestamp_nanos() / 1_000_000) as i64;
            return Some(millis);
        }
    }

    None
}

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

    #[test]
    fn matches_timestamp_comparisons() {
        let key = "k";
        let value_json = Value::Null;
        let ts_iso = "2024-01-01T12:00:00Z";
        let ts_ms =
            (time::OffsetDateTime::parse(ts_iso, &time::format_description::well_known::Rfc3339)
                .unwrap()
                .unix_timestamp_nanos()
                / 1_000_000) as i64;

        let expr_ge = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Ge,
            right: Literal::String(ts_iso.to_string()),
        };
        assert!(expr_ge.matches(key, &value_json, None, ts_ms));

        let expr_gt = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Gt,
            right: Literal::Number((ts_ms - 1) as f64),
        };
        assert!(expr_gt.matches(key, &value_json, None, ts_ms));

        let expr_lt = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Lt,
            right: Literal::String("2024-01-01T13:00:00Z".to_string()),
        };
        assert!(expr_lt.matches(key, &value_json, None, ts_ms));

        let expr_le = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Le,
            right: Literal::Number(ts_ms as f64),
        };
        assert!(expr_le.matches(key, &value_json, None, ts_ms));
    }

    #[test]
    fn timestamp_bounds_extract_ranges() {
        let lower = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Ge,
            right: Literal::String("2024-01-01T00:00:00Z".to_string()),
        };
        let bounds_lower = lower.timestamp_bounds().expect("lower bounds");
        assert!(bounds_lower.upper.is_none());
        let lb = bounds_lower.lower.expect("lower");
        assert!(lb.inclusive);

        let upper = Expr::Cmp {
            left: path(RootPath::Timestamp, &[]),
            op: CmpOp::Lt,
            right: Literal::String("2024-01-02T00:00:00".to_string()),
        };
        let bounds_upper = upper.timestamp_bounds().expect("upper bounds");
        assert!(bounds_upper.lower.is_none());
        let ub = bounds_upper.upper.expect("upper");
        assert!(!ub.inclusive);

        let both = Expr::And(Box::new(lower.clone()), Box::new(upper.clone()));
        let bounds = both.timestamp_bounds().expect("combined");
        assert!(bounds.lower.is_some());
        assert!(bounds.upper.is_some());

        let expr_or = Expr::Or(Box::new(lower), Box::new(upper));
        assert!(expr_or.timestamp_bounds().is_none());
    }
}
