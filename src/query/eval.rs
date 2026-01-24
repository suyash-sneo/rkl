use super::ast::{CmpOp, Expr, JsonPath, Literal, RootPath, parse_timestamp_literal_ms};
use serde_json::Value;
use std::borrow::Cow;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct CompiledExpr {
    kind: CompiledExprKind,
}

#[derive(Debug, Clone)]
enum CompiledExprKind {
    Cmp(CompiledCmp),
    And(Box<CompiledExpr>, Box<CompiledExpr>),
    Or(Box<CompiledExpr>, Box<CompiledExpr>),
}

#[derive(Debug, Clone)]
struct CompiledCmp {
    left: JsonPath,
    op: CmpOp,
    right: Literal,
    contains_needle: Option<String>,
    timestamp_rhs_ms: Option<i64>,
}

impl CompiledExpr {
    pub fn compile(expr: &Expr) -> Self {
        match expr {
            Expr::And(lhs, rhs) => CompiledExpr {
                kind: CompiledExprKind::And(
                    Box::new(CompiledExpr::compile(lhs)),
                    Box::new(CompiledExpr::compile(rhs)),
                ),
            },
            Expr::Or(lhs, rhs) => CompiledExpr {
                kind: CompiledExprKind::Or(
                    Box::new(CompiledExpr::compile(lhs)),
                    Box::new(CompiledExpr::compile(rhs)),
                ),
            },
            Expr::Cmp { left, op, right } => {
                let contains_needle = if matches!(op, CmpOp::Contains) {
                    Some(literal_to_string(right))
                } else {
                    None
                };
                let timestamp_rhs_ms =
                    if matches!(op, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge)
                        && matches!(left.root, RootPath::Timestamp)
                        && left.segments.is_empty()
                    {
                        match right {
                            Literal::String(s) => parse_timestamp_literal_ms(s),
                            Literal::Number(n) => Some(*n as i64),
                            _ => None,
                        }
                    } else {
                        None
                    };
                CompiledExpr {
                    kind: CompiledExprKind::Cmp(CompiledCmp {
                        left: left.clone(),
                        op: *op,
                        right: right.clone(),
                        contains_needle,
                        timestamp_rhs_ms,
                    }),
                }
            }
        }
    }

    pub fn matches(&self, ctx: &mut EvalContext<'_>) -> bool {
        match &self.kind {
            CompiledExprKind::And(lhs, rhs) => lhs.matches(ctx) && rhs.matches(ctx),
            CompiledExprKind::Or(lhs, rhs) => lhs.matches(ctx) || rhs.matches(ctx),
            CompiledExprKind::Cmp(cmp) => cmp.matches(ctx),
        }
    }
}

#[derive(Debug)]
pub struct EvalContext<'a> {
    key_bytes: Option<&'a [u8]>,
    value_bytes: Option<&'a [u8]>,
    key_str: Option<Cow<'a, str>>,
    value_str: Option<Cow<'a, str>>,
    value_json: Option<Value>,
    json_parsed: bool,
    pub timestamp_ms: i64,
}

impl<'a> EvalContext<'a> {
    pub fn new(key_bytes: Option<&'a [u8]>, value_bytes: Option<&'a [u8]>, timestamp_ms: i64) -> Self {
        Self {
            key_bytes,
            value_bytes,
            key_str: None,
            value_str: None,
            value_json: None,
            json_parsed: false,
            timestamp_ms,
        }
    }

    fn key_str(&mut self) -> &str {
        if self.key_str.is_none() {
            let cow = match self.key_bytes {
                Some(bytes) => String::from_utf8_lossy(bytes),
                None => Cow::Borrowed("null"),
            };
            self.key_str = Some(cow);
        }
        self.key_str.as_deref().unwrap_or("")
    }

    fn value_str(&mut self) -> &str {
        if self.value_str.is_none() {
            let cow = match self.value_bytes {
                Some(bytes) => String::from_utf8_lossy(bytes),
                None => Cow::Borrowed("null"),
            };
            self.value_str = Some(cow);
        }
        self.value_str.as_deref().unwrap_or("null")
    }

    pub fn take_key_string(&mut self) -> String {
        if let Some(cow) = self.key_str.take() {
            return cow.into_owned();
        }
        match self.key_bytes {
            Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            None => "null".to_string(),
        }
    }

    pub fn take_value_string(&mut self) -> Option<String> {
        let out = if let Some(cow) = self.value_str.take() {
            cow.into_owned()
        } else {
            match self.value_bytes {
                Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                None => "null".to_string(),
            }
        };
        Some(out)
    }

    fn value_json(&mut self) -> Option<&Value> {
        if !self.json_parsed {
            self.json_parsed = true;
            if let Some(bytes) = self.value_bytes {
                if let Ok(v) = serde_json::from_slice::<Value>(bytes) {
                    self.value_json = Some(v);
                }
            }
        }
        self.value_json.as_ref()
    }
}

impl CompiledCmp {
    fn matches(&self, ctx: &mut EvalContext<'_>) -> bool {
        match self.left.root {
            RootPath::Key => self.matches_key(ctx),
            RootPath::Timestamp => self.matches_timestamp(ctx),
            RootPath::Value => self.matches_value(ctx),
        }
    }

    fn matches_key(&self, ctx: &mut EvalContext<'_>) -> bool {
        let left = ctx.key_str();
        match self.op {
            CmpOp::Contains => left.contains(self.contains_needle.as_deref().unwrap_or("")),
            CmpOp::Eq => match &self.right {
                Literal::String(s) => left == s,
                _ => false,
            },
            CmpOp::Neq => match &self.right {
                Literal::String(s) => left != s,
                _ => true,
            },
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => match &self.right {
                Literal::String(s) => cmp_str_ordering(left, s, self.op),
                _ => false,
            },
        }
    }

    fn matches_timestamp(&self, ctx: &mut EvalContext<'_>) -> bool {
        match self.op {
            CmpOp::Contains => {
                let needle = self.contains_needle.as_deref().unwrap_or("");
                if needle.is_empty() {
                    return true;
                }
                ctx.timestamp_ms.to_string().contains(needle)
            }
            CmpOp::Eq => match &self.right {
                Literal::Number(n) => (ctx.timestamp_ms as f64 - *n).abs() < f64::EPSILON,
                _ => false,
            },
            CmpOp::Neq => match &self.right {
                Literal::Number(n) => (ctx.timestamp_ms as f64 - *n).abs() >= f64::EPSILON,
                _ => true,
            },
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                if let Some(rhs_ms) = self.timestamp_rhs_ms {
                    match self.op {
                        CmpOp::Lt => ctx.timestamp_ms < rhs_ms,
                        CmpOp::Le => ctx.timestamp_ms <= rhs_ms,
                        CmpOp::Gt => ctx.timestamp_ms > rhs_ms,
                        CmpOp::Ge => ctx.timestamp_ms >= rhs_ms,
                        _ => false,
                    }
                } else {
                    false
                }
            }
        }
    }

    fn matches_value(&self, ctx: &mut EvalContext<'_>) -> bool {
        let is_root = self.left.segments.is_empty();
        match self.op {
            CmpOp::Contains => {
                let needle = self.contains_needle.as_deref().unwrap_or("");
                if is_root {
                    ctx.value_str().contains(needle)
                } else {
                    let Some(v) = ctx.value_json() else {
                        return false;
                    };
                    let Some(v) = resolve_path_ref(v, &self.left.segments) else {
                        return false;
                    };
                    value_to_string(v).contains(needle)
                }
            }
            CmpOp::Eq | CmpOp::Neq => {
                if is_root {
                    if let Literal::String(s) = &self.right {
                        return if matches!(self.op, CmpOp::Eq) {
                            ctx.value_str() == s
                        } else {
                            ctx.value_str() != s
                        };
                    }
                }
                let value = if is_root {
                    ctx.value_json()
                } else {
                    ctx.value_json()
                        .and_then(|v| resolve_path_ref(v, &self.left.segments))
                };
                if value.is_none() {
                    return matches!(self.right, Literal::Null) && matches!(self.op, CmpOp::Eq);
                }
                let value = value.unwrap();
                let eq = cmp_eq(value, &self.right);
                if matches!(self.op, CmpOp::Eq) {
                    eq
                } else {
                    !eq
                }
            }
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                let value = if is_root {
                    ctx.value_json()
                } else {
                    ctx.value_json()
                        .and_then(|v| resolve_path_ref(v, &self.left.segments))
                };
                let Some(value) = value else {
                    return false;
                };
                cmp_ordering(value, &self.right, self.op)
            }
        }
    }
}

fn resolve_path_ref<'a>(value: &'a Value, segments: &[String]) -> Option<&'a Value> {
    let mut cur = value;
    for seg in segments {
        match cur {
            Value::Object(map) => {
                cur = map.get(seg)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn cmp_eq(left: &Value, right: &Literal) -> bool {
    match right {
        Literal::String(s) => left.as_str().map(|x| x == s).unwrap_or(false),
        Literal::Number(n) => left
            .as_f64()
            .map(|x| (x - *n).abs() < f64::EPSILON)
            .unwrap_or_else(|| {
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

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn literal_to_string(lit: &Literal) -> String {
    match lit {
        Literal::String(s) => s.clone(),
        Literal::Number(n) => n.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => "null".to_string(),
    }
}

fn cmp_str_ordering(left: &str, right: &str, op: CmpOp) -> bool {
    let ordering = left.cmp(right);
    matches!(
        (ordering, op),
        (Ordering::Less, CmpOp::Lt)
            | (Ordering::Less, CmpOp::Le)
            | (Ordering::Equal, CmpOp::Le)
            | (Ordering::Equal, CmpOp::Ge)
            | (Ordering::Greater, CmpOp::Gt)
            | (Ordering::Greater, CmpOp::Ge)
    )
}
