use super::ast::*;

#[derive(Debug)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken(String),
    ExpectedKeyword(String),
    ExpectedIdentifier,
    ExpectedNumber,
    ExpectedLiteral,
    ExpectedPath,
    InvalidOrderByField(String),
}

type PResult<T> = Result<T, ParseError>;

pub fn parse_query(input: &str) -> PResult<SelectQuery> {
    let mut p = Parser::new(input);
    p.consume_keyword("SELECT")?;
    let select = p.parse_select_list()?;
    p.consume_keyword("FROM")?;
    let from = p.parse_topic()?;
    let r#where = if p.try_consume_keyword("WHERE") {
        Some(p.parse_where_expr()?)
    } else {
        None
    };
    let order = if p.try_consume_keyword("ORDER") {
        p.consume_keyword("BY")?;
        Some(p.parse_order_by()?)
    } else {
        None
    };
    let limit = if p.try_consume_keyword("LIMIT") {
        Some(p.parse_usize()?)
    } else {
        None
    };
    p.skip_ws();
    if !p.is_eof() {
        return Err(ParseError::UnexpectedToken(p.remaining().to_string()));
    }
    Ok(SelectQuery {
        select,
        from,
        r#where,
        order,
        limit,
    })
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken(s) => write!(f, "unexpected token near: {}", s),
            ParseError::ExpectedKeyword(k) => write!(f, "expected keyword: {}", k),
            ParseError::ExpectedIdentifier => write!(f, "expected identifier"),
            ParseError::ExpectedNumber => write!(f, "expected number"),
            ParseError::ExpectedLiteral => write!(f, "expected literal"),
            ParseError::ExpectedPath => write!(f, "expected path (key|value|timestamp)"),
            ParseError::InvalidOrderByField(s) => write!(f, "invalid ORDER BY field near: {}", s),
        }
    }
}

impl std::error::Error for ParseError {}

struct Parser<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.s.len()
    }

    fn remaining(&self) -> &str {
        &self.s[self.pos..]
    }

    fn peek_char(&self) -> Option<char> {
        self.s[self.pos..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        if let Some(ch) = self.peek_char() {
            self.pos += ch.len_utf8();
            Some(ch)
        } else {
            None
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.bump();
            } else {
                break;
            }
        }
    }

    fn consume_keyword(&mut self, kw: &str) -> PResult<()> {
        self.skip_ws();
        let start = self.pos;
        let n = kw.len();
        if self.pos + n > self.s.len() {
            return Err(ParseError::ExpectedKeyword(kw.to_string()));
        }
        let slice = &self.s[self.pos..self.pos + n];
        if slice.eq_ignore_ascii_case(kw) {
            self.pos += n;
            // next must be boundary
            if let Some(c) = self.peek_char() {
                if c.is_alphanumeric() || c == '_' {
                    return Err(ParseError::ExpectedKeyword(kw.to_string()));
                }
            }
            Ok(())
        } else {
            self.pos = start;
            Err(ParseError::ExpectedKeyword(kw.to_string()))
        }
    }

    fn try_consume_keyword(&mut self, kw: &str) -> bool {
        let save = self.pos;
        if self.consume_keyword(kw).is_ok() {
            true
        } else {
            self.pos = save;
            false
        }
    }

    fn parse_identifier(&mut self) -> PResult<String> {
        self.skip_ws();
        let mut out = String::new();
        let mut it = self.s[self.pos..].chars().peekable();
        let mut consumed = 0;
        while let Some(&ch) = it.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                out.push(ch);
                it.next();
                consumed += ch.len_utf8();
            } else {
                break;
            }
        }
        if out.is_empty() {
            return Err(ParseError::ExpectedIdentifier);
        }
        self.pos += consumed;
        Ok(out)
    }

    fn parse_topic(&mut self) -> PResult<String> {
        // Accept anything non-whitespace until next keyword or end
        self.skip_ws();
        let mut out = String::new();
        let mut it = self.s[self.pos..].chars().peekable();
        let mut consumed = 0;
        while let Some(&ch) = it.peek() {
            if ch.is_whitespace() {
                break;
            }
            out.push(ch);
            it.next();
            consumed += ch.len_utf8();
        }
        if out.is_empty() {
            return Err(ParseError::ExpectedIdentifier);
        }
        self.pos += consumed;
        Ok(out)
    }

    fn parse_select_list(&mut self) -> PResult<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            if self.try_consume_word_case("partition") {
                items.push(SelectItem::Partition);
            } else if self.try_consume_word_case("offset") {
                items.push(SelectItem::Offset);
            } else if self.try_consume_word_case("timestamp") {
                items.push(SelectItem::Timestamp);
            } else if self.try_consume_word_case("key") {
                items.push(SelectItem::Key);
            } else if self.try_consume_word_case("value") {
                items.push(SelectItem::Value);
            } else {
                return Err(ParseError::UnexpectedToken(self.remaining().to_string()));
            }

            self.skip_ws();
            if self.try_consume_char(',') {
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn try_consume_word_case(&mut self, w: &str) -> bool {
        self.skip_ws();
        let save = self.pos;
        let n = w.len();
        if self.pos + n <= self.s.len() {
            let slice = &self.s[self.pos..self.pos + n];
            if slice.eq_ignore_ascii_case(w) {
                self.pos += n;
                // word boundary
                if let Some(c) = self.peek_char() {
                    if c.is_alphanumeric() || c == '_' {
                        self.pos = save;
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    fn try_consume_char(&mut self, ch: char) -> bool {
        self.skip_ws();
        if self.peek_char() == Some(ch) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn parse_where_expr(&mut self) -> PResult<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> PResult<Expr> {
        let mut expr = self.parse_and_expr()?;
        loop {
            if self.try_consume_keyword("OR") {
                let rhs = self.parse_and_expr()?;
                expr = Expr::Or(Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_and_expr(&mut self) -> PResult<Expr> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.try_consume_keyword("AND") {
                let rhs = self.parse_primary()?;
                expr = Expr::And(Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> PResult<Expr> {
        self.skip_ws();
        if self.try_consume_char('(') {
            let expr = self.parse_or_expr()?;
            if !self.try_consume_char(')') {
                return Err(ParseError::UnexpectedToken(self.remaining().to_string()));
            }
            Ok(expr)
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> PResult<Expr> {
        let left = self.parse_json_path()?;
        let op = self.parse_cmp_op()?;
        let right = self.parse_literal()?;
        Ok(Expr::Cmp { left, op, right })
    }

    fn parse_cmp_op(&mut self) -> PResult<CmpOp> {
        self.skip_ws();
        if self.try_consume_keyword("CONTAINS") {
            return Ok(CmpOp::Contains);
        }
        let rest = self.remaining();
        if rest.starts_with("!=") {
            self.pos += 2;
            return Ok(CmpOp::Neq);
        }
        if rest.starts_with("<>") {
            self.pos += 2;
            return Ok(CmpOp::Neq);
        }
        if rest.starts_with("=") {
            self.pos += 1;
            return Ok(CmpOp::Eq);
        }
        Err(ParseError::UnexpectedToken(self.remaining().to_string()))
    }

    fn parse_json_path(&mut self) -> PResult<JsonPath> {
        self.skip_ws();
        let root = if self.try_consume_word_case("value") {
            RootPath::Value
        } else if self.try_consume_word_case("key") {
            RootPath::Key
        } else if self.try_consume_word_case("timestamp") {
            RootPath::Timestamp
        } else {
            return Err(ParseError::ExpectedPath);
        };

        let mut segments = Vec::new();
        loop {
            self.skip_ws();
            // look for ->segment
            let save = self.pos;
            if self.try_consume_symbol_arrow() {
                let seg = self.parse_identifier()?;
                segments.push(seg);
            } else {
                self.pos = save;
                break;
            }
        }

        Ok(JsonPath { root, segments })
    }

    fn try_consume_symbol_arrow(&mut self) -> bool {
        self.skip_ws();
        let rest = self.remaining();
        if rest.starts_with("->") {
            self.pos += 2;
            true
        } else {
            false
        }
    }

    fn parse_literal(&mut self) -> PResult<Literal> {
        self.skip_ws();
        if let Some('\'') = self.peek_char() {
            return self.parse_string_lit().map(Literal::String);
        }
        // number, bool, null
        if self.try_consume_word_case("true") {
            return Ok(Literal::Bool(true));
        }
        if self.try_consume_word_case("false") {
            return Ok(Literal::Bool(false));
        }
        if self.try_consume_word_case("null") {
            return Ok(Literal::Null);
        }
        // number: simple float/ints
        if let Ok(n) = self.parse_number_opt() {
            return Ok(Literal::Number(n));
        }
        Err(ParseError::ExpectedLiteral)
    }

    fn parse_string_lit(&mut self) -> PResult<String> {
        // Simple single-quoted string, supports escaping of \' and \\.
        self.skip_ws();
        if self.bump() != Some('\'') {
            return Err(ParseError::ExpectedLiteral);
        }
        let mut out = String::new();
        while let Some(ch) = self.bump() {
            match ch {
                '\\' => {
                    if let Some(next) = self.bump() {
                        match next {
                            '\\' => out.push('\\'),
                            '\'' => out.push('\''),
                            other => {
                                out.push('\\');
                                out.push(other);
                            }
                        }
                    } else {
                        return Err(ParseError::UnexpectedEof);
                    }
                }
                '\'' => return Ok(out),
                c => out.push(c),
            }
        }
        Err(ParseError::UnexpectedEof)
    }

    fn parse_number_opt(&mut self) -> Result<f64, ()> {
        self.skip_ws();
        let mut it = self.s[self.pos..].chars().peekable();
        let mut buf = String::new();
        let mut consumed = 0;
        let mut seen_digit = false;
        if let Some(&'-') = it.peek() {
            buf.push('-');
            it.next();
            consumed += 1;
        }
        while let Some(&ch) = it.peek() {
            if ch.is_ascii_digit() {
                buf.push(ch);
                it.next();
                consumed += 1;
                seen_digit = true;
            } else {
                break;
            }
        }
        if let Some(&'.') = it.peek() {
            buf.push('.');
            it.next();
            consumed += 1;
            let mut frac = 0;
            while let Some(&ch) = it.peek() {
                if ch.is_ascii_digit() {
                    buf.push(ch);
                    it.next();
                    consumed += 1;
                    frac += 1;
                } else {
                    break;
                }
            }
            if frac == 0 {
                return Err(());
            }
        }
        if !seen_digit {
            return Err(());
        }
        self.pos += consumed;
        buf.parse::<f64>().map_err(|_| ())
    }

    fn parse_usize(&mut self) -> PResult<usize> {
        self.skip_ws();
        let mut it = self.s[self.pos..].chars().peekable();
        let mut buf = String::new();
        let mut consumed = 0;
        while let Some(&ch) = it.peek() {
            if ch.is_ascii_digit() {
                buf.push(ch);
                it.next();
                consumed += 1;
            } else {
                break;
            }
        }
        if buf.is_empty() {
            return Err(ParseError::ExpectedNumber);
        }
        self.pos += consumed;
        buf.parse::<usize>().map_err(|_| ParseError::ExpectedNumber)
    }

    fn parse_order_by(&mut self) -> PResult<OrderSpec> {
        self.skip_ws();
        // Only timestamp supported for now
        if !self.try_consume_word_case("timestamp") {
            // allow value->timestamp? but keep strict for now
            let mut preview = String::new();
            preview.push_str(self.remaining());
            return Err(ParseError::InvalidOrderByField(preview));
        }
        let dir = if self.try_consume_keyword("ASC") {
            OrderDir::Asc
        } else if self.try_consume_keyword("DESC") {
            OrderDir::Desc
        } else {
            OrderDir::Asc
        };
        Ok(OrderSpec {
            field: OrderField::Timestamp,
            dir,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_query() {
        let q = "SELECT key, value FROM stage::digital.input.event.topic WHERE value->payload->method = 'PUT' ORDER BY timestamp ASC LIMIT 10";
        let ast = parse_query(q).expect("parse ok");
        assert_eq!(ast.select, vec![SelectItem::Key, SelectItem::Value]);
        assert_eq!(ast.from, "stage::digital.input.event.topic");
        match ast.r#where {
            Some(Expr::Cmp { left, op, right }) => {
                assert_eq!(left.root, RootPath::Value);
                assert_eq!(
                    left.segments,
                    vec!["payload".to_string(), "method".to_string()]
                );
                assert_eq!(op, CmpOp::Eq);
                assert!(matches!(right, Literal::String(s) if s == "PUT"));
            }
            _ => panic!("expected where comparison"),
        }
        assert!(matches!(
            ast.order,
            Some(OrderSpec {
                field: OrderField::Timestamp,
                dir: OrderDir::Asc
            })
        ));
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn parses_extended_columns() {
        let q = "SELECT partition, OFFSET, Timestamp, key FROM foo";
        let ast = parse_query(q).expect("parse ok");
        assert_eq!(
            ast.select,
            vec![
                SelectItem::Partition,
                SelectItem::Offset,
                SelectItem::Timestamp,
                SelectItem::Key,
            ]
        );
    }

    fn where_expr(query: &str) -> Expr {
        parse_query(query)
            .expect("parse ok")
            .r#where
            .expect("where clause")
    }

    fn path(root: RootPath, segments: &[&str]) -> JsonPath {
        JsonPath {
            root,
            segments: segments.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn parses_or_and_precedence() {
        let expr =
            where_expr("SELECT key FROM t WHERE key = 'a' OR key = 'b' AND value->foo = 'x'");
        let expected = Expr::Or(
            Box::new(Expr::Cmp {
                left: path(RootPath::Key, &[]),
                op: CmpOp::Eq,
                right: Literal::String("a".to_string()),
            }),
            Box::new(Expr::And(
                Box::new(Expr::Cmp {
                    left: path(RootPath::Key, &[]),
                    op: CmpOp::Eq,
                    right: Literal::String("b".to_string()),
                }),
                Box::new(Expr::Cmp {
                    left: path(RootPath::Value, &["foo"]),
                    op: CmpOp::Eq,
                    right: Literal::String("x".to_string()),
                }),
            )),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn parses_parentheses_and_contains() {
        let expr = where_expr(
            "SELECT key FROM t WHERE (key = 'a' OR key = 'b') AND value->foo CONTAINS 'x'",
        );
        let expected = Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::Cmp {
                    left: path(RootPath::Key, &[]),
                    op: CmpOp::Eq,
                    right: Literal::String("a".to_string()),
                }),
                Box::new(Expr::Cmp {
                    left: path(RootPath::Key, &[]),
                    op: CmpOp::Eq,
                    right: Literal::String("b".to_string()),
                }),
            )),
            Box::new(Expr::Cmp {
                left: path(RootPath::Value, &["foo"]),
                op: CmpOp::Contains,
                right: Literal::String("x".to_string()),
            }),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn parses_not_equal_variants() {
        let expr_ne = where_expr("SELECT key FROM t WHERE value->method != 'PUT'");
        assert!(matches!(expr_ne, Expr::Cmp { op: CmpOp::Neq, .. }));

        let expr_alt = where_expr("SELECT key FROM t WHERE value->method <> 'PUT'");
        assert!(matches!(expr_alt, Expr::Cmp { op: CmpOp::Neq, .. }));
    }

    #[test]
    fn parses_contains_variants() {
        let expr_key = where_expr("SELECT key FROM t WHERE key CONTAINS '123'");
        assert!(matches!(
            expr_key,
            Expr::Cmp {
                op: CmpOp::Contains,
                ..
            }
        ));

        let expr_value = where_expr("SELECT key FROM t WHERE value CONTAINS 'abc'");
        assert!(matches!(
            expr_value,
            Expr::Cmp {
                op: CmpOp::Contains,
                ..
            }
        ));

        let expr_nested = where_expr("SELECT key FROM t WHERE value->msg CONTAINS 'err'");
        assert!(matches!(
            expr_nested,
            Expr::Cmp {
                op: CmpOp::Contains,
                ..
            }
        ));
    }
}
