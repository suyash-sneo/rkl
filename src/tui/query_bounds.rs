pub fn find_query_range(s: &str, cursor: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let cur = cursor.min(len);
    let cursor_semicolon = if cur < len && bytes[cur] == b';' {
        Some(cur)
    } else if cur > 0 && bytes[cur - 1] == b';' {
        Some(cur - 1)
    } else {
        None
    };
    let start_limit = cursor_semicolon.unwrap_or(cur);

    let mut last_stmt_start = 0usize;
    let mut last_semicolon: Option<usize> = None;
    let mut in_string = false;
    let mut string_delim = 0u8;
    let mut i = 0usize;

    while i < start_limit {
        let b = bytes[i];
        if in_string {
            if b == b'\\' && i + 1 < len {
                i += 2;
                continue;
            }
            if b == string_delim {
                in_string = false;
                string_delim = 0;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            in_string = true;
            string_delim = b;
            i += 1;
            continue;
        }
        if b == b';' {
            last_semicolon = Some(i);
            last_stmt_start = (i + 1).min(len);
            i += 1;
            continue;
        }
        if is_select_at(bytes, i) {
            if last_semicolon.map(|sc| i > sc).unwrap_or(true) {
                last_stmt_start = i;
            }
        }
        i += 1;
    }

    let start = last_stmt_start.min(len);

    let mut end = len;
    i = cursor_semicolon.unwrap_or(cur);
    while i < len {
        let b = bytes[i];
        if in_string {
            if b == b'\\' && i + 1 < len {
                i += 2;
                continue;
            }
            if b == string_delim {
                in_string = false;
                string_delim = 0;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            in_string = true;
            string_delim = b;
            i += 1;
            continue;
        }
        if b == b';' {
            end = i + 1;
            break;
        }
        i += 1;
    }

    (start.min(len), end.min(len))
}

pub fn strip_trailing_semicolon(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return s;
    }
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end > 0 && bytes[end - 1] == b';' {
        end -= 1;
        while end > 0 && bytes[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
    }
    &s[..end]
}

fn is_select_at(bytes: &[u8], idx: usize) -> bool {
    const KW: &[u8] = b"select";
    if idx + KW.len() > bytes.len() {
        return false;
    }
    for (a, b) in bytes[idx..idx + KW.len()].iter().zip(KW.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    is_word_boundary(bytes, idx, idx + KW.len())
}

fn is_word_boundary(bytes: &[u8], start: usize, end: usize) -> bool {
    let prev_is_word = start > 0 && is_word_byte(bytes[start - 1]);
    let next_is_word = end < bytes.len() && is_word_byte(bytes[end]);
    !prev_is_word && !next_is_word
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_query_around_cursor() {
        let text = "SELECT a FROM foo;\nSELECT b FROM bar;";
        let (s1, e1) = find_query_range(text, 5);
        assert_eq!(&text[s1..e1], "SELECT a FROM foo;");
        let (s2, e2) = find_query_range(text, text.len() - 2);
        assert_eq!(&text[s2..e2], "SELECT b FROM bar;");
    }

    #[test]
    fn handles_no_trailing_semicolon() {
        let text = "SELECT value FROM topic";
        let (s, e) = find_query_range(text, text.len());
        assert_eq!(s, 0);
        assert_eq!(e, text.len());
    }

    #[test]
    fn ignores_markers_in_strings() {
        let text = "SELECT 'semi;inside \"SELECT\"' FROM foo;\nSELECT c FROM bar";
        let (s1, e1) = find_query_range(text, 10);
        assert_eq!(&text[s1..e1], "SELECT 'semi;inside \"SELECT\"' FROM foo;");
        let (s2, e2) = find_query_range(text, text.len());
        assert_eq!(&text[s2..e2], "SELECT c FROM bar");
    }

    #[test]
    fn respects_keyword_boundaries() {
        let text = "PRESELECT something;\nSELECT real_query FROM t;";
        let first_cursor = 5;
        let (s1, _) = find_query_range(text, first_cursor);
        assert_eq!(s1, 0);
        let second_cursor = text.len() - 3;
        let (s2, _) = find_query_range(text, second_cursor);
        assert_eq!(&text[s2..s2 + 6], "SELECT");
    }

    #[test]
    fn trims_trailing_semicolons() {
        assert_eq!(
            strip_trailing_semicolon("SELECT * FROM t;"),
            "SELECT * FROM t"
        );
        assert_eq!(
            strip_trailing_semicolon("SELECT * FROM t;  \n"),
            "SELECT * FROM t"
        );
        assert_eq!(
            strip_trailing_semicolon("SELECT * FROM t"),
            "SELECT * FROM t"
        );
    }
}
