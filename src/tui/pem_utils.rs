pub fn normalize_pem_input(raw: &str) -> String {
    // Accept various escaped newline forms and normalize into real newlines.
    let mut out = String::new();
    let mut backslashes = 0usize;
    let cleaned = raw.replace("\r\n", "\n").replace('\r', "\n");
    let mut chars = cleaned.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            backslashes += 1;
            continue;
        }
        if backslashes > 0 {
            if ch == 'n' || ch == '\n' {
                out.push('\n');
                backslashes = 0;
                continue;
            } else {
                for _ in 0..backslashes {
                    out.push('\\');
                }
                backslashes = 0;
            }
        }
        out.push(ch);
    }
    if backslashes > 0 {
        for _ in 0..backslashes {
            out.push('\\');
        }
    }
    out
}

pub fn decode_literal_backslash_n(s: &str) -> String {
    s.replace("\\n", "\n")
}

pub fn encode_pem_for_storage(input_display: &str) -> String {
    normalize_pem_input(input_display).replace('\n', "\\n")
}
