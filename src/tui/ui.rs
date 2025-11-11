use crate::models::MessageEnvelope;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Paragraph, Row, Table, TableState, List, ListItem, Clear,
    Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap,
};

use super::app::{AppState, Focus, EnvFieldFocus};

pub(super) const COPY_BTN_LABEL: &str = "[ Copy ]";

pub fn draw(frame: &mut Frame, app: &AppState) {
    let size = frame.size();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),   // host input
            Constraint::Length(8),   // query input (taller, multiline)
            Constraint::Length(1),   // status
            Constraint::Fill(1),     // table grows to fill available space
            Constraint::Length(3),   // footer (needs 3: top+content+bottom)
        ])
        .split(size);

    draw_env_bar(frame, chunks[0], app);
    draw_input(frame, chunks[1], app);
    draw_status(frame, chunks[2], &app.status);
    draw_results(frame, chunks[3], app);
    draw_footer(frame, chunks[4], app);
    if app.show_env_modal {
        let area = centered_rect(80, 80, size);
        let block = Block::default().title("Environments").borders(Borders::ALL).border_style(Style::default().fg(Color::Cyan));
        frame.render_widget(Clear, area);
        frame.render_widget(block, area);
        draw_env_modal(frame, area, app);
    }
}

fn draw_input(frame: &mut Frame, area: Rect, app: &AppState) {
    let focused = app.focus == Focus::Query;
    let title = if focused { "Query (Enter: Newline | Ctrl-Enter: Run current) [FOCUSED]" } else { "Query (Enter: Newline | Ctrl-Enter: Run current)" };
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Split inner into gutter and content
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(6), Constraint::Min(1)])
        .split(inner);
    let gutter = cols[0];
    let content = cols[1];

    // Compute line starts to style per-line highlights, and find query ranges
    let text = &app.input;
    let lines: Vec<&str> = text.split('\n').collect();
    let line_starts: Vec<usize> = {
        let mut v = Vec::with_capacity(lines.len());
        let mut acc = 0usize;
        for (i, l) in lines.iter().enumerate() {
            v.push(acc);
            acc += l.len();
            if i + 1 < lines.len() { acc += 1; } // newline
        }
        v
    };
    let (cur_q_start, cur_q_end) = find_query_range(text, app.input_cursor);
    let last_range = app.last_run_query_range;

    // Build content lines with SQL-ish highlighting and per-line background for current/last-run query regions
    let mut out_lines: Vec<Line> = Vec::with_capacity(lines.len());
    for (i, &lstart) in line_starts.iter().enumerate() {
        let lend = lstart + lines[i].len();
        let mut line = Line::from(highlight_sql_line(lines[i]));
        if intersects(lstart, lend, cur_q_start, cur_q_end) {
            // Current query highlight
            line = line.style(Style::default().bg(Color::Rgb(35, 60, 100)));
        } else if let Some((ls, le)) = last_range {
            if intersects(lstart, lend, ls, le) {
                // Last run query highlight
                line = line.style(Style::default().bg(Color::DarkGray));
            }
        }
        out_lines.push(line);
    }

    // Render content paragraph with wrapping + vertical scroll
    let para = Paragraph::new(Text::from(out_lines))
        .wrap(Wrap { trim: false })
        .scroll((app.input_vscroll, 0));
    frame.render_widget(para, content);

    // Render gutter: line numbers and markers for current and last-run query
    let mut gut: Vec<Line> = Vec::with_capacity(lines.len());
    let _cur_first_line = byte_index_to_line(&line_starts, cur_q_start);
    let last_first_line = last_range.map(|(s, _)| byte_index_to_line(&line_starts, s));
    for (i, &lstart) in line_starts.iter().enumerate() {
        let lend = lstart + lines[i].len();
        let is_cur = intersects(lstart, lend, cur_q_start, cur_q_end);
        let is_last = last_range.map(|(ls, le)| intersects(lstart, lend, ls, le)).unwrap_or(false);
        let marker = if is_cur && Some(i) == last_first_line { "➤▶" } else if is_cur { "➤" } else if Some(i) == last_first_line || is_last { "▶" } else { " " };
        let no = format!("{:>3}", i + 1);
        // Add an extra trailing space after the line number to separate gutter from content
        let mut line = Line::from(vec![
            Span::styled(marker, Style::default().fg(Color::Yellow)),
            Span::raw(" "),
            Span::styled(no, Style::default().fg(Color::Gray)),
            Span::raw(" "),
        ]);
        if is_cur {
            line = line.style(Style::default().bg(Color::Rgb(35, 60, 100)));
        } else if is_last {
            line = line.style(Style::default().bg(Color::DarkGray));
        }
        gut.push(line);
    }
    let gp = Paragraph::new(Text::from(gut)).scroll((app.input_vscroll, 0));
    frame.render_widget(gp, gutter);

    // Position caret
    if focused {
        if let Some((cx, cy)) = caret_pos_multiline(content, text, app.input_cursor, app.input_vscroll) {
            frame.set_cursor(cx, cy);
        }
    }
}

fn draw_env_bar(frame: &mut Frame, area: Rect, app: &AppState) {
    let title = if app.focus == Focus::Host { "Environment [Enter: manage] [FOCUSED]" } else { "Environment [Enter: manage]" };
    let block = Block::default().borders(Borders::ALL).title(title);
    let name = app.selected_env().map(|e| e.name.clone()).unwrap_or_else(|| "(none)".to_string());
    let host = app.selected_env().map(|e| e.host.clone()).unwrap_or_default();
    let content = format!("{name}  —  host: {host}");
    let para = Paragraph::new(content).block(block);
    frame.render_widget(para, area);
}

fn draw_status(frame: &mut Frame, area: Rect, status: &str) {
    let block = Block::default().borders(Borders::ALL).title("Status");
    let para = Paragraph::new(status.to_string()).block(block);
    frame.render_widget(para, area);
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &AppState) {
    let total = app.rows.len();
    let row = if total == 0 { 0 } else { app.selected_row + 1 };
    let cols = if app.keys_only { 4 } else { 5 };
    let col = if cols == 0 { 0 } else { app.selected_col + 1 };
    let legend = format!(
        "Tab: Focus | Query: Enter newline, Ctrl-Enter run current | Results: arrows, Shift-←/→ h-scroll, Mouse | F5/C-y copy cell | Click [Copy] | Ctrl-Q/Ctrl-C: Quit | Row {row}/{total} Col {col}/{cols}"
    );
    let block = Block::default().borders(Borders::ALL).title("Help");
    let para = Paragraph::new(legend).block(block);
    frame.render_widget(para, area);
}

fn draw_env_modal(frame: &mut Frame, area: Rect, app: &AppState) {
    // Split modal into left list and right editor
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .margin(1)
        .split(area);

    // Left: environments list
    let items: Vec<ListItem> = app.env_store.envs.iter().map(|e| ListItem::new(e.name.clone())).collect();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Environments"))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD | Modifier::REVERSED));
    let mut state = ratatui::widgets::ListState::default();
    if let Some(i) = app.env_store.selected { state.select(Some(i)); }
    frame.render_stateful_widget(list, cols[0], &mut state);

    // Right: fields editor stacked vertically
    let ed = app.env_editor.as_ref();
    let fields = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Min(5),
            Constraint::Min(5),
            Constraint::Length(3),
            Constraint::Min(5),
        ])
        .split(cols[1]);

    let name_val = ed.map(|e| e.name.clone()).unwrap_or_default();
    let host_val = ed.map(|e| e.host.clone()).unwrap_or_default();
    let privk_val = ed.map(|e| e.private_key_pem.clone()).unwrap_or_default();
    let pubk_val = ed.map(|e| e.public_key_pem.clone()).unwrap_or_default();
    let ca_pem_val = ed.map(|e| e.ssl_ca_pem.clone()).unwrap_or_default();

    let title_name = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Name)) { "Name [FOCUSED]" } else { "Name" };
    let title_host = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Host)) { "Host [FOCUSED]" } else { "Host" };
    let title_pk_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PrivateKey)) { "Private Key (PEM) [FOCUSED]" } else { "Private Key (PEM)" };
    let title_pk = format!("{}  [Copy]", title_pk_base);
    let title_cert_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PublicKey)) { "Public/Certificate (PEM) [FOCUSED]" } else { "Public/Certificate (PEM)" };
    let title_cert = format!("{}  [Copy]", title_cert_base);
    let title_ca_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Ca)) { "SSL CA (PEM) [FOCUSED]" } else { "SSL CA (PEM)" };
    let title_ca = format!("{}  [Copy]", title_ca_base);

    frame.render_widget(Paragraph::new(name_val.clone()).block(Block::default().borders(Borders::ALL).title(title_name)), fields[0]);
    frame.render_widget(Paragraph::new(host_val.clone()).block(Block::default().borders(Borders::ALL).title(title_host)), fields[1]);
    // Render multi-line fields with manual v/h scroll cropping (no wrapping)
    render_scrolled_field(frame, fields[2], &title_pk, &privk_val, app.env_editor.as_ref().map(|e| e.private_key_vscroll).unwrap_or(0), app.env_editor.as_ref().map(|e| e.private_key_hscroll).unwrap_or(0));
    render_scrolled_field(frame, fields[3], &title_cert, &pubk_val, app.env_editor.as_ref().map(|e| e.public_key_vscroll).unwrap_or(0), app.env_editor.as_ref().map(|e| e.public_key_hscroll).unwrap_or(0));
    render_scrolled_field(frame, fields[4], &title_ca, &ca_pem_val, app.env_editor.as_ref().map(|e| e.ca_vscroll).unwrap_or(0), app.env_editor.as_ref().map(|e| e.ca_hscroll).unwrap_or(0));
    if let Some(ed) = app.env_editor.as_ref() {
        let (x, y) = match ed.field_focus {
            super::app::EnvFieldFocus::Name => caret_pos_in(fields[0], &name_val, ed.name_cursor),
            super::app::EnvFieldFocus::Host => caret_pos_in(fields[1], &host_val, ed.host_cursor),
            super::app::EnvFieldFocus::PrivateKey => caret_pos_in(fields[2], &privk_val, ed.private_key_cursor),
            super::app::EnvFieldFocus::PublicKey => caret_pos_in(fields[3], &pubk_val, ed.public_key_cursor),
            super::app::EnvFieldFocus::Ca => caret_pos_in_h(fields[4], &ca_pem_val, ed.ssl_ca_cursor, ed.ca_vscroll, ed.ca_hscroll),
            super::app::EnvFieldFocus::Conn => (0,0),
            super::app::EnvFieldFocus::Buttons => (0,0),
        };
        if x > 0 || y > 0 { frame.set_cursor(x, y); }
    }
    let help = "F1 New | F2 Edit | F3 Delete | F4 Save | F5 Test | F6 Next | F7 Prev | F9 Mouse select on/off | Tab/Shift-Tab Move | Up/Down Select | Shift-←/→ H-scroll | Esc Close";
    frame.render_widget(Paragraph::new(help).block(Block::default().borders(Borders::ALL).title("Actions")), fields[5]);

    // Connection status/progress area (scrollable)
    let status_text = if app.env_test_in_progress {
        // Simple spinner based on time
        let ch = match (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() / 250) % 4 {
            0 => "⠋", 1 => "⠙", 2 => "⠸", _ => "⠴",
        };
        let msg = app.env_test_message.as_deref().unwrap_or("Testing connection...");
        format!("{} {}", ch, msg)
    } else {
        app.env_test_message.clone().unwrap_or_else(|| "Ready".to_string())
    };
    let conn_title = if matches!(app.env_editor.as_ref().map(|e| e.field_focus), Some(EnvFieldFocus::Conn)) { "Connection [FOCUSED]  [Copy/F9 Select]" } else { "Connection  [Copy/F9 Select]" };
    let conn_block = Block::default().borders(Borders::ALL).title(conn_title);
    let conn_para = Paragraph::new(status_text).block(conn_block).scroll((app.env_conn_vscroll, 0));
    frame.render_widget(conn_para, fields[6]);
}

fn caret_pos_in(area: Rect, text: &str, cursor: usize) -> (u16, u16) {
    let inner_x = area.x.saturating_add(1);
    let inner_y = area.y.saturating_add(1);
    let max_w = area.width.saturating_sub(2);
    let max_h = area.height.saturating_sub(2);
    let idx = cursor.min(text.len());
    let mut line = 0u16;
    let mut col = 0u16;
    let mut count = 0usize;
    for (li, l) in text.split('\n').enumerate() {
        let llen = l.len();
        if count + llen >= idx {
            line = li as u16;
            col = (idx - count) as u16;
            break;
        } else {
            count += llen + 1; // account for newline
        }
    }
    if count >= idx { line = 0; col = idx as u16; }
    line = line.min(max_h.saturating_sub(1));
    col = col.min(max_w.saturating_sub(1));
    (inner_x + col, inner_y + line)
}

fn caret_pos_in_h(area: Rect, text: &str, cursor: usize, vscroll: u16, hscroll: u16) -> (u16, u16) {
    let inner_x = area.x.saturating_add(1);
    let inner_y = area.y.saturating_add(1);
    let max_w = area.width.saturating_sub(2) as usize;
    let max_h = area.height.saturating_sub(2) as usize;
    let (line, col) = line_col_at(text, cursor);
    let vis_line = line.saturating_sub(vscroll as usize);
    let vis_col = col.saturating_sub(hscroll as usize);
    let y = inner_y + vis_line.min(max_h.saturating_sub(1)) as u16;
    let x = inner_x + vis_col.min(max_w.saturating_sub(1)) as u16;
    (x, y)
}

fn render_scrolled_field(frame: &mut Frame, area: Rect, title: &str, text: &str, vscroll: u16, hscroll: u16) {
    let block = Block::default().borders(Borders::ALL).title(title.to_string());
    frame.render_widget(block.clone(), area);
    let inner = Rect { x: area.x.saturating_add(1), y: area.y.saturating_add(1), width: area.width.saturating_sub(2), height: area.height.saturating_sub(2) };
    let lines: Vec<Line> = crop_lines(text, vscroll as usize, hscroll as usize, inner.width as usize, inner.height as usize);
    let para = Paragraph::new(Text::from(lines));
    frame.render_widget(para, inner);
}

fn crop_lines(text: &str, vscroll: usize, hscroll: usize, max_w: usize, max_h: usize) -> Vec<Line<'static>> {
    let mut out: Vec<Line> = Vec::new();
    let mut skipped = 0usize;
    for (i, line) in text.split('\n').enumerate() {
        if i < vscroll { skipped += 1; continue; }
        if out.len() >= max_h { break; }
        let slice = if line.len() > hscroll { &line[hscroll.min(line.len())..] } else { "" };
        let visible = if slice.len() > max_w { &slice[..max_w] } else { slice };
        out.push(Line::from(visible.to_string()));
    }
    out
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn caret_pos_multiline(area: Rect, text: &str, cursor: usize, vscroll: u16) -> Option<(u16, u16)> {
    // Compute (line, col) in logical lines and map to screen using vscroll and wrapping
    let inner_x = area.x.saturating_add(0);
    let inner_y = area.y.saturating_add(0);
    let max_w = area.width;
    if max_w == 0 { return None; }
    let (line, col) = line_col_at(text, cursor);
    // With wrapping, we need to account for col overflow into visual lines
    let wrap_w = max_w as usize;
    let add_lines = col / wrap_w; // number of extra wrapped lines within this logical line
    let vis_line = line + add_lines;
    let vis_col = (col % wrap_w) as u16;
    let y = inner_y + vis_line.saturating_sub(vscroll as usize) as u16;
    let x = inner_x + vis_col;
    Some((x, y))
}

fn line_col_at(text: &str, cursor: usize) -> (usize, usize) {
    let idx = cursor.min(text.len());
    let mut line = 0usize;
    let mut col = 0usize;
    let mut count = 0usize;
    for l in text.split('\n') {
        let llen = l.len();
        if count + llen >= idx {
            col = idx - count;
            break;
        } else {
            count += llen + 1;
            line += 1;
        }
    }
    (line, col)
}

fn find_query_range(s: &str, cursor: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let cur = cursor.min(bytes.len());
    // prev boundary
    let mut start = 0usize;
    let mut i = cur;
    while i > 0 {
        i -= 1;
        let b = bytes[i];
        if b == b';' { start = i + 1; break; }
        if b == b'\n' && i > 0 && bytes[i - 1] == b'\n' { start = i + 1; break; }
    }
    // next boundary
    let mut end = bytes.len();
    i = cur;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b';' { end = i + 1; break; }
        if b == b'\n' && i + 1 < bytes.len() && bytes[i + 1] == b'\n' { end = i; break; }
        i += 1;
    }
    (start, end)
}

fn intersects(a_start: usize, a_end: usize, b_start: usize, b_end: usize) -> bool {
    // [a_start, a_end) intersects [b_start, b_end)
    a_start < b_end && b_start < a_end
}

fn byte_index_to_line(line_starts: &[usize], byte_idx: usize) -> usize {
    // find greatest line_starts[i] <= byte_idx
    let mut lo = 0usize;
    let mut hi = line_starts.len();
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if line_starts[mid] <= byte_idx { lo = mid; } else { hi = mid; }
    }
    lo
}

fn highlight_sql_line(s: &str) -> Vec<Span<'static>> {
    // Very small SQL-ish highlighter
    let mut spans: Vec<Span> = Vec::new();
    let mut word = String::new();
    let mut in_string = false;
    for ch in s.chars() {
        match ch {
            '\'' | '"' => {
                if !word.is_empty() { push_word(&mut spans, &word); word.clear(); }
                in_string = !in_string;
                spans.push(Span::styled(ch.to_string(), Style::default().fg(Color::Yellow)));
            }
            c if c.is_alphanumeric() || c == '_' => { word.push(c); }
            _ => {
                if !word.is_empty() { push_word(&mut spans, &word); word.clear(); }
                let color = if in_string { Color::Yellow } else { Color::Gray };
                spans.push(Span::styled(ch.to_string(), Style::default().fg(color)));
            }
        }
    }
    if !word.is_empty() { push_word(&mut spans, &word); }
    spans
}

fn push_word(spans: &mut Vec<Span<'static>>, w: &str) {
    let kw = [
        "select","from","where","and","or","limit","order","by","asc","desc",
        // note: treat Kafka columns like key/value as identifiers, not keywords
        "timestamp","partition","offset",
    ];
    if kw.contains(&w.to_ascii_lowercase().as_str()) {
        spans.push(Span::styled(w.to_uppercase(), Style::default().fg(Color::LightCyan).add_modifier(Modifier::BOLD)));
    } else if w.chars().all(|c| c.is_ascii_digit()) {
        spans.push(Span::styled(w.to_string(), Style::default().fg(Color::Cyan)));
    } else {
        spans.push(Span::raw(w.to_string()));
    }
}

fn draw_results(frame: &mut Frame, area: Rect, app: &AppState) {
    // Split results into left (table) and right (json detail)
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(area);

    draw_table(frame, cols[0], app);
    draw_json_detail(frame, cols[1], app);
}

fn draw_table(frame: &mut Frame, area: Rect, app: &AppState) {
    let headers = if app.keys_only {
        vec![
            Cell::from(header_span("Partition")),
            Cell::from(header_span("Offset")),
            Cell::from(header_span("Timestamp")),
            Cell::from(header_span("Key")),
        ]
    } else {
        vec![
            Cell::from(header_span("Partition")),
            Cell::from(header_span("Offset")),
            Cell::from(header_span("Timestamp")),
            Cell::from(header_span("Key")),
            Cell::from(header_span("Value")),
        ]
    };

    // Create single-line rows with truncated previews; full JSON moves to right pane
    let rows: Vec<Row> = app
        .rows
        .iter()
        .enumerate()
        .map(|(i, env)| make_row(i, env, app))
        .collect();

    let constraints = if app.keys_only {
        vec![
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(26),
            Constraint::Percentage(100),
        ]
    } else {
        vec![
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(26),
            Constraint::Length(30),
            Constraint::Percentage(100),
        ]
    };

    let table = Table::new(rows, constraints)
        .header(Row::new(headers).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title(match app.focus { Focus::Results => "Results [FOCUSED]", _ => "Results" }))
        .highlight_style(Style::default())
        .column_spacing(1);

    let mut state = TableState::default();
    if !app.rows.is_empty() {
        state.select(Some(app.selected_row.min(app.rows.len() - 1)));
    }
    frame.render_stateful_widget(table, area, &mut state);

    // Vertical scrollbar for table (binds to selected_row)
    let total_rows = app.rows.len();
    if total_rows > 0 {
        let mut vs = ScrollbarState::new(total_rows).position(app.selected_row.min(total_rows - 1));
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, area, &mut vs);
    }

    // Horizontal scrollbar for table (approximate by preview width)
    let content_w_estimate = estimate_table_content_width(app);
    let visible_w = area.width.saturating_sub(2) as usize; // minus borders
    let h_content = content_w_estimate.saturating_sub(visible_w).saturating_add(1);
    if h_content > 1 {
        let mut hs = ScrollbarState::new(h_content).position(app.table_hscroll.min(h_content - 1));
        let hbar = Scrollbar::new(ScrollbarOrientation::HorizontalBottom);
        frame.render_stateful_widget(hbar, area, &mut hs);
    }
}

fn header_span(text: &str) -> Span<'_> {
    Span::styled(text, Style::default().add_modifier(Modifier::BOLD))
}

fn make_row(idx: usize, env: &MessageEnvelope, app: &AppState) -> Row<'static> {
    let ts = fmt_ts(env.timestamp_ms);
    let keys_only = app.keys_only;
    let selected = idx == app.selected_row;
    if keys_only {
        let row = Row::new(vec![
            style_cell(Cell::from(env.partition.to_string()), selected && app.selected_col == 0),
            style_cell(Cell::from(env.offset.to_string()), selected && app.selected_col == 1),
            style_cell(Cell::from(ts), selected && app.selected_col == 2),
            style_cell(Cell::from(env.key.clone()), selected && app.selected_col == 3),
        ])
        .height(1);
        row
    } else {
        // For the table, render a one-line minified preview of the value with hscroll
        let raw_value = env.value.as_deref().unwrap_or("null");
        let preview = json_preview_minified(raw_value);
        let hscroll = app.table_hscroll;
        let preview = apply_hscroll(&preview, hscroll);
        let row = Row::new(vec![
            style_cell(Cell::from(env.partition.to_string()), selected && app.selected_col == 0),
            style_cell(Cell::from(env.offset.to_string()), selected && app.selected_col == 1),
            style_cell(Cell::from(ts), selected && app.selected_col == 2),
            style_cell(Cell::from(env.key.clone()), selected && app.selected_col == 3),
            style_cell(Cell::from(preview), selected && app.selected_col == 4),
        ])
        .height(1);
        row
    }
}

fn style_cell(mut cell: Cell<'static>, selected: bool) -> Cell<'static> {
    if selected {
        cell = cell.style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));
    }
    cell
}

fn fmt_ts(ms: i64) -> String {
    if ms <= 0 {
        return "0".to_string();
    }
    // Keep short human readable format
    let secs = ms / 1000;
    let tm = time::OffsetDateTime::from_unix_timestamp(secs as i64).unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH);
    tm.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| ms.to_string())
}

#[allow(dead_code)]
fn make_json_cell_and_height(s: &str) -> (Text<'static>, u16) {
    // Small highlighter for JSON-ish strings.
    // If it isn't JSON, return plain text with height 1.
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => {
            let lines = json_to_highlighted_lines(&v);
            let h = lines.len().max(1) as u16;
            (Text::from(lines), h)
        }
        Err(_) => (Text::from(s.to_string()), 1),
    }
}

fn json_to_highlighted_lines(v: &serde_json::Value) -> Vec<Line<'static>> {
    let mut lines: Vec<Line> = Vec::new();
    fn push_indent(buf: &mut std::string::String, n: usize) { for _ in 0..n { buf.push(' '); } }
    fn walk(v: &serde_json::Value, depth: usize, out: &mut Vec<Line>) {
        match v {
            serde_json::Value::Null => out.push(Line::from(vec![Span::styled("null", Style::default().fg(Color::DarkGray))])),
            serde_json::Value::Bool(b) => out.push(Line::from(vec![Span::styled(b.to_string(), Style::default().fg(Color::Magenta))])),
            serde_json::Value::Number(n) => out.push(Line::from(vec![Span::styled(n.to_string(), Style::default().fg(Color::Cyan))])),
            serde_json::Value::String(s) => out.push(Line::from(vec![Span::styled(format!("\"{}\"", s), Style::default().fg(Color::Yellow))])),
            serde_json::Value::Array(arr) => {
                out.push(Line::from(vec![Span::styled("[", Style::default().fg(Color::Gray))]));
                for (i, item) in arr.iter().enumerate() {
                    let mut buf = std::string::String::new();
                    push_indent(&mut buf, (depth + 1) * 2);
                    let mut inner: Vec<Span> = vec![Span::raw(buf)];
                    let mut tmp: Vec<Line> = Vec::new();
                    walk(item, depth + 1, &mut tmp);
                    if let Some(mut line) = tmp.into_iter().next() {
                        inner.append(&mut line.spans);
                    }
                    if i + 1 != arr.len() { inner.push(Span::styled(",", Style::default().fg(Color::Gray))); }
                    out.push(Line::from(inner));
                }
                let mut buf = std::string::String::new(); push_indent(&mut buf, depth * 2);
                out.push(Line::from(vec![Span::raw(buf), Span::styled("]", Style::default().fg(Color::Gray))]));
            }
            serde_json::Value::Object(map) => {
                out.push(Line::from(vec![Span::styled("{", Style::default().fg(Color::Gray))]));
                let len = map.len();
                for (i, (k, val)) in map.iter().enumerate() {
                    let mut buf = std::string::String::new(); push_indent(&mut buf, (depth + 1) * 2);
                    let mut spans = vec![
                        Span::raw(buf),
                        Span::styled(format!("\"{}\"", k), Style::default().fg(Color::Green)),
                        Span::styled(": ", Style::default().fg(Color::Gray)),
                    ];
                    let mut tmp: Vec<Line> = Vec::new(); walk(val, depth + 1, &mut tmp);
                    if let Some(mut first) = tmp.into_iter().next() {
                        spans.append(&mut first.spans);
                    }
                    if i + 1 != len { spans.push(Span::styled(",", Style::default().fg(Color::Gray))); }
                    out.push(Line::from(spans));
                }
                let mut buf = std::string::String::new(); push_indent(&mut buf, depth * 2);
                out.push(Line::from(vec![Span::raw(buf), Span::styled("}", Style::default().fg(Color::Gray))]));
            }
        }
    }
    walk(v, 0, &mut lines);
    lines
}

fn json_preview_minified(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| s.to_string()),
        Err(_) => {
            // Use the first line only for non-JSON
            s.lines().next().unwrap_or("").to_string()
        }
    }
}

fn apply_hscroll(s: &str, offset: usize) -> String {
    if offset == 0 { return s.to_string(); }
    s.chars().skip(offset).collect()
}

fn estimate_table_content_width(app: &AppState) -> usize {
    // Approximate widths of fixed columns + spacing + average key/value preview length
    let fixed: usize = 10 + 1 + 12 + 1 + 26 + 1 + 30 + 1; // partition+sp+offset+sp+ts+sp+key+sp
    if app.keys_only {
        // no value column
        return fixed.saturating_sub(1); // last spacing unnecessary
    }
    let mut max_preview = 0usize;
    for env in &app.rows {
        let raw = env.value.as_deref().unwrap_or("null");
        let p = json_preview_minified(raw);
        max_preview = max_preview.max(p.chars().count());
    }
    fixed + max_preview
}

fn draw_json_detail(frame: &mut Frame, area: Rect, app: &AppState) {
    // Show the currently selected cell content with wrapping and vertical scroll
    let (title_suffix, raw) = selected_cell_for_detail(app);
    let title = format!("Details ({})", title_suffix);
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    // Build Text using existing highlighter
    let text: Text = match raw.as_deref() {
        Some(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => Text::from(json_to_highlighted_lines(&v)),
            Err(_) => Text::from(s.to_string()),
        },
        None => Text::from(""),
    };

    let para = Paragraph::new(text)
        .wrap(Wrap { trim: false })
        .scroll((app.json_vscroll, 0));
    frame.render_widget(para, inner_area);

    // Draw Copy button at top-right of inner area
    let btn_w = COPY_BTN_LABEL.chars().count() as u16;
    if inner_area.width > btn_w {
        let btn_rect = Rect { x: inner_area.x + inner_area.width - btn_w, y: inner_area.y, width: btn_w, height: 1 };
        let style = if app.copy_btn_pressed {
            // pressed look
            Style::default().fg(Color::Black).bg(Color::LightYellow)
        } else {
            // raised look
            Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD)
        };
        let btn = Paragraph::new(COPY_BTN_LABEL).style(style);
        frame.render_widget(btn, btn_rect);
    }

    // Vertical scrollbar for JSON
    // Estimate content length by lines (simple; Paragraph wrap may change it, but this is sufficient)
    let content_len = match raw.as_deref() {
        Some(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => json_to_highlighted_lines(&v).len(),
            Err(_) => s.lines().count(),
        },
        None => 0,
    };
    if content_len > 0 {
        let mut vs = ScrollbarState::new(content_len).position(app.json_vscroll.min((content_len.saturating_sub(1)) as u16) as usize);
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, area, &mut vs);
    }
}

fn selected_cell_for_detail(app: &AppState) -> (String, Option<String>) {
    if app.rows.is_empty() {
        return ("none".to_string(), None);
    }
    let idx = app.selected_row.min(app.rows.len() - 1);
    let env = &app.rows[idx];
    let (title, s) = if app.keys_only {
        match app.selected_col.min(3) {
            0 => ("Partition", env.partition.to_string()),
            1 => ("Offset", env.offset.to_string()),
            2 => ("Timestamp", fmt_ts(env.timestamp_ms)),
            3 => ("Key", env.key.clone()),
            _ => ("", String::new()),
        }
    } else {
        match app.selected_col.min(4) {
            0 => ("Partition", env.partition.to_string()),
            1 => ("Offset", env.offset.to_string()),
            2 => ("Timestamp", fmt_ts(env.timestamp_ms)),
            3 => ("Key", env.key.clone()),
            4 => ("Value", env.value.as_deref().unwrap_or("null").to_string()),
            _ => ("", String::new()),
        }
    };
    (title.to_string(), Some(s))
}
