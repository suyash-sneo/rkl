use crate::models::MessageEnvelope;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, List, ListItem, Clear};

use super::app::{AppState, Focus, EnvFieldFocus};

pub fn draw(frame: &mut Frame, app: &AppState) {
    let size = frame.size();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),   // host input
            Constraint::Length(3),   // query input
            Constraint::Length(1),   // status
            Constraint::Fill(1),     // table grows to fill available space
            Constraint::Length(3),   // footer (needs 3: top+content+bottom)
        ])
        .split(size);

    draw_env_bar(frame, chunks[0], app);
    draw_input(frame, chunks[1], app);
    draw_status(frame, chunks[2], &app.status);
    draw_table(frame, chunks[3], app);
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
    let block = Block::default()
        .borders(Borders::ALL)
        .title(if focused { "Query (Enter to run) [FOCUSED]" } else { "Query (Enter to run)" });
    let para = Paragraph::new(app.input.clone()).block(block);
    frame.render_widget(para, area);
    if focused {
        let inner_x = area.x.saturating_add(1);
        let inner_y = area.y.saturating_add(1);
        let max_w = area.width.saturating_sub(2);
        let col = (app.input_cursor as u16).min(max_w);
        frame.set_cursor(inner_x + col, inner_y);
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
        "Tab: Focus | Enter: Run | Results: arrows navigate, F5/C-y copy | Ctrl-Q/Ctrl-C: Quit | Row {row}/{total} Col {col}/{cols}"
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
        ])
        .split(cols[1]);

    let name_val = ed.map(|e| e.name.clone()).unwrap_or_default();
    let host_val = ed.map(|e| e.host.clone()).unwrap_or_default();
    let privk_val = ed.map(|e| e.private_key_pem.clone()).unwrap_or_default();
    let pubk_val = ed.map(|e| e.public_key_pem.clone()).unwrap_or_default();
    let ca_val = ed.map(|e| e.ssl_ca_pem.clone()).unwrap_or_default();

    let title_name = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Name)) { "Name [FOCUSED]" } else { "Name" };
    let title_host = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Host)) { "Host [FOCUSED]" } else { "Host" };
    let title_pk = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PrivateKey)) { "Private Key (PEM) [FOCUSED]" } else { "Private Key (PEM)" };
    let title_cert = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PublicKey)) { "Public/Certificate (PEM) [FOCUSED]" } else { "Public/Certificate (PEM)" };
    let title_ca = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Ca)) { "SSL CA (PEM) [FOCUSED]" } else { "SSL CA (PEM)" };

    frame.render_widget(Paragraph::new(name_val.clone()).block(Block::default().borders(Borders::ALL).title(title_name)), fields[0]);
    frame.render_widget(Paragraph::new(host_val.clone()).block(Block::default().borders(Borders::ALL).title(title_host)), fields[1]);
    frame.render_widget(Paragraph::new(privk_val.clone()).block(Block::default().borders(Borders::ALL).title(title_pk)), fields[2]);
    frame.render_widget(Paragraph::new(pubk_val.clone()).block(Block::default().borders(Borders::ALL).title(title_cert)), fields[3]);
    frame.render_widget(Paragraph::new(ca_val.clone()).block(Block::default().borders(Borders::ALL).title(title_ca)), fields[4]);
    if let Some(ed) = app.env_editor.as_ref() {
        let (x, y) = match ed.field_focus {
            super::app::EnvFieldFocus::Name => caret_pos_in(fields[0], &name_val, ed.name_cursor),
            super::app::EnvFieldFocus::Host => caret_pos_in(fields[1], &host_val, ed.host_cursor),
            super::app::EnvFieldFocus::PrivateKey => caret_pos_in(fields[2], &privk_val, ed.private_key_cursor),
            super::app::EnvFieldFocus::PublicKey => caret_pos_in(fields[3], &pubk_val, ed.public_key_cursor),
            super::app::EnvFieldFocus::Ca => caret_pos_in(fields[4], &ca_val, ed.ssl_ca_cursor),
            super::app::EnvFieldFocus::Buttons => (0,0),
        };
        if x > 0 || y > 0 { frame.set_cursor(x, y); }
    }
    let help = "Enter: select/save | Tab/Shift-Tab: move | Up/Down: select env | n: new | d: delete | s: save | t: test | Esc: close";
    frame.render_widget(Paragraph::new(help).block(Block::default().borders(Borders::ALL).title("Actions")), fields[5]);
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
        ]);
        row
    } else {
        let value_text = env.value.as_deref().unwrap_or("null");
        let (value, height) = make_json_cell_and_height(value_text);
        let mut row = Row::new(vec![
            style_cell(Cell::from(env.partition.to_string()), selected && app.selected_col == 0),
            style_cell(Cell::from(env.offset.to_string()), selected && app.selected_col == 1),
            style_cell(Cell::from(ts), selected && app.selected_col == 2),
            style_cell(Cell::from(env.key.clone()), selected && app.selected_col == 3),
            style_cell(Cell::from(value), selected && app.selected_col == 4),
        ]);
        row = row.height(height);
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
