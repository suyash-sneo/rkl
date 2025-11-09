use crate::models::MessageEnvelope;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};

use super::app::{AppState, Focus};

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

    draw_host(frame, chunks[0], &app.host, app.focus == Focus::Host);
    draw_input(frame, chunks[1], &app.input, app.focus == Focus::Query);
    draw_status(frame, chunks[2], &app.status);
    draw_table(frame, chunks[3], app);
    draw_footer(frame, chunks[4], app);
}

fn draw_input(frame: &mut Frame, area: Rect, input: &str, focused: bool) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(if focused { "Query (Enter to run) [FOCUSED]" } else { "Query (Enter to run)" });
    let para = Paragraph::new(input.to_string()).block(block);
    frame.render_widget(para, area);
}

fn draw_host(frame: &mut Frame, area: Rect, host: &str, focused: bool) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(if focused { "Host (Kafka broker) [FOCUSED]" } else { "Host (Kafka broker)" });
    let para = Paragraph::new(host.to_string()).block(block);
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
        "Tab: Focus | Enter: Run | Results: arrows/hjkl to navigate, y: copy | q/Ctrl-C: Quit | Row {row}/{total} Col {col}/{cols}"
    );
    let block = Block::default().borders(Borders::ALL).title("Help");
    let para = Paragraph::new(legend).block(block);
    frame.render_widget(para, area);
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
