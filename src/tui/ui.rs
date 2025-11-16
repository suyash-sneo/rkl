use crate::models::MessageEnvelope;
use crate::query::SelectItem;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Table, TableState, Wrap,
};

use super::app::{AppState, EnvFieldFocus, Focus, ResultsMode, Screen};
use super::query_bounds::find_query_range;

pub(super) const COPY_BTN_LABEL: &str = "[ Copy ]";

pub fn draw(frame: &mut Frame, app: &AppState) {
    let size = frame.area();
    match app.screen {
        Screen::Home => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),  // env bar
                    Constraint::Length(10), // editor + status
                    Constraint::Fill(1),    // results
                    Constraint::Length(3),  // footer
                ])
                .split(size);

            draw_env_bar(frame, chunks[0], app);
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
                .split(chunks[1]);
            draw_input(frame, cols[0], app);
            draw_status_panel(frame, cols[1], app);
            draw_results(frame, chunks[2], app);
            draw_footer(frame, chunks[3], app);
        }
        Screen::Envs => {
            // Full-screen environments UI
            let block = Block::default()
                .title("Environments (F8 Home  F2 Envs  F12 Info  F10 Help)")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
            let area = block.inner(size);
            frame.render_widget(block, size);
            draw_env_modal(frame, area, app);
        }
        Screen::Info => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Fill(1),
                    Constraint::Length(3),
                ])
                .split(size);
            draw_env_bar(frame, chunks[0], app);
            draw_topics(frame, chunks[1], app);
            draw_footer(frame, chunks[2], app);
        }
    }

    if app.show_help {
        draw_help_overlay(frame, size, app);
    }
}

fn draw_input(frame: &mut Frame, area: Rect, app: &AppState) {
    let focused = app.focus == Focus::Query;
    let title = "Query (Ctrl-Enter runs current SELECT; ';' ends)";
    let border_style = if focused {
        Style::default().fg(Color::LightCyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(border_style);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Split inner into gutter and content. Gutter width is dynamic to always
    // preserve a visible gap between line numbers and content, even when
    // markers like the last-run pointer are shown.
    let text = &app.input;
    let lines: Vec<&str> = text.split('\n').collect();
    let max_lineno_digits = lines.len().max(1).to_string().len() as u16;
    let marker_max = 2u16; // e.g., "➤▶" can take two cells
    let gap = 1u16; // fixed one-space gap to content
    let gutter_width: u16 = (marker_max + 1 + max_lineno_digits + gap).max(6);
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(gutter_width), Constraint::Min(1)])
        .split(inner);
    let gutter = cols[0];
    let content = cols[1];

    // Compute line starts to style per-line highlights, and find query ranges
    let line_starts: Vec<usize> = {
        let mut v = Vec::with_capacity(lines.len());
        let mut acc = 0usize;
        for (i, l) in lines.iter().enumerate() {
            v.push(acc);
            acc += l.len();
            if i + 1 < lines.len() {
                acc += 1;
            } // newline
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
        let is_last = last_range
            .map(|(ls, le)| intersects(lstart, lend, ls, le))
            .unwrap_or(false);
        let marker = if is_cur && Some(i) == last_first_line {
            "➤▶"
        } else if is_cur {
            "➤"
        } else if Some(i) == last_first_line || is_last {
            "▶"
        } else {
            " "
        };
        // Align line numbers based on max digits to keep layout stable
        let no = format!("{:>width$}", i + 1, width = max_lineno_digits as usize);
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
        if let Some((cx, cy)) =
            caret_pos_multiline(content, text, app.input_cursor, app.input_vscroll)
        {
            frame.set_cursor_position(Position::new(cx, cy));
        }
    }

    if let Some(ac) = app.autocomplete.as_ref() {
        if ac.active && content.width > 0 && content.height > 2 {
            let max_visible = 6usize;
            let base_width = content.width.min(40);
            let available_height = content.height.saturating_sub(2);
            if base_width > 0 && available_height > 0 {
                let slots = max_visible.min(available_height as usize).max(1);
                let popup_width = base_width.max(10).min(content.width);
                let total = ac.suggestions.len();
                let window_len = slots.min(total.max(1));
                let (mut items, selection): (Vec<ListItem>, Option<usize>) = if total == 0 {
                    (
                        vec![ListItem::new(if app.topics.is_empty() {
                            "Loading topics..."
                        } else {
                            "No matches"
                        })],
                        None,
                    )
                } else {
                    let start = if total <= window_len {
                        0
                    } else {
                        ac.selected
                            .saturating_sub(window_len.saturating_sub(1))
                            .min(total - window_len)
                    };
                    let end = (start + window_len).min(total);
                    let sel = Some(ac.selected.saturating_sub(start));
                    (
                        ac.suggestions[start..end]
                            .iter()
                            .map(|s| ListItem::new(s.clone()))
                            .collect(),
                        sel,
                    )
                };
                while items.len() < slots {
                    items.push(ListItem::new(""));
                }
                let popup_height = slots as u16 + 2;
                let mut popup = Rect {
                    x: content.x.saturating_add(1),
                    y: content.y.saturating_add(1),
                    width: popup_width,
                    height: popup_height.min(content.height),
                };
                let content_bottom = content.y.saturating_add(content.height);
                if popup.y + popup.height > content_bottom {
                    let overflow = popup.y + popup.height - content_bottom;
                    popup.y = popup.y.saturating_sub(overflow);
                }
                frame.render_widget(Clear, popup);
                let title = if ac.filter.is_empty() {
                    "Topic Suggestions".to_string()
                } else {
                    format!("Topic Suggestions [{}]", ac.filter)
                };
                let list = List::new(items)
                    .block(Block::default().borders(Borders::ALL).title(title))
                    .highlight_style(
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD | Modifier::REVERSED),
                    );
                let mut state = ListState::default();
                state.select(selection);
                frame.render_stateful_widget(list, popup, &mut state);

                if total > slots && popup.height > 2 {
                    let mut vs = ScrollbarState::new(total)
                        .position(ac.selected.min(total.saturating_sub(1)));
                    let bar_height = popup.height.saturating_sub(2);
                    if bar_height > 0 {
                        let bar_area = Rect {
                            x: popup.x + popup.width - 1,
                            y: popup.y + 1,
                            width: 1,
                            height: bar_height,
                        };
                        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
                        frame.render_stateful_widget(bar, bar_area, &mut vs);
                    }
                }
            }
        }
    }
}

fn draw_env_bar(frame: &mut Frame, area: Rect, app: &AppState) {
    let title = "Environment (F2 to manage)";
    let border_style = if app.focus == Focus::Host {
        Style::default().fg(Color::LightCyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(border_style);
    let name = app
        .selected_env()
        .map(|e| e.name.clone())
        .unwrap_or_else(|| "(none)".to_string());
    let host = app
        .selected_env()
        .map(|e| e.host.clone())
        .unwrap_or_default();
    let content = format!("{name}  —  host: {host}");
    let para = Paragraph::new(content).block(block);
    frame.render_widget(para, area);
}

fn draw_status_panel(frame: &mut Frame, area: Rect, app: &AppState) {
    let block = Block::default().borders(Borders::ALL).title("Status");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let text = if app.status_buffer.is_empty() {
        app.status.clone()
    } else {
        app.status_buffer.clone()
    };
    let para = Paragraph::new(text.clone())
        .wrap(Wrap { trim: false })
        .scroll((app.status_vscroll, 0));
    frame.render_widget(para, inner);

    // Draw Copy button at top-right of inner area
    let btn_w = COPY_BTN_LABEL.chars().count() as u16;
    if inner.width >= btn_w {
        let btn_x = inner.x + inner.width - btn_w;
        let btn_rect = Rect {
            x: btn_x,
            y: inner.y,
            width: btn_w,
            height: 1,
        };
        let style = if app.copy_btn_pressed {
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Yellow)
        };
        let btn = Paragraph::new(COPY_BTN_LABEL).style(style);
        frame.render_widget(btn, btn_rect);
    }

    // Scrollbar
    let total_lines = text.lines().count().max(1);
    let vis = inner.height as usize;
    if total_lines > vis {
        let mut vs = ScrollbarState::new(total_lines).position(app.status_vscroll as usize);
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, inner, &mut vs);
    }
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &AppState) {
    let legend = footer_legend(app);
    let block = Block::default().borders(Borders::ALL).title("Help");
    let para = Paragraph::new(legend).block(block);
    frame.render_widget(para, area);
}

fn footer_legend(app: &AppState) -> String {
    match app.screen {
        Screen::Home => match app.focus {
            Focus::Query => "Tab focus | Query: Enter newline, Ctrl-Enter run, Right accept autocomplete, Ctrl-N/P navigate autocomplete | F10 Help | Ctrl-Q/C quit".to_string(),
            Focus::Results => "Tab focus | Results: arrows select, Shift-←/→ h-scroll, F5 copy value, F7 copy status | F10 Help | Ctrl-Q/C quit".to_string(),
            Focus::Host => "Tab focus | Host: Enter open envs, F2 Envs | F10 Help | Ctrl-Q/C quit".to_string(),
        },
        Screen::Envs => "F4 Save, F5 Test, Tab move, Up/Down select, Esc Close | F10 Help".to_string(),
        Screen::Info => "F6 Refresh, F8 Home | F10 Help | Ctrl-Q/C quit".to_string(),
    }
}

fn draw_env_modal(frame: &mut Frame, area: Rect, app: &AppState) {
    // Split modal into left list and right editor
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .margin(1)
        .split(area);

    // Left: environments list
    let items: Vec<ListItem> = app
        .env_store
        .envs
        .iter()
        .map(|e| ListItem::new(e.name.clone()))
        .collect();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Environments"))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED),
        );
    let mut state = ratatui::widgets::ListState::default();
    if let Some(i) = app.env_store.selected {
        state.select(Some(i));
    }
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
    // Values are drawn via TextAreas; no pre-rendered strings needed here.

    let title_name_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Name)) {
        "Name [FOCUSED]"
    } else {
        "Name"
    };
    let title_name = format!("{title_name_base}  [Copy] [Paste]");
    let title_host_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Host)) {
        "Host [FOCUSED]"
    } else {
        "Host"
    };
    let title_host = format!("{title_host_base}  [Copy] [Paste]");
    let title_pk_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PrivateKey)) {
        "Private Key (PEM) [FOCUSED]"
    } else {
        "Private Key (PEM)"
    };
    let title_pk = format!("{}  [Copy] [Paste] [Clear]", title_pk_base);
    let title_cert_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::PublicKey)) {
        "Public/Certificate (PEM) [FOCUSED]"
    } else {
        "Public/Certificate (PEM)"
    };
    let title_cert = format!("{}  [Copy] [Paste] [Clear]", title_cert_base);
    let title_ca_base = if matches!(ed.map(|e| e.field_focus), Some(EnvFieldFocus::Ca)) {
        "SSL CA (PEM) [FOCUSED]"
    } else {
        "SSL CA (PEM)"
    };
    let title_ca = format!("{}  [Copy] [Paste] [Clear]", title_ca_base);

    frame.render_widget(
        Paragraph::new(name_val.clone())
            .block(Block::default().borders(Borders::ALL).title(title_name)),
        fields[0],
    );
    frame.render_widget(
        Paragraph::new(host_val.clone())
            .block(Block::default().borders(Borders::ALL).title(title_host)),
        fields[1],
    );
    // Render multi-line fields using tui-textarea
    if let Some(edm) = app.env_editor.as_ref() {
        // Draw outer blocks for titles and copy affordance
        let block_pk = Block::default()
            .borders(Borders::ALL)
            .title(title_pk.clone());
        let block_pub = Block::default()
            .borders(Borders::ALL)
            .title(title_cert.clone());
        let block_ca = Block::default()
            .borders(Borders::ALL)
            .title(title_ca.clone());
        let inner_pk = block_pk.inner(fields[2]);
        let inner_pub = block_pub.inner(fields[3]);
        let inner_ca = block_ca.inner(fields[4]);
        frame.render_widget(block_pk, fields[2]);
        frame.render_widget(block_pub, fields[3]);
        frame.render_widget(block_ca, fields[4]);
        frame.render_widget(&edm.ta_private, inner_pk);
        frame.render_widget(&edm.ta_public, inner_pub);
        frame.render_widget(&edm.ta_ca, inner_ca);
    }
    if let Some(ed) = app.env_editor.as_ref() {
        let (x, y) = match ed.field_focus {
            super::app::EnvFieldFocus::Name => caret_pos_in(fields[0], &name_val, ed.name_cursor),
            super::app::EnvFieldFocus::Host => caret_pos_in(fields[1], &host_val, ed.host_cursor),
            // TextArea draws its own cursor; we skip frame.set_cursor for these
            super::app::EnvFieldFocus::PrivateKey => (0, 0),
            super::app::EnvFieldFocus::PublicKey => (0, 0),
            super::app::EnvFieldFocus::Ca => (0, 0),
            super::app::EnvFieldFocus::Conn => (0, 0),
            super::app::EnvFieldFocus::Buttons => (0, 0),
        };
        if x > 0 || y > 0 {
            frame.set_cursor_position(Position::new(x, y));
        }
    }
    let help = "F1 New | F2 Edit | F3 Delete | F4 Save | F5 Test | F6 Next | F7 Prev | F9 Mouse select on/off | Tab/Shift-Tab Move | Up/Down Select | Shift-←/→ H-scroll | Esc Close";
    frame.render_widget(
        Paragraph::new(help).block(Block::default().borders(Borders::ALL).title("Actions")),
        fields[5],
    );

    // Connection status/progress area (scrollable)
    let status_text = if app.env_test_in_progress {
        // Simple spinner based on time
        let ch = match (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            / 250)
            % 4
        {
            0 => "⠋",
            1 => "⠙",
            2 => "⠸",
            _ => "⠴",
        };
        let msg = app
            .env_test_message
            .as_deref()
            .unwrap_or("Testing connection...");
        format!("{} {}", ch, msg)
    } else {
        app.env_test_message
            .clone()
            .unwrap_or_else(|| "Ready".to_string())
    };
    let conn_title = if matches!(
        app.env_editor.as_ref().map(|e| e.field_focus),
        Some(EnvFieldFocus::Conn)
    ) {
        "Connection [FOCUSED]  [Copy] [Paste/F9 Select]"
    } else {
        "Connection  [Copy] [Paste/F9 Select]"
    };
    let conn_block = Block::default().borders(Borders::ALL).title(conn_title);
    let conn_para = Paragraph::new(status_text)
        .block(conn_block)
        .scroll((app.env_conn_vscroll, 0));
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
    if count >= idx {
        line = 0;
        col = idx as u16;
    }
    line = line.min(max_h.saturating_sub(1));
    col = col.min(max_w.saturating_sub(1));
    (inner_x + col, inner_y + line)
}

// Removed unused manual scrolled-field helpers in favor of tui-textarea

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
    if max_w == 0 {
        return None;
    }
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
        if line_starts[mid] <= byte_idx {
            lo = mid;
        } else {
            hi = mid;
        }
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
                if !word.is_empty() {
                    push_word(&mut spans, &word);
                    word.clear();
                }
                in_string = !in_string;
                spans.push(Span::styled(
                    ch.to_string(),
                    Style::default().fg(Color::Yellow),
                ));
            }
            c if c.is_alphanumeric() || c == '_' => {
                word.push(c);
            }
            _ => {
                if !word.is_empty() {
                    push_word(&mut spans, &word);
                    word.clear();
                }
                let color = if in_string {
                    Color::Yellow
                } else {
                    Color::Gray
                };
                spans.push(Span::styled(ch.to_string(), Style::default().fg(color)));
            }
        }
    }
    if !word.is_empty() {
        push_word(&mut spans, &word);
    }
    spans
}

fn push_word(spans: &mut Vec<Span<'static>>, w: &str) {
    let kw = [
        "select",
        "list",
        "from",
        "where",
        "and",
        "or",
        "limit",
        "order",
        "by",
        "asc",
        "desc",
        "contains",
        // note: treat Kafka columns like key/value as identifiers, not keywords
        "timestamp",
        "partition",
        "offset",
    ];
    if kw.contains(&w.to_ascii_lowercase().as_str()) {
        spans.push(Span::styled(
            w.to_uppercase(),
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        ));
    } else if w.chars().all(|c| c.is_ascii_digit()) {
        spans.push(Span::styled(
            w.to_string(),
            Style::default().fg(Color::Cyan),
        ));
    } else {
        spans.push(Span::raw(w.to_string()));
    }
}

fn draw_results(frame: &mut Frame, area: Rect, app: &AppState) {
    match app.results_mode {
        ResultsMode::Messages => {
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
                .split(area);
            draw_table(frame, cols[0], app);
            draw_json_detail(frame, cols[1], app);
        }
        ResultsMode::TopicList => {
            draw_topics_results_table(frame, area, app);
        }
    }
}

fn draw_topics_results_table(frame: &mut Frame, area: Rect, app: &AppState) {
    let headers = vec![
        Cell::from(header_span("Topic")),
        Cell::from(header_span("Partitions")),
    ];
    let rows: Vec<Row> = if app.topics_with_partitions.is_empty() {
        vec![Row::new(vec![Cell::from("No topics"), Cell::from("")])]
    } else {
        app.topics_with_partitions
            .iter()
            .map(|(topic, parts)| {
                Row::new(vec![
                    Cell::from(topic.clone()),
                    Cell::from(parts.to_string()),
                ])
            })
            .collect()
    };
    let border_style = if app.focus == Focus::Results {
        Style::default().fg(Color::LightCyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let table = Table::new(
        rows,
        [Constraint::Percentage(70), Constraint::Percentage(30)],
    )
    .header(Row::new(headers).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Topics")
            .border_style(border_style),
    )
    .row_highlight_style(Style::default())
    .column_spacing(2);
    let mut state = TableState::default();
    if !app.topics_with_partitions.is_empty() {
        state.select(Some(
            app.selected_row
                .min(app.topics_with_partitions.len().saturating_sub(1)),
        ));
    }
    frame.render_stateful_widget(table, area, &mut state);

    let total = app.topics_with_partitions.len();
    if total > 0 {
        let mut vs =
            ScrollbarState::new(total).position(app.selected_row.min(total.saturating_sub(1)));
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, area, &mut vs);
    }
}

fn draw_topics(frame: &mut Frame, area: Rect, app: &AppState) {
    let items: Vec<ListItem> = if app.topics.is_empty() {
        vec![ListItem::new("No topics loaded. Press F6 to refresh.")]
    } else {
        app.topics
            .iter()
            .map(|t| ListItem::new(t.clone()))
            .collect()
    };
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("Topics"));
    frame.render_widget(list, area);
}

fn draw_help_overlay(frame: &mut Frame, area: Rect, app: &AppState) {
    let popup = centered_rect(70, 70, area);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Help")
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let lines = build_help_lines();
    let total_lines = lines.len();
    let visible = inner.height.max(1) as usize;
    let max_scroll = total_lines.saturating_sub(visible);
    let requested = app.help_vscroll as usize;
    let scroll = requested.min(max_scroll);
    let scroll_u16 = scroll.min(u16::MAX as usize) as u16;

    let para = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .scroll((scroll_u16, 0));
    frame.render_widget(para, inner);

    if total_lines > visible {
        let mut vs = ScrollbarState::new(total_lines).position(scroll);
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, inner, &mut vs);
    }
}

pub fn help_content_line_count() -> usize {
    build_help_lines().len()
}

fn build_help_lines() -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    lines.push(heading_line("Global"));
    lines.push(Line::from("- F8 Home, F2 Envs, F12 Info, F10 Help"));
    lines.push(Line::from("- Ctrl-Q/C quit"));
    lines.push(Line::from(""));

    lines.push(heading_line("Home - Host bar"));
    lines.push(Line::from("- Tab focus; Enter open envs; F2 Envs for full screen"));
    lines.push(Line::from(""));

    lines.push(heading_line("Home - Query"));
    lines.push(Line::from("- Ctrl-Enter run current SELECT; Enter newline"));
    lines.push(Line::from(
        "- Right accept autocomplete; Ctrl-N/P navigate autocomplete",
    ));
    lines.push(Line::from(
        "- Ctrl/Alt+Left/Right move word; Ctrl/Alt+Backspace/Delete delete word",
    ));
    lines.push(Line::from(
        "- Ctrl+Home/End jump buffer; PageUp/PageDown scroll editor",
    ));
    lines.push(Line::from(""));

    lines.push(heading_line("Home - Results"));
    lines.push(Line::from(
        "- Arrows move selection; PageUp/PageDown step; Home/End jump",
    ));
    lines.push(Line::from(
        "- Shift-Left/Right horizontal scroll; F5 copy value; F7 copy status",
    ));
    lines.push(Line::from("- Mouse wheel scroll supported"));
    lines.push(Line::from(""));

    lines.push(heading_line("Environments"));
    lines.push(Line::from("- F1 New, F2 Edit, F3 Delete"));
    lines.push(Line::from("- F4 Save, F5 Test, Tab/Shift-Tab move fields"));
    lines.push(Line::from("- Up/Down select; F9 toggle mouse select; Esc close"));
    lines.push(Line::from("- Text areas accept typing and paste"));
    lines.push(Line::from(""));

    lines.push(heading_line("Info screen"));
    lines.push(Line::from("- F6 Refresh topics"));
    lines.push(Line::from(""));

    lines.push(heading_line("Query syntax"));
    lines.push(Line::from(
        "- SELECT columns FROM topic [WHERE expr] [ORDER BY timestamp ASC|DESC] [LIMIT n]",
    ));
    lines.push(Line::from("- JSON path via value->field->subfield"));
    lines.push(Line::from("- Operators: =, !=, <>, CONTAINS"));
    lines.push(Line::from(""));

    lines.push(heading_line("Examples"));
    lines.push(Line::from("  SELECT key, value FROM my_topic LIMIT 10;"));
    lines.push(Line::from(
        "  SELECT key FROM t WHERE value->response->msg CONTAINS 'error';",
    ));
    lines.push(Line::from(
        "  SELECT key, value FROM random-data WHERE value->event->type = 'purchase' AND value->response->status = 200;",
    ));
    lines.push(Line::from(
        "  SELECT key FROM t WHERE (key = 'a' OR key = 'b') AND value->foo CONTAINS 'x' ORDER BY timestamp DESC LIMIT 100;",
    ));
    lines.push(Line::from("- Special command: LIST topics;"));
    lines.push(Line::from(""));

    lines.push(heading_line("Autocomplete"));
    lines.push(Line::from(
        "- Triggered after typing FROM and a space in a SELECT",
    ));
    lines.push(Line::from("- Fuzzy-matched suggestions for topics"));
    lines.push(Line::from("- Right accepts; Ctrl-N/Ctrl-P move; Esc dismiss"));
    lines.push(Line::from(""));

    lines.push(heading_line("Help navigation"));
    lines.push(Line::from(
        "- Scroll with Up/Down or PageUp/PageDown; Home/End jump",
    ));

    lines
}

fn heading_line(text: &'static str) -> Line<'static> {
    Line::from(vec![Span::styled(
        text,
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )])
}

fn draw_table(frame: &mut Frame, area: Rect, app: &AppState) {
    let headers: Vec<Cell> = app
        .selected_columns
        .iter()
        .map(|col| Cell::from(header_span(column_label(col))))
        .collect();

    // Create single-line rows with truncated previews; full JSON moves to right pane
    let rows: Vec<Row> = app
        .rows
        .iter()
        .enumerate()
        .map(|(i, env)| make_row(i, env, app))
        .collect();

    let mut constraints: Vec<Constraint> =
        app.selected_columns.iter().map(column_constraint).collect();
    if let Some(last) = constraints.last_mut() {
        *last = Constraint::Percentage(100);
    } else {
        constraints.push(Constraint::Percentage(100));
    }

    let table = Table::new(rows, constraints)
        .header(Row::new(headers).style(Style::default().add_modifier(Modifier::BOLD)))
        .block({
            let border_style = if app.focus == Focus::Results {
                Style::default().fg(Color::LightCyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Block::default()
                .borders(Borders::ALL)
                .title("Results")
                .border_style(border_style)
        })
        .row_highlight_style(Style::default())
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
    if has_value_column(app) {
        let content_w_estimate = estimate_table_content_width(app);
        let visible_w = area.width.saturating_sub(2) as usize; // minus borders
        let h_content = content_w_estimate
            .saturating_sub(visible_w)
            .saturating_add(1);
        if h_content > 1 {
            let mut hs =
                ScrollbarState::new(h_content).position(app.table_hscroll.min(h_content - 1));
            let hbar = Scrollbar::new(ScrollbarOrientation::HorizontalBottom);
            frame.render_stateful_widget(hbar, area, &mut hs);
        }
    }
}

fn header_span(text: &str) -> Span<'_> {
    Span::styled(text, Style::default().add_modifier(Modifier::BOLD))
}

fn column_label(col: &SelectItem) -> &'static str {
    match col {
        SelectItem::Partition => "Partition",
        SelectItem::Offset => "Offset",
        SelectItem::Timestamp => "Timestamp",
        SelectItem::Key => "Key",
        SelectItem::Value => "Value",
    }
}

fn column_constraint(col: &SelectItem) -> Constraint {
    match col {
        SelectItem::Partition => Constraint::Length(10),
        SelectItem::Offset => Constraint::Length(12),
        SelectItem::Timestamp => Constraint::Length(26),
        SelectItem::Key => Constraint::Length(30),
        SelectItem::Value => Constraint::Length(30),
    }
}

fn make_row(idx: usize, env: &MessageEnvelope, app: &AppState) -> Row<'static> {
    let selected_row = idx == app.selected_row;
    let mut cells = Vec::new();
    for (col_idx, col) in app.selected_columns.iter().enumerate() {
        let text = match col {
            SelectItem::Value => {
                let raw_value = env.value.as_deref().unwrap_or("null");
                let preview = json_preview_minified(raw_value);
                apply_hscroll(&preview, app.table_hscroll)
            }
            _ => column_raw_text(env, *col),
        };
        cells.push(style_cell(
            Cell::from(text),
            selected_row && app.selected_col == col_idx,
        ));
    }
    Row::new(cells).height(1)
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
    let tm = time::OffsetDateTime::from_unix_timestamp(secs as i64)
        .unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH);
    tm.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| ms.to_string())
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
    // Pretty-print JSON into multiple lines with Postman-like colors:
    // - keys: green, strings: yellow, numbers: cyan, booleans: magenta, null: dark gray, punctuation: gray
    fn indent(depth: usize) -> Span<'static> {
        Span::raw(" ".repeat(depth * 2))
    }
    fn punct(s: &str) -> Span<'static> {
        Span::styled(s.to_string(), Style::default().fg(Color::Gray))
    }
    fn string_span(s: &str) -> Span<'static> {
        Span::styled(format!("\"{}\"", s), Style::default().fg(Color::Yellow))
    }
    fn number_span(n: &serde_json::Number) -> Span<'static> {
        Span::styled(n.to_string(), Style::default().fg(Color::Cyan))
    }
    fn bool_span(b: bool) -> Span<'static> {
        Span::styled(b.to_string(), Style::default().fg(Color::Magenta))
    }
    fn null_span() -> Span<'static> {
        Span::styled("null".to_string(), Style::default().fg(Color::DarkGray))
    }

    fn render_scalar(val: &serde_json::Value) -> Vec<Span<'static>> {
        match val {
            serde_json::Value::String(s) => vec![string_span(s)],
            serde_json::Value::Number(n) => vec![number_span(n)],
            serde_json::Value::Bool(b) => vec![bool_span(*b)],
            serde_json::Value::Null => vec![null_span()],
            _ => vec![Span::raw(String::new())],
        }
    }

    fn render_value(v: &serde_json::Value, depth: usize, out: &mut Vec<Line<'static>>) {
        match v {
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {
                let mut spans = Vec::new();
                spans.push(indent(depth));
                spans.extend(render_scalar(v));
                out.push(Line::from(spans));
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    out.push(Line::from(vec![indent(depth), punct("[]")]));
                } else {
                    out.push(Line::from(vec![indent(depth), punct("[")]));
                    for (i, item) in arr.iter().enumerate() {
                        let before_len = out.len();
                        render_value(item, depth + 1, out);
                        // append comma to the last rendered line for this item if not last
                        if i + 1 != arr.len() {
                            let idx = out.len().saturating_sub(1);
                            if let Some(last) = out.get_mut(idx) {
                                last.spans.push(punct(","));
                            }
                        }
                        // ensure at least one line was added
                        if out.len() == before_len {
                            out.push(Line::from(vec![indent(depth + 1), punct("")]));
                        }
                    }
                    out.push(Line::from(vec![indent(depth), punct("]")]));
                }
            }
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    out.push(Line::from(vec![indent(depth), punct("{}")]));
                } else {
                    out.push(Line::from(vec![indent(depth), punct("{")]));
                    let len = map.len();
                    for (i, (k, val)) in map.iter().enumerate() {
                        match val {
                            serde_json::Value::Null
                            | serde_json::Value::Bool(_)
                            | serde_json::Value::Number(_)
                            | serde_json::Value::String(_) => {
                                let mut spans = Vec::new();
                                spans.push(indent(depth + 1));
                                spans.push(Span::styled(
                                    format!("\"{}\"", k),
                                    Style::default().fg(Color::Green),
                                ));
                                spans.push(punct(": "));
                                spans.extend(render_scalar(val));
                                if i + 1 != len {
                                    spans.push(punct(","));
                                }
                                out.push(Line::from(spans));
                            }
                            _ => {
                                // complex value: print key on its own line, then nested structure
                                let mut key_line = Vec::new();
                                key_line.push(indent(depth + 1));
                                key_line.push(Span::styled(
                                    format!("\"{}\"", k),
                                    Style::default().fg(Color::Green),
                                ));
                                key_line.push(punct(":"));
                                out.push(Line::from(key_line));

                                let before_len = out.len();
                                render_value(val, depth + 1, out);
                                if i + 1 != len {
                                    let idx = out.len().saturating_sub(1);
                                    if let Some(last) = out.get_mut(idx) {
                                        last.spans.push(punct(","));
                                    }
                                }
                                if out.len() == before_len {
                                    out.push(Line::from(vec![indent(depth + 1), punct("")]));
                                }
                            }
                        }
                    }
                    out.push(Line::from(vec![indent(depth), punct("}")]));
                }
            }
        }
    }

    let mut lines: Vec<Line<'static>> = Vec::new();
    render_value(v, 0, &mut lines);
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
    if offset == 0 {
        return s.to_string();
    }
    s.chars().skip(offset).collect()
}

fn column_raw_text(env: &MessageEnvelope, col: SelectItem) -> String {
    match col {
        SelectItem::Partition => env.partition.to_string(),
        SelectItem::Offset => env.offset.to_string(),
        SelectItem::Timestamp => fmt_ts(env.timestamp_ms),
        SelectItem::Key => env.key.clone(),
        SelectItem::Value => env.value.as_deref().unwrap_or("null").to_string(),
    }
}

fn column_width_hint(col: SelectItem) -> usize {
    match col {
        SelectItem::Partition => 10,
        SelectItem::Offset => 12,
        SelectItem::Timestamp => 26,
        SelectItem::Key => 30,
        SelectItem::Value => 40,
    }
}

fn has_value_column(app: &AppState) -> bool {
    app.selected_columns
        .iter()
        .any(|c| matches!(c, SelectItem::Value))
}

fn estimate_table_content_width(app: &AppState) -> usize {
    // Approximate widths of fixed columns + spacing + average key/value preview length
    let mut fixed = 0usize;
    for (idx, col) in app.selected_columns.iter().enumerate() {
        if idx > 0 {
            fixed = fixed.saturating_add(1);
        }
        match col {
            SelectItem::Value => {}
            _ => fixed = fixed.saturating_add(column_width_hint(*col)),
        }
    }
    if !has_value_column(app) {
        return fixed;
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
        let btn_rect = Rect {
            x: inner_area.x + inner_area.width - btn_w,
            y: inner_area.y,
            width: btn_w,
            height: 1,
        };
        let style = if app.copy_btn_pressed {
            // pressed look
            Style::default().fg(Color::Black).bg(Color::LightYellow)
        } else {
            // raised look
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
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
        let mut vs = ScrollbarState::new(content_len)
            .position(app.json_vscroll.min((content_len.saturating_sub(1)) as u16) as usize);
        let vbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(vbar, area, &mut vs);
    }
}

fn selected_cell_for_detail(app: &AppState) -> (String, Option<String>) {
    if app.rows.is_empty() || app.selected_columns.is_empty() {
        return ("none".to_string(), None);
    }
    let idx = app.selected_row.min(app.rows.len() - 1);
    let env = &app.rows[idx];
    let col_idx = app
        .selected_col
        .min(app.selected_columns.len().saturating_sub(1));
    let col = app.selected_columns[col_idx];
    (
        column_label(&col).to_string(),
        Some(column_raw_text(env, col)),
    )
}
