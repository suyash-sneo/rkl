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
use tui_textarea::TextArea;

use super::app::{
    AppConfigFieldFocus, AppState, EnvEditor, EnvFieldFocus, EnvPemField, HomeFocus, QueryMode,
    ResultsMode, Screen, COMMAND_SPECS, SPINNER_FRAMES,
};
use super::timefmt::fmt_ts;

// 256-color-friendly palette to avoid odd tints across terminals.
const PANEL: Color = Color::Indexed(236); // dark gray
const RAISED: Color = Color::Indexed(238); // slightly lighter gray
const ROW_HL: Color = Color::Indexed(240); // row highlight
const CELL_HL: Color = Color::Indexed(220); // bright yellow for selected cell
const ACCENT: Color = Color::Cyan; // buttons/pills
const ACCENT_FADED: Color = Color::Gray;
const POSITIVE: Color = Color::Green;
const NEGATIVE: Color = Color::Red;
const PANEL_GAP: u16 = 1;

static HELP_LINES: &[&str] = &[
    "Home",
    "  • Ctrl-Enter run query (Shift+Ctrl-Enter rerun last)",
    "  • Tab / Shift-Tab move focus between filter, query, results, detail",
    "  • Ctrl-R opens history, Ctrl-Shift-R reruns last query",
    "  • Ctrl-P or ':' opens command palette, '?' opens help",
    "  • Ctrl-Y drops selected topic into advanced editor",
    "  • F2 Envs, F3 App config, F12 Topics, Esc resets focus to filter",
    "",
    "Basic mode",
    "  • Type to fuzzy-search topics; Enter jumps to fields",
    "  • Filters: search value contains, WHERE, time bounds, LIMIT, ORDER",
    "",
    "Advanced mode",
    "  • Full SQL editor with multi-line editing; query under cursor runs",
    "  • Double slash '//' at line start opens command palette",
    "",
    "Results",
    "  • Arrows move selection, Enter opens record detail view",
    "  • Shift-Left/Right scroll columns; timestamp toggle top-right",
    "",
    "Environments",
    "  • /s save, /t test, /n new, /d delete, /] /[ cycle envs, /] /[ switches PEM tab",
    "",
    "General",
    "  • Copy/paste works in text areas; status log scrolls with mouse/keys",
];

pub fn draw(frame: &mut Frame, app: &AppState) {
    let area = inset(frame.area(), PANEL_GAP);
    match app.screen {
        Screen::Home => draw_home(frame, area, app),
        Screen::Envs => draw_envs(frame, area, app),
        Screen::Info => draw_info(frame, area, app),
        Screen::AppConfig => draw_app_config(frame, area, app),
        Screen::Help => draw_help(frame, area, app),
        Screen::RecordDetail => draw_record_detail(frame, area, app),
    }

    if app.command_palette.open {
        draw_command_palette(frame, frame.area(), app);
    }
    if app.show_history_popup {
        draw_history_popup(frame, frame.area(), app);
    }
}

fn draw_home(frame: &mut Frame, area: Rect, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(2), Constraint::Min(14), Constraint::Length(6)])
        .split(area);

    draw_header(frame, layout[0], app);

    let workspace = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
        .split(layout[1]);

    draw_controls(frame, workspace[0], app);
    draw_results(frame, workspace[1], app);
    draw_status_log(frame, layout[2], app);
}

fn draw_header(frame: &mut Frame, area: Rect, app: &AppState) {
    let env_name = app
        .selected_env()
        .map(|e| e.name.as_str())
        .unwrap_or("Default");
    let host = app
        .selected_env()
        .map(|e| e.host.as_str())
        .unwrap_or(app.host.as_str());
    let mode = match app.query_mode {
        QueryMode::Basic => "Basic lookup",
        QueryMode::Advanced => "Advanced SQL",
    };
    let spinner = if app.query_in_progress {
        format!(
            "{} running… {}",
            SPINNER_FRAMES[app.query_spinner_idx % SPINNER_FRAMES.len()],
            app.query_rows_seen
        )
    } else {
        "Ready".to_string()
    };

    let content = Line::from(vec![
        Span::styled("Env ", Style::default().fg(ACCENT_FADED)),
        Span::styled(env_name, Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)),
        Span::raw("  "),
        Span::styled("Host ", Style::default().fg(ACCENT_FADED)),
        Span::styled(host, Style::default().fg(Color::White)),
        Span::raw("  "),
        Span::styled("Mode ", Style::default().fg(ACCENT_FADED)),
        Span::styled(mode, Style::default().fg(Color::White)),
        Span::raw("    "),
        Span::styled("Ctrl-P palette  ? help  /r rerun  Ctrl-Enter run", Style::default().fg(Color::Gray)),
        Span::raw("    "),
        Span::styled(spinner, Style::default().fg(ACCENT_FADED)),
    ]);

    frame.render_widget(
        Paragraph::new(content)
            .alignment(Alignment::Left)
            .style(Style::default().bg(RAISED)),
        area,
    );
}

fn draw_controls(frame: &mut Frame, area: Rect, app: &AppState) {
    let inner = Layout::default()
        .direction(Direction::Horizontal)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Percentage(36), Constraint::Percentage(64)])
        .split(area);

    draw_topic_panel(frame, inner[0], app);
    match app.query_mode {
        QueryMode::Basic => draw_basic_query(frame, inner[1], app),
        QueryMode::Advanced => draw_advanced_query(frame, inner[1], app),
    }
}

fn draw_topic_panel(frame: &mut Frame, area: Rect, app: &AppState) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Topics")
        .title_alignment(Alignment::Left)
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(4)])
        .split(inner);

    render_textarea(
        frame,
        layout[0],
        &app.topic_picker.filter,
        "Filter topics (fuzzy)",
        matches!(app.home_focus, HomeFocus::TopicFilter),
    );

    let mut items: Vec<ListItem> = Vec::new();
    for idx in app.topic_picker.matches.iter().copied() {
        if let Some(name) = app.topics.get(idx) {
            items.push(ListItem::new(name.clone()));
        }
    }
    if items.is_empty() {
        items.push(ListItem::new("No topics. Press F12 to refresh."));
    }
    let mut state = ListState::default();
    state.select(Some(
        app.topic_picker
            .selected
            .min(app.topic_picker.matches.len().saturating_sub(1)),
    ));
    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(ACCENT)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(PANEL))
        .block(
            Block::default()
                .borders(Borders::NONE)
                .title("Matches")
                .title_style(Style::default().fg(ACCENT_FADED))
                .style(Style::default().bg(PANEL)),
        );
    frame.render_stateful_widget(list, layout[1], &mut state);
}

fn draw_basic_query(frame: &mut Frame, area: Rect, app: &AppState) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Lookup builder")
        .title_style(Style::default().fg(ACCENT))
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(inner);

    let topic = selected_topic_label(app);
    frame.render_widget(
        Paragraph::new(topic)
            .style(Style::default().fg(Color::Gray))
            .alignment(Alignment::Left),
        layout[0],
    );

    render_labeled_textarea(
        frame,
        layout[1],
        "Value",
        &app.basic_query.search,
        matches!(app.home_focus, HomeFocus::BasicSearch),
    );
    render_labeled_textarea(
        frame,
        layout[2],
        "WHERE (optional)",
        &app.basic_query.where_clause,
        matches!(app.home_focus, HomeFocus::BasicWhere),
    );

    let limit_row = layout[3];
    render_labeled_textarea(
        frame,
        limit_row,
        "Limit (empty = auto)",
        &app.basic_query.limit,
        matches!(app.home_focus, HomeFocus::BasicLimit),
    );
    draw_order_pills(
        frame,
        layout[4],
        app.basic_query.order_field_idx,
        app.basic_query.order_dir_idx,
    );

    let tips = "Tab: topic → value → where → limit  •  /. order  •  /, dir  •  /r rerun last";
    frame.render_widget(
        Paragraph::new(tips)
            .style(Style::default().fg(Color::Gray))
            .alignment(Alignment::Left),
        layout[5],
    );
}

fn draw_order_pills(frame: &mut Frame, area: Rect, field_idx: usize, dir_idx: usize) {
    let mut spans = Vec::new();
    spans.push(Span::styled("Order ", Style::default().fg(ACCENT_FADED)));
    for (i, label) in ["timestamp", "poffset", "poffset_ts"].iter().enumerate() {
        let active = i == field_idx;
        spans.push(Span::raw(" "));
        spans.push(pill(label, active, true));
    }
    spans.push(Span::raw("   "));
    spans.push(Span::styled("Dir ", Style::default().fg(ACCENT_FADED)));
    for (i, label) in ["ASC", "DESC"].iter().enumerate() {
        let active = i == dir_idx;
        spans.push(Span::raw(" "));
        spans.push(pill(label, active, true));
    }

    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(PANEL)),
        area,
    );
}

fn draw_advanced_query(frame: &mut Frame, area: Rect, app: &AppState) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Advanced SQL")
        .title_style(Style::default().fg(ACCENT))
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(2)])
        .split(inner);

    render_textarea(
        frame,
        layout[0],
        &app.query_editor,
        "SELECT ... FROM ... WHERE ...",
        matches!(app.home_focus, HomeFocus::AdvancedQuery),
    );

    let parse_status = if app.parse_ok {
        Span::styled("Parse: OK", Style::default().fg(POSITIVE))
    } else if let Some(msg) = app.parse_error_msg.as_ref() {
        Span::styled(
            format!("Parse error: {}", truncate_with_ellipsis(msg, 80)),
            Style::default().fg(NEGATIVE),
        )
    } else {
        Span::raw("")
    };
    let hint = Line::from(vec![
        parse_status,
        Span::raw("    "),
        Span::styled("Ctrl-Enter runs query under cursor", Style::default().fg(Color::Gray)),
    ]);
    frame.render_widget(Paragraph::new(hint), layout[1]);
}

fn render_textarea(frame: &mut Frame, area: Rect, ta: &TextArea<'_>, label: &str, focused: bool) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title(label)
        .title_style(Style::default().fg(ACCENT_FADED))
        .style(Style::default().bg(if focused { RAISED } else { PANEL }));
    let mut widget = ta.clone();
    widget.set_block(block.clone());
    frame.render_widget(&widget, area);

    if focused {
        let inner = block.inner(area);
        let (row, col) = ta.cursor();
        let x = inner.x + col as u16;
        let y = inner.y + row as u16;
        frame.set_cursor_position(Position::new(x, y));
    }
}

fn render_labeled_textarea(
    frame: &mut Frame,
    area: Rect,
    label: &str,
    ta: &TextArea<'_>,
    focused: bool,
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(24), Constraint::Min(1)])
        .split(area);
    frame.render_widget(
        Paragraph::new(label)
            .alignment(Alignment::Right)
            .style(
                Style::default()
                    .fg(if focused { ACCENT } else { ACCENT_FADED })
                    .bg(if focused { RAISED } else { PANEL }),
            ),
        cols[0],
    );
    render_textarea(frame, cols[1], ta, "", focused);
}

fn draw_results(frame: &mut Frame, area: Rect, app: &AppState) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5)])
        .split(columns[0]);
    draw_results_header(frame, left[0], app);
    draw_results_table(frame, left[1], app);
    draw_detail_preview(frame, columns[1], app);
}

fn draw_results_header(frame: &mut Frame, area: Rect, app: &AppState) {
    let mut spans = Vec::new();
    spans.push(Span::styled(
        match app.results_mode {
            ResultsMode::Messages => "Results",
            ResultsMode::TopicList => "Topics",
        },
        Style::default().fg(ACCENT),
    ));
    spans.push(Span::raw("  "));
    spans.push(Span::styled(
        format!("{} rows", app.rows.len()),
        Style::default().fg(Color::Gray),
    ));
    if app.query_in_progress {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            format!(
                "{} streaming",
                SPINNER_FRAMES[app.query_spinner_idx % SPINNER_FRAMES.len()]
            ),
            Style::default().fg(ACCENT),
        ));
    }
    if app.should_show_timestamp_switch() {
        spans.push(Span::raw("    "));
        spans.push(Span::styled(
            app.timestamp_toggle_label(),
            Style::default()
                .fg(Color::Black)
                .bg(ACCENT)
                .add_modifier(Modifier::BOLD),
        ));
    }
    let line = Line::from(spans);
    frame.render_widget(
        Paragraph::new(line)
            .style(Style::default().bg(PANEL))
            .alignment(Alignment::Left),
        area,
    );
}

fn draw_results_table(frame: &mut Frame, area: Rect, app: &AppState) {
    match app.results_mode {
        ResultsMode::Messages => draw_message_table(frame, area, app),
        ResultsMode::TopicList => draw_topic_table(frame, area, app),
    }
}

fn draw_topic_table(frame: &mut Frame, area: Rect, app: &AppState) {
    let rows: Vec<Row> = app
        .topics_with_partitions
        .iter()
        .map(|(name, partitions)| {
            Row::new(vec![
                Cell::from(name.clone()),
                Cell::from(format!("{} partitions", partitions)),
            ])
        })
        .collect();
    let mut state = TableState::default();
    state.select(Some(
        app.selected_row
            .min(app.topics_with_partitions.len().saturating_sub(1)),
    ));

    let table = Table::new(rows, [Constraint::Percentage(70), Constraint::Percentage(30)])
        .column_spacing(2)
        .row_highlight_style(Style::default().bg(ROW_HL))
        .style(Style::default().bg(PANEL));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_message_table(frame: &mut Frame, area: Rect, app: &AppState) {
    if app.selected_columns.is_empty() {
        frame.render_widget(
            Paragraph::new("No columns selected").style(Style::default().bg(PANEL)),
            area,
        );
        return;
    }
    let headers: Vec<Cell> = app
        .selected_columns
        .iter()
        .map(|col| Cell::from(column_label(col)))
        .collect();
    let rows: Vec<Row> = app
        .rows
        .iter()
        .enumerate()
        .map(|(i, env)| build_row(i, env, app))
        .collect();
    let constraints: Vec<Constraint> = app
        .selected_columns
        .iter()
        .map(|c| column_constraint(c))
        .collect();
    let mut state = TableState::default();
    state.select(Some(app.selected_row.min(app.rows.len().saturating_sub(1))));

    let table = Table::new(rows, constraints)
        .header(
            Row::new(headers)
                .style(Style::default().fg(Color::Gray))
                .height(1),
        )
        .row_highlight_style(Style::default().bg(ROW_HL))
        .column_spacing(1)
        .style(Style::default().bg(PANEL));

    frame.render_stateful_widget(table, area, &mut state);

    if app.rows.len() > area.height as usize {
        let mut vs = ScrollbarState::new(app.rows.len())
            .position(app.selected_row.min(app.rows.len().saturating_sub(1)));
        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(bar, area, &mut vs);
    }
}

fn build_row(idx: usize, env: &MessageEnvelope, app: &AppState) -> Row<'static> {
    let row_selected = app.selected_row == idx;
    let mut cells = Vec::new();
    for (col_idx, col) in app.selected_columns.iter().enumerate() {
        let base_text = match col {
            SelectItem::Value => {
                let raw = env.value.as_deref().unwrap_or("null");
                let preview = json_preview(raw);
                apply_hscroll(&preview, app.table_hscroll)
            }
            _ => column_text(env, *col, app),
        };
        let base_fg = match col {
            SelectItem::Partition => Color::Gray,
            SelectItem::Offset => Color::Cyan,
            SelectItem::Timestamp => ACCENT_FADED,
            SelectItem::Key => Color::Yellow,
            SelectItem::Value => Color::White,
        };
        let mut style = Style::default().fg(base_fg);
        if row_selected {
            style = style.bg(ROW_HL);
        }
        if app.selected_row == idx && app.selected_col == col_idx {
            style = Style::default()
                .fg(Color::Black)
                .bg(CELL_HL)
                .add_modifier(Modifier::BOLD);
        }
        cells.push(Cell::from(Span::styled(base_text, style)));
    }
    Row::new(cells).height(1)
}

fn draw_detail_preview(frame: &mut Frame, area: Rect, app: &AppState) {
    let focused = matches!(app.home_focus, HomeFocus::Details);
    let (title, body) = selected_detail(app);
    let block = Block::default()
        .borders(Borders::NONE)
        .title(format!("Detail — {}", title))
        .title_style(Style::default().fg(ACCENT_FADED))
        .style(Style::default().bg(if focused { RAISED } else { PANEL }));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);

    let text = body
        .as_ref()
        .cloned()
        .unwrap_or_else(|| Text::from("No selection"));
    let para = Paragraph::new(text)
        .wrap(Wrap { trim: false })
        .scroll((app.json_vscroll, 0))
        .style(Style::default().bg(if focused { RAISED } else { PANEL }));
    frame.render_widget(para, inner);

    let content_len = paragraph_len(body.as_ref());
    if content_len > inner.height as usize {
        let mut vs = ScrollbarState::new(content_len)
            .position(app.json_vscroll.min(content_len.saturating_sub(1) as u16) as usize);
        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(bar, inner, &mut vs);
    }
}

fn draw_status_log(frame: &mut Frame, area: Rect, app: &AppState) {
    let mut lines: Vec<Line> = Vec::new();
    if let Some(last) = app.last_executed_query.as_ref() {
        lines.push(
            Line::from(vec![
                Span::styled("Last (/r): ", Style::default().fg(Color::Black).bg(ACCENT)),
                Span::styled(
                    truncate_with_ellipsis(last, 120),
                    Style::default().fg(Color::Black).bg(ACCENT),
                ),
            ]),
        );
    }
    if !app.status.is_empty() {
        lines.push(Line::from(app.status.clone()));
    }
    if !app.status_buffer.is_empty() {
        if !lines.is_empty() {
            lines.push(Line::from("──"));
        }
        lines.extend(app.status_buffer.lines().map(|l| Line::from(l.to_string())));
    }
    if lines.is_empty() {
        lines.push(Line::from("Ready"));
    }
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Status")
        .title_style(Style::default().fg(ACCENT))
        .style(Style::default().bg(PANEL));
    let inner = inset(area, PANEL_GAP);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(Text::from(lines.clone()))
            .wrap(Wrap { trim: false })
            .scroll((app.status_vscroll, 0))
            .style(Style::default().bg(PANEL)),
        inner,
    );

    let total = lines.len();
    if total > inner.height as usize {
        let mut vs = ScrollbarState::new(total).position(app.status_vscroll as usize);
        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(bar, inner, &mut vs);
    }
}

fn draw_envs(frame: &mut Frame, area: Rect, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(3), Constraint::Min(8), Constraint::Length(6)])
        .split(area);
    draw_env_header(frame, layout[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(layout[1]);
    draw_env_list(frame, body[0], app);
    draw_env_editor(frame, body[1], app);
    draw_status_log(frame, layout[2], app);
}

fn draw_env_header(frame: &mut Frame, area: Rect) {
    frame.render_widget(
        Paragraph::new("Environments — /n new • /d delete • /s save • /t test • /] /[ cycle")
            .style(Style::default().bg(RAISED).fg(Color::White)),
        area,
    );
}

fn draw_env_list(frame: &mut Frame, area: Rect, app: &AppState) {
    let items: Vec<ListItem> = app
        .env_store
        .envs
        .iter()
        .map(|e| ListItem::new(e.name.clone()))
        .collect();
    let mut state = ListState::default();
    state.select(app.env_store.selected);
    let focused = matches!(
        app.env_editor.as_ref().map(|e| e.field_focus),
        Some(EnvFieldFocus::List)
    );
    let list = List::new(items)
        .style(Style::default().bg(if focused { RAISED } else { PANEL }))
        .highlight_style(
            if focused {
                Style::default()
                    .bg(ACCENT)
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .bg(ROW_HL)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            },
        )
        .block(
            Block::default()
                .borders(Borders::NONE)
                .title("Profiles")
                .style(Style::default().bg(if focused { RAISED } else { PANEL })),
        );
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_env_editor(frame: &mut Frame, area: Rect, app: &AppState) {
    let focused = |f: EnvFieldFocus| {
        app.env_editor
            .as_ref()
            .map(|e| e.field_focus == f)
            .unwrap_or(false)
    };
    let ed = match app.env_editor.as_ref() {
        Some(v) => v,
        None => {
            frame.render_widget(
                Paragraph::new("No environment").style(Style::default().bg(PANEL)),
                area,
            );
            return;
        }
    };
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(5),
        ])
        .split(area);

    render_single_line(
        frame,
        layout[0],
        &ed.name,
        ed.name_cursor,
        "Name",
        focused(EnvFieldFocus::Name),
    );
    render_single_line(
        frame,
        layout[1],
        &ed.host,
        ed.host_cursor,
        "Host",
        focused(EnvFieldFocus::Host),
    );

    draw_pem_tabs(frame, layout[2], ed, focused(EnvFieldFocus::PemEditor));

    let active_ta = match ed.active_pem {
        EnvPemField::PrivateKey => (&ed.ta_private, "Private key (PEM)"),
        EnvPemField::PublicKey => (&ed.ta_public, "Certificate (PEM)"),
        EnvPemField::Ca => (&ed.ta_ca, "SSL CA (PEM)"),
    };
    render_textarea(
        frame,
        layout[3],
        active_ta.0,
        active_ta.1,
        focused(EnvFieldFocus::PemEditor),
    );

    draw_env_connection(frame, layout[4], app, focused(EnvFieldFocus::Conn));
}

fn draw_pem_tabs(frame: &mut Frame, area: Rect, ed: &EnvEditor, focused: bool) {
    let mut spans = Vec::new();
    spans.push(Span::styled(
        "PEM ",
        Style::default().fg(if focused { ACCENT } else { Color::Gray }),
    ));
    for (field, label) in [
        (EnvPemField::PrivateKey, "Private"),
        (EnvPemField::PublicKey, "Public/Cert"),
        (EnvPemField::Ca, "CA"),
    ] {
        let active = ed.active_pem == field;
        spans.push(Span::raw(" "));
        spans.push(pill(label, active, focused));
    }
    spans.push(Span::raw("    /] /[ switches tabs"));
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(PANEL)),
        area,
    );
}

fn render_single_line(
    frame: &mut Frame,
    area: Rect,
    text: &str,
    cursor: usize,
    label: &str,
    focused: bool,
) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title(label)
        .title_style(Style::default().fg(ACCENT_FADED))
        .style(Style::default().bg(if focused { RAISED } else { PANEL }));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);
    frame.render_widget(
        Paragraph::new(text.to_string())
            .style(Style::default().bg(if focused { RAISED } else { PANEL })),
        inner,
    );
    if focused {
        let pos = cursor.min(text.len()) as u16;
        frame.set_cursor_position(Position::new(inner.x + pos, inner.y));
    }
}

fn draw_env_connection(frame: &mut Frame, area: Rect, app: &AppState, focused: bool) {
    let mut lines: Vec<Line> = if app.env_test_log.trim().is_empty() {
        vec![Line::from(
            app.env_test_message
                .clone()
                .unwrap_or_else(|| "No test run yet".to_string()),
        )]
    } else {
        app.env_test_log
            .lines()
            .map(|l| Line::from(l.to_string()))
            .collect()
    };
    if lines.is_empty() {
        lines.push(Line::from("No test output"));
    }
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Connection log")
        .title_style(Style::default().fg(ACCENT_FADED))
        .style(Style::default().bg(if focused { RAISED } else { PANEL }));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);
    frame.render_widget(
        Paragraph::new(Text::from(lines.clone()))
            .scroll((app.env_conn_vscroll, 0))
            .style(Style::default().bg(if focused { RAISED } else { PANEL })),
        inner,
    );
    if lines.len() > inner.height as usize {
        let mut vs =
            ScrollbarState::new(lines.len()).position(app.env_conn_vscroll as usize);
        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
        frame.render_stateful_widget(bar, inner, &mut vs);
    }

    let actions = Line::from(vec![
        Span::styled("/s Save", Style::default().fg(POSITIVE)),
        Span::raw("  "),
        Span::styled("/t Test", Style::default().fg(ACCENT)),
        Span::raw("  "),
        Span::styled("/] /[ Cycle  Esc Home", Style::default().fg(Color::Gray)),
    ]);
    frame.render_widget(
        Paragraph::new(actions).style(Style::default().bg(PANEL)),
        Rect {
            x: inner.x,
            y: inner.y.saturating_add(inner.height.saturating_sub(1)),
            width: inner.width,
            height: 1,
        },
    );
}

fn draw_info(frame: &mut Frame, area: Rect, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(3), Constraint::Min(5)])
        .split(area);
    frame.render_widget(
        Paragraph::new("Broker topics — F6 refresh • Esc back")
            .style(Style::default().bg(RAISED)),
        layout[0],
    );
    draw_topic_table(frame, layout[1], app);
}

fn draw_app_config(frame: &mut Frame, area: Rect, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(3), Constraint::Min(2)])
        .split(area);

    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(2),
        ])
        .split(layout[1]);

    let header = Paragraph::new("App config — Tab/Shift-Tab moves • Enter toggles/save • Esc back")
        .style(Style::default().bg(RAISED));
    frame.render_widget(header, layout[0]);

    let Some(ed) = app.app_config_editor.as_ref() else {
        return;
    };
    render_single_line(
        frame,
        body[0],
        &ed.query_scan_multiplier,
        ed.query_scan_multiplier.len(),
        "Query scan multiplier",
        matches!(ed.field_focus, AppConfigFieldFocus::QueryScanMultiplier),
    );
    render_single_line(
        frame,
        body[1],
        &ed.default_limit,
        ed.default_limit.len(),
        "Default LIMIT (empty = auto)",
        matches!(ed.field_focus, AppConfigFieldFocus::DefaultLimit),
    );

    let order_row = Layout::default()
        .direction(Direction::Horizontal)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(body[2]);
    let mut spans = Vec::new();
    spans.push(Span::styled("Order field ", Style::default().fg(Color::Gray)));
    for (i, label) in ["timestamp", "poffset", "poffset_ts"].iter().enumerate() {
        spans.push(Span::raw(" "));
        spans.push(pill(
            label,
            ed.default_order_field_idx == i,
            matches!(ed.field_focus, AppConfigFieldFocus::DefaultOrderField),
        ));
    }
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(PANEL)),
        order_row[0],
    );

    let mut dir_spans = Vec::new();
    dir_spans.push(Span::styled("Direction ", Style::default().fg(Color::Gray)));
    for (i, label) in ["ASC", "DESC"].iter().enumerate() {
        dir_spans.push(Span::raw(" "));
        dir_spans.push(pill(
            label,
            ed.default_order_dir_idx == i,
            matches!(ed.field_focus, AppConfigFieldFocus::DefaultOrderDir),
        ));
    }
    frame.render_widget(
        Paragraph::new(Line::from(dir_spans)).style(Style::default().bg(PANEL)),
        order_row[1],
    );

    let mut ts_spans = Vec::new();
    ts_spans.push(Span::styled("Timestamp display ", Style::default().fg(Color::Gray)));
    for (label, active) in [("UTC", ed.timestamps_use_utc), ("Local", !ed.timestamps_use_utc)] {
        ts_spans.push(Span::raw(" "));
        ts_spans.push(pill(
            label,
            active,
            matches!(ed.field_focus, AppConfigFieldFocus::TimestampsUseUtc),
        ));
    }
    frame.render_widget(
        Paragraph::new(Line::from(ts_spans)).style(Style::default().bg(PANEL)),
        body[3],
    );

    let actions_style = if matches!(ed.field_focus, AppConfigFieldFocus::Buttons) {
        Style::default()
            .bg(RAISED)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Gray)
    };
    let actions = Paragraph::new("[Enter] save  •  [Backspace] reset field  •  Esc back")
        .style(actions_style);
    frame.render_widget(actions, body[4]);

    let summary = format!(
        "Defaults: order {} {}, limit {}, timestamps {}",
        match ed.default_order_field_idx {
            0 => "timestamp",
            1 => "poffset",
            _ => "poffset_ts",
        },
        if ed.default_order_dir_idx == 0 { "ASC" } else { "DESC" },
        if ed.default_limit.trim().is_empty() {
            "auto".to_string()
        } else {
            ed.default_limit.clone()
        },
        if ed.timestamps_use_utc { "UTC" } else { "Local" }
    );
    frame.render_widget(
        Paragraph::new(summary).style(Style::default().fg(Color::Gray)),
        body[5],
    );
}

fn draw_help(frame: &mut Frame, area: Rect, app: &AppState) {
    let block = Block::default()
        .borders(Borders::NONE)
        .title("Help")
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, area);
    let inner = inset(area, PANEL_GAP);
    let lines: Vec<Line> = HELP_LINES
        .iter()
        .map(|l| Line::from(*l))
        .collect();
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((app.help_vscroll as u16, 0))
            .wrap(Wrap { trim: false })
            .style(Style::default().bg(PANEL)),
        inner,
    );
}

fn draw_record_detail(frame: &mut Frame, area: Rect, app: &AppState) {
    let header = Paragraph::new("Record detail — Esc to return")
        .style(Style::default().bg(RAISED));
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(2), Constraint::Min(5)])
        .split(area);
    frame.render_widget(header, layout[0]);

    let body_block = Block::default()
        .borders(Borders::NONE)
        .style(Style::default().bg(PANEL));
    frame.render_widget(body_block, layout[1]);
    let body_area = inset(layout[1], PANEL_GAP);
    let (meta, body) = record_detail_text(app);
    let mut content = Vec::new();
    content.extend(meta);
    content.push(Line::from(" "));
    content.extend(body);
    let para = Paragraph::new(Text::from(content))
        .scroll((app.record_detail_scroll, 0))
        .wrap(Wrap { trim: false })
        .style(Style::default().bg(PANEL));
    frame.render_widget(para, body_area);
}

fn draw_command_palette(frame: &mut Frame, area: Rect, app: &AppState) {
    let popup = centered_rect(70, 60, area);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Command palette")
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, popup);
    let inner = inset(popup, PANEL_GAP);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .spacing(PANEL_GAP)
        .constraints([Constraint::Length(3), Constraint::Min(4)])
        .split(inner);

    render_textarea(frame, layout[0], &app.command_palette.input, "Type to filter", true);

    let mut items: Vec<ListItem> = Vec::new();
    for idx in app.command_palette.matches.iter().copied() {
        if let Some(spec) = COMMAND_SPECS.get(idx) {
            items.push(ListItem::new(format!("{} — {}", spec.label, spec.hint)));
        }
    }
    if items.is_empty() {
        items.push(ListItem::new("No commands match"));
    }
    let mut state = ListState::default();
    state.select(Some(
        app.command_palette
            .selected
            .min(app.command_palette.matches.len().saturating_sub(1)),
    ));
    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(ACCENT)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(PANEL));
    frame.render_stateful_widget(list, layout[1], &mut state);
}

fn draw_history_popup(frame: &mut Frame, area: Rect, app: &AppState) {
    let popup = centered_rect(70, 60, area);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Query history")
        .style(Style::default().bg(PANEL));
    frame.render_widget(block, popup);
    let inner = inset(popup, PANEL_GAP);

    let mut items: Vec<ListItem> = if app.query_history.is_empty() {
        vec![ListItem::new("No history")]
    } else {
        app.query_history
            .iter()
            .enumerate()
            .map(|(i, q)| {
                ListItem::new(format!(
                    "#{:03} {}",
                    i + 1,
                    truncate_with_ellipsis(q, inner.width as usize - 6)
                ))
            })
            .collect()
    };
    if items.is_empty() {
        items.push(ListItem::new("No history"));
    }
    let mut state = ListState::default();
    state.select(Some(
        app.history_selected_index
            .min(app.query_history.len().saturating_sub(1)),
    ));
    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(ACCENT)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(PANEL));
    frame.render_stateful_widget(list, inner, &mut state);
}

fn pill(label: &str, active: bool, focused: bool) -> Span<'static> {
    if active {
        Span::styled(
            format!(" {} ", label),
            Style::default()
                .fg(Color::Black)
                .bg(ACCENT)
                .add_modifier(Modifier::BOLD),
        )
    } else if focused {
        Span::styled(
            format!(" {} ", label),
            Style::default().fg(ACCENT_FADED).bg(RAISED),
        )
    } else {
        Span::styled(
            format!(" {} ", label),
            Style::default().fg(ACCENT_FADED).bg(PANEL),
        )
    }
}

fn inset(area: Rect, margin: u16) -> Rect {
    Rect {
        x: area.x + margin,
        y: area.y + margin,
        width: area.width.saturating_sub(margin * 2),
        height: area.height.saturating_sub(margin * 2),
    }
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
        SelectItem::Key => Constraint::Length(24),
        SelectItem::Value => Constraint::Percentage(100),
    }
}

fn column_text(env: &MessageEnvelope, col: SelectItem, app: &AppState) -> String {
    match col {
        SelectItem::Partition => env.partition.to_string(),
        SelectItem::Offset => env.offset.to_string(),
        SelectItem::Timestamp => fmt_ts(env.timestamp_ms, app.timestamps_use_utc),
        SelectItem::Key => env.key.clone(),
        SelectItem::Value => env.value.as_deref().unwrap_or("null").to_string(),
    }
}

fn selected_topic_label(app: &AppState) -> String {
    if let Some(name) = app
        .topic_picker
        .matches
        .get(app.topic_picker.selected)
        .and_then(|idx| app.topics.get(*idx))
    {
        format!("Topic: {}", name)
    } else {
        "Topic: none selected".to_string()
    }
}

fn selected_detail(app: &AppState) -> (String, Option<Text<'static>>) {
    if app.rows.is_empty() || app.selected_columns.is_empty() {
        return ("none".to_string(), None);
    }
    let row = app.selected_row.min(app.rows.len().saturating_sub(1));
    let col = app
        .selected_col
        .min(app.selected_columns.len().saturating_sub(1));
    let env = &app.rows[row];
    let col_name = column_label(&app.selected_columns[col]);
    let raw = column_text(env, app.selected_columns[col], app);
    let text = if app.selected_columns[col] == SelectItem::Value {
        render_json_text(&raw)
    } else {
        Text::from(raw)
    };
    (col_name.to_string(), Some(text))
}

fn paragraph_len(text: Option<&Text<'_>>) -> usize {
    text.map(|t| t.lines.len()).unwrap_or_default()
}

fn render_json_text(raw: &str) -> Text<'static> {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => Text::from(json_lines(&v, 0)),
        Err(_) => Text::from(raw.to_string()),
    }
}

fn json_lines(v: &serde_json::Value, depth: usize) -> Vec<Line<'static>> {
    fn render(
        v: &serde_json::Value,
        depth: usize,
        prefix: Option<&str>,
        out: &mut Vec<Line<'static>>,
    ) {
        let indent = "  ".repeat(depth);
        match v {
            serde_json::Value::Null => out.push(Line::from(vec![
                Span::raw(indent),
                prefix_span(prefix),
                Span::styled("null", Style::default().fg(Color::DarkGray)),
            ])),
            serde_json::Value::Bool(b) => out.push(Line::from(vec![
                Span::raw(indent),
                prefix_span(prefix),
                Span::styled(b.to_string(), Style::default().fg(Color::Magenta)),
            ])),
            serde_json::Value::Number(n) => out.push(Line::from(vec![
                Span::raw(indent),
                prefix_span(prefix),
                Span::styled(n.to_string(), Style::default().fg(Color::Cyan)),
            ])),
            serde_json::Value::String(s) => out.push(Line::from(vec![
                Span::raw(indent),
                prefix_span(prefix),
                Span::styled(
                    serde_json::to_string(s).unwrap_or_else(|_| s.to_string()),
                    Style::default().fg(Color::Yellow),
                ),
            ])),
            serde_json::Value::Array(arr) => {
                let mut first_line = vec![Span::raw(indent.clone()), prefix_span(prefix)];
                first_line.push(Span::styled("[", Style::default().fg(Color::Gray)));
                out.push(Line::from(first_line));
                for (i, item) in arr.iter().enumerate() {
                    let before = out.len();
                    render(item, depth + 1, None, out);
                    if let Some(last) = out.last_mut() {
                        if i + 1 != arr.len() {
                            last.spans.push(Span::styled(",", Style::default().fg(Color::Gray)));
                        }
                    }
                    if out.len() == before {
                        out.push(Line::from("  "));
                    }
                }
                out.push(Line::from(vec![
                    Span::raw(indent),
                    Span::styled("]", Style::default().fg(Color::Gray)),
                ]));
            }
            serde_json::Value::Object(map) => {
                let mut first_line = vec![Span::raw(indent.clone()), prefix_span(prefix)];
                first_line.push(Span::styled("{", Style::default().fg(Color::Gray)));
                out.push(Line::from(first_line));
                let len = map.len();
                for (i, (k, val)) in map.iter().enumerate() {
                    let key_prefix = format!("\"{}\": ", k);
                    let before = out.len();
                    render(val, depth + 1, Some(&key_prefix), out);
                    if let Some(last) = out.last_mut() {
                        if i + 1 != len {
                            last.spans.push(Span::styled(",", Style::default().fg(Color::Gray)));
                        }
                    }
                    if out.len() == before {
                        out.push(Line::from("  "));
                    }
                }
                out.push(Line::from(vec![
                    Span::raw(indent),
                    Span::styled("}", Style::default().fg(Color::Gray)),
                ]));
            }
        }
    }

    fn prefix_span(prefix: Option<&str>) -> Span<'static> {
        match prefix {
            Some(p) => Span::styled(p.to_string(), Style::default().fg(Color::Green)),
            None => Span::raw(""),
        }
    }

    let mut out = Vec::new();
    render(v, depth, None, &mut out);
    out
}

fn json_preview(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => serde_json::to_string(&v).unwrap_or_else(|_| raw.to_string()),
        Err(_) => raw.lines().next().unwrap_or("").to_string(),
    }
}

fn apply_hscroll(s: &str, offset: usize) -> String {
    if offset == 0 {
        s.to_string()
    } else {
        s.chars().skip(offset).collect()
    }
}

fn record_detail_text(app: &AppState) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    if app.rows.is_empty() || app.selected_row >= app.rows.len() {
        return (vec![Line::from("No record")], vec![]);
    }
    let env = &app.rows[app.selected_row];
    let mut meta = Vec::new();
    meta.push(Line::from(vec![
        Span::styled("Partition ", Style::default().fg(ACCENT_FADED).add_modifier(Modifier::BOLD)),
        Span::styled(env.partition.to_string(), Style::default().fg(Color::White)),
        Span::raw("   "),
        Span::styled("Offset ", Style::default().fg(ACCENT_FADED).add_modifier(Modifier::BOLD)),
        Span::styled(env.offset.to_string(), Style::default().fg(Color::White)),
    ]));
    meta.push(Line::from(vec![
        Span::styled("Timestamp ", Style::default().fg(ACCENT_FADED).add_modifier(Modifier::BOLD)),
        Span::styled(fmt_ts(env.timestamp_ms, app.timestamps_use_utc), Style::default().fg(ACCENT)),
    ]));
    let key_text = if env.key.is_empty() {
        "(empty)".to_string()
    } else {
        env.key.clone()
    };
    meta.push(Line::from(vec![
        Span::styled("Key ", Style::default().fg(ACCENT_FADED).add_modifier(Modifier::BOLD)),
        Span::styled(key_text, Style::default().fg(Color::Yellow)),
    ]));

    let body = if let Some(v) = env.value.as_ref() {
        json_lines(&serde_json::from_str::<serde_json::Value>(v).unwrap_or(serde_json::Value::Null), 0)
    } else {
        vec![Line::from("null")]
    };
    (meta, body)
}

fn truncate_with_ellipsis(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i + 1 >= max {
            break;
        }
        out.push(ch);
    }
    out.push('…');
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

pub fn help_content_line_count() -> usize {
    HELP_LINES.len()
}
