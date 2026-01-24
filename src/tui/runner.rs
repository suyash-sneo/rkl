use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use crossterm::event::{
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use crate::app_config::{AppConfig, DefaultOrderDir, DefaultOrderField};
use crate::args::RunArgs;
use crate::consumer::spawn_partition_consumer;
use crate::merger::run_merger;
use crate::models::{MessageEnvelope, OffsetSpec};
use crate::output::OutputSink;
use crate::query::parser::ParseError;
use crate::query::{
    Command, OrderDir, OrderField, OrderSpec, SelectItem, SelectQuery, TimestampBounds,
    parse_command, parse_query,
};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::{Consumer, StreamConsumer};

use super::app::{
    AppConfigFieldFocus, AppState, CommandId, EnvEditor, EnvFieldFocus, EnvPemField, HomeFocus,
    QueryMode, ResultsMode, SPINNER_FRAMES, Screen, TuiEvent, COMMAND_SPECS,
    build_app_config_editor,
};
use super::env_store::Environment;
use super::pem_utils::{decode_literal_backslash_n, normalize_pem_input};
use super::query_bounds::{find_query_range, strip_trailing_semicolon};
use super::timefmt::fmt_ts;
use super::ui::{draw, help_content_line_count};

fn next_unique_env_name(envs: &[Environment]) -> String {
    let base = "New Env";
    let mut n = 1;
    loop {
        let candidate = format!("{} {}", base, n);
        if !envs.iter().any(|e| e.name.eq_ignore_ascii_case(&candidate)) {
            return candidate;
        }
        n += 1;
    }
}
use tui_textarea::{Input as TAInput, Key as TAKey, TextArea};

pub async fn run(args: RunArgs) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    // Enter alt screen, enable mouse, and request enhanced keyboard so Ctrl-Enter is detectable on supporting terminals (kitty/wezterm/xterm)
    execute!(
        stdout,
        terminal::EnterAlternateScreen,
        PushKeyboardEnhancementFlags(
            KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                | KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                | KeyboardEnhancementFlags::REPORT_ALTERNATE_KEYS,
        )
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tx_evt, mut rx_evt) = mpsc::unbounded_channel::<TuiEvent>();
    let mut app = AppState::new(args.query.clone().unwrap_or_default(), args.broker.clone());
    update_parse_status(&mut app);
    app.topics_last_fetched_at = Some(Instant::now());
    fetch_topics_async(&app, tx_evt.clone());

    let mut run_counter: u64 = 0;

    // Initial draw
    terminal.draw(|f| draw(f, &app))?;

    // Main loop
    let res = loop {
        // Handle transient pressed button animation
        if app.copy_btn_pressed {
            if let Some(deadline) = app.copy_btn_deadline {
                if Instant::now() >= deadline {
                    app.copy_btn_pressed = false;
                    app.copy_btn_deadline = None;
                }
            } else {
                app.copy_btn_pressed = false;
            }
        }
        if app.timestamp_switch_pressed {
            if let Some(deadline) = app.timestamp_switch_deadline {
                if Instant::now() >= deadline {
                    app.timestamp_switch_pressed = false;
                    app.timestamp_switch_deadline = None;
                }
            } else {
                app.timestamp_switch_pressed = false;
            }
        }
        if app.env_save_pressed {
            if let Some(deadline) = app.env_save_deadline {
                if Instant::now() >= deadline {
                    app.env_save_pressed = false;
                    app.env_save_deadline = None;
                }
            } else {
                app.env_save_pressed = false;
            }
        }
        if app.app_config_save_pressed {
            if let Some(deadline) = app.app_config_save_deadline {
                if Instant::now() >= deadline {
                    app.app_config_save_pressed = false;
                    app.app_config_save_deadline = None;
                }
            } else {
                app.app_config_save_pressed = false;
            }
        }
        if app.query_in_progress {
            app.query_spinner_idx = (app.query_spinner_idx + 1) % SPINNER_FRAMES.len();
        }

        if app.parse_status_dirty {
            update_parse_status(&mut app);
            app.parse_status_dirty = false;
        }

        if matches!(app.screen, Screen::Envs) && app.env_editor.is_none() {
            ensure_env_editor(&mut app);
        }

        // Draw UI
        terminal.draw(|f| draw(f, &app))?;

        // Drain any events from pipeline
        while let Ok(ev) = rx_evt.try_recv() {
            match ev {
                TuiEvent::Batch {
                    run_id,
                    mut rows,
                    total_emitted,
                } => {
                    if Some(run_id) == app.current_run {
                        app.push_rows(std::mem::take(&mut rows));
                        app.clamp_selection();
                        app.update_query_progress_rows(total_emitted);
                    }
                }
                TuiEvent::Snapshot {
                    run_id,
                    mut rows,
                    total_emitted,
                } => {
                    if Some(run_id) == app.current_run {
                        app.clear_rows();
                        app.push_rows(std::mem::take(&mut rows));
                        app.clamp_selection();
                        app.update_query_progress_rows(total_emitted);
                    }
                }
                TuiEvent::QueryPlan {
                    run_id,
                    planned_limit,
                } => {
                    if Some(run_id) == app.current_run {
                        app.query_limit = Some(planned_limit);
                        app.query_rows_seen = 0;
                    }
                }
                TuiEvent::Done { run_id } => {
                    if Some(run_id) == app.current_run {
                        app.query_in_progress = false;
                        app.query_started_at = None;
                        push_status_line(&mut app, format!("✔ Completed run {run_id}"));
                    }
                }
                TuiEvent::Error { run_id, message } => {
                    if Some(run_id) == app.current_run {
                        app.query_in_progress = false;
                        app.query_started_at = None;
                        push_status_line(&mut app, format!("✘ Error (run {run_id}): {message}"));
                    }
                }
                TuiEvent::EnvTestProgress { message } => {
                    app.env_test_in_progress = true;
                    app.env_test_message = Some(message.clone());
                    if !app.env_test_log.is_empty() {
                        app.env_test_log.push('\n');
                    }
                    app.env_test_log.push_str(&message);
                    let total_lines = app.env_test_log.lines().count();
                    app.env_conn_vscroll =
                        total_lines.saturating_sub(1).min(u16::MAX as usize) as u16;
                }
                TuiEvent::EnvTestDone { message } => {
                    app.env_test_in_progress = false;
                    app.env_test_message = Some(message.clone());
                    if !app.env_test_log.is_empty() {
                        app.env_test_log.push('\n');
                    }
                    app.env_test_log.push_str(&message);
                    let total_lines = app.env_test_log.lines().count();
                    app.env_conn_vscroll =
                        total_lines.saturating_sub(1).min(u16::MAX as usize) as u16;
                }
                TuiEvent::Topics(list) => {
                    app.topics = list;
                    refresh_topic_matches(&mut app);
                }
                TuiEvent::TopicsWithPartitions(list) => {
                    app.topics_with_partitions = list;
                    app.selected_row = 0;
                    if app.topics_with_partitions.len() == 1
                        && app.topics_with_partitions[0].0.starts_with("Error:")
                    {
                        let msg = app.topics_with_partitions[0].0.clone();
                        push_status_line(&mut app, msg);
                    } else if app.topics_with_partitions.is_empty() {
                        push_status_line(&mut app, "No topics found".to_string());
                    } else {
                        let count = app.topics_with_partitions.len();
                        push_status_line(&mut app, format!("Found {} topics", count));
                    }
                    app.clamp_selection();
                }
            }
        }

        // Handle key input (non-blocking poll)
        if crossterm::event::poll(Duration::from_millis(50))? {
            match crossterm::event::read()? {
                Event::Key(key) => {
                    if !(key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat) {
                        continue;
                    }
                    let KeyEvent { code, modifiers, .. } = key;
                    if matches!(code, KeyCode::Char('c') | KeyCode::Char('q'))
                        && modifiers.contains(KeyModifiers::CONTROL)
                    {
                        break Ok(());
                    }
                    if app.command_palette.open {
                        handle_command_palette_key(&mut app, key, &tx_evt);
                        continue;
                    }
                    if app.show_history_popup {
                        handle_history_popup_key(&mut app, code, modifiers);
                        continue;
                    }
                    if handle_global_shortcuts(&mut app, key, &tx_evt) {
                        continue;
                    }
                    match app.screen {
                        Screen::Home => {
                            handle_home_key(&mut app, &args, &tx_evt, &mut run_counter, key).await;
                        }
                        Screen::Envs => {
                            handle_env_key(&mut app, key, &tx_evt);
                        }
                        Screen::Info => {
                            handle_info_key(&mut app, key, &tx_evt);
                        }
                        Screen::AppConfig => {
                            handle_app_config_key(&mut app, key);
                        }
                        Screen::Help => {
                            handle_help_key(&mut app, key);
                        }
                        Screen::RecordDetail => {
                            handle_record_detail_key(&mut app, key);
                        }
                    }
                }
                Event::Mouse(_) => {}
                Event::Paste(s) => {
                    handle_paste_event(&mut app, &s);
                }
                _ => {}
            }
        }
    };

    // Restore terminal
    disable_raw_mode().ok();
    // Use crossterm global execute to restore screen
    execute!(
        std::io::stdout(),
        crossterm::event::DisableMouseCapture,
        PopKeyboardEnhancementFlags,
        terminal::LeaveAlternateScreen,
        crossterm::cursor::Show
    )
    .ok();

    res
}

async fn run_query_from_editor(
    app: &mut AppState,
    args: &RunArgs,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
    run_counter: &mut u64,
) {
    let text = textarea_text(&app.query_editor);
    let cursor = textarea_cursor_offset(&app.query_editor);
    let (qs, qe) = find_query_range(&text, cursor);
    let raw = text.get(qs..qe).unwrap_or("");
    let query = strip_trailing_semicolon(raw).trim().to_string();
    if query.is_empty() {
        push_status_line(app, "Please enter a query".to_string());
        return;
    }
    dispatch_query(app, args, tx_evt, run_counter, query, false).await;
}

async fn rerun_last_query(
    app: &mut AppState,
    args: &RunArgs,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
    run_counter: &mut u64,
) {
    if let Some(query) = app.last_executed_query.clone() {
        dispatch_query(app, args, tx_evt, run_counter, query, true).await;
    } else {
        push_status_line(app, "No previous query to re-run".to_string());
    }
}

async fn dispatch_query(
    app: &mut AppState,
    args: &RunArgs,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
    run_counter: &mut u64,
    query: String,
    is_rerun: bool,
) {
    match parse_command(&query) {
        Ok(Command::Select(ast)) => {
            let columns = ast.select.clone();
            app.results_mode = ResultsMode::Messages;
            app.selected_columns = columns;
            app.table_hscroll = 0;
            app.clear_rows();
            app.topics_with_partitions.clear();
            *run_counter += 1;
            app.current_run = Some(*run_counter);
            let query_limit = ast.limit.or(args.max_messages);
            app.query_in_progress = true;
            app.query_limit = query_limit;
            app.query_rows_seen = 0;
            app.query_started_at = Some(Instant::now());
            app.query_spinner_idx = 0;
            let env_host = app
                .selected_env()
                .map(|e| e.host.clone())
                .unwrap_or(app.host.clone());
            let default_order_override = if ast.order.is_none() {
                Some(app.app_config.default_order())
            } else {
                None
            };
            let plan_desc = describe_query_plan(&ast, default_order_override);
            let prefix = if is_rerun {
                "Re-running last query"
            } else {
                "Running"
            };
            push_status_line(
                app,
                format!(
                    "{} (run {}): topic '{}' on {} | {}. Press q to quit.",
                    prefix, *run_counter, ast.from, env_host, plan_desc
                ),
            );
            let mut run_args = args.clone();
            run_args.broker = env_host;
            app.clamp_selection();
            let ssl = app.current_ssl_config();
            spawn_pipeline_with_ssl(
                run_args,
                query.clone(),
                *run_counter,
                tx_evt.clone(),
                ssl,
                app.app_config.clone(),
            )
            .await;
            app.record_query_history(&query);
            app.last_executed_query = Some(query);
        }
        Ok(Command::ListTopics) => {
            app.results_mode = ResultsMode::TopicList;
            app.table_hscroll = 0;
            app.clear_rows();
            app.topics_with_partitions.clear();
            app.current_run = None;
            app.query_in_progress = false;
            app.query_started_at = None;
            app.query_rows_seen = 0;
            app.query_limit = None;
            app.selected_row = 0;
            app.json_vscroll = 0;
            let env_host = app
                .selected_env()
                .map(|e| e.host.clone())
                .unwrap_or(app.host.clone());
            push_status_line(app, format!("Listing topics from {}...", env_host));
            fetch_topics_with_partitions_async(app, tx_evt.clone());
            app.clamp_selection();
        }
        Err(e) => {
            handle_syntax_error(app, &e);
        }
    }
}

struct TuiOutput {
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    buffer: Vec<MessageEnvelope>,
    snapshot_mode: bool,
    total_emitted: usize,
}

impl TuiOutput {
    fn new(run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>, snapshot_mode: bool) -> Self {
        Self {
            run_id,
            tx,
            buffer: Vec::with_capacity(256),
            snapshot_mode,
            total_emitted: 0,
        }
    }
}

impl OutputSink for TuiOutput {
    fn push(&mut self, env: &MessageEnvelope) {
        self.total_emitted += 1;
        self.buffer.push(env.clone());
    }
    fn flush_block(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.buffer);
        let evt = if self.snapshot_mode {
            TuiEvent::Snapshot {
                run_id: self.run_id,
                rows: out,
                total_emitted: self.total_emitted,
            }
        } else {
            TuiEvent::Batch {
                run_id: self.run_id,
                rows: out,
                total_emitted: self.total_emitted,
            }
        };
        let _ = self.tx.send(evt);
    }
}

// Spawn pipeline but with ssl provided
async fn spawn_pipeline_with_ssl(
    args: RunArgs,
    query_text: String,
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    ssl: Option<crate::models::SslConfig>,
    app_config: AppConfig,
) {
    tokio::spawn(async move {
        if let Err(e) =
            run_pipeline_with_ssl(args, query_text, run_id, tx.clone(), ssl, app_config).await
        {
            let _ = tx.send(TuiEvent::Error {
                run_id,
                message: e.to_string(),
            });
        }
    });
}

async fn run_pipeline_with_ssl(
    args: RunArgs,
    query_text: String,
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    ssl: Option<crate::models::SslConfig>,
    app_config: AppConfig,
) -> Result<()> {
    let mut ast = parse_query(&query_text).context("Failed to parse query")?;
    let topic = ast.from.clone();
    let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &args.broker)
        .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true");
    cfg.set_log_level(RDKafkaLogLevel::Emerg);
    if let Some(ssl) = &ssl {
        if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
            cfg.set("security.protocol", "ssl");
            if let Some(ref s) = ssl.ca_pem {
                cfg.set("ssl.ca.pem", s);
            }
            if let Some(ref s) = ssl.cert_pem {
                cfg.set("ssl.certificate.pem", s);
            }
            if let Some(ref s) = ssl.key_pem {
                cfg.set("ssl.key.pem", s);
            }
        }
    }
    struct QuietContext;
    impl ClientContext for QuietContext {
        fn log(&self, _level: RDKafkaLogLevel, _fac: &str, _log_message: &str) {}
    }
    impl ConsumerContext for QuietContext {}

    let probe_consumer: StreamConsumer<QuietContext> = cfg
        .create_with_context(QuietContext)
        .context("Failed to create probe consumer")?;

    let metadata = probe_consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(10))
        .context("Failed to fetch metadata")?;
    let topic_md = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("Topic not found: {}", topic))?;
    let partitions: Vec<i32> = topic_md.partitions().iter().map(|p| p.id()).collect();
    let partition_count = partitions.len().max(1);
    if ast.order.is_none() {
        let (field, dir) = app_config.default_order();
        ast.order = Some(OrderSpec { field, dir });
    }
    let base_limit = ast
        .limit
        .or(args.max_messages)
        .or_else(|| app_config.default_limit)
        .or_else(|| Some(app_config.query_scan_multiplier * partition_count));
    let plan = ast.execution_plan(partition_count, base_limit);
    let max_messages_global = Some(plan.n_global);
    let order_desc = plan.order_desc;
    let per_partition_limit = plan.per_partition_limit;
    let global_sort_by_timestamp = plan.global_sort_by_timestamp;
    let _ = tx.send(TuiEvent::QueryPlan {
        run_id,
        planned_limit: plan.n_global,
    });

    let (tx_msg, rx_msg) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);
    let offset_spec = OffsetSpec::from_str(&args.offset).unwrap_or(OffsetSpec::Beginning);
    let query_arc = std::sync::Arc::new(ast.clone());
    let query_limit = per_partition_limit;

    let mut joinset = tokio::task::JoinSet::new();
    for &p in &partitions {
        let txp = tx_msg.clone();
        let mut a = args.clone();
        a.topic = Some(topic.clone());
        a.keys_only = keys_only;
        a.max_messages = None;
        let q = Some(query_arc.clone());
        let limit = query_limit;
        let ssl_clone = ssl.clone();
        let multiplier = app_config.query_scan_multiplier;
        joinset.spawn(async move {
            spawn_partition_consumer(a, p, offset_spec, txp, q, limit, ssl_clone, multiplier).await
        });
    }
    drop(tx_msg);

    let snapshot_mode = max_messages_global.is_some();
    let mut sink = TuiOutput::new(run_id, tx.clone(), snapshot_mode);
    run_merger(
        rx_msg,
        &mut sink,
        args.watermark,
        args.flush_interval_ms,
        max_messages_global,
        order_desc,
        partition_count,
        true,
        global_sort_by_timestamp,
    )
    .await?;

    while let Some(res) = joinset.join_next().await {
        let _ = res;
    }

    let _ = tx.send(TuiEvent::Done { run_id });
    Ok(())
}

fn selected_cell_text(app: &AppState) -> Option<String> {
    if app.rows.is_empty() {
        return None;
    }
    if app.selected_columns.is_empty() {
        return None;
    }
    let idx = app.selected_row.min(app.rows.len() - 1);
    let env = &app.rows[idx];
    let col_idx = app
        .selected_col
        .min(app.selected_columns.len().saturating_sub(1));
    let col = app.selected_columns[col_idx];
    Some(runner_column_text(env, col, app.timestamps_use_utc))
}

fn runner_column_text(env: &MessageEnvelope, col: SelectItem, use_utc: bool) -> String {
    match col {
        SelectItem::Partition => env.partition.to_string(),
        SelectItem::Offset => env.offset.to_string(),
        SelectItem::Timestamp => fmt_ts(env.timestamp_ms, use_utc),
        SelectItem::Key => env.key.clone(),
        SelectItem::Value => env.value.as_deref().unwrap_or("null").to_string(),
    }
}

fn copy_to_clipboard(s: &str) -> Result<()> {
    let mut cb = arboard::Clipboard::new().context("open clipboard")?;
    cb.set_text(s.to_string()).context("set clipboard text")?;
    Ok(())
}

fn describe_query_plan(ast: &SelectQuery, default_order: Option<(OrderField, OrderDir)>) -> String {
    let (field, dir, is_default, from_config) = match (ast.order.as_ref(), default_order) {
        (Some(spec), _) => (spec.field, spec.dir, false, false),
        (None, Some((f, d))) => (f, d, true, true),
        (None, None) => (OrderField::Poffset, OrderDir::Desc, true, false),
    };
    let mut order_label = match (field, dir) {
        (OrderField::Timestamp, OrderDir::Asc) => "timestamp ASC".to_string(),
        (OrderField::Timestamp, OrderDir::Desc) => "timestamp DESC".to_string(),
        (OrderField::Poffset, OrderDir::Asc) => "poffset ASC".to_string(),
        (OrderField::Poffset, OrderDir::Desc) => "poffset DESC".to_string(),
        (OrderField::PoffsetTs, OrderDir::Asc) => {
            "poffset_ts ASC (scan by offset, sort by timestamp)".to_string()
        }
        (OrderField::PoffsetTs, OrderDir::Desc) => {
            "poffset_ts DESC (scan by offset, sort by timestamp)".to_string()
        }
    };
    if is_default {
        if from_config {
            order_label.push_str(" (default from config)");
        } else {
            order_label.push_str(" (default)");
        }
    }
    let mut parts = vec![format!("order={}", order_label)];
    if let Some(bounds) = ast
        .r#where
        .as_ref()
        .and_then(|expr| expr.timestamp_bounds())
    {
        if let Some(text) = format_timestamp_bounds(bounds) {
            parts.push(text);
        }
    }
    parts.join(" | ")
}

fn handle_syntax_error(app: &mut AppState, err: &ParseError) {
    let message = format_syntax_error_message(err);
    push_status_line(app, message);
    app.query_in_progress = false;
    app.query_started_at = None;
}

fn push_status_line(app: &mut AppState, message: impl Into<String>) {
    if !app.status.is_empty() {
        if !app.status_buffer.is_empty() {
            app.status_buffer.push_str("\n───\n");
        }
        app.status_buffer.push_str(&app.status);
    }
    app.status = message.into();
    app.status_vscroll = 0;
}

fn format_syntax_error_message(err: &ParseError) -> String {
    if let Some(hint) = parse_error_hint(err) {
        format!("✘ Syntax error: {}. Hint: {}", err, hint)
    } else {
        format!("✘ Syntax error: {}", err)
    }
}

fn parse_error_hint(err: &ParseError) -> Option<String> {
    use ParseError::*;
    match err {
        UnexpectedEof => Some(
            "The statement ended unexpectedly; check for missing FROM, WHERE, or closing parentheses."
                .to_string(),
        ),
        UnexpectedToken(tok) => {
            let snippet = preview_error_snippet(tok);
            if snippet.is_empty() {
                Some("Check for stray characters and ensure the query follows SELECT ... FROM ... syntax.".to_string())
            } else {
                Some(format!(
                    "Check the syntax near '{}'; ensure clauses like SELECT, FROM, WHERE, and ORDER BY are in the right order.",
                    snippet
                ))
            }
        }
        ExpectedKeyword(kw) => Some(format!("Add the '{}' keyword at this position.", kw)),
        ExpectedIdentifier => Some(
            "Provide a topic or column name after this keyword (e.g. FROM my_topic).".to_string(),
        ),
        ExpectedNumber => Some("Provide a numeric value (e.g. LIMIT 100).".to_string()),
        ExpectedLiteral => Some(
            "Provide a literal value (string/number/bool) to compare against, such as 'value' or 42."
                .to_string(),
        ),
        ExpectedPath => Some(
            "JSON paths must start with key, value, or timestamp (e.g. value->field).".to_string(),
        ),
        InvalidOrderByField(_) => Some(
            "ORDER BY supports timestamp, poffset, or poffset_ts.".to_string(),
        ),
    }
}

fn preview_error_snippet(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        let mut snippet: String = trimmed.chars().take(40).collect();
        if trimmed.chars().count() > 40 {
            snippet.push('…');
        }
        snippet
    }
}

fn format_timestamp_bounds(bounds: TimestampBounds) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(lower) = bounds.lower {
        let op = if lower.inclusive { ">=" } else { ">" };
        parts.push(format!("timestamp {} {}", op, fmt_ts(lower.millis, true)));
    }
    if let Some(upper) = bounds.upper {
        let op = if upper.inclusive { "<=" } else { "<" };
        parts.push(format!("timestamp {} {}", op, fmt_ts(upper.millis, true)));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

fn handle_env_editor_paste(app: &mut AppState, raw: &str) -> bool {
    if matches!(
        app.env_editor.as_ref().map(|e| e.field_focus),
        Some(EnvFieldFocus::Conn)
    ) {
        let text = normalize_plain_input(raw);
        app.env_test_message = Some(text);
        return true;
    }
    let mut handled = false;
    let mut meta_changed = false;
    if let Some(ed) = app.env_editor.as_mut() {
        match ed.field_focus {
            EnvFieldFocus::Name => {
                let text = normalize_plain_input(raw);
                handled = true;
                if !text.is_empty() {
                    insert_text_at_cursor(&mut ed.name, &mut ed.name_cursor, &text);
                    meta_changed = true;
                }
            }
            EnvFieldFocus::Host => {
                let text = normalize_plain_input(raw);
                handled = true;
                if !text.is_empty() {
                    insert_text_at_cursor(&mut ed.host, &mut ed.host_cursor, &text);
                    meta_changed = true;
                }
            }
            EnvFieldFocus::PemEditor => {
                let text = normalize_pem_input(raw);
                handled = true;
                match ed.active_pem {
                    EnvPemField::PrivateKey => {
                        ed.ta_private.insert_str(text);
                    }
                    EnvPemField::PublicKey => {
                        ed.ta_public.insert_str(text);
                    }
                    EnvPemField::Ca => {
                        ed.ta_ca.insert_str(text);
                    }
                }
            }
            EnvFieldFocus::Conn | EnvFieldFocus::Buttons | EnvFieldFocus::List => {}
        }
    }
    if meta_changed {
        sync_env_metadata_from_editor(app);
    }
    handled
}

fn ensure_app_config_editor(app: &mut AppState) {
    if app.app_config_editor.is_none() {
        let ed = build_app_config_editor(&app.app_config);
        app.app_config_editor = Some(ed);
    }
}

fn ensure_env_editor(app: &mut AppState) {
    if app.env_editor.is_none() {
        let (idx, env) = if let Some(i) = app.env_store.selected {
            if let Some(e) = app.env_store.envs.get(i) {
                (Some(i), e.clone())
            } else {
                (None, Environment::default())
            }
        } else {
            (None, Environment::default())
        };
        let mut editor = build_env_editor_from_env(&env, idx);
        editor.name_cursor = editor.name.len();
        editor.host_cursor = editor.host.len();
        app.env_editor = Some(editor);
    }
}

fn save_app_config_from_editor(app: &mut AppState) -> Result<()> {
    let Some(ed) = app.app_config_editor.as_ref() else {
        return Ok(());
    };
    let mul: usize = ed
        .query_scan_multiplier
        .trim()
        .parse()
        .context("query scan multiplier must be a positive number")?;
    if mul == 0 {
        anyhow::bail!("query scan multiplier must be > 0");
    }
    let default_limit = if ed.default_limit.trim().is_empty() {
        None
    } else {
        let parsed: usize = ed
            .default_limit
            .trim()
            .parse()
            .context("default LIMIT must be a positive number")?;
        Some(parsed)
    };
    let order_field = match ed.default_order_field_idx {
        0 => DefaultOrderField::Timestamp,
        1 => DefaultOrderField::Poffset,
        _ => DefaultOrderField::PoffsetTs,
    };
    let order_dir = match ed.default_order_dir_idx {
        0 => DefaultOrderDir::Asc,
        _ => DefaultOrderDir::Desc,
    };
    let mut cfg = app.app_config.clone();
    cfg.query_scan_multiplier = mul;
    cfg.default_limit = default_limit;
    cfg.default_order_field = order_field;
    cfg.default_order_dir = order_dir;
    cfg.default_timestamps_use_utc = ed.timestamps_use_utc;
    cfg.save()?;
    app.app_config = cfg;
    app.timestamps_use_utc = app.app_config.default_timestamps_use_utc;
    Ok(())
}

fn attempt_save_app_config(app: &mut AppState) {
    match save_app_config_from_editor(app) {
        Ok(()) => {
            app.app_config_save_pressed = true;
            app.app_config_save_deadline = Some(Instant::now() + Duration::from_millis(150));
            push_status_line(app, "✔ App config saved".to_string());
        }
        Err(e) => push_status_line(app, format!("App config error: {}", e)),
    }
}

fn move_env_selection(app: &mut AppState, delta: isize) {
    if app.env_store.envs.is_empty() {
        return;
    }
    let len = app.env_store.envs.len() as isize;
    let current = app
        .env_store
        .selected
        .unwrap_or(0)
        .min(len.saturating_sub(1) as usize);
    let mut next = current as isize + delta;
    if next < 0 {
        next = 0;
    }
    if next >= len {
        next = len - 1;
    }
    if current == next as usize {
        return;
    }
    app.env_store.selected = Some(next as usize);
    sync_env_editor_to_selection(app);
}

fn sync_env_editor_to_selection(app: &mut AppState) {
    if let (Some(ed), Some(idx)) = (app.env_editor.as_mut(), app.env_store.selected) {
        if let Some(env) = app.env_store.envs.get(idx) {
            load_env_into_editor(ed, env, idx);
        }
    }
}

fn load_env_into_editor(ed: &mut EnvEditor, env: &Environment, idx: usize) {
    ed.idx = Some(idx);
    ed.name = env.name.clone();
    ed.host = env.host.clone();
    ed.name_cursor = ed.name_cursor.min(ed.name.len());
    ed.host_cursor = ed.host_cursor.min(ed.host.len());
    ed.ta_private = text_area_from_string(env.private_key_pem.clone().unwrap_or_default());
    ed.ta_public = text_area_from_string(env.public_key_pem.clone().unwrap_or_default());
    ed.ta_ca = text_area_from_string(env.ssl_ca_pem.clone().unwrap_or_default());
}

fn text_area_from_string(input: String) -> TextArea<'static> {
    let decoded = decode_literal_backslash_n(&input);
    let mut ta = TextArea::from(decoded.lines());
    ta.set_tab_length(0);
    ta
}

fn build_env_editor_from_env(env: &Environment, idx: Option<usize>) -> EnvEditor {
    EnvEditor {
        idx,
        name: env.name.clone(),
        name_cursor: 0,
        host: env.host.clone(),
        host_cursor: 0,
        ta_private: text_area_from_string(env.private_key_pem.clone().unwrap_or_default()),
        ta_public: text_area_from_string(env.public_key_pem.clone().unwrap_or_default()),
        ta_ca: text_area_from_string(env.ssl_ca_pem.clone().unwrap_or_default()),
        active_pem: EnvPemField::PrivateKey,
        field_focus: EnvFieldFocus::List,
    }
}

fn sync_env_metadata_from_editor(app: &mut AppState) {
    let (idx, name, host) = if let Some(ed) = app.env_editor.as_ref() {
        (ed.idx, ed.name.clone(), ed.host.clone())
    } else {
        return;
    };
    if let Some(idx) = idx {
        if let Some(env) = app.env_store.envs.get_mut(idx) {
            env.name = name;
            env.host = host;
        }
    }
}

// (Removed unused test_connection)

fn handle_history_popup_key(app: &mut AppState, code: KeyCode, modifiers: KeyModifiers) {
    if app.query_history.is_empty() {
        app.show_history_popup = false;
        return;
    }
    let len = app.query_history.len();
    if app.history_selected_index >= len {
        app.history_selected_index = len.saturating_sub(1);
    }
    match code {
        KeyCode::Up => {
            if app.history_selected_index > 0 {
                app.history_selected_index -= 1;
            }
        }
        KeyCode::Down => {
            if app.history_selected_index + 1 < len {
                app.history_selected_index += 1;
            }
        }
        KeyCode::PageUp => {
            let step = 5.min(len);
            app.history_selected_index = app.history_selected_index.saturating_sub(step);
        }
        KeyCode::PageDown => {
            if len > 0 {
                let step = 5.min(len);
                let max_idx = len.saturating_sub(1);
                let next = app.history_selected_index.saturating_add(step);
                app.history_selected_index = next.min(max_idx);
            }
        }
        KeyCode::Enter => {
            load_history_entry_into_editor(app);
        }
        KeyCode::Esc => {
            app.show_history_popup = false;
        }
        KeyCode::Char('r') if modifiers.contains(KeyModifiers::CONTROL) => {
            app.show_history_popup = false;
        }
        _ => {}
    }
}

fn load_history_entry_into_editor(app: &mut AppState) {
    if app.query_history.is_empty() {
        app.show_history_popup = false;
        return;
    }
    let idx = app
        .history_selected_index
        .min(app.query_history.len().saturating_sub(1));
    let entry = app.query_history[idx].clone();
    app.query_mode = QueryMode::Advanced;
    app.home_focus = HomeFocus::AdvancedQuery;
    reset_query_editor(app, &entry);
    app.show_history_popup = false;
    app.parse_status_dirty = true;
}

fn normalize_plain_input(raw: &str) -> String {
    raw.replace('\r', "")
}

fn insert_text_at_cursor(target: &mut String, cursor: &mut usize, text: &str) {
    if text.is_empty() {
        return;
    }
    let idx = (*cursor).min(target.len());
    target.insert_str(idx, text);
    *cursor = idx + text.len();
}

fn ta_input_from_key(key: KeyEvent) -> TAInput {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);
    let shift = key.modifiers.contains(KeyModifiers::SHIFT);
    let tkey = match key.code {
        KeyCode::Char(c) => TAKey::Char(c),
        KeyCode::Enter => TAKey::Enter,
        KeyCode::Backspace => TAKey::Backspace,
        KeyCode::Delete => TAKey::Delete,
        KeyCode::Left => TAKey::Left,
        KeyCode::Right => TAKey::Right,
        KeyCode::Up => TAKey::Up,
        KeyCode::Down => TAKey::Down,
        KeyCode::Home => TAKey::Home,
        KeyCode::End => TAKey::End,
        KeyCode::PageUp => TAKey::PageUp,
        KeyCode::PageDown => TAKey::PageDown,
        KeyCode::Tab => TAKey::Tab,
        _ => TAKey::Null,
    };
    TAInput {
        key: tkey,
        ctrl,
        alt,
        shift,
    }
}

fn textarea_text(ta: &TextArea) -> String {
    ta.lines().join("\n")
}

fn textarea_cursor_offset(ta: &TextArea) -> usize {
    let (row, col) = ta.cursor();
    let mut offset = 0usize;
    for (idx, line) in ta.lines().iter().enumerate() {
        if idx == row {
            offset += line
                .chars()
                .take(col)
                .map(|c| c.len_utf8())
                .sum::<usize>();
            return offset;
        }
        offset += line.len() + 1;
    }
    offset
}

fn reset_query_editor(app: &mut AppState, text: &str) {
    let mut ta = if text.trim().is_empty() {
        TextArea::default()
    } else {
        TextArea::from(text.lines())
    };
    ta.set_tab_length(2);
    ta.set_placeholder_text("Write a SELECT query...");
    app.query_editor = ta;
}

fn open_command_palette(app: &mut AppState) {
    app.command_palette.open = true;
    let mut ta = TextArea::default();
    ta.set_tab_length(2);
    ta.set_placeholder_text("Type a command");
    app.command_palette.input = ta;
    app.command_palette.selected = 0;
    update_command_palette_matches(app);
}

fn update_command_palette_matches(app: &mut AppState) {
    let filter = textarea_text(&app.command_palette.input);
    let filter = filter.trim();
    let mut matches: Vec<usize> = Vec::new();
    if filter.is_empty() {
        matches.extend(0..COMMAND_SPECS.len());
    } else {
        let matcher = SkimMatcherV2::default();
        let mut scored: Vec<(i64, usize)> = Vec::new();
        for (idx, cmd) in COMMAND_SPECS.iter().enumerate() {
            if let Some(score) = matcher.fuzzy_match(cmd.label, filter) {
                scored.push((score, idx));
            }
        }
        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| COMMAND_SPECS[a.1].label.cmp(COMMAND_SPECS[b.1].label))
        });
        matches.extend(scored.into_iter().map(|(_, idx)| idx));
    }
    app.command_palette.matches = matches;
    if app.command_palette.matches.is_empty() {
        app.command_palette.selected = 0;
    } else {
        app.command_palette.selected = app
            .command_palette
            .selected
            .min(app.command_palette.matches.len().saturating_sub(1));
    }
}

fn execute_command(app: &mut AppState, cmd_idx: usize, tx_evt: &mpsc::UnboundedSender<TuiEvent>) {
    let Some(cmd) = COMMAND_SPECS.get(cmd_idx) else {
        return;
    };
    match cmd.id {
        CommandId::SwitchToBasic => {
            app.query_mode = QueryMode::Basic;
            app.home_focus = HomeFocus::TopicFilter;
            app.screen = Screen::Home;
            app.parse_status_dirty = true;
        }
        CommandId::SwitchToAdvanced => {
            app.query_mode = QueryMode::Advanced;
            app.home_focus = HomeFocus::AdvancedQuery;
            app.screen = Screen::Home;
            app.parse_status_dirty = true;
        }
        CommandId::OpenEnvs => {
            app.screen = Screen::Envs;
            ensure_env_editor(app);
        }
        CommandId::OpenAppConfig => {
            app.screen = Screen::AppConfig;
            ensure_app_config_editor(app);
        }
        CommandId::OpenInfo => {
            app.screen = Screen::Info;
            app.topics_last_fetched_at = Some(Instant::now());
            fetch_topics_async(app, tx_evt.clone());
        }
        CommandId::OpenHelp => {
            app.last_screen_before_help = Some(app.screen);
            app.screen = Screen::Help;
            app.help_vscroll = 0;
        }
        CommandId::OpenHistory => {
            if app.query_history.is_empty() {
                push_status_line(app, "No query history yet".to_string());
            } else {
                app.show_history_popup = true;
                app.history_selected_index = app.query_history.len().saturating_sub(1);
            }
        }
        CommandId::RefreshTopics => {
            app.topics_last_fetched_at = Some(Instant::now());
            fetch_topics_async(app, tx_evt.clone());
        }
        CommandId::ToggleTimestampMode => {
            toggle_timestamp_display(app);
        }
        CommandId::ClearResults => {
            app.clear_rows();
            app.topics_with_partitions.clear();
            app.selected_row = 0;
            app.selected_col = 0;
            app.json_vscroll = 0;
            app.table_hscroll = 0;
        }
    }
}

fn handle_command_palette_key(
    app: &mut AppState,
    key: KeyEvent,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
) {
    match key.code {
        KeyCode::Esc => {
            app.command_palette.open = false;
        }
        KeyCode::Enter => {
            if let Some(idx) = app
                .command_palette
                .matches
                .get(app.command_palette.selected)
                .copied()
            {
                execute_command(app, idx, tx_evt);
            }
            app.command_palette.open = false;
        }
        KeyCode::Up => {
            if app.command_palette.selected > 0 {
                app.command_palette.selected -= 1;
            }
        }
        KeyCode::Down => {
            if app.command_palette.selected + 1 < app.command_palette.matches.len() {
                app.command_palette.selected += 1;
            }
        }
        KeyCode::PageUp => {
            let step = 5.min(app.command_palette.matches.len());
            app.command_palette.selected = app.command_palette.selected.saturating_sub(step);
        }
        KeyCode::PageDown => {
            let len = app.command_palette.matches.len();
            if len > 0 {
                let step = 5.min(len);
                let next = app.command_palette.selected.saturating_add(step);
                app.command_palette.selected = next.min(len.saturating_sub(1));
            }
        }
        KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete | KeyCode::Left | KeyCode::Right
        | KeyCode::Home | KeyCode::End => {
            let modified = app.command_palette.input.input(ta_input_from_key(key));
            if modified {
                update_command_palette_matches(app);
            }
        }
        _ => {}
    }
}

fn handle_global_shortcuts(
    app: &mut AppState,
    key: KeyEvent,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
) -> bool {
    let code = key.code;
    let modifiers = key.modifiers;
    if modifiers.is_empty() && matches!(code, KeyCode::Char(':')) {
        open_command_palette(app);
        return true;
    }
    if modifiers.contains(KeyModifiers::CONTROL) && matches!(code, KeyCode::Char('p')) {
        open_command_palette(app);
        return true;
    }
    if modifiers.is_empty() && matches!(code, KeyCode::Char('?')) {
        app.last_screen_before_help = Some(app.screen);
        app.screen = Screen::Help;
        app.help_vscroll = 0;
        return true;
    }
    if matches!(code, KeyCode::F(2)) {
        app.screen = Screen::Envs;
        app.show_history_popup = false;
        ensure_env_editor(app);
        return true;
    }
    if matches!(code, KeyCode::F(3)) {
        app.screen = Screen::AppConfig;
        app.show_history_popup = false;
        ensure_app_config_editor(app);
        return true;
    }
    if matches!(code, KeyCode::F(12)) {
        app.screen = Screen::Info;
        app.show_history_popup = false;
        app.topics_last_fetched_at = Some(Instant::now());
        fetch_topics_async(app, tx_evt.clone());
        return true;
    }
    if matches!(code, KeyCode::F(8)) {
        app.screen = Screen::Home;
        return true;
    }
    false
}

async fn handle_home_key(
    app: &mut AppState,
    args: &RunArgs,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
    run_counter: &mut u64,
    key: KeyEvent,
) {
    let code = key.code;
    let modifiers = key.modifiers;
    let ctrl = modifiers.contains(KeyModifiers::CONTROL);
    let shift = modifiers.contains(KeyModifiers::SHIFT);

    if ctrl && matches!(code, KeyCode::Char('r')) {
        if shift {
            rerun_last_query(app, args, tx_evt, run_counter).await;
        } else if app.query_history.is_empty() {
            push_status_line(app, "No query history yet".to_string());
        } else {
            app.show_history_popup = true;
            app.history_selected_index = app.query_history.len().saturating_sub(1);
        }
        return;
    }

    if ctrl
        && (matches!(code, KeyCode::Enter)
            || matches!(code, KeyCode::Char('j') | KeyCode::Char('m')))
    {
        if shift {
            rerun_last_query(app, args, tx_evt, run_counter).await;
        } else if matches!(app.query_mode, QueryMode::Basic) {
            run_basic_query(app, args, tx_evt, run_counter).await;
        } else {
            run_query_from_editor(app, args, tx_evt, run_counter).await;
        }
        return;
    }

    if ctrl && matches!(code, KeyCode::Char('y')) {
        if matches!(app.query_mode, QueryMode::Advanced) {
            insert_selected_topic_into_query(app);
            app.parse_status_dirty = true;
        }
        return;
    }

    if matches!(code, KeyCode::Tab | KeyCode::Char('\t')) {
        cycle_home_focus(app, true);
        return;
    }
    if matches!(code, KeyCode::BackTab) {
        cycle_home_focus(app, false);
        return;
    }

    if matches!(code, KeyCode::F(5)) {
        if let Some(s) = selected_cell_text(app) {
            if let Err(e) = copy_to_clipboard(&s) {
                push_status_line(app, format!("Clipboard error: {}", e));
            } else {
                push_status_line(app, "Copied to clipboard".to_string());
            }
        }
        return;
    }

    if matches!(code, KeyCode::Esc) {
        app.home_focus = HomeFocus::TopicFilter;
        return;
    }

    match app.home_focus {
        HomeFocus::TopicFilter => {
            if matches!(code, KeyCode::Enter | KeyCode::Down) {
                app.home_focus = HomeFocus::TopicList;
                return;
            }
            let (open_palette, modified) = {
                let Some(ta) = home_focus_textarea_mut(app, HomeFocus::TopicFilter) else {
                    return;
                };
                if should_open_palette_on_double_slash(ta, key) {
                    (true, false)
                } else {
                    (false, ta.input(ta_input_from_key(key)))
                }
            };
            if open_palette {
                open_command_palette(app);
                return;
            }
            if modified {
                refresh_topic_matches(app);
            }
        }
        HomeFocus::TopicList => {
            let total = app.topic_picker.matches.len();
            match code {
                KeyCode::Up => {
                    if app.topic_picker.selected > 0 {
                        app.topic_picker.selected -= 1;
                    }
                }
                KeyCode::Down => {
                    if app.topic_picker.selected + 1 < total {
                        app.topic_picker.selected += 1;
                    }
                }
                KeyCode::PageUp => {
                    let step = 5.min(total);
                    app.topic_picker.selected = app.topic_picker.selected.saturating_sub(step);
                }
                KeyCode::PageDown => {
                    if total > 0 {
                        let step = 5.min(total);
                        let next = app.topic_picker.selected.saturating_add(step);
                        app.topic_picker.selected = next.min(total.saturating_sub(1));
                    }
                }
                KeyCode::Left => {
                    app.home_focus = HomeFocus::TopicFilter;
                }
                KeyCode::Right | KeyCode::Enter => {
                    app.home_focus = if matches!(app.query_mode, QueryMode::Basic) {
                        HomeFocus::BasicSearch
                    } else {
                        HomeFocus::AdvancedQuery
                    };
                }
                _ => {}
            }
        }
        HomeFocus::BasicSearch
        | HomeFocus::BasicWhere
        | HomeFocus::BasicSince
        | HomeFocus::BasicUntil
        | HomeFocus::BasicLimit => {
            if matches!(code, KeyCode::Enter) {
                cycle_home_focus(app, true);
                return;
            }
            let open_palette = {
                let Some(ta) = home_focus_textarea_mut(app, app.home_focus) else {
                    return;
                };
                if should_open_palette_on_double_slash(ta, key) {
                    true
                } else {
                    let _ = ta.input(ta_input_from_key(key));
                    false
                }
            };
            if open_palette {
                open_command_palette(app);
                return;
            }
        }
        HomeFocus::BasicOrderField => {
            match code {
                KeyCode::Left => {
                    if app.basic_query.order_field_idx > 0 {
                        app.basic_query.order_field_idx -= 1;
                    }
                }
                KeyCode::Right => {
                    if app.basic_query.order_field_idx < 2 {
                        app.basic_query.order_field_idx += 1;
                    }
                }
                KeyCode::Enter => {
                    cycle_home_focus(app, true);
                }
                _ => {}
            }
        }
        HomeFocus::BasicOrderDir => {
            match code {
                KeyCode::Left | KeyCode::Right => {
                    app.basic_query.order_dir_idx = if app.basic_query.order_dir_idx == 0 {
                        1
                    } else {
                        0
                    };
                }
                KeyCode::Enter => {
                    cycle_home_focus(app, true);
                }
                _ => {}
            }
        }
        HomeFocus::AdvancedQuery => {
            let open_palette = should_open_palette_on_double_slash(&mut app.query_editor, key);
            if open_palette {
                open_command_palette(app);
                return;
            }
            let modified = app.query_editor.input(ta_input_from_key(key));
            if modified {
                app.parse_status_dirty = true;
            }
        }
        HomeFocus::Results => {
            let total = total_results_rows(app);
            if modifiers.contains(KeyModifiers::SHIFT)
                && matches!(code, KeyCode::Left | KeyCode::Right)
            {
                if matches!(code, KeyCode::Left) {
                    app.table_hscroll = app.table_hscroll.saturating_sub(1);
                } else {
                    app.table_hscroll = app.table_hscroll.saturating_add(1);
                }
                return;
            }
            match code {
                KeyCode::Up => {
                    if app.selected_row > 0 {
                        app.selected_row -= 1;
                        if matches!(app.results_mode, ResultsMode::Messages) {
                            app.json_vscroll = 0;
                        }
                    }
                }
                KeyCode::Down => {
                    if total > 0 {
                        let max_idx = total.saturating_sub(1);
                        let next = app.selected_row.saturating_add(1);
                        app.selected_row = next.min(max_idx);
                        if matches!(app.results_mode, ResultsMode::Messages) {
                            app.json_vscroll = 0;
                        }
                    }
                }
                KeyCode::Left => {
                    if app.selected_col > 0 {
                        app.selected_col -= 1;
                        app.json_vscroll = 0;
                    }
                }
                KeyCode::Right => {
                    let cols = app.selected_columns.len();
                    if app.selected_col + 1 < cols {
                        app.selected_col += 1;
                        app.json_vscroll = 0;
                    }
                }
                KeyCode::PageUp => {
                    if app.selected_row > 10 {
                        app.selected_row -= 10;
                    } else {
                        app.selected_row = 0;
                    }
                    if matches!(app.results_mode, ResultsMode::Messages) {
                        app.json_vscroll = 0;
                    }
                }
                KeyCode::PageDown => {
                    if total > 0 {
                        let max_idx = total.saturating_sub(1);
                        let next = app.selected_row.saturating_add(10);
                        app.selected_row = next.min(max_idx);
                        if matches!(app.results_mode, ResultsMode::Messages) {
                            app.json_vscroll = 0;
                        }
                    }
                }
                KeyCode::Home => {
                    app.selected_row = 0;
                    if matches!(app.results_mode, ResultsMode::Messages) {
                        app.json_vscroll = 0;
                    }
                }
                KeyCode::End => {
                    if total > 0 {
                        app.selected_row = total.saturating_sub(1);
                        if matches!(app.results_mode, ResultsMode::Messages) {
                            app.json_vscroll = 0;
                        }
                    }
                }
                KeyCode::Enter => {
                    if !app.rows.is_empty() {
                        app.screen = Screen::RecordDetail;
                        app.record_detail_scroll = 0;
                    }
                }
                _ => {}
            }
        }
        HomeFocus::Details => match code {
            KeyCode::Up => {
                app.json_vscroll = app.json_vscroll.saturating_sub(1);
            }
            KeyCode::Down => {
                app.json_vscroll = app.json_vscroll.saturating_add(1);
            }
            KeyCode::PageUp => {
                app.json_vscroll = app.json_vscroll.saturating_sub(5);
            }
            KeyCode::PageDown => {
                app.json_vscroll = app.json_vscroll.saturating_add(5);
            }
            KeyCode::Home => {
                app.json_vscroll = 0;
            }
            KeyCode::End => {
                app.json_vscroll = u16::MAX;
            }
            _ => {}
        },
    }
}

fn handle_env_key(app: &mut AppState, key: KeyEvent, tx_evt: &mpsc::UnboundedSender<TuiEvent>) {
    let code = key.code;
    let modifiers = key.modifiers;
    if matches!(code, KeyCode::Esc) {
        app.screen = Screen::Home;
        return;
    }
    if matches!(code, KeyCode::Tab | KeyCode::Char('\t')) {
        if let Some(ed) = app.env_editor.as_mut() {
            let order = [
                EnvFieldFocus::List,
                EnvFieldFocus::Name,
                EnvFieldFocus::Host,
                EnvFieldFocus::PemEditor,
                EnvFieldFocus::Conn,
                EnvFieldFocus::Buttons,
            ];
            let idx = order
                .iter()
                .position(|f| *f == ed.field_focus)
                .unwrap_or(0);
            ed.field_focus = order[(idx + 1) % order.len()];
        }
        return;
    }
    if matches!(code, KeyCode::BackTab) {
        if let Some(ed) = app.env_editor.as_mut() {
            let order = [
                EnvFieldFocus::List,
                EnvFieldFocus::Name,
                EnvFieldFocus::Host,
                EnvFieldFocus::PemEditor,
                EnvFieldFocus::Conn,
                EnvFieldFocus::Buttons,
            ];
            let idx = order
                .iter()
                .position(|f| *f == ed.field_focus)
                .unwrap_or(0);
            let next = if idx == 0 { order.len() - 1 } else { idx - 1 };
            ed.field_focus = order[next];
        }
        return;
    }

    match code {
        KeyCode::F(1) => {
            let name = next_unique_env_name(&app.env_store.envs);
            app.env_store.envs.push(Environment {
                name: name.clone(),
                host: String::new(),
                private_key_pem: None,
                public_key_pem: None,
                ssl_ca_pem: None,
            });
            let idx = app.env_store.envs.len().saturating_sub(1);
            app.env_store.selected = Some(idx);
            if let Some(env) = app.env_store.envs.get(idx) {
                let mut editor = build_env_editor_from_env(env, Some(idx));
                editor.name_cursor = editor.name.len();
                editor.host_cursor = editor.host.len();
                app.env_editor = Some(editor);
            }
            return;
        }
        KeyCode::F(3) => {
            if let Some(i) = app.env_store.selected {
                if i < app.env_store.envs.len() {
                    app.env_store.envs.remove(i);
                    app.env_store.selected = if app.env_store.envs.is_empty() {
                        None
                    } else {
                        Some(i.min(app.env_store.envs.len() - 1))
                    };
                    let _ = app.env_store.save();
                    sync_env_editor_to_selection(app);
                }
            }
            return;
        }
        KeyCode::F(4) => {
            save_env_from_editor(app);
            return;
        }
        KeyCode::F(5) => {
            start_env_connection_test(app, tx_evt.clone());
            return;
        }
        KeyCode::F(6) => {
            move_env_selection(app, 1);
            return;
        }
        KeyCode::F(7) => {
            move_env_selection(app, -1);
            return;
        }
        _ => {}
    }

    let focus = app.env_editor.as_ref().map(|ed| ed.field_focus);
    if matches!(focus, Some(EnvFieldFocus::List)) {
        match code {
            KeyCode::Up => move_env_selection(app, -1),
            KeyCode::Down => move_env_selection(app, 1),
            KeyCode::Enter => {
                if let Some(ed) = app.env_editor.as_mut() {
                    ed.field_focus = EnvFieldFocus::Name;
                }
            }
            _ => {}
        }
        return;
    }
    if matches!(focus, Some(EnvFieldFocus::Conn)) {
        match code {
            KeyCode::Up => app.env_conn_vscroll = app.env_conn_vscroll.saturating_sub(1),
            KeyCode::Down => app.env_conn_vscroll = app.env_conn_vscroll.saturating_add(1),
            KeyCode::PageUp => app.env_conn_vscroll = app.env_conn_vscroll.saturating_sub(5),
            KeyCode::PageDown => app.env_conn_vscroll = app.env_conn_vscroll.saturating_add(5),
            KeyCode::Home => app.env_conn_vscroll = 0,
            KeyCode::End => app.env_conn_vscroll = u16::MAX,
            _ => {}
        }
        return;
    }
    if matches!(focus, Some(EnvFieldFocus::Buttons)) {
        if matches!(code, KeyCode::Enter) {
            save_env_from_editor(app);
        }
        return;
    }

    let Some(ed) = app.env_editor.as_mut() else {
        return;
    };
    let mut meta_changed = false;

    match ed.field_focus {
        EnvFieldFocus::Name => {
            meta_changed = handle_single_line_edit(&mut ed.name, &mut ed.name_cursor, key);
        }
        EnvFieldFocus::Host => {
            meta_changed = handle_single_line_edit(&mut ed.host, &mut ed.host_cursor, key);
        }
        EnvFieldFocus::PemEditor => {
            if modifiers.contains(KeyModifiers::CONTROL)
                && matches!(code, KeyCode::Left | KeyCode::Right)
            {
                ed.active_pem = match (ed.active_pem, code) {
                    (EnvPemField::PrivateKey, KeyCode::Left) => EnvPemField::Ca,
                    (EnvPemField::PrivateKey, KeyCode::Right) => EnvPemField::PublicKey,
                    (EnvPemField::PublicKey, KeyCode::Left) => EnvPemField::PrivateKey,
                    (EnvPemField::PublicKey, KeyCode::Right) => EnvPemField::Ca,
                    (EnvPemField::Ca, KeyCode::Left) => EnvPemField::PublicKey,
                    (EnvPemField::Ca, KeyCode::Right) => EnvPemField::PrivateKey,
                    (cur, _) => cur,
                };
                return;
            }
            let input = ta_input_from_key(key);
            match ed.active_pem {
                EnvPemField::PrivateKey => {
                    ed.ta_private.input(input);
                }
                EnvPemField::PublicKey => {
                    ed.ta_public.input(input);
                }
                EnvPemField::Ca => {
                    ed.ta_ca.input(input);
                }
            }
        }
        EnvFieldFocus::Conn | EnvFieldFocus::Buttons | EnvFieldFocus::List => {}
    }
    if meta_changed {
        sync_env_metadata_from_editor(app);
    }
}

fn handle_info_key(app: &mut AppState, key: KeyEvent, tx_evt: &mpsc::UnboundedSender<TuiEvent>) {
    let code = key.code;
    if matches!(code, KeyCode::Esc) {
        app.screen = Screen::Home;
        return;
    }
    if matches!(code, KeyCode::F(6) | KeyCode::Char('r')) {
        app.topics_last_fetched_at = Some(Instant::now());
        fetch_topics_async(app, tx_evt.clone());
        return;
    }
    let total = app.topics.len();
    match code {
        KeyCode::Up => {
            if app.selected_row > 0 {
                app.selected_row -= 1;
            }
        }
        KeyCode::Down => {
            if app.selected_row + 1 < total {
                app.selected_row += 1;
            }
        }
        KeyCode::PageUp => {
            let step = 5.min(total);
            app.selected_row = app.selected_row.saturating_sub(step);
        }
        KeyCode::PageDown => {
            if total > 0 {
                let step = 5.min(total);
                let next = app.selected_row.saturating_add(step);
                app.selected_row = next.min(total.saturating_sub(1));
            }
        }
        KeyCode::Home => app.selected_row = 0,
        KeyCode::End => {
            if total > 0 {
                app.selected_row = total.saturating_sub(1);
            }
        }
        _ => {}
    }
}

fn handle_app_config_key(app: &mut AppState, key: KeyEvent) {
    let code = key.code;
    if matches!(code, KeyCode::Esc) {
        app.screen = Screen::Home;
        return;
    }
    ensure_app_config_editor(app);
    let Some(ed) = app.app_config_editor.as_mut() else {
        return;
    };
    match code {
        KeyCode::Tab | KeyCode::Char('\t') => {
            ed.field_focus = match ed.field_focus {
                AppConfigFieldFocus::QueryScanMultiplier => AppConfigFieldFocus::DefaultLimit,
                AppConfigFieldFocus::DefaultLimit => AppConfigFieldFocus::DefaultOrderField,
                AppConfigFieldFocus::DefaultOrderField => AppConfigFieldFocus::DefaultOrderDir,
                AppConfigFieldFocus::DefaultOrderDir => AppConfigFieldFocus::TimestampsUseUtc,
                AppConfigFieldFocus::TimestampsUseUtc => AppConfigFieldFocus::Buttons,
                AppConfigFieldFocus::Buttons => AppConfigFieldFocus::QueryScanMultiplier,
            };
            return;
        }
        KeyCode::BackTab => {
            ed.field_focus = match ed.field_focus {
                AppConfigFieldFocus::QueryScanMultiplier => AppConfigFieldFocus::Buttons,
                AppConfigFieldFocus::DefaultLimit => AppConfigFieldFocus::QueryScanMultiplier,
                AppConfigFieldFocus::DefaultOrderField => AppConfigFieldFocus::DefaultLimit,
                AppConfigFieldFocus::DefaultOrderDir => AppConfigFieldFocus::DefaultOrderField,
                AppConfigFieldFocus::TimestampsUseUtc => AppConfigFieldFocus::DefaultOrderDir,
                AppConfigFieldFocus::Buttons => AppConfigFieldFocus::TimestampsUseUtc,
            };
            return;
        }
        _ => {}
    }

    match ed.field_focus {
        AppConfigFieldFocus::QueryScanMultiplier => match code {
            KeyCode::Char(ch) => ed.query_scan_multiplier.push(ch),
            KeyCode::Backspace | KeyCode::Delete => {
                ed.query_scan_multiplier.pop();
            }
            _ => {}
        },
        AppConfigFieldFocus::DefaultLimit => match code {
            KeyCode::Char(ch) => ed.default_limit.push(ch),
            KeyCode::Backspace | KeyCode::Delete => {
                ed.default_limit.pop();
            }
            _ => {}
        },
        AppConfigFieldFocus::DefaultOrderField => match code {
            KeyCode::Left => {
                if ed.default_order_field_idx > 0 {
                    ed.default_order_field_idx -= 1;
                }
            }
            KeyCode::Right => {
                if ed.default_order_field_idx < 2 {
                    ed.default_order_field_idx += 1;
                }
            }
            _ => {}
        },
        AppConfigFieldFocus::DefaultOrderDir => match code {
            KeyCode::Left => {
                if ed.default_order_dir_idx > 0 {
                    ed.default_order_dir_idx -= 1;
                }
            }
            KeyCode::Right => {
                if ed.default_order_dir_idx < 1 {
                    ed.default_order_dir_idx += 1;
                }
            }
            _ => {}
        },
        AppConfigFieldFocus::TimestampsUseUtc => match code {
            KeyCode::Enter | KeyCode::Char(' ') => {
                ed.timestamps_use_utc = !ed.timestamps_use_utc;
            }
            _ => {}
        },
        AppConfigFieldFocus::Buttons => match code {
            KeyCode::Enter => attempt_save_app_config(app),
            _ => {}
        },
    }
}

fn handle_help_key(app: &mut AppState, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = app.last_screen_before_help.unwrap_or(Screen::Home);
        }
        KeyCode::Up => scroll_help(app, -1),
        KeyCode::Down => scroll_help(app, 1),
        KeyCode::PageUp => scroll_help(app, -10),
        KeyCode::PageDown => scroll_help(app, 10),
        KeyCode::Home => app.help_vscroll = 0,
        KeyCode::End => jump_help_to_end(app),
        _ => {}
    }
}

fn handle_record_detail_key(app: &mut AppState, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Home;
        }
        KeyCode::Up => {
            app.record_detail_scroll = app.record_detail_scroll.saturating_sub(1);
        }
        KeyCode::Down => {
            app.record_detail_scroll = app.record_detail_scroll.saturating_add(1);
        }
        KeyCode::PageUp => {
            app.record_detail_scroll = app.record_detail_scroll.saturating_sub(5);
        }
        KeyCode::PageDown => {
            app.record_detail_scroll = app.record_detail_scroll.saturating_add(5);
        }
        KeyCode::Home => {
            app.record_detail_scroll = 0;
        }
        KeyCode::End => {
            app.record_detail_scroll = u16::MAX;
        }
        _ => {}
    }
}

fn handle_paste_event(app: &mut AppState, raw: &str) {
    if app.command_palette.open {
        let text = normalize_plain_input(raw);
        if !text.is_empty() {
            app.command_palette.input.insert_str(text);
            update_command_palette_matches(app);
        }
        return;
    }

    match app.screen {
        Screen::Home => {
            let focus = app.home_focus;
            let text = normalize_plain_input(raw);
            if !text.is_empty() {
                let mut applied = false;
                if let Some(ta) = home_focus_textarea_mut(app, focus) {
                    ta.insert_str(text);
                    applied = true;
                }
                if applied {
                    if matches!(focus, HomeFocus::AdvancedQuery) {
                        app.parse_status_dirty = true;
                    }
                    if matches!(focus, HomeFocus::TopicFilter) {
                        refresh_topic_matches(app);
                    }
                }
            }
        }
        Screen::Envs => {
            let _ = handle_env_editor_paste(app, raw);
        }
        Screen::AppConfig => {
            ensure_app_config_editor(app);
            if let Some(ed) = app.app_config_editor.as_mut() {
                let text = normalize_plain_input(raw);
                match ed.field_focus {
                    AppConfigFieldFocus::QueryScanMultiplier => {
                        ed.query_scan_multiplier.push_str(&text);
                    }
                    AppConfigFieldFocus::DefaultLimit => {
                        ed.default_limit.push_str(&text);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn should_open_palette_on_double_slash(ta: &mut TextArea, key: KeyEvent) -> bool {
    if !matches!(key.code, KeyCode::Char('/')) || !key.modifiers.is_empty() {
        return false;
    }
    let (row, col) = ta.cursor();
    let Some(line) = ta.lines().get(row) else {
        return false;
    };
    if col == 0 {
        return false;
    }
    let prev = line.chars().nth(col.saturating_sub(1)).unwrap_or(' ');
    if prev != '/' {
        return false;
    }
    let prefix: String = line.chars().take(col.saturating_sub(1)).collect();
    if !prefix.trim().is_empty() {
        return false;
    }
    ta.input(TAInput {
        key: TAKey::Backspace,
        ctrl: false,
        alt: false,
        shift: false,
    });
    true
}

const BASIC_FOCUS_ORDER: &[HomeFocus] = &[
    HomeFocus::TopicFilter,
    HomeFocus::TopicList,
    HomeFocus::BasicSearch,
    HomeFocus::BasicWhere,
    HomeFocus::BasicSince,
    HomeFocus::BasicUntil,
    HomeFocus::BasicLimit,
    HomeFocus::BasicOrderField,
    HomeFocus::BasicOrderDir,
    HomeFocus::Results,
    HomeFocus::Details,
];

const ADVANCED_FOCUS_ORDER: &[HomeFocus] = &[
    HomeFocus::TopicFilter,
    HomeFocus::TopicList,
    HomeFocus::AdvancedQuery,
    HomeFocus::Results,
    HomeFocus::Details,
];

fn home_focus_order(mode: QueryMode) -> &'static [HomeFocus] {
    match mode {
        QueryMode::Basic => BASIC_FOCUS_ORDER,
        QueryMode::Advanced => ADVANCED_FOCUS_ORDER,
    }
}

fn cycle_home_focus(app: &mut AppState, forward: bool) {
    let order = home_focus_order(app.query_mode);
    let idx = order.iter().position(|f| *f == app.home_focus).unwrap_or(0);
    let next = if forward {
        (idx + 1) % order.len()
    } else if idx == 0 {
        order.len() - 1
    } else {
        idx - 1
    };
    app.home_focus = order[next];
}

fn home_focus_textarea_mut(
    app: &mut AppState,
    focus: HomeFocus,
) -> Option<&mut TextArea<'static>> {
    match focus {
        HomeFocus::TopicFilter => Some(&mut app.topic_picker.filter),
        HomeFocus::BasicSearch => Some(&mut app.basic_query.search),
        HomeFocus::BasicWhere => Some(&mut app.basic_query.where_clause),
        HomeFocus::BasicSince => Some(&mut app.basic_query.since),
        HomeFocus::BasicUntil => Some(&mut app.basic_query.until),
        HomeFocus::BasicLimit => Some(&mut app.basic_query.limit),
        HomeFocus::AdvancedQuery => Some(&mut app.query_editor),
        _ => None,
    }
}

fn refresh_topic_matches(app: &mut AppState) {
    const MAX_TOPIC_MATCHES: usize = 200;
    let filter = textarea_text(&app.topic_picker.filter);
    let filter = filter.trim();
    let mut matches: Vec<usize> = Vec::new();
    if app.topics.is_empty() {
        app.topic_picker.matches.clear();
        app.topic_picker.selected = 0;
        return;
    }
    if filter.is_empty() {
        matches.extend(0..app.topics.len());
        matches.sort_by(|a, b| app.topics[*a].cmp(&app.topics[*b]));
    } else {
        let matcher = SkimMatcherV2::default();
        let mut scored: Vec<(i64, usize)> = Vec::new();
        for (idx, name) in app.topics.iter().enumerate() {
            if let Some(score) = matcher.fuzzy_match(name, filter) {
                scored.push((score, idx));
            }
        }
        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| app.topics[a.1].cmp(&app.topics[b.1]))
        });
        matches.extend(scored.into_iter().map(|(_, idx)| idx));
    }
    if matches.len() > MAX_TOPIC_MATCHES {
        matches.truncate(MAX_TOPIC_MATCHES);
    }
    app.topic_picker.matches = matches;
    if app.topic_picker.matches.is_empty() {
        app.topic_picker.selected = 0;
    } else {
        app.topic_picker.selected = app
            .topic_picker
            .selected
            .min(app.topic_picker.matches.len().saturating_sub(1));
    }
}

fn selected_topic_name(app: &AppState) -> Option<&str> {
    let idx = app.topic_picker.matches.get(app.topic_picker.selected)?;
    app.topics.get(*idx).map(|s| s.as_str())
}

fn insert_selected_topic_into_query(app: &mut AppState) {
    let Some(topic) = selected_topic_name(app).map(|s| s.to_string()) else {
        return;
    };
    app.query_editor.insert_str(topic);
}

fn normalize_where_clause(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("where ") {
        let offset = trimmed.len().saturating_sub(rest.len());
        return trimmed[offset..].trim().to_string();
    }
    trimmed.to_string()
}

fn escape_sql_literal(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

fn normalize_timestamp_literal(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        trimmed.to_string()
    } else {
        format!("'{}'", escape_sql_literal(trimmed))
    }
}

fn build_basic_query_text(app: &AppState) -> Result<String, String> {
    let Some(topic) = selected_topic_name(app) else {
        return Err("Select a topic first".to_string());
    };
    let search = textarea_text(&app.basic_query.search).trim().to_string();
    let where_raw = textarea_text(&app.basic_query.where_clause).trim().to_string();
    let since = textarea_text(&app.basic_query.since).trim().to_string();
    let until = textarea_text(&app.basic_query.until).trim().to_string();
    let limit_raw = textarea_text(&app.basic_query.limit).trim().to_string();

    let mut clauses: Vec<String> = Vec::new();
    if !search.is_empty() {
        clauses.push(format!(
            "value CONTAINS '{}'",
            escape_sql_literal(search.trim())
        ));
    }
    let where_clause = normalize_where_clause(&where_raw);
    if !where_clause.is_empty() {
        clauses.push(format!("({})", where_clause));
    }
    if !since.is_empty() {
        clauses.push(format!(
            "timestamp >= {}",
            normalize_timestamp_literal(&since)
        ));
    }
    if !until.is_empty() {
        clauses.push(format!(
            "timestamp <= {}",
            normalize_timestamp_literal(&until)
        ));
    }

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    let order_field = match app.basic_query.order_field_idx {
        0 => "timestamp",
        1 => "poffset",
        _ => "poffset_ts",
    };
    let order_dir = if app.basic_query.order_dir_idx == 0 {
        "ASC"
    } else {
        "DESC"
    };

    let mut query = format!(
        "SELECT partition, offset, timestamp, key, value FROM {}{} ORDER BY {} {}",
        topic, where_sql, order_field, order_dir
    );

    if !limit_raw.is_empty() {
        let limit = limit_raw
            .parse::<usize>()
            .map_err(|_| "LIMIT must be a number".to_string())?;
        if limit == 0 {
            return Err("LIMIT must be > 0".to_string());
        }
        query.push_str(&format!(" LIMIT {}", limit));
    }

    query.push(';');
    Ok(query)
}

async fn run_basic_query(
    app: &mut AppState,
    args: &RunArgs,
    tx_evt: &mpsc::UnboundedSender<TuiEvent>,
    run_counter: &mut u64,
) {
    match build_basic_query_text(app) {
        Ok(query) => {
            dispatch_query(app, args, tx_evt, run_counter, query, false).await;
        }
        Err(msg) => {
            push_status_line(app, msg);
        }
    }
}

fn toggle_timestamp_display(app: &mut AppState) {
    app.timestamps_use_utc = !app.timestamps_use_utc;
    app.timestamp_switch_pressed = true;
    app.timestamp_switch_deadline = Some(Instant::now() + Duration::from_millis(150));
}

fn total_results_rows(app: &AppState) -> usize {
    match app.results_mode {
        ResultsMode::Messages => app.rows.len(),
        ResultsMode::TopicList => app.topics_with_partitions.len(),
    }
}

fn scroll_help(app: &mut AppState, delta: i32) {
    let max = help_content_line_count().saturating_sub(1) as i32;
    let mut next = app.help_vscroll as i32 + delta;
    if next < 0 {
        next = 0;
    }
    if next > max {
        next = max;
    }
    app.help_vscroll = next as u32;
}

fn jump_help_to_end(app: &mut AppState) {
    let max = help_content_line_count().saturating_sub(1);
    app.help_vscroll = max as u32;
}

fn update_parse_status(app: &mut AppState) {
    if matches!(app.query_mode, QueryMode::Basic) {
        app.parse_ok = true;
        app.parse_error_msg = None;
        return;
    }
    let text = textarea_text(&app.query_editor);
    let cursor = textarea_cursor_offset(&app.query_editor);
    let (qs, qe) = find_query_range(&text, cursor);
    let raw = text.get(qs..qe).unwrap_or("");
    let trimmed = strip_trailing_semicolon(raw).trim();
    if trimmed.is_empty() {
        app.parse_ok = true;
        app.parse_error_msg = None;
        return;
    }
    match parse_command(trimmed) {
        Ok(_) => {
            app.parse_ok = true;
            app.parse_error_msg = None;
        }
        Err(e) => {
            app.parse_ok = false;
            app.parse_error_msg = Some(format!("{}", e));
        }
    }
}

fn handle_single_line_edit(target: &mut String, cursor: &mut usize, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Char(ch) => {
            target.insert(*cursor, ch);
            *cursor += 1;
            true
        }
        KeyCode::Backspace => {
            if *cursor > 0 {
                target.remove(*cursor - 1);
                *cursor -= 1;
                return true;
            }
            false
        }
        KeyCode::Delete => {
            if *cursor < target.len() {
                target.remove(*cursor);
                return true;
            }
            false
        }
        KeyCode::Left => {
            if *cursor > 0 {
                *cursor -= 1;
            }
            false
        }
        KeyCode::Right => {
            if *cursor < target.len() {
                *cursor += 1;
            }
            false
        }
        _ => false,
    }
}

fn textarea_to_string(ta: &TextArea<'_>) -> Option<String> {
    let joined = ta.lines().join("\n");
    if joined.trim().is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn save_env_from_editor(app: &mut AppState) {
    let (idx, name, host, private_key_pem, public_key_pem, ssl_ca_pem) = {
        let Some(ed) = app.env_editor.as_ref() else {
            return;
        };
        (
            ed.idx,
            ed.name.trim().to_string(),
            ed.host.trim().to_string(),
            textarea_to_string(&ed.ta_private),
            textarea_to_string(&ed.ta_public),
            textarea_to_string(&ed.ta_ca),
        )
    };

    if name.is_empty() {
        push_status_line(app, "Environment name cannot be empty".to_string());
        return;
    }
    if host.is_empty() {
        push_status_line(app, "Environment host cannot be empty".to_string());
        return;
    }
    if app.env_store.envs.iter().enumerate().any(|(i, e)| {
        Some(i) != idx && e.name.eq_ignore_ascii_case(&name)
    }) {
        push_status_line(
            app,
            "Environment name already exists. Choose a unique name.".to_string(),
        );
        return;
    }

    let new_env = Environment {
        name,
        host,
        private_key_pem,
        public_key_pem,
        ssl_ca_pem,
    };

    if let Some(i) = idx {
        if i < app.env_store.envs.len() {
            app.env_store.envs[i] = new_env.clone();
            app.env_store.selected = Some(i);
        } else {
            app.env_store.envs.push(new_env.clone());
            app.env_store.selected = Some(app.env_store.envs.len() - 1);
        }
    } else {
        app.env_store.envs.push(new_env.clone());
        app.env_store.selected = Some(app.env_store.envs.len() - 1);
    }
    match app.env_store.save() {
        Ok(()) => {
            app.env_save_pressed = true;
            app.env_save_deadline = Some(Instant::now() + Duration::from_millis(800));
            push_status_line(app, "Environments saved".to_string());
        }
        Err(e) => {
            push_status_line(app, format!("Save failed: {}", e));
        }
    }
    if let Some(sel) = app.env_store.selected {
        if let Some(env) = app.env_store.envs.get(sel) {
            app.env_editor = Some(build_env_editor_from_env(env, Some(sel)));
        }
    }
}

fn start_env_connection_test(app: &mut AppState, tx_evt: mpsc::UnboundedSender<TuiEvent>) {
    let env = {
        let Some(ed) = app.env_editor.as_ref() else {
            return;
        };
        Environment {
            name: ed.name.clone(),
            host: ed.host.clone(),
            private_key_pem: textarea_to_string(&ed.ta_private),
            public_key_pem: textarea_to_string(&ed.ta_public),
            ssl_ca_pem: textarea_to_string(&ed.ta_ca),
        }
    };
    app.env_test_log.clear();
    app.env_conn_vscroll = 0;
    app.env_test_in_progress = true;
    tokio::spawn(async move {
        let _ = tx_evt.send(TuiEvent::EnvTestProgress {
            message: "Testing connection...".to_string(),
        });
        let result = tokio::task::spawn_blocking(move || test_env_connection(env)).await;
        match result {
            Ok(Ok(())) => {
                let _ = tx_evt.send(TuiEvent::EnvTestDone {
                    message: "Connection OK".to_string(),
                });
            }
            Ok(Err(e)) => {
                let _ = tx_evt.send(TuiEvent::EnvTestDone {
                    message: format!("Connection error: {}", e),
                });
            }
            Err(e) => {
                let _ = tx_evt.send(TuiEvent::EnvTestDone {
                    message: format!("Connection error: {}", e),
                });
            }
        }
    });
}

fn test_env_connection(env: Environment) -> Result<()> {
    let ssl = if env.private_key_pem.is_some()
        || env.public_key_pem.is_some()
        || env.ssl_ca_pem.is_some()
    {
        Some(crate::models::SslConfig {
            ca_pem: env.ssl_ca_pem.clone(),
            cert_pem: env.public_key_pem.clone(),
            key_pem: env.private_key_pem.clone(),
        })
    } else {
        None
    };
    let cfg = build_client_config(&env.host, ssl);
    let consumer: StreamConsumer = cfg.create().context("create consumer")?;
    let _ = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .context("fetch metadata")?;
    Ok(())
}

fn build_client_config(broker: &str, ssl: Option<crate::models::SslConfig>) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", broker)
        .set("group.id", format!("rkl-meta-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set("enable.partition.eof", "true");
    cfg.set_log_level(RDKafkaLogLevel::Emerg);
    if let Some(ssl) = ssl {
        if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
            cfg.set("security.protocol", "ssl");
            if let Some(ref s) = ssl.ca_pem {
                cfg.set("ssl.ca.pem", s);
            }
            if let Some(ref s) = ssl.cert_pem {
                cfg.set("ssl.certificate.pem", s);
            }
            if let Some(ref s) = ssl.key_pem {
                cfg.set("ssl.key.pem", s);
            }
        }
    }
    cfg
}

fn fetch_topics_async(app: &AppState, tx: mpsc::UnboundedSender<TuiEvent>) {
    let broker = app
        .selected_env()
        .map(|e| e.host.clone())
        .unwrap_or_else(|| app.host.clone());
    let ssl = app.current_ssl_config();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let cfg = build_client_config(&broker, ssl);
            let consumer: StreamConsumer = cfg.create().context("create consumer")?;
            let md = consumer
                .fetch_metadata(None, Duration::from_secs(10))
                .context("fetch metadata")?;
            let mut topics: Vec<String> = md.topics().iter().map(|t| t.name().to_string()).collect();
            topics.sort();
            Ok::<_, anyhow::Error>(topics)
        })
        .await;
        if let Ok(Ok(list)) = result {
            let _ = tx.send(TuiEvent::Topics(list));
        }
    });
}

fn fetch_topics_with_partitions_async(app: &AppState, tx: mpsc::UnboundedSender<TuiEvent>) {
    let broker = app
        .selected_env()
        .map(|e| e.host.clone())
        .unwrap_or_else(|| app.host.clone());
    let ssl = app.current_ssl_config();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let cfg = build_client_config(&broker, ssl);
            let consumer: StreamConsumer = cfg.create().context("create consumer")?;
            let md = consumer
                .fetch_metadata(None, Duration::from_secs(10))
                .context("fetch metadata")?;
            let mut entries: Vec<(String, usize)> = md
                .topics()
                .iter()
                .map(|t| (t.name().to_string(), t.partitions().len()))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Ok::<_, anyhow::Error>(entries)
        })
        .await;
        match result {
            Ok(Ok(list)) => {
                let _ = tx.send(TuiEvent::TopicsWithPartitions(list));
            }
            Ok(Err(e)) => {
                let _ = tx.send(TuiEvent::TopicsWithPartitions(vec![(
                    format!("Error: {}", e),
                    0,
                )]));
            }
            Err(e) => {
                let _ = tx.send(TuiEvent::TopicsWithPartitions(vec![(
                    format!("Error: {}", e),
                    0,
                )]));
            }
        }
    });
}
