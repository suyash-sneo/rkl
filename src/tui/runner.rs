use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::mpsc;

use crate::args::RunArgs;
use crate::consumer::spawn_partition_consumer;
use crate::merger::run_merger;
use crate::models::{MessageEnvelope, OffsetSpec};
use crate::output::OutputSink;
use crate::query::{parse_query, OrderDir, SelectItem};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};

use super::app::{AppState, TuiEvent};
use super::ui::draw;

pub async fn run(args: RunArgs) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, terminal::EnterAlternateScreen, crossterm::cursor::Hide)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tx_evt, mut rx_evt) = mpsc::unbounded_channel::<TuiEvent>();
    let mut app = AppState::new(args.query.clone().unwrap_or_default(), args.broker.clone());

    let mut run_counter: u64 = 0;

    // Initial draw
    terminal.draw(|f| draw(f, &app))?;

    // Main loop
    let res = loop {
        // Draw UI
        terminal.draw(|f| draw(f, &app))?;

        // Drain any events from pipeline
        while let Ok(ev) = rx_evt.try_recv() {
            match ev {
                TuiEvent::Batch { run_id, mut rows } => {
                    if Some(run_id) == app.current_run {
                        app.push_rows(std::mem::take(&mut rows));
                        app.clamp_selection();
                    }
                }
                TuiEvent::Done { run_id } => {
                    if Some(run_id) == app.current_run { app.status = format!("Run {run_id} complete"); }
                }
                TuiEvent::Error { run_id, message } => {
                    if Some(run_id) == app.current_run { app.status = format!("Error: {message}"); }
                }
            }
        }

        // Handle key input (non-blocking poll)
        if crossterm::event::poll(Duration::from_millis(50))? {
            match crossterm::event::read()? {
                Event::Key(KeyEvent { code, modifiers, .. }) => {
                    match (code, modifiers) {
                        (KeyCode::Char('c'), KeyModifiers::CONTROL) => break Ok(()),
                        (KeyCode::Char('q'), _) => break Ok(()),
                        (KeyCode::Enter, _) => {
                            let input = app.input.trim().to_string();
                            if input.is_empty() { app.status = "Please enter a query".to_string(); continue; }
                            // Parse to determine columns visibility early
                            match parse_query(&input) {
                                Ok(ast) => {
                                    let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
                                    app.keys_only = keys_only;
                                    app.clear_rows();
                                    run_counter += 1;
                                    app.current_run = Some(run_counter);
                                    app.status = format!("Running (run {}): topic '{}' on {}. Press q to quit.", run_counter, ast.from, app.host);
                                    let mut run_args = args.clone();
                                    run_args.broker = app.host.clone();
                                    app.clamp_selection();
                                    spawn_pipeline(run_args, input, run_counter, tx_evt.clone()).await;
                                }
                                Err(e) => { app.status = format!("Parse error: {}", e); }
                            }
                        }
                        (KeyCode::Backspace, _) => {
                            match app.focus {
                                super::app::Focus::Host => { app.host.pop(); }
                                super::app::Focus::Query => { app.input.pop(); }
                                super::app::Focus::Results => {}
                            }
                        }
                        (KeyCode::Char('\t'), _) | (KeyCode::Tab, _) => { app.next_focus(); }
                        (KeyCode::Char(ch), _) => {
                            match app.focus {
                                super::app::Focus::Results => {
                                    match ch {
                                        // hjkl navigation
                                        'h' | 'H' => { if app.selected_col > 0 { app.selected_col -= 1; } }
                                        'l' | 'L' => { let cols = if app.keys_only { 4 } else { 5 }; if app.selected_col + 1 < cols { app.selected_col += 1; } }
                                        'k' | 'K' => { if app.selected_row > 0 { app.selected_row -= 1; } }
                                        'j' | 'J' => { if app.selected_row + 1 < app.rows.len() { app.selected_row += 1; } }
                                        // copy
                                        'y' | 'Y' => {
                                            if let Some(s) = selected_cell_text(&app) {
                                                match copy_to_clipboard(&s) { Ok(()) => app.status = "Copied to clipboard".to_string(), Err(e) => app.status = format!("Clipboard error: {}", e) }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                super::app::Focus::Host => { app.host.push(ch); }
                                super::app::Focus::Query => { app.input.push(ch); }
                            }
                        }
                        (KeyCode::Esc, _) => { if matches!(app.focus, super::app::Focus::Query) { app.input.clear(); } }
                        // Navigation in results
                        (KeyCode::Up, _) => { if matches!(app.focus, super::app::Focus::Results) { if app.selected_row > 0 { app.selected_row -= 1; } } }
                        (KeyCode::Down, _) => { if matches!(app.focus, super::app::Focus::Results) { if app.selected_row + 1 < app.rows.len() { app.selected_row += 1; } } }
                        (KeyCode::Left, _) => { if matches!(app.focus, super::app::Focus::Results) { if app.selected_col > 0 { app.selected_col -= 1; } else { app.selected_col = 0; } } }
                        (KeyCode::Right, _) => { if matches!(app.focus, super::app::Focus::Results) { let cols = if app.keys_only { 4 } else { 5 }; if app.selected_col + 1 < cols { app.selected_col += 1; } } }
                        (KeyCode::PageUp, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; app.selected_row = app.selected_row.saturating_sub(step); } }
                        (KeyCode::PageDown, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; if app.rows.len() > 0 { app.selected_row = (app.selected_row + step).min(app.rows.len()-1); } } }
                        (KeyCode::Home, _) => { if matches!(app.focus, super::app::Focus::Results) { app.selected_row = 0; } }
                        (KeyCode::End, _) => { if matches!(app.focus, super::app::Focus::Results) { if app.rows.len() > 0 { app.selected_row = app.rows.len()-1; } } }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    };

    // Restore terminal
    disable_raw_mode().ok();
    // Use crossterm global execute to restore screen
    execute!(std::io::stdout(), terminal::LeaveAlternateScreen, crossterm::cursor::Show).ok();

    res
}

async fn spawn_pipeline(args: RunArgs, query_text: String, run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>) {
    // Spawn as a tokio task to avoid blocking UI
    tokio::spawn(async move {
        if let Err(e) = run_pipeline(args, query_text, run_id, tx.clone()).await {
            let _ = tx.send(TuiEvent::Error { run_id, message: e.to_string() });
        }
    });
}

async fn run_pipeline(args: RunArgs, query_text: String, run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>) -> Result<()> {
    // Parse
    let ast = parse_query(&query_text).context("Failed to parse query")?;
    let topic = ast.from.clone();
    let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
    let max_messages_global = ast.limit.or(args.max_messages).or(Some(100));
    let order_desc = ast.order.as_ref().map(|o| matches!(o.dir, OrderDir::Desc)).unwrap_or(false);

    // Probe partitions
    let probe_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create()
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

    // Channels and tasks
    let (tx_msg, rx_msg) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);
    let offset_spec = OffsetSpec::from_str(&args.offset).unwrap_or_else(|_| OffsetSpec::Beginning);
    let query_arc = std::sync::Arc::new(ast.clone());

    let mut joinset = tokio::task::JoinSet::new();
    for &p in &partitions {
        let txp = tx_msg.clone();
        let mut a = args.clone();
        a.topic = Some(topic.clone());
        a.keys_only = keys_only;
        // Let merger enforce global limit
        a.max_messages = None;
        let q = Some(query_arc.clone());
        joinset.spawn(async move { spawn_partition_consumer(a, p, offset_spec, txp, q).await });
    }
    drop(tx_msg);

    let mut sink = TuiOutput::new(run_id, tx.clone());
    run_merger(
        rx_msg,
        &mut sink,
        args.watermark,
        args.flush_interval_ms,
        max_messages_global,
        order_desc,
    )
    .await?;

    while let Some(res) = joinset.join_next().await { let _ = res; }

    let _ = tx.send(TuiEvent::Done { run_id });
    Ok(())
}

struct TuiOutput {
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    buffer: Vec<MessageEnvelope>,
}

impl TuiOutput { fn new(run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>) -> Self { Self { run_id, tx, buffer: Vec::with_capacity(256) } } }

impl OutputSink for TuiOutput {
    fn push(&mut self, env: &MessageEnvelope) {
        self.buffer.push(env.clone());
    }
    fn flush_block(&mut self) {
        if self.buffer.is_empty() { return; }
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.buffer);
        let _ = self.tx.send(TuiEvent::Batch { run_id: self.run_id, rows: out });
    }
}

fn selected_cell_text(app: &AppState) -> Option<String> {
    if app.rows.is_empty() { return None; }
    let idx = app.selected_row.min(app.rows.len() - 1);
    let env = &app.rows[idx];
    let col = app.selected_col;
    let cols = if app.keys_only { 4 } else { 5 };
    if col >= cols { return None; }
    let s = match col {
        0 => env.partition.to_string(),
        1 => env.offset.to_string(),
        2 => fmt_ts(env.timestamp_ms),
        3 => env.key.clone(),
        4 => env.value.as_deref().unwrap_or("null").to_string(),
        _ => return None,
    };
    Some(s)
}

fn copy_to_clipboard(s: &str) -> Result<()> {
    let mut cb = arboard::Clipboard::new().context("open clipboard")?;
    cb.set_text(s.to_string()).context("set clipboard text")?;
    Ok(())
}

fn fmt_ts(ms: i64) -> String {
    if ms <= 0 { return "0".to_string(); }
    let secs = ms / 1000;
    let tm = time::OffsetDateTime::from_unix_timestamp(secs as i64).unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH);
    tm.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| ms.to_string())
}
