use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind, MouseButton};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
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

use super::app::{AppState, TuiEvent, EnvEditor, EnvFieldFocus};
use super::env_store::Environment;
use super::ui::draw;

pub async fn run(args: RunArgs) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, terminal::EnterAlternateScreen, crossterm::event::EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tx_evt, mut rx_evt) = mpsc::unbounded_channel::<TuiEvent>();
    let mut app = AppState::new(args.query.clone().unwrap_or_default(), args.broker.clone());

    let mut run_counter: u64 = 0;

    // Initial draw
    terminal.draw(|f| draw(f, &app))?;

    // Main loop
    let res = loop {
        // Handle transient pressed button animation
        if app.copy_btn_pressed {
            if let Some(deadline) = app.copy_btn_deadline {
                if Instant::now() >= deadline { app.copy_btn_pressed = false; app.copy_btn_deadline = None; }
            } else {
                app.copy_btn_pressed = false;
            }
        }

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
                        (KeyCode::Char('q'), KeyModifiers::CONTROL) => break Ok(()),
                        (KeyCode::Enter, _) => {
                            if app.show_env_modal {
                                // Save current editor as env (create or update) and close
                                if let Some(ed) = app.env_editor.as_mut() {
                                    let new_env = Environment {
                                        name: ed.name.clone(),
                                        host: ed.host.clone(),
                                        private_key_pem: if ed.private_key_pem.trim().is_empty() { None } else { Some(ed.private_key_pem.clone()) },
                                        public_key_pem: if ed.public_key_pem.trim().is_empty() { None } else { Some(ed.public_key_pem.clone()) },
                                        ssl_ca_pem: if ed.ssl_ca_pem.trim().is_empty() { None } else { Some(ed.ssl_ca_pem.clone()) },
                                    };
                                    if let Some(i) = ed.idx {
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
                                    let _ = app.env_store.save();
                                    if let Some(sel) = app.env_store.selected {
                                        if let Some(e) = app.env_store.envs.get(sel) { app.host = e.host.clone(); }
                                    }
                                    app.show_env_modal = false;
                                }
                            } else if matches!(app.focus, super::app::Focus::Host) {
                                // Open env modal
                                let (idx, name, host, privk, pubk, ca) = if let Some(env) = app.selected_env() {
                                    (app.env_store.selected, env.name.clone(), env.host.clone(), env.private_key_pem.clone().unwrap_or_default(), env.public_key_pem.clone().unwrap_or_default(), env.ssl_ca_pem.clone().unwrap_or_default())
                                } else {
                                    (None, String::new(), app.host.clone(), String::new(), String::new(), String::new())
                                };
                                app.env_editor = Some(EnvEditor { idx, name_cursor: 0, name, host_cursor: 0, host, private_key_cursor: 0, private_key_pem: privk, public_key_cursor: 0, public_key_pem: pubk, ssl_ca_cursor: 0, ssl_ca_pem: ca, field_focus: EnvFieldFocus::Name });
                                app.show_env_modal = true;
                            } else {
                                let input = app.input.trim().to_string();
                                if input.is_empty() { app.status = "Please enter a query".to_string(); continue; }
                                match parse_query(&input) {
                                    Ok(ast) => {
                                        let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
                                        app.keys_only = keys_only;
                                        app.clear_rows();
                                        run_counter += 1;
                                        app.current_run = Some(run_counter);
                                        let env_host = app.selected_env().map(|e| e.host.clone()).unwrap_or(app.host.clone());
                                        app.status = format!("Running (run {}): topic '{}' on {}. Press q to quit.", run_counter, ast.from, env_host);
                                        let mut run_args = args.clone();
                                        run_args.broker = env_host;
                                        app.clamp_selection();
                                        let ssl = app.current_ssl_config();
                                        spawn_pipeline_with_ssl(run_args, input, run_counter, tx_evt.clone(), ssl).await;
                                    }
                                    Err(e) => { app.status = format!("Parse error: {}", e); }
                                }
                            }
                        }
                        (KeyCode::Backspace, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => { if ed.name_cursor>0 { ed.name.remove(ed.name_cursor-1); ed.name_cursor-=1; } }
                                        EnvFieldFocus::Host => { if ed.host_cursor>0 { ed.host.remove(ed.host_cursor-1); ed.host_cursor-=1; } }
                                        EnvFieldFocus::PrivateKey => { if ed.private_key_cursor>0 { ed.private_key_pem.remove(ed.private_key_cursor-1); ed.private_key_cursor-=1; } }
                                        EnvFieldFocus::PublicKey => { if ed.public_key_cursor>0 { ed.public_key_pem.remove(ed.public_key_cursor-1); ed.public_key_cursor-=1; } }
                                        EnvFieldFocus::Ca => { if ed.ssl_ca_cursor>0 { ed.ssl_ca_pem.remove(ed.ssl_ca_cursor-1); ed.ssl_ca_cursor-=1; } }
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                                continue;
                            }
                            match app.focus {
                                super::app::Focus::Host => { /* no-op */ }
                                super::app::Focus::Query => { if app.input_cursor>0 { app.input.remove(app.input_cursor-1); app.input_cursor-=1; } }
                                super::app::Focus::Results => {}
                            }
                        }
                        (KeyCode::Delete, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => { if ed.name_cursor<ed.name.len() { ed.name.remove(ed.name_cursor); } }
                                        EnvFieldFocus::Host => { if ed.host_cursor<ed.host.len() { ed.host.remove(ed.host_cursor); } }
                                        EnvFieldFocus::PrivateKey => { if ed.private_key_cursor<ed.private_key_pem.len() { ed.private_key_pem.remove(ed.private_key_cursor); } }
                                        EnvFieldFocus::PublicKey => { if ed.public_key_cursor<ed.public_key_pem.len() { ed.public_key_pem.remove(ed.public_key_cursor); } }
                                        EnvFieldFocus::Ca => { if ed.ssl_ca_cursor<ed.ssl_ca_pem.len() { ed.ssl_ca_pem.remove(ed.ssl_ca_cursor); } }
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if app.input_cursor<app.input.len() { app.input.remove(app.input_cursor); }
                            }
                        }
                        (KeyCode::Char('\t'), _) | (KeyCode::Tab, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    ed.field_focus = match ed.field_focus {
                                        EnvFieldFocus::Name => EnvFieldFocus::Host,
                                        EnvFieldFocus::Host => EnvFieldFocus::PrivateKey,
                                        EnvFieldFocus::PrivateKey => EnvFieldFocus::PublicKey,
                                        EnvFieldFocus::PublicKey => EnvFieldFocus::Ca,
                                        EnvFieldFocus::Ca => EnvFieldFocus::Buttons,
                                        EnvFieldFocus::Buttons => EnvFieldFocus::Name,
                                    };
                                }
                            } else {
                                app.next_focus();
                            }
                        }
                        (KeyCode::BackTab, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    ed.field_focus = match ed.field_focus {
                                        EnvFieldFocus::Name => EnvFieldFocus::Buttons,
                                        EnvFieldFocus::Host => EnvFieldFocus::Name,
                                        EnvFieldFocus::PrivateKey => EnvFieldFocus::Host,
                                        EnvFieldFocus::PublicKey => EnvFieldFocus::PrivateKey,
                                        EnvFieldFocus::Ca => EnvFieldFocus::PublicKey,
                                        EnvFieldFocus::Buttons => EnvFieldFocus::Ca,
                                    };
                                }
                            }
                        }
                        // Control shortcuts (commands): avoid plain letters
                        (KeyCode::Char('s'), KeyModifiers::CONTROL) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    let new_env = Environment {
                                        name: ed.name.clone(),
                                        host: ed.host.clone(),
                                        private_key_pem: if ed.private_key_pem.trim().is_empty() { None } else { Some(ed.private_key_pem.clone()) },
                                        public_key_pem: if ed.public_key_pem.trim().is_empty() { None } else { Some(ed.public_key_pem.clone()) },
                                        ssl_ca_pem: if ed.ssl_ca_pem.trim().is_empty() { None } else { Some(ed.ssl_ca_pem.clone()) },
                                    };
                                    if let Some(i) = ed.idx { if i < app.env_store.envs.len() { app.env_store.envs[i] = new_env.clone(); app.env_store.selected = Some(i); } else { app.env_store.envs.push(new_env.clone()); app.env_store.selected = Some(app.env_store.envs.len()-1); } }
                                    else { app.env_store.envs.push(new_env.clone()); app.env_store.selected = Some(app.env_store.envs.len()-1); }
                                    let _ = app.env_store.save();
                                    if let Some(sel) = app.env_store.selected { if let Some(e) = app.env_store.envs.get(sel) { app.host = e.host.clone(); } }
                                    app.show_env_modal = false;
                                }
                            }
                        }
                        (KeyCode::Char('n'), KeyModifiers::CONTROL) => {
                            if app.show_env_modal { if let Some(ed) = app.env_editor.as_mut() { ed.idx=None; ed.name.clear(); ed.host.clear(); ed.private_key_pem.clear(); ed.public_key_pem.clear(); ed.ssl_ca_pem.clear(); ed.name_cursor=0; ed.host_cursor=0; ed.private_key_cursor=0; ed.public_key_cursor=0; ed.ssl_ca_cursor=0; ed.field_focus=EnvFieldFocus::Name; } }
                        }
                        (KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                            if app.show_env_modal { if let Some(i) = app.env_store.selected { if i<app.env_store.envs.len() { app.env_store.envs.remove(i); app.env_store.selected = if app.env_store.envs.is_empty() { None } else { Some((i).min(app.env_store.envs.len()-1)) }; let _=app.env_store.save(); } } }
                        }
                        (KeyCode::Char('t'), KeyModifiers::CONTROL) => {
                            if app.show_env_modal { if let Some(ed) = app.env_editor.as_ref() { let host = ed.host.clone(); let ssl = crate::models::SslConfig { ca_pem: if ed.ssl_ca_pem.trim().is_empty(){None}else{Some(ed.ssl_ca_pem.clone())}, cert_pem: if ed.public_key_pem.trim().is_empty(){None}else{Some(ed.public_key_pem.clone())}, key_pem: if ed.private_key_pem.trim().is_empty(){None}else{Some(ed.private_key_pem.clone())} }; match test_connection(&host, ssl).await { Ok(_) => app.status = format!("Connection OK: {}", host), Err(e) => app.status = format!("Connection failed: {}", e) } } }
                        }
                        (KeyCode::F(5), _) | (KeyCode::Char('y'), KeyModifiers::CONTROL) => {
                            if matches!(app.focus, super::app::Focus::Results) { if let Some(s) = selected_cell_text(&app) { match copy_to_clipboard(&s) { Ok(()) => app.status = "Copied to clipboard".to_string(), Err(e) => app.status = format!("Clipboard error: {}", e) } } }
                        }
                        (KeyCode::F(6), _) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                if let Some(s) = selected_cell_text(&app) {
                                    match copy_to_clipboard(&s) { Ok(()) => app.status = "Copied".to_string(), Err(e) => app.status = format!("Clipboard error: {}", e) }
                                    app.copy_btn_pressed = true; app.copy_btn_deadline = Some(Instant::now() + Duration::from_millis(150));
                                } else { app.status = "No content to copy".to_string(); }
                            }
                        }
                        (KeyCode::Char(ch), _) => {
                            if app.show_env_modal {
                                // Modal text input and commands
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => { ed.name.insert(ed.name_cursor, ch); ed.name_cursor+=1; }
                                        EnvFieldFocus::Host => { ed.host.insert(ed.host_cursor, ch); ed.host_cursor+=1; }
                                        EnvFieldFocus::PrivateKey => { ed.private_key_pem.insert(ed.private_key_cursor, ch); ed.private_key_cursor+=1; }
                                        EnvFieldFocus::PublicKey => { ed.public_key_pem.insert(ed.public_key_cursor, ch); ed.public_key_cursor+=1; }
                                        EnvFieldFocus::Ca => { ed.ssl_ca_pem.insert(ed.ssl_ca_cursor, ch); ed.ssl_ca_cursor+=1; }
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                                continue;
                            }
                            match app.focus {
                                super::app::Focus::Results => {
                                    // ignore normal chars in results
                                }
                                super::app::Focus::Host => {
                                    if app.show_env_modal {
                                        // NOP (handled below in modal)
                                    } else {
                                        // Previously host edit; now do nothing
                                    }
                                }
                                super::app::Focus::Query => { app.input.insert(app.input_cursor, ch); app.input_cursor+=1; }
                            }
                        }
                        (KeyCode::Esc, _) => {
                            if app.show_env_modal { app.show_env_modal = false; }
                            else if matches!(app.focus, super::app::Focus::Query) { app.input.clear(); }
                        }
                        // Navigation: results or env list
                        (KeyCode::Up, _) => {
                            if app.show_env_modal {
                                if let Some(sel) = app.env_store.selected { if sel > 0 { app.env_store.selected = Some(sel - 1); } }
                                else if !app.env_store.envs.is_empty() { app.env_store.selected = Some(0); }
                                if let Some(i) = app.env_store.selected { if let Some(e) = app.env_store.envs.get(i) { if let Some(ed) = app.env_editor.as_mut() { ed.idx = Some(i); ed.name = e.name.clone(); ed.host = e.host.clone(); ed.private_key_pem = e.private_key_pem.clone().unwrap_or_default(); ed.public_key_pem = e.public_key_pem.clone().unwrap_or_default(); ed.ssl_ca_pem = e.ssl_ca_pem.clone().unwrap_or_default(); } } }
                            } else if matches!(app.focus, super::app::Focus::Results) { if app.selected_row > 0 { app.selected_row -= 1; app.json_vscroll = 0; } }
                        }
                        (KeyCode::Down, _) => {
                            if app.show_env_modal {
                                if let Some(sel) = app.env_store.selected { if sel + 1 < app.env_store.envs.len() { app.env_store.selected = Some(sel + 1); } }
                                else if !app.env_store.envs.is_empty() { app.env_store.selected = Some(0); }
                                if let Some(i) = app.env_store.selected { if let Some(e) = app.env_store.envs.get(i) { if let Some(ed) = app.env_editor.as_mut() { ed.idx = Some(i); ed.name = e.name.clone(); ed.host = e.host.clone(); ed.private_key_pem = e.private_key_pem.clone().unwrap_or_default(); ed.public_key_pem = e.public_key_pem.clone().unwrap_or_default(); ed.ssl_ca_pem = e.ssl_ca_pem.clone().unwrap_or_default(); } } }
                            } else if matches!(app.focus, super::app::Focus::Results) { if app.selected_row + 1 < app.rows.len() { app.selected_row += 1; app.json_vscroll = 0; } }
                        }
                        (KeyCode::Left, KeyModifiers::SHIFT) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                app.table_hscroll = app.table_hscroll.saturating_sub(2);
                            }
                        }
                        (KeyCode::Right, KeyModifiers::SHIFT) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                app.table_hscroll = app.table_hscroll.saturating_add(2);
                            }
                        }
                        (KeyCode::Left, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => { if ed.name_cursor>0 { ed.name_cursor-=1; } }
                                        EnvFieldFocus::Host => { if ed.host_cursor>0 { ed.host_cursor-=1; } }
                                        EnvFieldFocus::PrivateKey => { if ed.private_key_cursor>0 { ed.private_key_cursor-=1; } }
                                        EnvFieldFocus::PublicKey => { if ed.public_key_cursor>0 { ed.public_key_cursor-=1; } }
                                        EnvFieldFocus::Ca => { if ed.ssl_ca_cursor>0 { ed.ssl_ca_cursor-=1; } }
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if app.selected_col > 0 { app.selected_col -= 1; } else { app.selected_col = 0; }
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if app.input_cursor>0 { app.input_cursor-=1; }
                            }
                        }
                        (KeyCode::Right, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => { if ed.name_cursor<ed.name.len(){ ed.name_cursor+=1; } }
                                        EnvFieldFocus::Host => { if ed.host_cursor<ed.host.len(){ ed.host_cursor+=1; } }
                                        EnvFieldFocus::PrivateKey => { if ed.private_key_cursor<ed.private_key_pem.len(){ ed.private_key_cursor+=1; } }
                                        EnvFieldFocus::PublicKey => { if ed.public_key_cursor<ed.public_key_pem.len(){ ed.public_key_cursor+=1; } }
                                        EnvFieldFocus::Ca => { if ed.ssl_ca_cursor<ed.ssl_ca_pem.len(){ ed.ssl_ca_cursor+=1; } }
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                let cols = if app.keys_only { 4 } else { 5 }; if app.selected_col + 1 < cols { app.selected_col += 1; }
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if app.input_cursor<app.input.len() { app.input_cursor+=1; }
                            }
                        }
                        (KeyCode::PageUp, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; app.selected_row = app.selected_row.saturating_sub(step); app.json_vscroll = 0; } }
                        (KeyCode::PageDown, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; if !app.rows.is_empty() { app.selected_row = (app.selected_row + step).min(app.rows.len()-1); app.json_vscroll = 0; } } }
                        (KeyCode::Home, _) => { if matches!(app.focus, super::app::Focus::Results) { app.selected_row = 0; app.json_vscroll = 0; } }
                        (KeyCode::End, _) => { if matches!(app.focus, super::app::Focus::Results) { if !app.rows.is_empty() { app.selected_row = app.rows.len()-1; app.json_vscroll = 0; } } }
                        _ => {}
                    }
                }
                Event::Mouse(me) => {
                    handle_mouse(&mut app, me);
                }
                _ => {}
            }
        }
    };

    // Restore terminal
    disable_raw_mode().ok();
    // Use crossterm global execute to restore screen
    execute!(std::io::stdout(), crossterm::event::DisableMouseCapture, terminal::LeaveAlternateScreen, crossterm::cursor::Show).ok();

    res
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

// Spawn pipeline but with ssl provided
async fn spawn_pipeline_with_ssl(args: RunArgs, query_text: String, run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>, ssl: Option<crate::models::SslConfig>) {
    tokio::spawn(async move {
        if let Err(e) = run_pipeline_with_ssl(args, query_text, run_id, tx.clone(), ssl).await {
            let _ = tx.send(TuiEvent::Error { run_id, message: e.to_string() });
        }
    });
}

async fn run_pipeline_with_ssl(args: RunArgs, query_text: String, run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>, ssl: Option<crate::models::SslConfig>) -> Result<()> {
    let ast = parse_query(&query_text).context("Failed to parse query")?;
    let topic = ast.from.clone();
    let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
    let max_messages_global = ast.limit.or(args.max_messages).or(Some(100));
    let order_desc = ast.order.as_ref().map(|o| matches!(o.dir, OrderDir::Desc)).unwrap_or(false);

    let mut cfg = ClientConfig::new();
    cfg
        .set("bootstrap.servers", &args.broker)
        .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true");
    if let Some(ssl) = &ssl {
        if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
            cfg.set("security.protocol", "ssl");
            if let Some(ref s) = ssl.ca_pem { cfg.set("ssl.ca.pem", s); }
            if let Some(ref s) = ssl.cert_pem { cfg.set("ssl.certificate.pem", s); }
            if let Some(ref s) = ssl.key_pem { cfg.set("ssl.key.pem", s); }
        }
    }
    let probe_consumer: StreamConsumer = cfg
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

    let (tx_msg, rx_msg) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);
    let offset_spec = OffsetSpec::from_str(&args.offset).unwrap_or_else(|_| OffsetSpec::Beginning);
    let query_arc = std::sync::Arc::new(ast.clone());

    let mut joinset = tokio::task::JoinSet::new();
    for &p in &partitions {
        let txp = tx_msg.clone();
        let mut a = args.clone();
        a.topic = Some(topic.clone());
        a.keys_only = keys_only;
        a.max_messages = None;
        let q = Some(query_arc.clone());
        let ssl_clone = ssl.clone();
        joinset.spawn(async move { spawn_partition_consumer(a, p, offset_spec, txp, q, ssl_clone).await });
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

async fn test_connection(host: &str, ssl: crate::models::SslConfig) -> Result<()> {
    let mut cfg = ClientConfig::new();
    cfg
        .set("bootstrap.servers", host)
        .set("group.id", format!("rkl-test-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true");
    if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
        cfg.set("security.protocol", "ssl");
        if let Some(s) = ssl.ca_pem { cfg.set("ssl.ca.pem", &s); }
        if let Some(s) = ssl.cert_pem { cfg.set("ssl.certificate.pem", &s); }
        if let Some(s) = ssl.key_pem { cfg.set("ssl.key.pem", &s); }
    }
    let consumer: StreamConsumer = cfg.create().context("create consumer")?;
    let _ = consumer.fetch_metadata(None, Duration::from_secs(5)).context("fetch metadata")?;
    Ok(())
}

fn handle_mouse(app: &mut AppState, me: MouseEvent) {
    // Compute the layout rects like ui.rs to know where the table and json panes are
    let (w, h) = match crossterm::terminal::size() { Ok(x) => x, Err(_) => (0, 0) };
    let root = Rect { x: 0, y: 0, width: w, height: h };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .split(root);
    let results_area = rows[3];
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(results_area);
    let table_rect = cols[0];
    let json_rect = cols[1];
    let json_inner = Rect { x: json_rect.x.saturating_add(1), y: json_rect.y.saturating_add(1), width: json_rect.width.saturating_sub(2), height: json_rect.height.saturating_sub(2) };

    let mx = me.column;
    let my = me.row;

    match me.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            if point_in(mx, my, table_rect) {
                // Map click Y to an approximate row index
                // account for borders + header (top border + header row)
                let data_start_y = table_rect.y.saturating_add(2);
                if my >= data_start_y && my < table_rect.y.saturating_add(table_rect.height.saturating_sub(1)) {
                    let y_rel = (my - data_start_y) as usize;
                    let visible_rows = table_rect.height.saturating_sub(3) as usize; // top border + header + bottom border
                    let approx_first = app.selected_row.saturating_sub(visible_rows / 2);
                    let new_row = (approx_first + y_rel).min(app.rows.len().saturating_sub(1));
                    if new_row != app.selected_row { app.selected_row = new_row; app.json_vscroll = 0; }
                }

                // Map click X to column index (approximate using constraints)
                let inner_x = table_rect.x.saturating_add(1);
                if mx >= inner_x {
                    let mut x_rel = (mx - inner_x) as usize;
                    // Partition (10)
                    let mut col = 0usize;
                    let mut widths = vec![10usize, 12, 26, 30];
                    if !app.keys_only { widths.push(usize::MAX); }
                    for (i, w) in widths.iter().enumerate() {
                        if i + 1 == widths.len() && *w == usize::MAX {
                            col = i; break;
                        }
                        if x_rel < *w { col = i; break; } else { x_rel = x_rel.saturating_sub(*w + 1); }
                    }
                    let max_cols = if app.keys_only { 4 } else { 5 };
                    if col < max_cols { if app.selected_col != col { app.selected_col = col; app.json_vscroll = 0; } }
                }
            } else if point_in(mx, my, json_rect) {
                // Detect click on Copy button in the JSON pane (top-right of inner area)
                let label = "[ Copy ]";
                let btn_w = label.chars().count() as u16;
                if json_inner.width >= btn_w {
                    let btn_rect = Rect { x: json_inner.x + json_inner.width - btn_w, y: json_inner.y, width: btn_w, height: 1 };
                    if point_in(mx, my, btn_rect) {
                        if let Some(s) = selected_cell_text(app) {
                            if let Err(e) = copy_to_clipboard(&s) { app.status = format!("Clipboard error: {}", e); } else { app.status = "Payload copied".to_string(); }
                            app.copy_btn_pressed = true; app.copy_btn_deadline = Some(Instant::now() + Duration::from_millis(150));
                        } else {
                            app.status = "No payload to copy".to_string();
                        }
                        return; // handled
                    }
                }
                // Otherwise, ignore; allow native selection by terminal
            }
        }
        MouseEventKind::ScrollUp => {
            if point_in(mx, my, table_rect) {
                if app.selected_row > 0 { app.selected_row -= 1; }
            } else if point_in(mx, my, json_rect) {
                app.json_vscroll = app.json_vscroll.saturating_sub(1);
            }
        }
        MouseEventKind::ScrollDown => {
            if point_in(mx, my, table_rect) {
                if app.selected_row + 1 < app.rows.len() { app.selected_row += 1; }
            } else if point_in(mx, my, json_rect) {
                app.json_vscroll = app.json_vscroll.saturating_add(1);
            }
        }
        MouseEventKind::ScrollLeft => {
            if point_in(mx, my, table_rect) {
                app.table_hscroll = app.table_hscroll.saturating_sub(4);
            }
        }
        MouseEventKind::ScrollRight => {
            if point_in(mx, my, table_rect) {
                app.table_hscroll = app.table_hscroll.saturating_add(4);
            }
        }
        _ => {}
    }
}

fn point_in(x: u16, y: u16, r: Rect) -> bool {
    x >= r.x && x < r.x.saturating_add(r.width) && y >= r.y && y < r.y.saturating_add(r.height)
}
