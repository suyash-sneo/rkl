use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind, MouseButton};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use crossterm::event::{PushKeyboardEnhancementFlags, PopKeyboardEnhancementFlags, KeyboardEnhancementFlags};
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
use super::env_store::config_dir;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write as _;
use super::env_store::Environment;
use super::ui::draw;

pub async fn run(args: RunArgs) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    // Enter alt screen, enable mouse, and request enhanced keyboard so Ctrl-Enter is detectable on supporting terminals (kitty/wezterm/xterm)
    execute!(
        stdout,
        terminal::EnterAlternateScreen,
        crossterm::event::EnableMouseCapture,
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
                TuiEvent::EnvTestProgress { message } => {
                    app.env_test_in_progress = true;
                    app.env_test_message = Some(message);
                }
                TuiEvent::EnvTestDone { message } => {
                    app.env_test_in_progress = false;
                    app.env_test_message = Some(message);
                }
            }
        }

        // Handle key input (non-blocking poll)
        if crossterm::event::poll(Duration::from_millis(50))? {
            match crossterm::event::read()? {
                Event::Key(key) => {
                    // With keyboard enhancement flags, terminals can emit Press/Repeat/Release.
                    // Only act on Press to avoid duplicate input.
                    if key.kind != KeyEventKind::Press { continue; }
                    let KeyEvent { code, modifiers, .. } = key;
                    match (code, modifiers) {
                        (KeyCode::Char('c'), KeyModifiers::CONTROL) => break Ok(()),
                        (KeyCode::Char('q'), KeyModifiers::CONTROL) => break Ok(()),
                        // Some macOS terminals send Ctrl-Enter as Ctrl-J (LF) or Ctrl-M (CR)
                        // Ctrl-Enter (and common terminal fallbacks) → run
                        (KeyCode::Char('j'), m) | (KeyCode::Char('m'), m) if m.contains(KeyModifiers::CONTROL) => {
                            if !app.show_env_modal && matches!(app.focus, super::app::Focus::Query) {
                                let (qs, qe) = find_query_range(&app.input, app.input_cursor);
                                let input = app.input[qs..qe].trim().to_string();
                                if input.is_empty() { app.status = "Please enter a query".to_string(); continue; }
                                match parse_query(&input) {
                                    Ok(ast) => {
                                        let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
                                        app.keys_only = keys_only;
                                        app.clear_rows();
                                        run_counter += 1;
                                        app.current_run = Some(run_counter);
                                        app.last_run_query_range = Some((qs, qe));
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
                        (KeyCode::Enter, m) if m.contains(KeyModifiers::CONTROL) => {
                            if !app.show_env_modal && matches!(app.focus, super::app::Focus::Query) {
                                let (qs, qe) = find_query_range(&app.input, app.input_cursor);
                                let input = app.input[qs..qe].trim().to_string();
                                if input.is_empty() { app.status = "Please enter a query".to_string(); continue; }
                                match parse_query(&input) {
                                    Ok(ast) => {
                                        let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
                                        app.keys_only = keys_only;
                                        app.clear_rows();
                                        run_counter += 1;
                                        app.current_run = Some(run_counter);
                                        app.last_run_query_range = Some((qs, qe));
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
                        // Enter: editor newline; open env modal from host bar
                        (KeyCode::Enter, _) => {
                            if app.show_env_modal {
                                // In modal: Enter inserts newline in multiline fields
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::PrivateKey => { ed.private_key_pem.insert(ed.private_key_cursor, '\n'); ed.private_key_cursor+=1; }
                                        EnvFieldFocus::PublicKey => { ed.public_key_pem.insert(ed.public_key_cursor, '\n'); ed.public_key_cursor+=1; }
                                        EnvFieldFocus::Ca => { ed.ssl_ca_pem.insert(ed.ssl_ca_cursor, '\n'); ed.ssl_ca_cursor+=1; }
                                        _ => { /* ignore */ }
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Host) {
                                // Open env modal
                                let (idx, name, host, privk, pubk, ca_pem) = if let Some(env) = app.selected_env() {
                                    (app.env_store.selected, env.name.clone(), env.host.clone(), env.private_key_pem.clone().unwrap_or_default(), env.public_key_pem.clone().unwrap_or_default(), env.ssl_ca_pem.clone().unwrap_or_default())
                                } else {
                                    (None, String::new(), app.host.clone(), String::new(), String::new(), String::new())
                                };
                                app.env_editor = Some(EnvEditor { idx, name_cursor: 0, name, host_cursor: 0, host, private_key_cursor: 0, private_key_pem: privk, public_key_cursor: 0, public_key_pem: pubk, ssl_ca_cursor: 0, ssl_ca_pem: ca_pem, private_key_vscroll: 0, public_key_vscroll: 0, ca_vscroll: 0, private_key_hscroll: 0, public_key_hscroll: 0, ca_hscroll: 0, field_focus: EnvFieldFocus::Name });
                                app.show_env_modal = true;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                // GUI-like: Enter inserts newline in editor, ensure caret stays visible
                                app.input.insert(app.input_cursor, '\n');
                                app.input_cursor += 1;
                                ensure_input_cursor_visible(&mut app);
                            } else {
                                // Results: ignore Enter
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
                                        EnvFieldFocus::Conn => {}
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
                                        EnvFieldFocus::Conn => {}
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
                                    ed.field_focus = match ed.field_focus { EnvFieldFocus::Name => EnvFieldFocus::Host, EnvFieldFocus::Host => EnvFieldFocus::PrivateKey, EnvFieldFocus::PrivateKey => EnvFieldFocus::PublicKey, EnvFieldFocus::PublicKey => EnvFieldFocus::Ca, EnvFieldFocus::Ca => EnvFieldFocus::Conn, EnvFieldFocus::Conn => EnvFieldFocus::Buttons, EnvFieldFocus::Buttons => EnvFieldFocus::Name };
                                }
                            } else {
                                app.next_focus();
                            }
                        }
                        (KeyCode::BackTab, _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    ed.field_focus = match ed.field_focus { EnvFieldFocus::Name => EnvFieldFocus::Buttons, EnvFieldFocus::Host => EnvFieldFocus::Name, EnvFieldFocus::PrivateKey => EnvFieldFocus::Host, EnvFieldFocus::PublicKey => EnvFieldFocus::PrivateKey, EnvFieldFocus::Ca => EnvFieldFocus::PublicKey, EnvFieldFocus::Conn => EnvFieldFocus::Ca, EnvFieldFocus::Buttons => EnvFieldFocus::Conn };
                                }
                            }
                        }
                        // Save (F4)
                        (KeyCode::F(4), _) => {
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
                        // New (F1)
                        (KeyCode::F(1), _) => { if app.show_env_modal { if let Some(ed) = app.env_editor.as_mut() { ed.idx=None; ed.name.clear(); ed.host.clear(); ed.private_key_pem.clear(); ed.public_key_pem.clear(); ed.ssl_ca_pem.clear(); ed.name_cursor=0; ed.host_cursor=0; ed.private_key_cursor=0; ed.public_key_cursor=0; ed.ssl_ca_cursor=0; ed.private_key_vscroll=0; ed.public_key_vscroll=0; ed.ca_vscroll=0; ed.private_key_hscroll=0; ed.public_key_hscroll=0; ed.ca_hscroll=0; ed.field_focus=EnvFieldFocus::Name; } } }
                        // Delete (F3)
                        (KeyCode::F(3), _) => {
                            if app.show_env_modal { if let Some(i) = app.env_store.selected { if i<app.env_store.envs.len() { app.env_store.envs.remove(i); app.env_store.selected = if app.env_store.envs.is_empty() { None } else { Some((i).min(app.env_store.envs.len()-1)) }; let _=app.env_store.save(); } } }
                        }
                        // F5 is context-sensitive: in env modal -> test connection; in results -> copy cell
                        (KeyCode::F(5), _) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_ref() {
                                    let host = ed.host.clone();
                                    let ssl = crate::models::SslConfig { ca_pem: if ed.ssl_ca_pem.trim().is_empty(){None}else{Some(ed.ssl_ca_pem.clone())}, cert_pem: if ed.public_key_pem.trim().is_empty(){None}else{Some(ed.public_key_pem.clone())}, key_pem: if ed.private_key_pem.trim().is_empty(){None}else{Some(ed.private_key_pem.clone())} };
                                    // Prefer CA PEM; do not auto-create ssl.ca.location if PEM is provided
                                    // Start debug log
                                    let _ = start_test_log(&host, &ssl);
                                    app.env_test_in_progress = true;
                                    app.env_test_message = Some(format!("Connecting to {}...", host));
                                    let txp = tx_evt.clone();
                                    tokio::spawn(async move {
                                        let _ = txp.send(TuiEvent::EnvTestProgress { message: format!("Configuring client for {}", host) });
                                        append_test_log_line(&format!("[step] configure client for host={}", host));
                                        let mut cfg = ClientConfig::new();
                                        cfg
                                            .set("bootstrap.servers", &host)
                                            .set("group.id", format!("rkl-test-{}", uuid::Uuid::new_v4()))
                                            .set("enable.auto.commit", "false")
                                            .set("auto.offset.reset", "earliest")
                                            .set("enable.partition.eof", "true");
                                        if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
                                            cfg.set("security.protocol", "ssl");
                                            if let Some(ref s) = ssl.ca_pem { cfg.set("ssl.ca.pem", s); }
                                            if let Some(ref s) = ssl.cert_pem { cfg.set("ssl.certificate.pem", s); }
                                            if let Some(ref s) = ssl.key_pem { cfg.set("ssl.key.pem", s); }
                                            // Use supported debug contexts; omit "ssl" token (not recognized in some builds)
                                            cfg.set("debug", "security,broker,protocol");
                                        }
                                        // Record effective TLS params (redacted)
                                        append_test_log_line(&format!(
                                            "[params] security.protocol=ssl, using_ca=pem, ca.pem_len={}, cert.pem_len={}, key.pem_len={}",
                                            ssl.ca_pem.as_ref().map(|s| s.len()).unwrap_or(0),
                                            ssl.cert_pem.as_ref().map(|s| s.len()).unwrap_or(0),
                                            ssl.key_pem.as_ref().map(|s| s.len()).unwrap_or(0)
                                        ));
                                        if let Some(ref s) = ssl.ca_pem { append_test_log_line(&format!("[params] ssl.ca.pem head={}.. len={}", &s.chars().take(24).collect::<String>(), s.len())); }
                                        if let Some(ref s) = ssl.cert_pem { append_test_log_line(&format!("[params] ssl.certificate.pem head={}.. len={}", &s.chars().take(24).collect::<String>(), s.len())); }
                                        if let Some(ref s) = ssl.key_pem { append_test_log_line(&format!("[params] ssl.key.pem head={}.. len={}", &s.chars().take(24).collect::<String>(), s.len())); }
                                        let _ = txp.send(TuiEvent::EnvTestProgress { message: "Creating consumer".to_string() });
                                        append_test_log_line("[step] create consumer");
                                        let consumer: Result<StreamConsumer, _> = cfg.create();
                                        match consumer {
                                            Ok(c) => {
                                                append_test_log_line("[ok] consumer created");
                                                let _ = txp.send(TuiEvent::EnvTestProgress { message: "Fetching metadata".to_string() });
                                                append_test_log_line("[step] fetch metadata (timeout=5s)");
                                                match c.fetch_metadata(None, Duration::from_secs(5)) {
                                                    Ok(md) => { append_test_log_line(&format!("[ok] metadata: brokers={}, topics={}", md.brokers().len(), md.topics().len())); let _ = txp.send(TuiEvent::EnvTestDone { message: format!("Connection OK: {}", host) }); },
                                                    Err(e) => { append_test_log_line(&format!("[err] metadata fetch: {:?}", e)); let _ = txp.send(TuiEvent::EnvTestDone { message: format!("Metadata error: {}", e) }); },
                                                }
                                            }
                                            Err(e) => {
                                                append_test_log_line(&format!("[err] consumer create: {:?}", e));
                                                let _ = txp.send(TuiEvent::EnvTestDone { message: format!("Create error: {}", e) });
                                            }
                                        }
                                    });
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if let Some(s) = selected_cell_text(&app) { match copy_to_clipboard(&s) { Ok(()) => app.status = "Copied to clipboard".to_string(), Err(e) => app.status = format!("Clipboard error: {}", e) } }
                            }
                        }
                        // (F8 removed)
                        // Field navigation via function keys (in modal)
                        (KeyCode::F(6), _) => { if app.show_env_modal { if let Some(ed) = app.env_editor.as_mut() { ed.field_focus = match ed.field_focus { EnvFieldFocus::Name => EnvFieldFocus::Host, EnvFieldFocus::Host => EnvFieldFocus::PrivateKey, EnvFieldFocus::PrivateKey => EnvFieldFocus::PublicKey, EnvFieldFocus::PublicKey => EnvFieldFocus::Ca, EnvFieldFocus::Ca => EnvFieldFocus::Conn, EnvFieldFocus::Conn => EnvFieldFocus::Buttons, EnvFieldFocus::Buttons => EnvFieldFocus::Name }; } } }
                        (KeyCode::F(7), _) => { if app.show_env_modal { if let Some(ed) = app.env_editor.as_mut() { ed.field_focus = match ed.field_focus { EnvFieldFocus::Name => EnvFieldFocus::Buttons, EnvFieldFocus::Host => EnvFieldFocus::Name, EnvFieldFocus::PrivateKey => EnvFieldFocus::Host, EnvFieldFocus::PublicKey => EnvFieldFocus::PrivateKey, EnvFieldFocus::Ca => EnvFieldFocus::PublicKey, EnvFieldFocus::Conn => EnvFieldFocus::Ca, EnvFieldFocus::Buttons => EnvFieldFocus::Conn }; } } }
                        // Toggle mouse selection mode (disable/enable mouse capture)
                        (KeyCode::F(9), _) => {
                            if app.mouse_selection_mode {
                                let _ = crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture);
                                app.mouse_selection_mode = false;
                                app.status = "Mouse capture enabled".to_string();
                            } else {
                                let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
                                app.mouse_selection_mode = true;
                                app.status = "Mouse selection mode: drag to select/copy; F9 to return".to_string();
                            }
                        }
                        // Edit preselected item (F2)
                        (KeyCode::F(2), _) => {
                            if app.show_env_modal {
                                if let Some(i) = app.env_store.selected { if let Some(e) = app.env_store.envs.get(i) { if let Some(ed) = app.env_editor.as_mut() { ed.idx = Some(i); ed.name = e.name.clone(); ed.host = e.host.clone(); ed.private_key_pem = e.private_key_pem.clone().unwrap_or_default(); ed.public_key_pem = e.public_key_pem.clone().unwrap_or_default(); ed.ssl_ca_pem = e.ssl_ca_pem.clone().unwrap_or_default(); ed.field_focus = EnvFieldFocus::Name; ed.name_cursor = ed.name.len(); ed.host_cursor = ed.host.len(); ed.private_key_cursor = ed.private_key_pem.len(); ed.public_key_cursor = ed.public_key_pem.len(); ed.ssl_ca_cursor = ed.ssl_ca_pem.len(); } } }
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
                                        EnvFieldFocus::Conn => {}
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
                            else if matches!(app.focus, super::app::Focus::Query) { move_cursor_up(&mut app); }
                        }
                        (KeyCode::Down, _) => {
                            if app.show_env_modal {
                                if let Some(sel) = app.env_store.selected { if sel + 1 < app.env_store.envs.len() { app.env_store.selected = Some(sel + 1); } }
                                else if !app.env_store.envs.is_empty() { app.env_store.selected = Some(0); }
                                if let Some(i) = app.env_store.selected { if let Some(e) = app.env_store.envs.get(i) { if let Some(ed) = app.env_editor.as_mut() { ed.idx = Some(i); ed.name = e.name.clone(); ed.host = e.host.clone(); ed.private_key_pem = e.private_key_pem.clone().unwrap_or_default(); ed.public_key_pem = e.public_key_pem.clone().unwrap_or_default(); ed.ssl_ca_pem = e.ssl_ca_pem.clone().unwrap_or_default(); } } }
                            } else if matches!(app.focus, super::app::Focus::Results) { if app.selected_row + 1 < app.rows.len() { app.selected_row += 1; app.json_vscroll = 0; } }
                            else if matches!(app.focus, super::app::Focus::Query) { move_cursor_down(&mut app); }
                        }
                        (KeyCode::Left, KeyModifiers::SHIFT) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus { EnvFieldFocus::PrivateKey => { ed.private_key_hscroll = ed.private_key_hscroll.saturating_sub(2); }, EnvFieldFocus::PublicKey => { ed.public_key_hscroll = ed.public_key_hscroll.saturating_sub(2); }, EnvFieldFocus::Ca => { ed.ca_hscroll = ed.ca_hscroll.saturating_sub(2); }, _ => {} }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) { app.table_hscroll = app.table_hscroll.saturating_sub(2); }
                        }
                        (KeyCode::Right, KeyModifiers::SHIFT) => {
                            if app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus { EnvFieldFocus::PrivateKey => { ed.private_key_hscroll = ed.private_key_hscroll.saturating_add(2); }, EnvFieldFocus::PublicKey => { ed.public_key_hscroll = ed.public_key_hscroll.saturating_add(2); }, EnvFieldFocus::Ca => { ed.ca_hscroll = ed.ca_hscroll.saturating_add(2); }, _ => {} }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) { app.table_hscroll = app.table_hscroll.saturating_add(2); }
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
                                        EnvFieldFocus::Conn => {}
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
                                        EnvFieldFocus::Conn => {}
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                let cols = if app.keys_only { 4 } else { 5 }; if app.selected_col + 1 < cols { app.selected_col += 1; }
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if app.input_cursor<app.input.len() { app.input_cursor+=1; ensure_input_cursor_visible(&mut app); }
                            }
                        }
                        (KeyCode::PageUp, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; app.selected_row = app.selected_row.saturating_sub(step); app.json_vscroll = 0; } else if matches!(app.focus, super::app::Focus::Query) { scroll_input(&mut app, true); } }
                        (KeyCode::PageDown, _) => { if matches!(app.focus, super::app::Focus::Results) { let step = 10; if !app.rows.is_empty() { app.selected_row = (app.selected_row + step).min(app.rows.len()-1); app.json_vscroll = 0; } } else if matches!(app.focus, super::app::Focus::Query) { scroll_input(&mut app, false); } }
                        (KeyCode::Home, _) => { if matches!(app.focus, super::app::Focus::Results) { app.selected_row = 0; app.json_vscroll = 0; } else if matches!(app.focus, super::app::Focus::Query) { move_cursor_line_home(&mut app); } }
                        (KeyCode::End, _) => { if matches!(app.focus, super::app::Focus::Results) { if !app.rows.is_empty() { app.selected_row = app.rows.len()-1; app.json_vscroll = 0; } } else if matches!(app.focus, super::app::Focus::Query) { move_cursor_line_end(&mut app); } }
                        _ => {}
                    }
                }
                Event::Mouse(me) => {
                    handle_mouse(&mut app, me);
                }
                Event::Paste(s) => {
                    if app.show_env_modal {
                        if let Some(ed) = app.env_editor.as_mut() {
                            match ed.field_focus {
                                EnvFieldFocus::PrivateKey => { for ch in s.chars() { ed.private_key_pem.insert(ed.private_key_cursor, ch); ed.private_key_cursor+=1; } }
                                EnvFieldFocus::PublicKey => { for ch in s.chars() { ed.public_key_pem.insert(ed.public_key_cursor, ch); ed.public_key_cursor+=1; } }
                                EnvFieldFocus::Ca => { for ch in s.chars() { ed.ssl_ca_pem.insert(ed.ssl_ca_cursor, ch); ed.ssl_ca_cursor+=1; } }
                                EnvFieldFocus::Name => { for ch in s.chars() { ed.name.insert(ed.name_cursor, ch); ed.name_cursor+=1; } }
                                EnvFieldFocus::Host => { for ch in s.chars() { ed.host.insert(ed.host_cursor, ch); ed.host_cursor+=1; } }
                                _ => {}
                            }
                        }
                    } else if matches!(app.focus, super::app::Focus::Query) {
                        for ch in s.chars() { app.input.insert(app.input_cursor, ch); app.input_cursor+=1; }
                        ensure_input_cursor_visible(&mut app);
                    }
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
    ).ok();

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

fn ensure_ca_file_for_env(name_hint: &str, pem: &str) -> Result<String> {
    let dir = config_dir();
    std::fs::create_dir_all(&dir).context("create env dir for CA")?;
    let fname = format!("{}-ca.pem", sanitize(name_hint));
    let path = dir.join(fname);
    fs::write(&path, pem).context("write CA pem file")?;
    Ok(path.to_string_lossy().to_string())
}

fn sanitize(name: &str) -> String {
    name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

fn logs_dir() -> std::path::PathBuf {
    std::env::var("HOME").map(|h| std::path::PathBuf::from(h).join(".rkl").join("logs")).unwrap_or_else(|_| std::path::PathBuf::from(".rkl").join("logs"))
}

fn append_test_log_line(line: &str) {
    let dir = logs_dir();
    if let Err(e) = fs::create_dir_all(&dir) {
        eprintln!("[rkl] failed to create logs dir {:?}: {}", dir, e);
        return;
    }
    let fpath = dir.join("test-connection.out");
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&fpath) {
        let ts = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "".into());
        let _ = writeln!(f, "{} {}", ts, line);
    }
}

fn start_test_log(host: &str, ssl: &crate::models::SslConfig) -> Result<()> {
    let dir = logs_dir();
    fs::create_dir_all(&dir).ok();
    let fpath = dir.join("test-connection.out");
    // Truncate file at start of each test for clarity
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&fpath).context("open test log file")?;
    let ts = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "".into());
    let _ = writeln!(f, "{} [start] test connection host={} ca_pem_len={} cert_pem_len={} key_pem_len={}",
        ts,
        host,
        ssl.ca_pem.as_ref().map(|s| s.len()).unwrap_or(0),
        ssl.cert_pem.as_ref().map(|s| s.len()).unwrap_or(0),
        ssl.key_pem.as_ref().map(|s| s.len()).unwrap_or(0),
    );
    Ok(())
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

// (Removed unused test_connection)

fn handle_mouse(app: &mut AppState, me: MouseEvent) {
    if app.mouse_selection_mode { return; }
    // Compute the layout rects like ui.rs to know where the table and json panes are
    let (w, h) = match crossterm::terminal::size() { Ok(x) => x, Err(_) => (0, 0) };
    let root = Rect { x: 0, y: 0, width: w, height: h };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .split(root);
    let query_area = rows[1];
    // Derive editor inner & content rects (gutter width 6, border 1)
    let q_inner = Rect { x: query_area.x.saturating_add(1), y: query_area.y.saturating_add(1), width: query_area.width.saturating_sub(2), height: query_area.height.saturating_sub(2) };
    let q_cols = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Length(6), Constraint::Min(1)]).split(q_inner);
    let _q_gutter = q_cols[0]; let q_content = q_cols[1];
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
            if app.show_env_modal {
                // Recreate modal layout for hit testing
                let popup_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Percentage(10),
                        Constraint::Percentage(80),
                        Constraint::Percentage(10),
                    ])
                    .split(root);
                let center_v = popup_rows[1];
                let popup_cols = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(10),
                        Constraint::Percentage(80),
                        Constraint::Percentage(10),
                    ])
                    .split(center_v);
                let modal_area = popup_cols[1];
                let area = modal_area; // matches centered_rect(80,80)
                let cols = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
                    .margin(1)
                    .split(area);
                let fields = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(5),
                        Constraint::Min(5),
                        Constraint::Min(5),
                        Constraint::Length(3),
                        Constraint::Length(3),
                    ])
                    .split(cols[1]);
                // Connection [Copy] in title row (approximate: top line of block, right side)
                let conn = fields[6];
                if point_in(mx, my, conn) {
                    // treat top border line as y == conn.y
                    let inner = Rect { x: conn.x.saturating_add(1), y: conn.y.saturating_add(1), width: conn.width.saturating_sub(2), height: conn.height.saturating_sub(2) };
                    if mx >= inner.x + inner.width.saturating_sub(8) && my == conn.y {
                        if let Some(ref s) = app.env_test_message { let _ = copy_to_clipboard(s); }
                        return;
                    }
                }
                // Copy for large fields
                let maybe_copy_field = |rect: Rect, content: &str| -> bool {
                    if point_in(mx, my, rect) {
                        let inner = Rect { x: rect.x.saturating_add(1), y: rect.y.saturating_add(1), width: rect.width.saturating_sub(2), height: rect.height.saturating_sub(2) };
                        if mx >= inner.x + inner.width.saturating_sub(8) && my == rect.y {
                            let _ = copy_to_clipboard(content);
                            return true;
                        }
                    }
                    false
                };
                if let Some(ed) = app.env_editor.as_ref() {
                    if maybe_copy_field(fields[2], &ed.private_key_pem) { return; }
                    if maybe_copy_field(fields[3], &ed.public_key_pem) { return; }
                    if maybe_copy_field(fields[4], &ed.ssl_ca_pem) { return; }
                }
            }
            if point_in(mx, my, q_content) {
                // Position cursor by click
                let y_rel = my.saturating_sub(q_content.y) as usize;
                let target_line = app.input_vscroll as usize + y_rel;
                let line_starts = compute_line_starts(&app.input);
                let line = target_line.min(line_starts.len().saturating_sub(1));
                let line_start = line_starts[line];
                let line_end = if line + 1 < line_starts.len() { line_starts[line + 1] - 1 } else { app.input.len() };
                let x_rel = mx.saturating_sub(q_content.x) as usize;
                let col = x_rel.min(line_end.saturating_sub(line_start));
                app.input_cursor = line_start + col;
                ensure_input_cursor_visible(app);
                return;
            }
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
            if app.show_env_modal {
                // Build modal fields again
                let popup_rows = Layout::default().direction(Direction::Vertical).constraints([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)]).split(root);
                let center_v = popup_rows[1];
                let popup_cols = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)]).split(center_v);
                let area = popup_cols[1];
                let cols2 = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage(30), Constraint::Percentage(70)]).margin(1).split(area);
                let fields = Layout::default().direction(Direction::Vertical).constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(5), Constraint::Min(5), Constraint::Min(5), Constraint::Length(3), Constraint::Length(3)]).split(cols2[1]);
                if let Some(ed) = app.env_editor.as_mut() {
                    if point_in(mx, my, fields[2]) { ed.private_key_vscroll = ed.private_key_vscroll.saturating_sub(1); return; }
                    if point_in(mx, my, fields[3]) { ed.public_key_vscroll = ed.public_key_vscroll.saturating_sub(1); return; }
                    if point_in(mx, my, fields[4]) { ed.ca_vscroll = ed.ca_vscroll.saturating_sub(1); return; }
                }
                if point_in(mx, my, fields[6]) { app.env_conn_vscroll = app.env_conn_vscroll.saturating_sub(1); return; }
            }
            if point_in(mx, my, q_content) {
                app.input_vscroll = app.input_vscroll.saturating_sub(1);
            } else if point_in(mx, my, table_rect) {
                if app.selected_row > 0 { app.selected_row -= 1; }
            } else if point_in(mx, my, json_rect) {
                app.json_vscroll = app.json_vscroll.saturating_sub(1);
            }
        }
        MouseEventKind::ScrollDown => {
            if app.show_env_modal {
                let popup_rows = Layout::default().direction(Direction::Vertical).constraints([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)]).split(root);
                let center_v = popup_rows[1];
                let popup_cols = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)]).split(center_v);
                let area = popup_cols[1];
                let cols2 = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage(30), Constraint::Percentage(70)]).margin(1).split(area);
                let fields = Layout::default().direction(Direction::Vertical).constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(5), Constraint::Min(5), Constraint::Min(5), Constraint::Length(3), Constraint::Length(3)]).split(cols2[1]);
                if let Some(ed) = app.env_editor.as_mut() {
                    if point_in(mx, my, fields[2]) { ed.private_key_vscroll = ed.private_key_vscroll.saturating_add(1); return; }
                    if point_in(mx, my, fields[3]) { ed.public_key_vscroll = ed.public_key_vscroll.saturating_add(1); return; }
                    if point_in(mx, my, fields[4]) { ed.ca_vscroll = ed.ca_vscroll.saturating_add(1); return; }
                }
                if point_in(mx, my, fields[6]) { app.env_conn_vscroll = app.env_conn_vscroll.saturating_add(1); return; }
            }
            if point_in(mx, my, q_content) {
                app.input_vscroll = app.input_vscroll.saturating_add(1);
            } else if point_in(mx, my, table_rect) {
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

fn compute_line_starts(text: &str) -> Vec<usize> {
    let mut v = Vec::new();
    let mut acc = 0usize;
    for (i, l) in text.split('\n').enumerate() {
        v.push(acc);
        acc += l.len();
        if i + 1 < text.split('\n').count() { acc += 1; }
    }
    if v.is_empty() { v.push(0); }
    v
}

fn find_query_range(s: &str, cursor: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let cur = cursor.min(bytes.len());
    let mut start = 0usize;
    let mut i = cur;
    while i > 0 {
        i -= 1;
        let b = bytes[i];
        if b == b';' { start = i + 1; break; }
        if b == b'\n' && i > 0 && bytes[i - 1] == b'\n' { start = i + 1; break; }
    }
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

fn move_cursor_up(app: &mut AppState) {
    let (line, col) = line_col(&app.input, app.input_cursor);
    if line == 0 { return; }
    let prev_start = nth_line_start(&app.input, line - 1);
    let prev_len = line_len(&app.input, line - 1);
    app.input_cursor = prev_start + col.min(prev_len);
    ensure_input_cursor_visible(app);
}

fn move_cursor_down(app: &mut AppState) {
    let (line, col) = line_col(&app.input, app.input_cursor);
    let total = app.input.split('\n').count();
    if line + 1 >= total { return; }
    let next_start = nth_line_start(&app.input, line + 1);
    let next_len = line_len(&app.input, line + 1);
    app.input_cursor = next_start + col.min(next_len);
    ensure_input_cursor_visible(app);
}

fn move_cursor_line_home(app: &mut AppState) {
    let (line, _) = line_col(&app.input, app.input_cursor);
    app.input_cursor = nth_line_start(&app.input, line);
    ensure_input_cursor_visible(app);
}

fn move_cursor_line_end(app: &mut AppState) {
    let (line, _) = line_col(&app.input, app.input_cursor);
    let start = nth_line_start(&app.input, line);
    let len = line_len(&app.input, line);
    app.input_cursor = start + len;
    ensure_input_cursor_visible(app);
}

fn line_col(text: &str, cursor: usize) -> (usize, usize) {
    let idx = cursor.min(text.len());
    let mut count = 0usize;
    for (i, l) in text.split('\n').enumerate() {
        let llen = l.len();
        if count + llen >= idx { return (i, idx - count); } else { count += llen + 1; }
    }
    (0, 0)
}

fn nth_line_start(text: &str, n: usize) -> usize {
    if n == 0 { return 0; }
    let mut count = 0usize;
    for (i, l) in text.split('\n').enumerate() {
        if i == n { return count; }
        count += l.len() + 1;
    }
    text.len()
}

fn line_len(text: &str, n: usize) -> usize {
    text.split('\n').nth(n).map(|l| l.len()).unwrap_or(0)
}

fn ensure_input_cursor_visible(app: &mut AppState) {
    // Keep cursor within the visible editor viewport using actual layout metrics
    let (w, h) = crossterm::terminal::size().unwrap_or((0, 0));
    if w == 0 || h == 0 { return; }
    // Mirror ui.rs layout
    let root = Rect { x: 0, y: 0, width: w, height: h };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // host
            Constraint::Length(8), // editor
            Constraint::Length(1), // status
            Constraint::Fill(1),   // results
            Constraint::Length(3), // footer
        ])
        .split(root);
    let query_area = rows[1];
    let inner = Rect { x: query_area.x.saturating_add(1), y: query_area.y.saturating_add(1), width: query_area.width.saturating_sub(2), height: query_area.height.saturating_sub(2) };
    let cols = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Length(6), Constraint::Min(1)]).split(inner);
    let content = cols[1];
    let visible_lines = content.height.max(1) as usize;

    let (line, col) = line_col(&app.input, app.input_cursor);
    let wrap_w = content.width.max(1) as usize;
    let vis_line = line + (col / wrap_w);
    let top = app.input_vscroll as usize;
    let bottom_excl = top + visible_lines;
    if vis_line < top {
        app.input_vscroll = vis_line as u16;
    } else if vis_line >= bottom_excl {
        app.input_vscroll = (vis_line + 1 - visible_lines) as u16;
    }
}

fn scroll_input(app: &mut AppState, up: bool) {
    if up { app.input_vscroll = app.input_vscroll.saturating_sub(5); } else { app.input_vscroll = app.input_vscroll.saturating_add(5); }
}
