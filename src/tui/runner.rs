use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::event::{
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tokio::sync::mpsc;

use crate::args::RunArgs;
use crate::consumer::spawn_partition_consumer;
use crate::merger::run_merger;
use crate::models::{MessageEnvelope, OffsetSpec};
use crate::output::OutputSink;
use crate::query::{OrderDir, SelectItem, parse_query};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::{Consumer, StreamConsumer};

use super::app::{AppState, EnvEditor, EnvFieldFocus, Screen, TuiEvent};
use super::env_store::Environment;
use super::env_store::config_dir;
use super::query_bounds::{find_query_range, strip_trailing_semicolon};
use super::ui::draw;

const ENV_COPY_LABEL: &str = "[Copy]";
const ENV_PASTE_LABEL: &str = "[Paste]";
const ENV_CLEAR_LABEL: &str = "[Clear]";
const ENV_CONN_PASTE_LABEL: &str = "[Paste/F9 Select]";

fn decode_display(s: &str) -> String {
    s.replace("\\n", "\n")
}

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
#[cfg(unix)]
use libc;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use tui_textarea::{Input as TAInput, Key as TAKey, TextArea};

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
                if Instant::now() >= deadline {
                    app.copy_btn_pressed = false;
                    app.copy_btn_deadline = None;
                }
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
                    if Some(run_id) == app.current_run {
                        app.status = format!("Run {run_id} complete");
                        if !app.status_buffer.is_empty() {
                            app.status_buffer.push('\n');
                        }
                        app.status_buffer
                            .push_str(&format!("✔ Completed run {}", run_id));
                    }
                }
                TuiEvent::Error { run_id, message } => {
                    if Some(run_id) == app.current_run {
                        app.status = format!("Error: {message}");
                        if !app.status_buffer.is_empty() {
                            app.status_buffer.push('\n');
                        }
                        app.status_buffer
                            .push_str(&format!("✘ Error (run {}): {}", run_id, message));
                    }
                }
                TuiEvent::EnvTestProgress { message } => {
                    app.env_test_in_progress = true;
                    app.env_test_message = Some(message.clone());
                    if !app.status_buffer.is_empty() {
                        app.status_buffer.push('\n');
                    }
                    app.status_buffer
                        .push_str(&format!("[env-test] {}", message));
                }
                TuiEvent::EnvTestDone { message } => {
                    app.env_test_in_progress = false;
                    app.env_test_message = Some(message.clone());
                    if !app.status_buffer.is_empty() {
                        app.status_buffer.push('\n');
                    }
                    app.status_buffer
                        .push_str(&format!("[env-test] {}", message));
                }
                TuiEvent::Topics(list) => {
                    app.topics = list;
                }
            }
        }

        // Handle key input (non-blocking poll)
        if crossterm::event::poll(Duration::from_millis(50))? {
            match crossterm::event::read()? {
                Event::Key(key) => {
                    // Honor both Press and Repeat so held keys accelerate movement/editing.
                    if !(key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat) {
                        continue;
                    }
                    let KeyEvent {
                        code, modifiers, ..
                    } = key;
                    match (code, modifiers) {
                        (KeyCode::Char('c'), KeyModifiers::CONTROL) => break Ok(()),
                        (KeyCode::Char('q'), KeyModifiers::CONTROL) => break Ok(()),
                        (KeyCode::F(10), _) => {
                            app.show_help = !app.show_help;
                        }
                        (KeyCode::F(8), _) => {
                            app.screen = Screen::Home;
                        }
                        (KeyCode::F(2), _) => {
                            app.screen = Screen::Envs;
                            if app.env_editor.is_none() {
                                if let Some(i) = app.env_store.selected {
                                    if let Some(e) = app.env_store.envs.get(i) {
                                        app.env_editor =
                                            Some(build_env_editor_from_env(e, Some(i)));
                                    }
                                }
                            }
                        }
                        (KeyCode::F(12), _) => {
                            app.screen = Screen::Info;
                            fetch_topics_async(&app, tx_evt.clone());
                        }
                        (KeyCode::F(6), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                move_env_selection(&mut app, 1);
                            } else if matches!(app.screen, Screen::Info) {
                                fetch_topics_async(&app, tx_evt.clone());
                            }
                        }
                        (KeyCode::F(7), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                move_env_selection(&mut app, -1);
                            } else {
                                let txt = if app.status_buffer.is_empty() {
                                    app.status.clone()
                                } else {
                                    app.status_buffer.clone()
                                };
                                if !txt.trim().is_empty() {
                                    let _ = copy_to_clipboard(&txt);
                                }
                            }
                        }
                        // Some macOS terminals send Ctrl-Enter as Ctrl-J (LF) or Ctrl-M (CR)
                        // Ctrl-Enter (and common terminal fallbacks) → run
                        (KeyCode::Char('j'), m) | (KeyCode::Char('m'), m)
                            if m.contains(KeyModifiers::CONTROL) =>
                        {
                            if matches!(app.screen, Screen::Home)
                                && !app.show_env_modal
                                && matches!(app.focus, super::app::Focus::Query)
                            {
                                let (qs, qe) = find_query_range(&app.input, app.input_cursor);
                                let raw = &app.input[qs..qe];
                                let query = strip_trailing_semicolon(raw).trim().to_string();
                                if query.is_empty() {
                                    app.status = "Please enter a query".to_string();
                                    continue;
                                }
                                match parse_query(&query) {
                                    Ok(ast) => {
                                        let columns = ast.select.clone();
                                        app.selected_columns = columns;
                                        app.table_hscroll = 0;
                                        app.clear_rows();
                                        run_counter += 1;
                                        app.current_run = Some(run_counter);
                                        app.last_run_query_range = Some((qs, qe));
                                        let env_host = app
                                            .selected_env()
                                            .map(|e| e.host.clone())
                                            .unwrap_or(app.host.clone());
                                        app.status = format!(
                                            "Running (run {}): topic '{}' on {}. Press q to quit.",
                                            run_counter, ast.from, env_host
                                        );
                                        let mut run_args = args.clone();
                                        run_args.broker = env_host;
                                        app.clamp_selection();
                                        let ssl = app.current_ssl_config();
                                        spawn_pipeline_with_ssl(
                                            run_args,
                                            query,
                                            run_counter,
                                            tx_evt.clone(),
                                            ssl,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        app.status = format!("Parse error: {}", e);
                                    }
                                }
                            }
                        }
                        (KeyCode::Enter, m) if m.contains(KeyModifiers::CONTROL) => {
                            if matches!(app.screen, Screen::Home)
                                && !app.show_env_modal
                                && matches!(app.focus, super::app::Focus::Query)
                            {
                                let (qs, qe) = find_query_range(&app.input, app.input_cursor);
                                let raw = &app.input[qs..qe];
                                let query = strip_trailing_semicolon(raw).trim().to_string();
                                if query.is_empty() {
                                    app.status = "Please enter a query".to_string();
                                    continue;
                                }
                                match parse_query(&query) {
                                    Ok(ast) => {
                                        let columns = ast.select.clone();
                                        app.selected_columns = columns;
                                        app.table_hscroll = 0;
                                        app.clear_rows();
                                        run_counter += 1;
                                        app.current_run = Some(run_counter);
                                        app.last_run_query_range = Some((qs, qe));
                                        let env_host = app
                                            .selected_env()
                                            .map(|e| e.host.clone())
                                            .unwrap_or(app.host.clone());
                                        app.status = format!(
                                            "Running (run {}): topic '{}' on {}. Press q to quit.",
                                            run_counter, ast.from, env_host
                                        );
                                        let mut run_args = args.clone();
                                        run_args.broker = env_host;
                                        app.clamp_selection();
                                        let ssl = app.current_ssl_config();
                                        spawn_pipeline_with_ssl(
                                            run_args,
                                            query,
                                            run_counter,
                                            tx_evt.clone(),
                                            ssl,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        app.status = format!("Parse error: {}", e);
                                    }
                                }
                            }
                        }
                        // Enter: editor newline; open env screen from host bar
                        (KeyCode::Enter, _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Name => {}
                                        EnvFieldFocus::Host => {}
                                        _ => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Host) {
                                // Open env screen
                                let (idx, env) = if let Some(env) = app.selected_env() {
                                    (app.env_store.selected, env.clone())
                                } else {
                                    (
                                        None,
                                        Environment {
                                            name: String::new(),
                                            host: app.host.clone(),
                                            private_key_pem: None,
                                            public_key_pem: None,
                                            ssl_ca_pem: None,
                                        },
                                    )
                                };
                                let mut editor = build_env_editor_from_env(&env, idx);
                                editor.name_cursor = editor.name.len();
                                editor.host_cursor = editor.host.len();
                                app.env_editor = Some(editor);
                                app.screen = Screen::Envs;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                // Enter inserts newline in editor, ensure caret stays visible
                                app.input.insert(app.input_cursor, '\n');
                                app.input_cursor += 1;
                                ensure_input_cursor_visible(&mut app);
                            } else {
                                // Results: ignore Enter
                            }
                        }
                        (KeyCode::Backspace, m) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                let mut meta_changed = false;
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => {
                                            if ed.name_cursor > 0 {
                                                ed.name.remove(ed.name_cursor - 1);
                                                ed.name_cursor -= 1;
                                                meta_changed = true;
                                            }
                                        }
                                        EnvFieldFocus::Host => {
                                            if ed.host_cursor > 0 {
                                                ed.host.remove(ed.host_cursor - 1);
                                                ed.host_cursor -= 1;
                                                meta_changed = true;
                                            }
                                        }
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                        }
                                        _ => {}
                                    }
                                }
                                if meta_changed {
                                    sync_env_metadata_from_editor(&mut app);
                                }
                                continue;
                            }
                            match app.focus {
                                super::app::Focus::Host => { /* no-op */ }
                                super::app::Focus::Query => {
                                    if has_ctrl_or_alt(m) {
                                        delete_prev_word(&mut app);
                                    } else if app.input_cursor > 0 {
                                        app.input.remove(app.input_cursor - 1);
                                        app.input_cursor -= 1;
                                    }
                                }
                                super::app::Focus::Results => {}
                            }
                        }
                        (KeyCode::Delete, m) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                let mut meta_changed = false;
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => {
                                            if ed.name_cursor < ed.name.len() {
                                                ed.name.remove(ed.name_cursor);
                                                meta_changed = true;
                                            }
                                        }
                                        EnvFieldFocus::Host => {
                                            if ed.host_cursor < ed.host.len() {
                                                ed.host.remove(ed.host_cursor);
                                                meta_changed = true;
                                            }
                                        }
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                        }
                                        _ => {}
                                    }
                                }
                                if meta_changed {
                                    sync_env_metadata_from_editor(&mut app);
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if has_ctrl_or_alt(m) {
                                    delete_next_word(&mut app);
                                } else if app.input_cursor < app.input.len() {
                                    app.input.remove(app.input_cursor);
                                }
                            }
                        }
                        (KeyCode::Char('\t'), _) | (KeyCode::Tab, _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    ed.field_focus = match ed.field_focus {
                                        EnvFieldFocus::Name => EnvFieldFocus::Host,
                                        EnvFieldFocus::Host => EnvFieldFocus::PrivateKey,
                                        EnvFieldFocus::PrivateKey => EnvFieldFocus::PublicKey,
                                        EnvFieldFocus::PublicKey => EnvFieldFocus::Ca,
                                        EnvFieldFocus::Ca => EnvFieldFocus::Conn,
                                        EnvFieldFocus::Conn => EnvFieldFocus::Buttons,
                                        EnvFieldFocus::Buttons => EnvFieldFocus::Name,
                                    };
                                }
                            } else {
                                app.next_focus();
                            }
                        }
                        (KeyCode::BackTab, _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    ed.field_focus = match ed.field_focus {
                                        EnvFieldFocus::Name => EnvFieldFocus::Buttons,
                                        EnvFieldFocus::Host => EnvFieldFocus::Name,
                                        EnvFieldFocus::PrivateKey => EnvFieldFocus::Host,
                                        EnvFieldFocus::PublicKey => EnvFieldFocus::PrivateKey,
                                        EnvFieldFocus::Ca => EnvFieldFocus::PublicKey,
                                        EnvFieldFocus::Conn => EnvFieldFocus::Ca,
                                        EnvFieldFocus::Buttons => EnvFieldFocus::Conn,
                                    };
                                }
                            }
                        }
                        // Save (F4)
                        (KeyCode::F(4), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    let pk = ed.ta_private.lines().join("\n");
                                    let cert = ed.ta_public.lines().join("\n");
                                    let ca = ed.ta_ca.lines().join("\n");
                                    let exists_name =
                                        app.env_store.envs.iter().enumerate().any(|(i, e)| {
                                            i != ed.idx.unwrap_or(usize::MAX)
                                                && e.name.eq_ignore_ascii_case(&ed.name)
                                        });
                                    if ed.name.trim().is_empty() {
                                        app.status = "Environment name cannot be empty".to_string();
                                        continue;
                                    }
                                    if ed.idx.is_none() && exists_name {
                                        app.status = "Environment name already exists. Choose a unique name.".to_string();
                                        continue;
                                    }
                                    let new_env = Environment {
                                        name: ed.name.clone(),
                                        host: ed.host.clone(),
                                        private_key_pem: if pk.trim().is_empty() {
                                            None
                                        } else {
                                            Some(pk)
                                        },
                                        public_key_pem: if cert.trim().is_empty() {
                                            None
                                        } else {
                                            Some(cert)
                                        },
                                        ssl_ca_pem: if ca.trim().is_empty() {
                                            None
                                        } else {
                                            Some(ca)
                                        },
                                    };
                                    if let Some(i) = ed.idx {
                                        if i < app.env_store.envs.len() {
                                            app.env_store.envs[i] = new_env.clone();
                                            app.env_store.selected = Some(i);
                                        } else {
                                            app.env_store.envs.push(new_env.clone());
                                            app.env_store.selected =
                                                Some(app.env_store.envs.len() - 1);
                                        }
                                    } else {
                                        app.env_store.envs.push(new_env.clone());
                                        app.env_store.selected = Some(app.env_store.envs.len() - 1);
                                    }
                                    let _ = app.env_store.save();
                                    if let Some(sel) = app.env_store.selected {
                                        if let Some(e) = app.env_store.envs.get(sel) {
                                            app.host = e.host.clone();
                                        }
                                    }
                                    if app.show_env_modal {
                                        app.show_env_modal = false;
                                    }
                                }
                            }
                        }
                        // New (F1)
                        (KeyCode::F(1), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
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
                            }
                        }
                        // Delete (F3)
                        (KeyCode::F(3), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(i) = app.env_store.selected {
                                    if i < app.env_store.envs.len() {
                                        app.env_store.envs.remove(i);
                                        app.env_store.selected = if app.env_store.envs.is_empty() {
                                            None
                                        } else {
                                            Some((i).min(app.env_store.envs.len() - 1))
                                        };
                                        let _ = app.env_store.save();
                                        sync_env_editor_to_selection(&mut app);
                                    }
                                }
                            }
                        }
                        // F5 is context-sensitive: in env modal -> test connection; in results -> copy cell
                        (KeyCode::F(5), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_ref() {
                                    let host = ed.host.clone();
                                    let pk = ed.ta_private.lines().join("\n");
                                    let cert = ed.ta_public.lines().join("\n");
                                    let ca = ed.ta_ca.lines().join("\n");
                                    let ssl = crate::models::SslConfig {
                                        ca_pem: if ca.trim().is_empty() { None } else { Some(ca) },
                                        cert_pem: if cert.trim().is_empty() {
                                            None
                                        } else {
                                            Some(cert)
                                        },
                                        key_pem: if pk.trim().is_empty() { None } else { Some(pk) },
                                    };
                                    // Prefer CA PEM; do not auto-create ssl.ca.location if PEM is provided
                                    // Start debug log
                                    let _ = start_test_log(&host, &ssl);
                                    app.env_test_in_progress = true;
                                    app.env_test_message =
                                        Some(format!("Connecting to {}...", host));
                                    let txp = tx_evt.clone();
                                    tokio::spawn(async move {
                                        // Ensure anything printed by the SSL libs is redirected to log file only.
                                        #[cfg(unix)]
                                        let _guard = redirect_stdio_to_file(
                                            &logs_dir().join("test-connection.out"),
                                        )
                                        .ok();
                                        let _ = txp.send(TuiEvent::EnvTestProgress {
                                            message: format!("Configuring client for {}", host),
                                        });
                                        append_test_log_line(&format!(
                                            "[step] configure client for host={}",
                                            host
                                        ));
                                        let mut cfg = ClientConfig::new();
                                        cfg.set("bootstrap.servers", &host)
                                            .set(
                                                "group.id",
                                                format!("rkl-test-{}", uuid::Uuid::new_v4()),
                                            )
                                            .set("enable.auto.commit", "false")
                                            .set("auto.offset.reset", "earliest")
                                            .set("enable.partition.eof", "true");
                                        if ssl.ca_pem.is_some()
                                            || ssl.cert_pem.is_some()
                                            || ssl.key_pem.is_some()
                                        {
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
                                        if let Some(ref s) = ssl.ca_pem {
                                            append_test_log_line(&format!(
                                                "[params] ssl.ca.pem head={}.. len={}",
                                                &s.chars().take(24).collect::<String>(),
                                                s.len()
                                            ));
                                        }
                                        if let Some(ref s) = ssl.cert_pem {
                                            append_test_log_line(&format!(
                                                "[params] ssl.certificate.pem head={}.. len={}",
                                                &s.chars().take(24).collect::<String>(),
                                                s.len()
                                            ));
                                        }
                                        if let Some(ref s) = ssl.key_pem {
                                            append_test_log_line(&format!(
                                                "[params] ssl.key.pem head={}.. len={}",
                                                &s.chars().take(24).collect::<String>(),
                                                s.len()
                                            ));
                                        }
                                        cfg.set("log_level", "1");
                                        let _ = txp.send(TuiEvent::EnvTestProgress {
                                            message: "Creating consumer".to_string(),
                                        });
                                        append_test_log_line("[step] create consumer");
                                        let consumer: Result<StreamConsumer, _> = cfg.create();
                                        match consumer {
                                            Ok(c) => {
                                                append_test_log_line("[ok] consumer created");
                                                let _ = txp.send(TuiEvent::EnvTestProgress {
                                                    message: "Fetching metadata".to_string(),
                                                });
                                                append_test_log_line(
                                                    "[step] fetch metadata (timeout=5s)",
                                                );
                                                match c.fetch_metadata(None, Duration::from_secs(5))
                                                {
                                                    Ok(md) => {
                                                        append_test_log_line(&format!(
                                                            "[ok] metadata: brokers={}, topics={}",
                                                            md.brokers().len(),
                                                            md.topics().len()
                                                        ));
                                                        let _ = txp.send(TuiEvent::EnvTestDone {
                                                            message: format!(
                                                                "Connection OK: {}",
                                                                host
                                                            ),
                                                        });
                                                    }
                                                    Err(e) => {
                                                        append_test_log_line(&format!(
                                                            "[err] metadata fetch: {:?}",
                                                            e
                                                        ));
                                                        let _ = txp.send(TuiEvent::EnvTestDone {
                                                            message: format!(
                                                                "Metadata error: {}",
                                                                e
                                                            ),
                                                        });
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                append_test_log_line(&format!(
                                                    "[err] consumer create: {:?}",
                                                    e
                                                ));
                                                let _ = txp.send(TuiEvent::EnvTestDone {
                                                    message: format!("Create error: {}", e),
                                                });
                                            }
                                        }
                                    });
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if let Some(s) = selected_cell_text(&app) {
                                    match copy_to_clipboard(&s) {
                                        Ok(()) => app.status = "Copied to clipboard".to_string(),
                                        Err(e) => app.status = format!("Clipboard error: {}", e),
                                    }
                                }
                            }
                        }
                        // (F8 removed)
                        // Toggle mouse selection mode (disable/enable mouse capture)
                        (KeyCode::F(9), _) => {
                            if app.mouse_selection_mode {
                                let _ = crossterm::execute!(
                                    std::io::stdout(),
                                    crossterm::event::EnableMouseCapture
                                );
                                app.mouse_selection_mode = false;
                                app.status = "Mouse capture enabled".to_string();
                            } else {
                                let _ = crossterm::execute!(
                                    std::io::stdout(),
                                    crossterm::event::DisableMouseCapture
                                );
                                app.mouse_selection_mode = true;
                                app.status =
                                    "Mouse selection mode: drag to select/copy; F9 to return"
                                        .to_string();
                            }
                        }
                        (KeyCode::Char(ch), _) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                let mut meta_changed = false;
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => {
                                            ed.name.insert(ed.name_cursor, ch);
                                            ed.name_cursor += 1;
                                            meta_changed = true;
                                        }
                                        EnvFieldFocus::Host => {
                                            ed.host.insert(ed.host_cursor, ch);
                                            ed.host_cursor += 1;
                                            meta_changed = true;
                                        }
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(TAInput {
                                                key: TAKey::Char(ch),
                                                ctrl: false,
                                                alt: false,
                                                shift: false,
                                            });
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(TAInput {
                                                key: TAKey::Char(ch),
                                                ctrl: false,
                                                alt: false,
                                                shift: false,
                                            });
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(TAInput {
                                                key: TAKey::Char(ch),
                                                ctrl: false,
                                                alt: false,
                                                shift: false,
                                            });
                                        }
                                        _ => {}
                                    }
                                }
                                if meta_changed {
                                    sync_env_metadata_from_editor(&mut app);
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
                                super::app::Focus::Query => {
                                    app.input.insert(app.input_cursor, ch);
                                    app.input_cursor += 1;
                                }
                            }
                        }
                        (KeyCode::Esc, _) => {
                            if app.show_env_modal {
                                app.show_env_modal = false;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                app.input.clear();
                                app.input_cursor = 0;
                                ensure_input_cursor_visible(&mut app);
                            }
                        }
                        // Navigation: results or env list / textareas
                        (KeyCode::Up, _) => {
                            if matches!(app.screen, Screen::Envs) {
                                let mut handled = false;
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        _ => {}
                                    }
                                }
                                if !handled {
                                    move_env_selection(&mut app, -1);
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if app.selected_row > 0 {
                                    app.selected_row -= 1;
                                    app.json_vscroll = 0;
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                move_cursor_up(&mut app);
                            }
                        }
                        (KeyCode::Down, _) => {
                            if matches!(app.screen, Screen::Envs) {
                                let mut handled = false;
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                            handled = true;
                                        }
                                        _ => {}
                                    }
                                }
                                if !handled {
                                    move_env_selection(&mut app, 1);
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if app.selected_row + 1 < app.rows.len() {
                                    app.selected_row += 1;
                                    app.json_vscroll = 0;
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                move_cursor_down(&mut app);
                            }
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
                        (KeyCode::Left, m) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => {
                                            if ed.name_cursor > 0 {
                                                ed.name_cursor -= 1;
                                            }
                                        }
                                        EnvFieldFocus::Host => {
                                            if ed.host_cursor > 0 {
                                                ed.host_cursor -= 1;
                                            }
                                        }
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Conn => {}
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                if app.selected_col > 0 {
                                    app.selected_col -= 1;
                                } else {
                                    app.selected_col = 0;
                                }
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if has_ctrl_or_alt(m) {
                                    move_prev_word(&mut app);
                                } else if app.input_cursor > 0 {
                                    app.input_cursor -= 1;
                                }
                            }
                        }
                        (KeyCode::Right, m) => {
                            if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                                if let Some(ed) = app.env_editor.as_mut() {
                                    match ed.field_focus {
                                        EnvFieldFocus::Name => {
                                            if ed.name_cursor < ed.name.len() {
                                                ed.name_cursor += 1;
                                            }
                                        }
                                        EnvFieldFocus::Host => {
                                            if ed.host_cursor < ed.host.len() {
                                                ed.host_cursor += 1;
                                            }
                                        }
                                        EnvFieldFocus::PrivateKey => {
                                            ed.ta_private.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::PublicKey => {
                                            ed.ta_public.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Ca => {
                                            ed.ta_ca.input(ta_input_from_key(key));
                                        }
                                        EnvFieldFocus::Conn => {}
                                        EnvFieldFocus::Buttons => {}
                                    }
                                }
                            } else if matches!(app.focus, super::app::Focus::Results) {
                                let cols = app.selected_columns.len();
                                if cols > 0 && app.selected_col + 1 < cols {
                                    app.selected_col += 1;
                                }
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if has_ctrl_or_alt(m) {
                                    move_next_word(&mut app);
                                } else if app.input_cursor < app.input.len() {
                                    app.input_cursor += 1;
                                    ensure_input_cursor_visible(&mut app);
                                }
                            }
                        }
                        (KeyCode::PageUp, _) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                let step = 10;
                                app.selected_row = app.selected_row.saturating_sub(step);
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                scroll_input(&mut app, true);
                            }
                        }
                        (KeyCode::PageDown, _) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                let step = 10;
                                if !app.rows.is_empty() {
                                    app.selected_row =
                                        (app.selected_row + step).min(app.rows.len() - 1);
                                    app.json_vscroll = 0;
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                scroll_input(&mut app, false);
                            }
                        }
                        (KeyCode::Home, m) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                app.selected_row = 0;
                                app.json_vscroll = 0;
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if m.contains(KeyModifiers::CONTROL) {
                                    goto_start_of_doc(&mut app);
                                } else {
                                    move_cursor_line_home(&mut app);
                                }
                            }
                        }
                        (KeyCode::End, m) => {
                            if matches!(app.focus, super::app::Focus::Results) {
                                if !app.rows.is_empty() {
                                    app.selected_row = app.rows.len() - 1;
                                    app.json_vscroll = 0;
                                }
                            } else if matches!(app.focus, super::app::Focus::Query) {
                                if m.contains(KeyModifiers::CONTROL) {
                                    goto_end_of_doc(&mut app);
                                } else {
                                    move_cursor_line_end(&mut app);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Event::Mouse(me) => {
                    // Also route to textareas in Envs screen for scroll/paste-like mouse actions
                    if matches!(app.screen, Screen::Envs) {
                        if let Some(ed) = app.env_editor.as_mut() {
                            let inp = ta_input_from_mouse(me);
                            ed.ta_private.input(inp.clone());
                            ed.ta_public.input(inp.clone());
                            ed.ta_ca.input(inp);
                        }
                    }
                    handle_mouse(&mut app, me);
                }
                Event::Paste(s) => {
                    let mut handled = false;
                    if matches!(app.screen, Screen::Envs) || app.show_env_modal {
                        handled = handle_env_editor_paste(&mut app, &s);
                    }
                    if !handled && matches!(app.focus, super::app::Focus::Query) {
                        for ch in s.chars() {
                            app.input.insert(app.input_cursor, ch);
                            app.input_cursor += 1;
                        }
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
    )
    .ok();

    res
}

struct TuiOutput {
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    buffer: Vec<MessageEnvelope>,
}

impl TuiOutput {
    fn new(run_id: u64, tx: mpsc::UnboundedSender<TuiEvent>) -> Self {
        Self {
            run_id,
            tx,
            buffer: Vec::with_capacity(256),
        }
    }
}

impl OutputSink for TuiOutput {
    fn push(&mut self, env: &MessageEnvelope) {
        self.buffer.push(env.clone());
    }
    fn flush_block(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.buffer);
        let _ = self.tx.send(TuiEvent::Batch {
            run_id: self.run_id,
            rows: out,
        });
    }
}

// Spawn pipeline but with ssl provided
async fn spawn_pipeline_with_ssl(
    args: RunArgs,
    query_text: String,
    run_id: u64,
    tx: mpsc::UnboundedSender<TuiEvent>,
    ssl: Option<crate::models::SslConfig>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_pipeline_with_ssl(args, query_text, run_id, tx.clone(), ssl).await {
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
) -> Result<()> {
    let ast = parse_query(&query_text).context("Failed to parse query")?;
    let topic = ast.from.clone();
    let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
    let max_messages_global = ast.limit.or(args.max_messages).or(Some(100));
    let order_desc = ast
        .order
        .as_ref()
        .map(|o| matches!(o.dir, OrderDir::Desc))
        .unwrap_or(false);

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &args.broker)
        .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true");
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
        joinset.spawn(async move {
            spawn_partition_consumer(a, p, offset_spec, txp, q, ssl_clone).await
        });
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
    Some(runner_column_text(env, col))
}

fn runner_column_text(env: &MessageEnvelope, col: SelectItem) -> String {
    match col {
        SelectItem::Partition => env.partition.to_string(),
        SelectItem::Offset => env.offset.to_string(),
        SelectItem::Timestamp => fmt_ts(env.timestamp_ms),
        SelectItem::Key => env.key.clone(),
        SelectItem::Value => env.value.as_deref().unwrap_or("null").to_string(),
    }
}

fn runner_column_width_hint(col: SelectItem) -> usize {
    match col {
        SelectItem::Partition => 10,
        SelectItem::Offset => 12,
        SelectItem::Timestamp => 26,
        SelectItem::Key => 30,
        SelectItem::Value => usize::MAX,
    }
}

#[allow(dead_code)]
fn ensure_ca_file_for_env(name_hint: &str, pem: &str) -> Result<String> {
    let dir = config_dir();
    std::fs::create_dir_all(&dir).context("create env dir for CA")?;
    let fname = format!("{}-ca.pem", sanitize(name_hint));
    let path = dir.join(fname);
    fs::write(&path, pem).context("write CA pem file")?;
    Ok(path.to_string_lossy().to_string())
}

#[allow(dead_code)]
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn logs_dir() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(|h| std::path::PathBuf::from(h).join(".rkl").join("logs"))
        .unwrap_or_else(|_| std::path::PathBuf::from(".rkl").join("logs"))
}

fn append_test_log_line(line: &str) {
    let dir = logs_dir();
    let _ = fs::create_dir_all(&dir);
    let fpath = dir.join("test-connection.out");
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&fpath) {
        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "".into());
        let _ = writeln!(f, "{} {}", ts, line);
    }
}

fn start_test_log(host: &str, ssl: &crate::models::SslConfig) -> Result<()> {
    let dir = logs_dir();
    fs::create_dir_all(&dir).ok();
    let fpath = dir.join("test-connection.out");
    // Truncate file at start of each test for clarity
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&fpath)
        .context("open test log file")?;
    let ts = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "".into());
    let _ = writeln!(
        f,
        "{} [start] test connection host={} ca_pem_len={} cert_pem_len={} key_pem_len={}",
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
    if ms <= 0 {
        return "0".to_string();
    }
    let secs = ms / 1000;
    let tm = time::OffsetDateTime::from_unix_timestamp(secs as i64)
        .unwrap_or_else(|_| time::OffsetDateTime::UNIX_EPOCH);
    tm.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| ms.to_string())
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
            EnvFieldFocus::PrivateKey => {
                ed.ta_private.insert_str(normalize_pem_input(raw));
                handled = true;
            }
            EnvFieldFocus::PublicKey => {
                ed.ta_public.insert_str(normalize_pem_input(raw));
                handled = true;
            }
            EnvFieldFocus::Ca => {
                ed.ta_ca.insert_str(normalize_pem_input(raw));
                handled = true;
            }
            EnvFieldFocus::Conn | EnvFieldFocus::Buttons => {}
        }
    }
    if meta_changed {
        sync_env_metadata_from_editor(app);
    }
    handled
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
    let decoded = decode_display(&input);
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
        ssl_ca_cursor: 0,
        field_focus: EnvFieldFocus::Name,
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

fn handle_mouse(app: &mut AppState, me: MouseEvent) {
    if app.mouse_selection_mode {
        return;
    }
    // Compute the layout rects like ui.rs to know where the table and json panes are
    let (w, h) = match crossterm::terminal::size() {
        Ok(x) => x,
        Err(_) => (0, 0),
    };
    let root = Rect {
        x: 0,
        y: 0,
        width: w,
        height: h,
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(10),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .split(root);
    let query_area = rows[1];
    // Split row into editor and status
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(query_area);
    let status_rect = cols[1];
    let status_inner = Rect {
        x: status_rect.x.saturating_add(1),
        y: status_rect.y.saturating_add(1),
        width: status_rect.width.saturating_sub(2),
        height: status_rect.height.saturating_sub(2),
    };
    // Derive editor inner & content rects (gutter width 6, border 1)
    let q_inner = Rect {
        x: query_area.x.saturating_add(1),
        y: query_area.y.saturating_add(1),
        width: query_area.width.saturating_sub(2),
        height: query_area.height.saturating_sub(2),
    };
    let q_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(6), Constraint::Min(1)])
        .split(q_inner);
    let _q_gutter = q_cols[0];
    let q_content = q_cols[1];
    let results_area = rows[2];
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(results_area);
    let table_rect = cols[0];
    let json_rect = cols[1];
    let json_inner = Rect {
        x: json_rect.x.saturating_add(1),
        y: json_rect.y.saturating_add(1),
        width: json_rect.width.saturating_sub(2),
        height: json_rect.height.saturating_sub(2),
    };

    let mx = me.column;
    let my = me.row;

    match me.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            if let Some(field_rects) = env_editor_fields(app, root) {
                if handle_env_copy_paste_click(app, &field_rects, mx, my) {
                    return;
                }
            }
            // Status copy button click
            {
                let label = "[ Copy ]";
                let btn_w = label.chars().count() as u16;
                if status_inner.width >= btn_w {
                    let btn_rect = Rect {
                        x: status_inner.x + status_inner.width - btn_w,
                        y: status_inner.y,
                        width: btn_w,
                        height: 1,
                    };
                    if point_in(mx, my, btn_rect) {
                        let text = if app.status_buffer.is_empty() {
                            app.status.clone()
                        } else {
                            app.status_buffer.clone()
                        };
                        if !text.trim().is_empty() {
                            let _ = copy_to_clipboard(&text);
                            app.copy_btn_pressed = true;
                            app.copy_btn_deadline =
                                Some(Instant::now() + Duration::from_millis(150));
                        }
                        return;
                    }
                }
            }

            if point_in(mx, my, q_content) {
                // Position cursor by click
                let y_rel = my.saturating_sub(q_content.y) as usize;
                let target_line = app.input_vscroll as usize + y_rel;
                let line_starts = compute_line_starts(&app.input);
                let line = target_line.min(line_starts.len().saturating_sub(1));
                let line_start = line_starts[line];
                let line_end = if line + 1 < line_starts.len() {
                    line_starts[line + 1] - 1
                } else {
                    app.input.len()
                };
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
                if my >= data_start_y
                    && my
                        < table_rect
                            .y
                            .saturating_add(table_rect.height.saturating_sub(1))
                {
                    let y_rel = (my - data_start_y) as usize;
                    let visible_rows = table_rect.height.saturating_sub(3) as usize; // top border + header + bottom border
                    let approx_first = app.selected_row.saturating_sub(visible_rows / 2);
                    let new_row = (approx_first + y_rel).min(app.rows.len().saturating_sub(1));
                    if new_row != app.selected_row {
                        app.selected_row = new_row;
                        app.json_vscroll = 0;
                    }
                }

                // Map click X to column index (approximate using constraints)
                let inner_x = table_rect.x.saturating_add(1);
                if mx >= inner_x {
                    let mut x_rel = (mx - inner_x) as usize;
                    let mut col = 0usize;
                    let widths: Vec<usize> = app
                        .selected_columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| {
                            let mut w = runner_column_width_hint(*c);
                            if i + 1 < app.selected_columns.len() {
                                w = w.saturating_add(1);
                            }
                            w
                        })
                        .collect();
                    if !widths.is_empty() {
                        for (i, w) in widths.iter().enumerate() {
                            if *w == usize::MAX {
                                col = i;
                                break;
                            }
                            if x_rel < *w {
                                col = i;
                                break;
                            } else {
                                x_rel = x_rel.saturating_sub(*w);
                            }
                        }
                        if col >= widths.len() {
                            col = widths.len() - 1;
                        }
                        if app.selected_col != col {
                            app.selected_col = col;
                            app.json_vscroll = 0;
                        }
                    }
                }
            } else if point_in(mx, my, json_rect) {
                // Detect click on Copy button in the JSON pane (top-right of inner area)
                let label = "[ Copy ]";
                let btn_w = label.chars().count() as u16;
                if json_inner.width >= btn_w {
                    let btn_rect = Rect {
                        x: json_inner.x + json_inner.width - btn_w,
                        y: json_inner.y,
                        width: btn_w,
                        height: 1,
                    };
                    if point_in(mx, my, btn_rect) {
                        if let Some(s) = selected_cell_text(app) {
                            if let Err(e) = copy_to_clipboard(&s) {
                                app.status = format!("Clipboard error: {}", e);
                            } else {
                                app.status = "Payload copied".to_string();
                            }
                            app.copy_btn_pressed = true;
                            app.copy_btn_deadline =
                                Some(Instant::now() + Duration::from_millis(150));
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
                let area = popup_cols[1];
                let cols2 = Layout::default()
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
                    .split(cols2[1]);
                if let Some(ed) = app.env_editor.as_mut() {
                    // route scroll to textareas
                    let inp = ta_input_from_mouse(me);
                    ed.ta_private.input(inp.clone());
                    ed.ta_public.input(inp.clone());
                    ed.ta_ca.input(inp);
                }
                if point_in(mx, my, fields[6]) {
                    app.env_conn_vscroll = app.env_conn_vscroll.saturating_sub(1);
                    return;
                }
            }
            if point_in(mx, my, q_content) {
                app.input_vscroll = app.input_vscroll.saturating_sub(1);
            } else if point_in(mx, my, table_rect) {
                if app.selected_row > 0 {
                    app.selected_row -= 1;
                }
            } else if point_in(mx, my, json_rect) {
                app.json_vscroll = app.json_vscroll.saturating_sub(1);
            }
        }
        MouseEventKind::ScrollDown => {
            if app.show_env_modal {
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
                let area = popup_cols[1];
                let cols2 = Layout::default()
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
                    .split(cols2[1]);
                if let Some(ed) = app.env_editor.as_mut() {
                    let inp = ta_input_from_mouse(me);
                    ed.ta_private.input(inp.clone());
                    ed.ta_public.input(inp.clone());
                    ed.ta_ca.input(inp);
                }
                if point_in(mx, my, fields[6]) {
                    app.env_conn_vscroll = app.env_conn_vscroll.saturating_add(1);
                    return;
                }
            }
            if point_in(mx, my, q_content) {
                app.input_vscroll = app.input_vscroll.saturating_add(1);
            } else if point_in(mx, my, table_rect) {
                if app.selected_row + 1 < app.rows.len() {
                    app.selected_row += 1;
                }
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

fn fetch_topics_async(app: &AppState, tx: mpsc::UnboundedSender<TuiEvent>) {
    let host = app
        .selected_env()
        .map(|e| e.host.clone())
        .unwrap_or_else(|| app.host.clone());
    let ssl = app.current_ssl_config();
    tokio::spawn(async move {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &host)
            .set("group.id", format!("rkl-list-{}", uuid::Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "true");
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
        let list = async {
            struct QuietContext;
            impl ClientContext for QuietContext {
                fn log(&self, _level: RDKafkaLogLevel, _fac: &str, _log_message: &str) {}
            }
            impl ConsumerContext for QuietContext {}
            let c: StreamConsumer<QuietContext> = cfg
                .create_with_context(QuietContext)
                .context("create consumer")?;
            let md = c
                .fetch_metadata(None, std::time::Duration::from_secs(10))
                .context("fetch metadata")?;
            let mut names: Vec<String> = md.topics().iter().map(|t| t.name().to_string()).collect();
            names.sort();
            Ok::<_, anyhow::Error>(names)
        }
        .await;
        match list {
            Ok(v) => {
                let _ = tx.send(TuiEvent::Topics(v));
            }
            Err(e) => {
                let _ = tx.send(TuiEvent::Topics(vec![format!("Error: {}", e)]));
            }
        }
    });
}

fn env_editor_fields(app: &AppState, root: Rect) -> Option<Vec<Rect>> {
    let area = if app.show_env_modal {
        let popup_rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(10),
                Constraint::Percentage(80),
                Constraint::Percentage(10),
            ])
            .split(root);
        let center_v = popup_rows.get(1)?.to_owned();
        let popup_cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(10),
                Constraint::Percentage(80),
                Constraint::Percentage(10),
            ])
            .split(center_v);
        popup_cols.get(1).copied()?
    } else if matches!(app.screen, Screen::Envs) {
        if root.width <= 2 || root.height <= 2 {
            return None;
        }
        Rect {
            x: root.x.saturating_add(1),
            y: root.y.saturating_add(1),
            width: root.width.saturating_sub(2),
            height: root.height.saturating_sub(2),
        }
    } else {
        return None;
    };
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .margin(1)
        .split(area);
    let editor = cols.get(1).copied()?;
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
        .split(editor);
    Some(fields.to_vec())
}

fn handle_env_copy_paste_click(app: &mut AppState, fields: &[Rect], mx: u16, my: u16) -> bool {
    if fields.len() < 7 || app.env_editor.is_none() {
        return false;
    }
    if let Some(button) = detect_title_button(
        fields[0],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_PASTE_LABEL),
        ],
    ) {
        let mut meta_changed = false;
        match button {
            TitleButton::Copy => {
                if let Some(name) = app.env_editor.as_ref().map(|ed| ed.name.clone()) {
                    let _ = copy_to_clipboard(&name);
                }
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    let normalized = normalize_plain_input(&text);
                    if let Some(ed) = app.env_editor.as_mut() {
                        if !normalized.is_empty() {
                            insert_text_at_cursor(&mut ed.name, &mut ed.name_cursor, &normalized);
                            meta_changed = true;
                        }
                    }
                }
            }
            TitleButton::Clear => {}
        }
        if meta_changed {
            sync_env_metadata_from_editor(app);
        }
        return true;
    }
    if let Some(button) = detect_title_button(
        fields[1],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_PASTE_LABEL),
        ],
    ) {
        let mut meta_changed = false;
        match button {
            TitleButton::Copy => {
                if let Some(host) = app.env_editor.as_ref().map(|ed| ed.host.clone()) {
                    let _ = copy_to_clipboard(&host);
                }
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    let normalized = normalize_plain_input(&text);
                    if let Some(ed) = app.env_editor.as_mut() {
                        if !normalized.is_empty() {
                            insert_text_at_cursor(&mut ed.host, &mut ed.host_cursor, &normalized);
                            meta_changed = true;
                        }
                    }
                }
            }
            TitleButton::Clear => {}
        }
        if meta_changed {
            sync_env_metadata_from_editor(app);
        }
        return true;
    }
    if let Some(button) = detect_title_button(
        fields[2],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_PASTE_LABEL),
            (TitleButton::Clear, ENV_CLEAR_LABEL),
        ],
    ) {
        match button {
            TitleButton::Copy => {
                if let Some(text) = app
                    .env_editor
                    .as_ref()
                    .map(|ed| ed.ta_private.lines().join("\n"))
                {
                    let _ = copy_to_clipboard(&text);
                }
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    if let Some(ed) = app.env_editor.as_mut() {
                        ed.ta_private.insert_str(normalize_pem_input(&text));
                    }
                }
            }
            TitleButton::Clear => {
                if let Some(ed) = app.env_editor.as_mut() {
                    ed.ta_private = text_area_from_string(String::new());
                }
            }
        }
        return true;
    }
    if let Some(button) = detect_title_button(
        fields[3],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_PASTE_LABEL),
            (TitleButton::Clear, ENV_CLEAR_LABEL),
        ],
    ) {
        match button {
            TitleButton::Copy => {
                if let Some(text) = app
                    .env_editor
                    .as_ref()
                    .map(|ed| ed.ta_public.lines().join("\n"))
                {
                    let _ = copy_to_clipboard(&text);
                }
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    if let Some(ed) = app.env_editor.as_mut() {
                        ed.ta_public.insert_str(normalize_pem_input(&text));
                    }
                }
            }
            TitleButton::Clear => {
                if let Some(ed) = app.env_editor.as_mut() {
                    ed.ta_public = text_area_from_string(String::new());
                }
            }
        }
        return true;
    }
    if let Some(button) = detect_title_button(
        fields[4],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_PASTE_LABEL),
            (TitleButton::Clear, ENV_CLEAR_LABEL),
        ],
    ) {
        match button {
            TitleButton::Copy => {
                if let Some(text) = app
                    .env_editor
                    .as_ref()
                    .map(|ed| ed.ta_ca.lines().join("\n"))
                {
                    let _ = copy_to_clipboard(&text);
                }
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    if let Some(ed) = app.env_editor.as_mut() {
                        ed.ta_ca.insert_str(normalize_pem_input(&text));
                    }
                }
            }
            TitleButton::Clear => {
                if let Some(ed) = app.env_editor.as_mut() {
                    ed.ta_ca = text_area_from_string(String::new());
                }
            }
        }
        return true;
    }
    if let Some(button) = detect_title_button(
        fields[6],
        mx,
        my,
        &[
            (TitleButton::Copy, ENV_COPY_LABEL),
            (TitleButton::Paste, ENV_CONN_PASTE_LABEL),
        ],
    ) {
        match button {
            TitleButton::Copy => {
                let text = app
                    .env_test_message
                    .clone()
                    .unwrap_or_else(|| "Ready".to_string());
                let _ = copy_to_clipboard(&text);
            }
            TitleButton::Paste => {
                if let Some(text) = read_clipboard_text() {
                    app.env_test_message = Some(normalize_plain_input(&text));
                }
            }
            TitleButton::Clear => {}
        }
        return true;
    }
    false
}

#[derive(Copy, Clone)]
enum TitleButton {
    Copy,
    Paste,
    Clear,
}

fn detect_title_button(
    rect: Rect,
    mx: u16,
    my: u16,
    labels: &[(TitleButton, &str)],
) -> Option<TitleButton> {
    if my != rect.y || rect.width <= 2 || labels.is_empty() {
        return None;
    }
    let inner = Rect {
        x: rect.x.saturating_add(1),
        y: rect.y.saturating_add(1),
        width: rect.width.saturating_sub(2),
        height: rect.height.saturating_sub(2),
    };
    if inner.width == 0 {
        return None;
    }
    let mut cursor = inner.x + inner.width;
    for (button, label) in labels.iter().rev() {
        let label_width = label.chars().count() as u16;
        if label_width == 0 {
            continue;
        }
        if cursor <= inner.x {
            break;
        }
        let start = cursor.saturating_sub(label_width);
        if mx >= start && mx < cursor {
            return Some(*button);
        }
        if start > inner.x {
            cursor = start - 1;
        } else {
            cursor = inner.x;
        }
    }
    None
}

fn read_clipboard_text() -> Option<String> {
    let mut cb = arboard::Clipboard::new().ok()?;
    cb.get_text().ok()
}

fn normalize_pem_input(raw: &str) -> String {
    let normalized = raw.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.contains('\n') {
        normalized
    } else {
        decode_display(&normalized)
    }
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

fn ta_input_from_mouse(me: MouseEvent) -> TAInput {
    let ctrl = me.modifiers.contains(KeyModifiers::CONTROL);
    let alt = me.modifiers.contains(KeyModifiers::ALT);
    let shift = me.modifiers.contains(KeyModifiers::SHIFT);
    let tkey = match me.kind {
        MouseEventKind::ScrollUp => TAKey::MouseScrollUp,
        MouseEventKind::ScrollDown => TAKey::MouseScrollDown,
        _ => TAKey::Null,
    };
    TAInput {
        key: tkey,
        ctrl,
        alt,
        shift,
    }
}

#[cfg(unix)]
struct StdioRedirectGuard {
    orig_out: i32,
    orig_err: i32,
}
#[cfg(unix)]
impl Drop for StdioRedirectGuard {
    fn drop(&mut self) {
        unsafe {
            libc::fflush(std::ptr::null_mut());
            libc::dup2(self.orig_out, libc::STDOUT_FILENO);
            libc::dup2(self.orig_err, libc::STDERR_FILENO);
            libc::close(self.orig_out);
            libc::close(self.orig_err);
        }
    }
}

#[cfg(unix)]
fn redirect_stdio_to_file(path: &std::path::Path) -> std::io::Result<StdioRedirectGuard> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    unsafe {
        libc::fflush(std::ptr::null_mut());
        let orig_out = libc::dup(libc::STDOUT_FILENO);
        let orig_err = libc::dup(libc::STDERR_FILENO);
        libc::dup2(file.as_raw_fd(), libc::STDOUT_FILENO);
        libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
        Ok(StdioRedirectGuard { orig_out, orig_err })
    }
}

#[cfg(not(unix))]
fn redirect_stdio_to_file(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
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
        if i + 1 < text.split('\n').count() {
            acc += 1;
        }
    }
    if v.is_empty() {
        v.push(0);
    }
    v
}

fn move_cursor_up(app: &mut AppState) {
    let (line, col) = line_col(&app.input, app.input_cursor);
    if line == 0 {
        return;
    }
    let prev_start = nth_line_start(&app.input, line - 1);
    let prev_len = line_len(&app.input, line - 1);
    app.input_cursor = prev_start + col.min(prev_len);
    ensure_input_cursor_visible(app);
}

fn move_cursor_down(app: &mut AppState) {
    let (line, col) = line_col(&app.input, app.input_cursor);
    let total = app.input.split('\n').count();
    if line + 1 >= total {
        return;
    }
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

fn goto_start_of_doc(app: &mut AppState) {
    app.input_cursor = 0;
    ensure_input_cursor_visible(app);
}

fn goto_end_of_doc(app: &mut AppState) {
    app.input_cursor = app.input.len();
    ensure_input_cursor_visible(app);
}

fn move_prev_word(app: &mut AppState) {
    let target = find_prev_word_boundary(&app.input, app.input_cursor);
    app.input_cursor = target;
    ensure_input_cursor_visible(app);
}

fn move_next_word(app: &mut AppState) {
    let target = find_next_word_boundary(&app.input, app.input_cursor);
    app.input_cursor = target;
    ensure_input_cursor_visible(app);
}

fn delete_prev_word(app: &mut AppState) {
    let start = find_prev_word_boundary(&app.input, app.input_cursor);
    if start < app.input_cursor {
        app.input.replace_range(start..app.input_cursor, "");
        app.input_cursor = start;
        ensure_input_cursor_visible(app);
    }
}

fn delete_next_word(app: &mut AppState) {
    let end = find_next_word_boundary(&app.input, app.input_cursor);
    if end > app.input_cursor {
        app.input.replace_range(app.input_cursor..end, "");
        ensure_input_cursor_visible(app);
    }
}

fn find_prev_word_boundary(text: &str, cursor: usize) -> usize {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return 0;
    }
    let mut idx = cursor.min(bytes.len());
    idx = skip_left_while_bytes(bytes, idx, |b| b.is_ascii_whitespace());
    let word_idx = skip_left_while_bytes(bytes, idx, is_word_char_byte);
    if word_idx != idx {
        return word_idx;
    }
    idx = skip_left_while_bytes(bytes, idx, |b| {
        !is_word_char_byte(b) && !b.is_ascii_whitespace()
    });
    idx = skip_left_while_bytes(bytes, idx, |b| b.is_ascii_whitespace());
    skip_left_while_bytes(bytes, idx, is_word_char_byte)
}

fn find_next_word_boundary(text: &str, cursor: usize) -> usize {
    let bytes = text.as_bytes();
    let mut idx = cursor.min(bytes.len());
    if idx >= bytes.len() {
        return bytes.len();
    }
    if is_word_char_byte(bytes[idx]) {
        idx = skip_right_while_bytes(bytes, idx, is_word_char_byte);
    }
    skip_right_while_bytes(bytes, idx, |b| !is_word_char_byte(b))
}

fn skip_left_while_bytes<F>(bytes: &[u8], mut idx: usize, mut predicate: F) -> usize
where
    F: FnMut(u8) -> bool,
{
    while idx > 0 {
        let b = bytes[idx - 1];
        if !predicate(b) {
            break;
        }
        idx -= 1;
    }
    idx
}

fn skip_right_while_bytes<F>(bytes: &[u8], mut idx: usize, mut predicate: F) -> usize
where
    F: FnMut(u8) -> bool,
{
    while idx < bytes.len() {
        let b = bytes[idx];
        if !predicate(b) {
            break;
        }
        idx += 1;
    }
    idx
}

fn is_word_char_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn has_ctrl_or_alt(m: KeyModifiers) -> bool {
    m.contains(KeyModifiers::CONTROL) || m.contains(KeyModifiers::ALT)
}

fn line_col(text: &str, cursor: usize) -> (usize, usize) {
    let idx = cursor.min(text.len());
    let mut count = 0usize;
    for (i, l) in text.split('\n').enumerate() {
        let llen = l.len();
        if count + llen >= idx {
            return (i, idx - count);
        } else {
            count += llen + 1;
        }
    }
    (0, 0)
}

fn nth_line_start(text: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut count = 0usize;
    for (i, l) in text.split('\n').enumerate() {
        if i == n {
            return count;
        }
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
    if w == 0 || h == 0 {
        return;
    }
    // Mirror ui.rs layout
    let root = Rect {
        x: 0,
        y: 0,
        width: w,
        height: h,
    };
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
    let inner = Rect {
        x: query_area.x.saturating_add(1),
        y: query_area.y.saturating_add(1),
        width: query_area.width.saturating_sub(2),
        height: query_area.height.saturating_sub(2),
    };
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(6), Constraint::Min(1)])
        .split(inner);
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
    if up {
        app.input_vscroll = app.input_vscroll.saturating_sub(5);
    } else {
        app.input_vscroll = app.input_vscroll.saturating_add(5);
    }
}
