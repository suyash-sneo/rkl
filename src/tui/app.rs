use super::env_store::{EnvStore, Environment};
use super::pem_utils::decode_literal_backslash_n;
use crate::app_config::{AppConfig, DefaultOrderDir, DefaultOrderField};
use crate::models::{MessageEnvelope, SslConfig};
use crate::query::SelectItem;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tui_textarea::TextArea;

pub const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

pub const QUERY_HISTORY_LIMIT: usize = 200;

#[derive(Default)]
pub struct AppState {
    pub input: String,
    pub input_cursor: usize,
    pub input_vscroll: u16,
    pub status: String,
    pub status_buffer: String,
    pub status_vscroll: u16,
    pub rows: Vec<MessageEnvelope>,
    pub topics_with_partitions: Vec<(String, usize)>,
    pub results_mode: ResultsMode,
    pub selected_columns: Vec<SelectItem>,
    pub current_run: Option<u64>,
    pub max_rows_in_memory: usize,
    pub host: String,
    pub focus: Focus,
    pub selected_row: usize,
    pub selected_col: usize,
    pub env_store: EnvStore,
    pub show_env_modal: bool,
    pub env_editor: Option<EnvEditor>,
    pub app_config: AppConfig,
    pub app_config_editor: Option<AppConfigEditor>,
    pub app_config_save_pressed: bool,
    pub app_config_save_deadline: Option<Instant>,
    // Results/table view state
    pub table_hscroll: usize,
    pub json_vscroll: u16,
    pub copy_btn_pressed: bool,
    pub copy_btn_deadline: Option<Instant>,
    pub last_run_query_range: Option<(usize, usize)>,
    // Env test status within the modal
    pub env_test_in_progress: bool,
    pub env_test_message: Option<String>,
    pub env_test_log: String,
    pub env_conn_vscroll: u16,
    pub env_save_pressed: bool,
    pub env_save_deadline: Option<Instant>,
    pub mouse_selection_mode: bool,
    // Screens
    pub screen: Screen,
    pub help_vscroll: u32,
    pub last_screen_before_about: Option<Screen>,
    pub menu_pressed_screen: Option<Screen>,
    pub menu_pressed_deadline: Option<Instant>,
    pub timestamps_use_utc: bool,
    pub timestamp_switch_pressed: bool,
    pub timestamp_switch_deadline: Option<Instant>,
    // Info screen
    pub topics: Vec<String>,
    pub autocomplete: Option<AutoCompleteState>,
    pub topics_last_fetched_at: Option<Instant>,
    pub autocomplete_frozen_token: Option<(usize, usize, String)>,
    pub autocomplete_dirty: bool,
    pub query_in_progress: bool,
    pub query_limit: Option<usize>,
    pub query_rows_seen: usize,
    pub query_started_at: Option<Instant>,
    pub query_spinner_idx: usize,
    pub query_history: Vec<String>,
    pub show_history_popup: bool,
    pub history_selected_index: usize,
    pub parse_ok: bool,
    pub parse_error_msg: Option<String>,
    pub parse_status_dirty: bool,
    pub last_executed_query: Option<String>,
}

impl AppState {
    pub fn new(initial_input: String, host: String) -> Self {
        let app_config = AppConfig::load();
        let mut env_store = EnvStore::load();
        if env_store.envs.is_empty() {
            env_store.envs.push(Environment {
                name: "Default".to_string(),
                host: host.clone(),
                private_key_pem: None,
                public_key_pem: None,
                ssl_ca_pem: None,
            });
            env_store.selected = Some(0);
            let _ = env_store.save();
        }
        let history = load_query_history_from_disk();
        let history_idx = history.len().saturating_sub(1);
        Self {
            input: initial_input.clone(),
            input_cursor: initial_input.len(),
            input_vscroll: 0,
            status: String::from("Enter a query and press Ctrl-Enter to run"),
            status_buffer: String::new(),
            status_vscroll: 0,
            rows: Vec::new(),
            topics_with_partitions: Vec::new(),
            results_mode: ResultsMode::Messages,
            selected_columns: SelectItem::standard(true),
            current_run: None,
            max_rows_in_memory: 2000,
            host,
            focus: Focus::Host,
            selected_row: 0,
            selected_col: 0,
            env_store,
            show_env_modal: false,
            env_editor: None,
            app_config: app_config.clone(),
            app_config_editor: Some(build_app_config_editor(&app_config)),
            app_config_save_pressed: false,
            app_config_save_deadline: None,
            table_hscroll: 0,
            json_vscroll: 0,
            copy_btn_pressed: false,
            copy_btn_deadline: None,
            last_run_query_range: None,
            env_test_in_progress: false,
            env_test_message: None,
            env_test_log: String::new(),
            env_conn_vscroll: 0,
            env_save_pressed: false,
            env_save_deadline: None,
            mouse_selection_mode: false,
            screen: Screen::Home,
            help_vscroll: 0,
            last_screen_before_about: None,
            menu_pressed_screen: None,
            menu_pressed_deadline: None,
            timestamps_use_utc: app_config.default_timestamps_use_utc,
            timestamp_switch_pressed: false,
            timestamp_switch_deadline: None,
            topics: Vec::new(),
            autocomplete: None,
            topics_last_fetched_at: None,
            autocomplete_frozen_token: None,
            autocomplete_dirty: false,
            query_in_progress: false,
            query_limit: None,
            query_rows_seen: 0,
            query_started_at: None,
            query_spinner_idx: 0,
            query_history: history,
            show_history_popup: false,
            history_selected_index: history_idx,
            parse_ok: true,
            parse_error_msg: None,
            parse_status_dirty: false,
            last_executed_query: None,
        }
    }

    pub fn clear_rows(&mut self) {
        self.rows.clear();
    }

    pub fn push_rows(&mut self, mut batch: Vec<MessageEnvelope>) {
        // Keep memory bounded
        if self.rows.len() + batch.len() > self.max_rows_in_memory {
            let overflow = self.rows.len() + batch.len() - self.max_rows_in_memory;
            let drop_n = overflow.min(self.rows.len());
            if drop_n > 0 {
                self.rows.drain(0..drop_n);
            }
        }
        self.rows.append(&mut batch);
    }

    pub fn update_query_progress_rows(&mut self, total_emitted: usize) {
        self.query_rows_seen = total_emitted;
    }
}

pub fn build_app_config_editor(config: &AppConfig) -> AppConfigEditor {
    let default_limit = config
        .default_limit
        .map(|v| v.to_string())
        .unwrap_or_default();
    let default_order_field_idx = match config.default_order_field {
        DefaultOrderField::Timestamp => 0,
        DefaultOrderField::Poffset => 1,
        DefaultOrderField::PoffsetTs => 2,
    };
    let default_order_dir_idx = match config.default_order_dir {
        DefaultOrderDir::Asc => 0,
        DefaultOrderDir::Desc => 1,
    };
    AppConfigEditor {
        query_scan_multiplier: config.query_scan_multiplier.to_string(),
        default_limit,
        default_order_field_idx,
        default_order_dir_idx,
        timestamps_use_utc: config.default_timestamps_use_utc,
        field_focus: AppConfigFieldFocus::QueryScanMultiplier,
    }
}

#[derive(Debug)]
pub enum TuiEvent {
    Batch {
        run_id: u64,
        rows: Vec<MessageEnvelope>,
        total_emitted: usize,
    },
    Snapshot {
        run_id: u64,
        rows: Vec<MessageEnvelope>,
        total_emitted: usize,
    },
    Done {
        run_id: u64,
    },
    Error {
        run_id: u64,
        message: String,
    },
    EnvTestProgress {
        message: String,
    },
    EnvTestDone {
        message: String,
    },
    Topics(Vec<String>),
    TopicsWithPartitions(Vec<(String, usize)>),
    QueryPlan {
        run_id: u64,
        planned_limit: usize,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum Focus {
    #[default]
    Host,
    Query,
    Results,
}

impl AppState {
    pub fn next_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Host => Focus::Query,
            Focus::Query => Focus::Results,
            Focus::Results => Focus::Host,
        };
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum ResultsMode {
    #[default]
    Messages,
    TopicList,
}

#[derive(Debug, Clone)]
pub struct AutoCompleteState {
    pub active: bool,
    pub filter: String,
    pub suggestions: Vec<String>,
    pub selected: usize,
    pub token_abs_start: usize,
    pub token_abs_end: usize,
}

impl AppState {
    pub fn clamp_selection(&mut self) {
        let total_rows = match self.results_mode {
            ResultsMode::Messages => self.rows.len(),
            ResultsMode::TopicList => self.topics_with_partitions.len(),
        };
        if total_rows == 0 {
            self.selected_row = 0;
        } else if self.selected_row >= total_rows {
            self.selected_row = total_rows.saturating_sub(1);
        }
        let cols = match self.results_mode {
            ResultsMode::Messages => self.selected_columns.len().max(1),
            ResultsMode::TopicList => 1,
        };
        if self.selected_col >= cols {
            self.selected_col = cols.saturating_sub(1);
        }
    }

    pub fn selected_env(&self) -> Option<&Environment> {
        self.env_store
            .selected
            .and_then(|i| self.env_store.envs.get(i))
    }
    pub fn current_ssl_config(&self) -> Option<SslConfig> {
        self.selected_env().map(|e| {
            // Ensure we pass actual newlines to librdkafka
            let decode =
                |s: &Option<String>| s.as_ref().map(|v| decode_literal_backslash_n(v.as_str()));
            SslConfig {
                ca_pem: decode(&e.ssl_ca_pem),
                cert_pem: decode(&e.public_key_pem),
                key_pem: decode(&e.private_key_pem),
            }
        })
    }

    pub fn has_timestamp_column(&self) -> bool {
        self.selected_columns
            .iter()
            .any(|c| matches!(c, SelectItem::Timestamp))
    }

    pub fn should_show_timestamp_switch(&self) -> bool {
        self.results_mode == ResultsMode::Messages && self.has_timestamp_column()
    }

    pub fn timestamp_toggle_label(&self) -> &'static str {
        if self.timestamps_use_utc {
            "[Local time]"
        } else {
            "[UTC time]"
        }
    }

    pub fn record_query_history(&mut self, query: &str) {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return;
        }
        let entry = normalize_history_entry(query);
        if self
            .query_history
            .last()
            .map(|q| q == &entry)
            .unwrap_or(false)
        {
            return;
        }
        self.query_history.push(entry);
        if self.query_history.len() > QUERY_HISTORY_LIMIT {
            let drop_n = self.query_history.len() - QUERY_HISTORY_LIMIT;
            self.query_history.drain(0..drop_n);
        }
        self.history_selected_index = self.query_history.len().saturating_sub(1);
        let _ = save_query_history_to_disk(&self.query_history);
    }
}

#[derive(Debug, Clone)]
pub struct EnvEditor {
    pub idx: Option<usize>,
    pub name: String,
    pub name_cursor: usize,
    pub host: String,
    pub host_cursor: usize,
    pub ta_private: TextArea<'static>,
    pub ta_public: TextArea<'static>,
    pub ta_ca: TextArea<'static>,
    #[allow(dead_code)]
    pub ssl_ca_cursor: usize,
    pub field_focus: EnvFieldFocus,
}

#[derive(Debug, Clone)]
pub struct AppConfigEditor {
    pub query_scan_multiplier: String,
    pub default_limit: String,
    pub default_order_field_idx: usize,
    pub default_order_dir_idx: usize,
    pub timestamps_use_utc: bool,
    pub field_focus: AppConfigFieldFocus,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EnvFieldFocus {
    Name,
    Host,
    PrivateKey,
    PublicKey,
    Ca,
    Conn,
    Buttons,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AppConfigFieldFocus {
    QueryScanMultiplier,
    DefaultLimit,
    DefaultOrderField,
    DefaultOrderDir,
    TimestampsUseUtc,
    Buttons,
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CaInputMode {
    Pem,
    Location,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum Screen {
    #[default]
    Home,
    Envs,
    Info,
    AppConfig,
    About,
}

fn history_file_path() -> PathBuf {
    crate::paths::history_file_path()
}

fn legacy_history_file_path() -> PathBuf {
    crate::paths::legacy_history_file_path()
}

fn load_query_history_from_disk() -> Vec<String> {
    let raw = fs::read_to_string(history_file_path())
        .or_else(|_| fs::read_to_string(legacy_history_file_path()));
    let Ok(raw) = raw else {
        return Vec::new();
    };
    let mut entries: Vec<String> = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(decode_history_entry)
        .collect();
    if entries.len() > QUERY_HISTORY_LIMIT {
        let drop_n = entries.len() - QUERY_HISTORY_LIMIT;
        entries.drain(0..drop_n);
    }
    entries
        .into_iter()
        .map(|e| normalize_history_entry(&e))
        .collect()
}

fn save_query_history_to_disk(entries: &[String]) -> std::io::Result<()> {
    let path = history_file_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let serialized = entries
        .iter()
        .map(|q| encode_history_entry(q))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, serialized)
}

fn encode_history_entry(entry: &str) -> String {
    entry.replace('\n', "\\n")
}

fn decode_history_entry(entry: &str) -> String {
    entry.replace("\\n", "\n")
}

fn normalize_history_entry(query: &str) -> String {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{};", trimmed)
    }
}
