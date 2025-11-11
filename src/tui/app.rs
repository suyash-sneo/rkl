use crate::models::{MessageEnvelope, SslConfig};
use super::env_store::{EnvStore, Environment};
use std::time::Instant;

#[derive(Default)]
pub struct AppState {
    pub input: String,
    pub input_cursor: usize,
    pub input_vscroll: u16,
    pub status: String,
    pub rows: Vec<MessageEnvelope>,
    pub keys_only: bool,
    pub current_run: Option<u64>,
    pub max_rows_in_memory: usize,
    pub host: String,
    pub focus: Focus,
    pub selected_row: usize,
    pub selected_col: usize,
    pub env_store: EnvStore,
    pub show_env_modal: bool,
    pub env_editor: Option<EnvEditor>,
    // Results/table view state
    pub table_hscroll: usize,
    pub json_vscroll: u16,
    pub copy_btn_pressed: bool,
    pub copy_btn_deadline: Option<Instant>,
    pub last_run_query_range: Option<(usize, usize)>,
    // Env test status within the modal
    pub env_test_in_progress: bool,
    pub env_test_message: Option<String>,
    pub env_conn_vscroll: u16,
    pub mouse_selection_mode: bool,
}

impl AppState {
    pub fn new(initial_input: String, host: String) -> Self {
        let mut env_store = EnvStore::load();
        if env_store.envs.is_empty() {
            env_store.envs.push(Environment { name: "Default".to_string(), host: host.clone(), private_key_pem: None, public_key_pem: None, ssl_ca_pem: None });
            env_store.selected = Some(0);
            let _ = env_store.save();
        }
        Self {
            input: initial_input.clone(),
            input_cursor: initial_input.len(),
            input_vscroll: 0,
            status: String::from("Enter a query and press Enter"),
            rows: Vec::new(),
            keys_only: false,
            current_run: None,
            max_rows_in_memory: 2000,
            host,
            focus: Focus::Host,
            selected_row: 0,
            selected_col: 0,
            env_store,
            show_env_modal: false,
            env_editor: None,
            table_hscroll: 0,
            json_vscroll: 0,
            copy_btn_pressed: false,
            copy_btn_deadline: None,
            last_run_query_range: None,
            env_test_in_progress: false,
            env_test_message: None,
            env_conn_vscroll: 0,
            mouse_selection_mode: false,
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
}

#[derive(Debug)]
pub enum TuiEvent {
    Batch { run_id: u64, rows: Vec<MessageEnvelope> },
    Done { run_id: u64 },
    Error { run_id: u64, message: String },
    EnvTestProgress { message: String },
    EnvTestDone { message: String },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Focus { Host, Query, Results }

impl AppState {
    pub fn next_focus(&mut self) {
        self.focus = match self.focus { Focus::Host => Focus::Query, Focus::Query => Focus::Results, Focus::Results => Focus::Host };
    }
}

impl Default for Focus {
    fn default() -> Self { Focus::Host }
}

impl AppState {
    pub fn clamp_selection(&mut self) {
        if self.rows.is_empty() {
            self.selected_row = 0;
        } else if self.selected_row >= self.rows.len() {
            self.selected_row = self.rows.len().saturating_sub(1);
        }
        let cols = if self.keys_only { 4 } else { 5 };
        if self.selected_col >= cols { self.selected_col = cols.saturating_sub(1); }
    }

    pub fn selected_env(&self) -> Option<&Environment> {
        self.env_store.selected.and_then(|i| self.env_store.envs.get(i))
    }
    pub fn current_ssl_config(&self) -> Option<SslConfig> {
        self.selected_env().map(|e| SslConfig {
            ca_pem: e.ssl_ca_pem.clone(),
            cert_pem: e.public_key_pem.clone(),
            key_pem: e.private_key_pem.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EnvEditor {
    pub idx: Option<usize>,
    pub name: String,
    pub name_cursor: usize,
    pub host: String,
    pub host_cursor: usize,
    pub private_key_pem: String,
    pub private_key_cursor: usize,
    pub public_key_pem: String,
    pub public_key_cursor: usize,
    pub ssl_ca_pem: String,
    pub ssl_ca_cursor: usize,
    // Scroll positions for multi-line fields
    pub private_key_vscroll: u16,
    pub public_key_vscroll: u16,
    pub ca_vscroll: u16,
    pub private_key_hscroll: u16,
    pub public_key_hscroll: u16,
    pub ca_hscroll: u16,
    pub field_focus: EnvFieldFocus,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EnvFieldFocus { Name, Host, PrivateKey, PublicKey, Ca, Conn, Buttons }

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CaInputMode { Pem, Location }
