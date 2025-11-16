use super::env_store::{EnvStore, Environment};
use crate::models::{MessageEnvelope, SslConfig};
use crate::query::SelectItem;
use std::time::Instant;
use tui_textarea::TextArea;

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
    // Screens
    pub screen: Screen,
    pub show_help: bool,
    // Info screen
    pub topics: Vec<String>,
    pub autocomplete: Option<AutoCompleteState>,
    pub topics_last_fetched_at: Option<Instant>,
    pub autocomplete_frozen_token: Option<(usize, usize, String)>,
    pub autocomplete_dirty: bool,
}

impl AppState {
    pub fn new(initial_input: String, host: String) -> Self {
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
            table_hscroll: 0,
            json_vscroll: 0,
            copy_btn_pressed: false,
            copy_btn_deadline: None,
            last_run_query_range: None,
            env_test_in_progress: false,
            env_test_message: None,
            env_conn_vscroll: 0,
            mouse_selection_mode: false,
            screen: Screen::Home,
            show_help: false,
            topics: Vec::new(),
            autocomplete: None,
            topics_last_fetched_at: None,
            autocomplete_frozen_token: None,
            autocomplete_dirty: false,
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
    Batch {
        run_id: u64,
        rows: Vec<MessageEnvelope>,
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Focus {
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

impl Default for Focus {
    fn default() -> Self {
        Focus::Host
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ResultsMode {
    Messages,
    TopicList,
}

impl Default for ResultsMode {
    fn default() -> Self {
        ResultsMode::Messages
    }
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
            let decode = |s: &Option<String>| s.as_ref().map(|v| v.replace("\\n", "\n"));
            SslConfig {
                ca_pem: decode(&e.ssl_ca_pem),
                cert_pem: decode(&e.public_key_pem),
                key_pem: decode(&e.private_key_pem),
            }
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
    pub ta_private: TextArea<'static>,
    pub ta_public: TextArea<'static>,
    pub ta_ca: TextArea<'static>,
    #[allow(dead_code)]
    pub ssl_ca_cursor: usize,
    pub field_focus: EnvFieldFocus,
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

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CaInputMode {
    Pem,
    Location,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Screen {
    Home,
    Envs,
    Info,
}

impl Default for Screen {
    fn default() -> Self {
        Screen::Home
    }
}
