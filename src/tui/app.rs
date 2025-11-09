use crate::models::MessageEnvelope;

#[derive(Default)]
pub struct AppState {
    pub input: String,
    pub status: String,
    pub rows: Vec<MessageEnvelope>,
    pub keys_only: bool,
    pub current_run: Option<u64>,
    pub max_rows_in_memory: usize,
    pub host: String,
    pub focus: Focus,
    pub selected_row: usize,
    pub selected_col: usize,
}

impl AppState {
    pub fn new(initial_input: String, host: String) -> Self {
        Self {
            input: initial_input,
            status: String::from("Enter a query and press Enter"),
            rows: Vec::new(),
            keys_only: false,
            current_run: None,
            max_rows_in_memory: 2000,
            host,
            focus: Focus::Host,
            selected_row: 0,
            selected_col: 0,
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
}
