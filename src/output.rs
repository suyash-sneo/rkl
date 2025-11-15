use crate::models::MessageEnvelope;
use crate::query::SelectItem;
use comfy_table::{Attribute, Cell, ContentArrangement, Table, presets::UTF8_FULL};
use time::{OffsetDateTime, format_description::well_known::Iso8601};

/// Generic sink trait used by the merger to emit rows in batches.
pub trait OutputSink {
    fn push(&mut self, env: &MessageEnvelope);
    fn flush_block(&mut self);
}

pub struct TableOutput {
    table: Table,
    no_color: bool,
    columns: Vec<SelectItem>,
    max_cell_width: usize, // used as an approximate table width hint
    rows_buffered: usize,
}

impl TableOutput {
    pub fn new(no_color: bool, columns: Vec<SelectItem>, max_cell_width: usize) -> Self {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        // comfy-table v7: use set_width on the whole table
        if max_cell_width > 0 {
            table.set_width((max_cell_width * 2) as u16);
        }

        table.set_header(make_header(&columns, no_color));

        Self {
            table,
            no_color,
            columns,
            max_cell_width,
            rows_buffered: 0,
        }
    }
}

impl OutputSink for TableOutput {
    fn push(&mut self, env: &MessageEnvelope) {
        let row = self
            .columns
            .iter()
            .map(|col| match col {
                SelectItem::Partition => cell(env.partition, self.no_color),
                SelectItem::Offset => cell(env.offset, self.no_color),
                SelectItem::Timestamp => cell(fmt_ts(env.timestamp_ms), self.no_color),
                SelectItem::Key => cell(&env.key, self.no_color),
                SelectItem::Value => cell(env.value.as_deref().unwrap_or("null"), self.no_color),
            })
            .collect::<Vec<_>>();
        self.table.add_row(row);
        self.rows_buffered += 1;
    }

    fn flush_block(&mut self) {
        if self.rows_buffered == 0 {
            return;
        }
        println!("{}", self.table);

        // Recreate table with same header so each block prints a header
        self.table = Table::new();
        self.table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        if self.max_cell_width > 0 {
            self.table.set_width((self.max_cell_width * 2) as u16);
        }
        self.table
            .set_header(make_header(&self.columns, self.no_color));
        self.rows_buffered = 0;
    }
}

impl TableOutput {
    pub fn finish(&mut self) {
        self.flush_block();
    }
}

fn fmt_ts(ms: i64) -> String {
    if ms <= 0 {
        return "0".to_string();
    }
    let secs = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as i128;
    if let Ok(dt) =
        OffsetDateTime::from_unix_timestamp_nanos((secs as i128) * 1_000_000_000 + nanos)
    {
        dt.format(&Iso8601::DEFAULT)
            .unwrap_or_else(|_| ms.to_string())
    } else {
        ms.to_string()
    }
}

fn hdr(text: &str, _no_color: bool) -> Cell {
    Cell::new(text).add_attribute(Attribute::Bold)
}

fn cell<T: std::fmt::Display>(v: T, _no_color: bool) -> Cell {
    Cell::new(v)
}

fn make_header(columns: &[SelectItem], no_color: bool) -> Vec<Cell> {
    columns
        .iter()
        .map(|col| {
            let label = match col {
                SelectItem::Partition => "Partition",
                SelectItem::Offset => "Offset",
                SelectItem::Timestamp => "Timestamp",
                SelectItem::Key => "Key",
                SelectItem::Value => "Value (JSON / Text)",
            };
            hdr(label, no_color)
        })
        .collect()
}
