use crate::models::MessageEnvelope;
use comfy_table::{Attribute, Cell, ContentArrangement, Table, presets::UTF8_FULL};
use time::{OffsetDateTime, format_description::well_known::Iso8601};

pub struct TableOutput {
    table: Table,
    no_color: bool,
    keys_only: bool,
    max_cell_width: usize, // used as an approximate table width hint
    rows_buffered: usize,
}

impl TableOutput {
    pub fn new(no_color: bool, keys_only: bool, max_cell_width: usize) -> Self {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        // comfy-table v7: use set_width on the whole table
        if max_cell_width > 0 {
            table.set_width((max_cell_width * 2) as u16);
        }

        let mut header = vec![
            hdr("Partition", no_color),
            hdr("Offset", no_color),
            hdr("Timestamp", no_color),
            hdr("Key", no_color),
        ];
        if !keys_only {
            header.push(hdr("Value (JSON / Text)", no_color));
        }
        table.set_header(header);

        Self {
            table,
            no_color,
            keys_only,
            max_cell_width,
            rows_buffered: 0,
        }
    }

    pub fn push(&mut self, env: &MessageEnvelope) {
        let ts_str = fmt_ts(env.timestamp_ms);
        let mut row = vec![
            cell(env.partition, self.no_color),
            cell(env.offset, self.no_color),
            cell(ts_str, self.no_color),
            cell(&env.key, self.no_color),
        ];

        if !self.keys_only {
            let v = env.value.as_deref().unwrap_or("null");
            row.push(cell(v, self.no_color));
        }

        self.table.add_row(row);
        self.rows_buffered += 1;
    }

    pub fn flush_block(&mut self) {
        if self.rows_buffered == 0 {
            return;
        }
        println!("{}", self.table);

        // Recreate table with same header so each block prints a header
        let header = self.table.header().cloned();
        self.table = Table::new();
        self.table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        if self.max_cell_width > 0 {
            self.table.set_width((self.max_cell_width * 2) as u16);
        }
        if let Some(h) = header {
            self.table.set_header(h);
        }
        self.rows_buffered = 0;
    }

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
