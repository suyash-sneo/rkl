use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "rkl")]
#[command(about = "Search Kafka topics without committing offsets", long_about = None)]
pub struct Args {
    /// Kafka broker address
    #[arg(short, long, default_value = "localhost:9092")]
    pub broker: String,

    /// Topic to search (required unless --query is provided)
    #[arg(short, long, required_unless_present = "query")]
    pub topic: Option<String>,

    /// Search term (applies to key and JSON/text value). Conflicts with --query.
    #[arg(short, long, conflicts_with = "query")]
    pub search: Option<String>,

    /// SQL-like query. When provided, topic is taken from FROM and overrides --offset/--max-messages/--keys-only as applicable.
    /// Example:
    ///   SELECT key, value FROM my.topic WHERE value->payload->method = 'PUT' ORDER BY timestamp DESC LIMIT 10
    #[arg(long)]
    pub query: Option<String>,

    /// Maximum number of messages to read (default: all)
    #[arg(short, long)]
    pub max_messages: Option<usize>,

    /// Specific partition to read from (default: all partitions)
    #[arg(short, long)]
    pub partition: Option<i32>,

    /// Starting offset: "beginning" | "end" | <number>
    #[arg(short, long, default_value = "beginning")]
    pub offset: String,

    /// Show only keys (omit value column)
    #[arg(long)]
    pub keys_only: bool,

    /// Disable terminal colors
    #[arg(long, default_value_t = false)]
    pub no_color: bool,

    /// Max cell width for table wrapping (0 = no wrap, default 120)
    #[arg(long, default_value_t = 120)]
    pub max_cell_width: usize,

    /// Channel capacity (messages buffered between consumers and merger)
    #[arg(long, default_value_t = 2048)]
    pub channel_capacity: usize,

    /// Watermark (min-heap size before we flush oldest-by-timestamp)
    #[arg(long, default_value_t = 256)]
    pub watermark: usize,

    /// Flush interval in milliseconds (drains heap on tick)
    #[arg(long, default_value_t = 250)]
    pub flush_interval_ms: u64,
}

impl Args {
    pub fn parse_cli() -> Self {
        Self::parse()
    }
}
