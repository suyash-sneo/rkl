use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(name = "rkl")]
#[command(about = "Search Kafka topics without committing offsets", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run once with a query or search, printing a table
    Run(RunArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct RunArgs {
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

    /// SSL: CA PEM inline (librdkafka: ssl.ca.pem)
    #[arg(long)]
    pub ssl_ca_pem: Option<String>,

    /// SSL: Certificate PEM inline (librdkafka: ssl.certificate.pem)
    #[arg(long)]
    pub ssl_certificate_pem: Option<String>,

    /// SSL: Private key PEM inline (librdkafka: ssl.key.pem)
    #[arg(long)]
    pub ssl_key_pem: Option<String>,
}

impl Cli {
    pub fn parse_cli() -> Self { Self::parse() }
}

impl Default for RunArgs {
    fn default() -> Self {
        Self {
            broker: "localhost:9092".to_string(),
            topic: None,
            search: None,
            query: None,
            max_messages: None,
            partition: None,
            offset: "beginning".to_string(),
            keys_only: false,
            no_color: false,
            max_cell_width: 120,
            channel_capacity: 2048,
            watermark: 256,
            flush_interval_ms: 250,
            ssl_ca_pem: None,
            ssl_certificate_pem: None,
            ssl_key_pem: None,
        }
    }
}
