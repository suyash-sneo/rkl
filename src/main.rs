mod args;
mod consumer;
mod merger;
mod models;
mod output;
mod query;
mod tui;

use anyhow::{Context, Result};
use args::{Cli, Commands, RunArgs};
use colored::*;
use consumer::spawn_partition_consumer;
use merger::run_merger;
use models::{MessageEnvelope, OffsetSpec};
use output::TableOutput;
use query::{parse_query, OrderDir, SelectItem};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_cli();
    match cli.command {
        None => {
            // No subcommand -> launch TUI with default settings
            return tui::run(RunArgs::default()).await;
        }
        Some(Commands::Run(args)) => {
            let args = args;

    // Parse --query if provided and compute effective settings
    println!(
        "{}",
        format!("Connecting to Kafka broker: {}", args.broker).cyan()
    );
    let (query_ast, topic, keys_only, max_messages, order_desc) = if let Some(ref q) = args.query {
        let ast = parse_query(q).context("Failed to parse --query")?;
        let keys_only = !ast.select.iter().any(|i| matches!(i, SelectItem::Value));
        let max_messages = ast.limit.or(args.max_messages);
        let order_desc = ast
            .order
            .as_ref()
            .map(|o| matches!(o.dir, OrderDir::Desc))
            .unwrap_or(false);
        println!("{}", format!("Using query: {}", q).cyan());
        println!("{}", format!("Topic: {}", ast.from).cyan());
        let topic_name = ast.from.clone();
        (Some(ast), topic_name, keys_only, max_messages, order_desc)
    } else {
        let topic_value = args
            .topic
            .clone()
            .expect("topic is required unless --query is provided");
        println!("{}", format!("Topic: {}", topic_value).cyan());
        (None, topic_value, args.keys_only, args.max_messages, false)
    };

    // One-time consumer just to fetch metadata / partitions
    let probe_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("group.id", format!("rkl-probe-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create()
        .context("Failed to create probe consumer")?;

    let metadata = probe_consumer
        .fetch_metadata(Some(&topic), Duration::from_secs(10))
        .context("Failed to fetch metadata")?;

    let topic_md = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .context("Topic not found")?;

    let partitions: Vec<i32> = if let Some(p) = args.partition {
        vec![p]
    } else {
        topic_md.partitions().iter().map(|p| p.id()).collect()
    };

    println!(
        "{}",
        format!("Found {} partition(s): {:?}", partitions.len(), partitions).green()
    );
    println!("{}", "Starting readers (one per partition)...".yellow());

    // Message channel: producers = partition tasks, consumer = merger task
    let (tx, rx) = mpsc::channel::<MessageEnvelope>(args.channel_capacity);

    // Spawn per-partition consumers
    let mut joinset = JoinSet::new();
    let offset_spec = OffsetSpec::from_str(&args.offset).unwrap_or_else(|_| OffsetSpec::Beginning);
    let query_arc = query_ast.clone().map(std::sync::Arc::new);
    for &p in &partitions {
        let txp = tx.clone();
        let mut a = args.clone();
        // Override effective args when using a query
        a.topic = Some(topic.clone());
        a.keys_only = keys_only;
        if query_ast.is_some() { a.max_messages = None; }
        let q = query_arc.clone();
        joinset.spawn(async move { spawn_partition_consumer(a, p, offset_spec, txp, q).await });
    }
    drop(tx); // merger will know when producers are done

    // Output sink (table)
    let mut table_out = TableOutput::new(args.no_color, keys_only, args.max_cell_width);

    // Merge + print
    run_merger(
        rx,
        &mut table_out,
        args.watermark,
        args.flush_interval_ms,
        max_messages,
        order_desc,
    )
    .await?;

    // Await all consumer tasks (and surface errors if any)
    while let Some(res) = joinset.join_next().await {
        res??;
    }

    table_out.finish();
    return Ok(());
        }
    }
}
