mod args;
mod consumer;
mod merger;
mod models;
mod output;

use anyhow::{Context, Result};
use args::Args;
use colored::*;
use consumer::spawn_partition_consumer;
use merger::run_merger;
use models::{MessageEnvelope, OffsetSpec};
use output::TableOutput;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse_cli();

    println!(
        "{}",
        format!("Connecting to Kafka broker: {}", args.broker).cyan()
    );
    println!("{}", format!("Topic: {}", args.topic).cyan());

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
        .fetch_metadata(Some(&args.topic), Duration::from_secs(10))
        .context("Failed to fetch metadata")?;

    let topic_md = metadata
        .topics()
        .iter()
        .find(|t| t.name() == args.topic)
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
    for &p in &partitions {
        let txp = tx.clone();
        let a = args.clone();
        joinset.spawn(async move { spawn_partition_consumer(a, p, offset_spec, txp).await });
    }
    drop(tx); // merger will know when producers are done

    // Output sink (table)
    let mut table_out = TableOutput::new(args.no_color, args.keys_only, args.max_cell_width);

    // Merge + print
    run_merger(
        rx,
        &mut table_out,
        args.watermark,
        args.flush_interval_ms,
        args.max_messages,
    )
    .await?;

    // Await all consumer tasks (and surface errors if any)
    while let Some(res) = joinset.join_next().await {
        res??;
    }

    table_out.finish();
    Ok(())
}
