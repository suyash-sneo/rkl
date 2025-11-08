use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use rdkafka::Offset;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use serde_json::Value;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "rkl")]
#[command(about = "Search Kafka topics without committing offsets", long_about = None)]
struct Args {
    /// Kafka broker address
    #[arg(short, long, default_value = "localhost:9092")]
    broker: String,

    /// Topic to search
    #[arg(short, long)]
    topic: String,

    /// Search term (searches in both keys and values)
    #[arg(short, long)]
    search: Option<String>,

    /// Maximum number of messages to read (default: all messages)
    #[arg(short, long)]
    max_messages: Option<usize>,

    /// Specific partition to read from (default: all partitions)
    #[arg(short, long)]
    partition: Option<i32>,

    /// Starting offset (default: beginning)
    #[arg(short, long, default_value = "beginning")]
    offset: String,

    /// Show only keys
    #[arg(long)]
    keys_only: bool,

    /// Show raw output (no colors or formatting)
    #[arg(long)]
    raw: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "{}",
        format!("Connecting to Kafka broker: {}", args.broker).cyan()
    );
    println!("{}", format!("Topic: {}", args.topic).cyan());

    // Create consumer with a unique group ID but disable commits
    // We need a group.id for rdkafka but we won't commit offsets
    let group_id = format!("kafka-search-{}", uuid::Uuid::new_v4());

    // Create consumer without a group ID to avoid offset commits
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create()
        .context("Failed to create consumer")?;

    // Get metadata to find partitions
    let metadata = consumer
        .fetch_metadata(Some(&args.topic), Duration::from_secs(10))
        .context("Failed to fetch metadata")?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == args.topic)
        .context("Topic not found")?;

    let partitions: Vec<i32> = if let Some(p) = args.partition {
        vec![p]
    } else {
        topic_metadata.partitions().iter().map(|p| p.id()).collect()
    };

    println!(
        "{}",
        format!("Found {} partition(s): {:?}", partitions.len(), partitions).green()
    );

    // Assign partitions manually with specified offset
    let mut tpl = TopicPartitionList::new();
    for partition in &partitions {
        let offset = match args.offset.as_str() {
            "beginning" => Offset::Beginning,
            "end" => Offset::End,
            num => Offset::Offset(num.parse().context("Invalid offset number")?),
        };
        tpl.add_partition_offset(&args.topic, *partition, offset)?;
    }

    consumer
        .assign(&tpl)
        .context("Failed to assign partitions")?;

    println!("{}\n", "Starting to read messages...".yellow());

    let mut count = 0;
    let mut matches = 0;
    let max = args.max_messages.unwrap_or(usize::MAX);

    loop {
        if count >= max {
            break;
        }

        match consumer.recv().await {
            Ok(msg) => {
                let key = msg
                    .key()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "null".to_string());

                let payload = msg
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).to_string())
                    .unwrap_or_else(|| "null".to_string());

                // Check if message matches search term
                let is_match = if let Some(ref search_term) = args.search {
                    key.contains(search_term) || payload.contains(search_term)
                } else {
                    true
                };

                if is_match {
                    matches += 1;

                    if !args.raw {
                        println!(
                            "{}",
                            format!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                                .bright_black()
                        );
                        println!(
                            "{} {} | {} {} | {} {}",
                            "Partition:".bright_blue(),
                            msg.partition(),
                            "Offset:".bright_blue(),
                            msg.offset(),
                            "Timestamp:".bright_blue(),
                            msg.timestamp().to_millis().unwrap_or(0)
                        );
                        println!("{} {}", "Key:".bright_green(), key);

                        if !args.keys_only {
                            // Try to pretty print JSON
                            if let Ok(json) = serde_json::from_str::<Value>(&payload) {
                                println!(
                                    "{}\n{}",
                                    "Value:".bright_green(),
                                    serde_json::to_string_pretty(&json).unwrap()
                                );
                            } else {
                                println!("{} {}", "Value:".bright_green(), payload);
                            }
                        }
                    } else {
                        // Raw output
                        if args.keys_only {
                            println!("{}", key);
                        } else {
                            println!("{} : {}", key, payload);
                        }
                    }
                }

                count += 1;

                if !args.raw && count % 100 == 0 {
                    eprintln!("{}", format!("Processed {} messages...", count).dimmed());
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    println!(
        "\n{}",
        format!(
            "Finished! Processed {} messages, found {} matches",
            count, matches
        )
        .bright_yellow()
    );

    Ok(())
}
