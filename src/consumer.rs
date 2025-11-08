use crate::args::Args;
use crate::models::{MessageEnvelope, OffsetSpec};
use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

pub async fn spawn_partition_consumer(
    args: Args,
    partition: i32,
    offset_spec: OffsetSpec,
    tx: Sender<MessageEnvelope>,
) -> Result<()> {
    // unique group id (we never commit)
    let group_id = format!("rkl-{}-p{}", uuid::Uuid::new_v4(), partition);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create()
        .context("Failed to create consumer")?;

    // Manual assignment to this specific partition + offset
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&args.topic, partition, offset_spec.to_rdkafka())?;
    consumer
        .assign(&tpl)
        .context("Failed to assign partition")?;

    let mut processed: usize = 0;

    loop {
        // Backpressure-friendly, async receive
        match consumer.recv().await {
            Ok(msg) => {
                // End-of-partition marker
                if msg.payload().is_none()
                    && msg.key().is_none()
                    && msg.timestamp().to_millis().is_none()
                {
                    // Keep reading; librdkafka emits EOFs—don’t break, we want “tail” as well if offset=end
                }

                let key = msg
                    .key()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "null".to_string());

                // Optional search filter across key + value (raw or JSON text)
                let payload_str = msg
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).to_string());

                // Apply search if provided
                let matches = if let Some(ref needle) = args.search {
                    let hay1 = &key;
                    let hay2 = if let Some(ref s) = payload_str { s } else { "" };
                    hay1.contains(needle) || hay2.contains(needle)
                } else {
                    true
                };

                if matches {
                    // If keys_only -> set value None, else pretty-print JSON if possible
                    let value_print = if args.keys_only {
                        None
                    } else if let Some(ref s) = payload_str {
                        if let Ok(json) = serde_json::from_str::<Value>(s) {
                            Some(serde_json::to_string_pretty(&json).unwrap())
                        } else {
                            Some(s.clone())
                        }
                    } else {
                        Some("null".to_string())
                    };

                    let env = MessageEnvelope {
                        partition,
                        offset: msg.offset(),
                        timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
                        key,
                        value: value_print,
                    };

                    if tx.send(env).await.is_err() {
                        // merger dropped—shut down gracefully
                        break;
                    }
                    processed += 1;

                    if let Some(max) = args.max_messages {
                        if processed >= max {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[partition {partition}] error receiving: {e}");
                // Keep going; transient errors happen
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Ok(())
}
