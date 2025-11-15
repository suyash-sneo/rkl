use crate::args::RunArgs;
use crate::models::{MessageEnvelope, OffsetSpec, SslConfig};
use crate::query::SelectQuery;
use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use serde_json::Value;
use std::io::Write as _;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

pub async fn spawn_partition_consumer(
    args: RunArgs,
    partition: i32,
    offset_spec: OffsetSpec,
    tx: Sender<MessageEnvelope>,
    query: Option<std::sync::Arc<SelectQuery>>,
    ssl: Option<SslConfig>,
) -> Result<()> {
    // unique group id (we never commit)
    let group_id = format!("rkl-{}-p{}", uuid::Uuid::new_v4(), partition);

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &args.broker)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true");
    if let Some(ssl) = &ssl {
        if ssl.ca_pem.is_some() || ssl.cert_pem.is_some() || ssl.key_pem.is_some() {
            cfg.set("security.protocol", "ssl");
            if let Some(ref s) = ssl.ca_pem {
                cfg.set("ssl.ca.pem", s);
            }
            if let Some(ref s) = ssl.cert_pem {
                cfg.set("ssl.certificate.pem", s);
            }
            if let Some(ref s) = ssl.key_pem {
                cfg.set("ssl.key.pem", s);
            }
        }
    }
    let consumer: StreamConsumer = cfg.create().context("Failed to create consumer")?;

    // Manual assignment to this specific partition + offset
    let mut tpl = TopicPartitionList::new();
    let topic = args
        .topic
        .as_ref()
        .expect("topic should be set by main before spawning consumers");
    tpl.add_partition_offset(topic, partition, offset_spec.to_rdkafka())?;
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

                // Prepare payload as String and JSON
                let payload_str = msg
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).to_string());
                let payload_json: serde_json::Value = payload_str
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .unwrap_or(serde_json::Value::Null);

                // Apply query WHERE if provided; else fallback to simple --search
                let matches = if let Some(ref q) = query {
                    if let Some(ref expr) = q.r#where {
                        expr.matches(
                            &key,
                            &payload_json,
                            payload_str.as_deref(),
                            msg.timestamp().to_millis().unwrap_or(0),
                        )
                    } else {
                        true
                    }
                } else if let Some(ref needle) = args.search {
                    let hay1 = &key;
                    let hay2 = if let Some(ref s) = payload_str { s } else { "" };
                    hay1.contains(needle) || hay2.contains(needle)
                } else {
                    true
                };

                if matches {
                    // If keys_only -> set value None, else pretty-print JSON if possible
                    let keys_only = args.keys_only; // effective keys_only computed in main when using query
                    let value_print = if keys_only {
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
                // Log errors to ~/.rkl/logs instead of printing over the TUI
                if let Some(home) = std::env::var_os("HOME") {
                    let path = std::path::PathBuf::from(home)
                        .join(".rkl")
                        .join("logs")
                        .join("consumer.err.log");
                    let _ = std::fs::create_dir_all(path.parent().unwrap());
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                    {
                        let ts = time::OffsetDateTime::now_utc()
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_else(|_| "".into());
                        let _ = writeln!(f, "{} [partition {}] {}", ts, partition, e);
                    }
                }
                // Keep going; transient errors happen
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Ok(())
}
