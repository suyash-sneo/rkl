use crate::args::RunArgs;
use crate::models::{MessageEnvelope, OffsetSpec, SslConfig};
use crate::query::{OrderDir, SelectQuery};
use anyhow::{Context, Result};
use rdkafka::Offset;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
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
    query_limit: Option<usize>,
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
            if let Some(s) = ssl.ca_pem.as_ref() {
                cfg.set("ssl.ca.pem", s);
            }
            if let Some(s) = ssl.cert_pem.as_ref() {
                cfg.set("ssl.certificate.pem", s);
            }
            if let Some(s) = ssl.key_pem.as_ref() {
                cfg.set("ssl.key.pem", s);
            }
        }
    }
    let consumer: StreamConsumer = cfg.create().context("Failed to create consumer")?;

    let topic = args
        .topic
        .as_ref()
        .expect("topic should be set by main before spawning consumers")
        .clone();

    if let Some(query) = query {
        run_query_partition_consumer(args, partition, consumer, topic, query, query_limit, tx).await
    } else {
        run_search_partition_consumer(args, partition, offset_spec, consumer, topic, tx).await
    }
}

async fn run_search_partition_consumer(
    args: RunArgs,
    partition: i32,
    offset_spec: OffsetSpec,
    consumer: StreamConsumer,
    topic: String,
    tx: Sender<MessageEnvelope>,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, partition, offset_spec.to_rdkafka())?;
    consumer
        .assign(&tpl)
        .context("Failed to assign partition")?;

    let mut processed: usize = 0;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if is_partition_eof(&msg) {
                    continue;
                }

                let key = decode_key(&msg);
                let (payload_str, payload_json, payload_valid) = decode_payload(&msg);
                let hay_value = payload_str.as_deref().unwrap_or("");
                let matches = if let Some(needle) = args.search.as_ref() {
                    key.contains(needle) || hay_value.contains(needle)
                } else {
                    true
                };

                if matches {
                    let value_print = format_value_column(
                        args.keys_only,
                        &payload_str,
                        &payload_json,
                        payload_valid,
                    );

                    let env = MessageEnvelope {
                        partition,
                        offset: msg.offset(),
                        timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
                        key,
                        value: value_print,
                    };

                    if tx.send(env).await.is_err() {
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
                log_partition_error(partition, &format!("{}", e));
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Ok(())
}

async fn run_query_partition_consumer(
    args: RunArgs,
    partition: i32,
    consumer: StreamConsumer,
    topic: String,
    query: std::sync::Arc<SelectQuery>,
    query_limit: Option<usize>,
    tx: Sender<MessageEnvelope>,
) -> Result<()> {
    let (low_watermark, high_watermark) = consumer
        .fetch_watermarks(&topic, partition, Duration::from_secs(5))
        .context("fetch_watermarks")?;

    if low_watermark >= high_watermark {
        return Ok(());
    }

    let bounds = query
        .r#where
        .as_ref()
        .and_then(|expr| expr.timestamp_bounds());

    const FUDGE_MS: i64 = 2000;
    let mut effective_start = low_watermark;
    let mut effective_end_exclusive = high_watermark;

    if let Some(b) = bounds {
        if let Some(lower) = b.lower {
            let seek_ts = lower.millis.saturating_sub(FUDGE_MS);
            let offset = offset_for_timestamp(
                &consumer,
                &topic,
                partition,
                seek_ts,
                low_watermark,
                high_watermark,
            )?;
            effective_start = effective_start.max(offset);
        }
        if let Some(upper) = b.upper {
            let seek_ts = upper.millis.saturating_add(FUDGE_MS);
            let offset = offset_for_timestamp(
                &consumer,
                &topic,
                partition,
                seek_ts,
                low_watermark,
                high_watermark,
            )?;
            effective_end_exclusive = effective_end_exclusive.min(offset);
        }
    }

    if effective_start >= effective_end_exclusive {
        return Ok(());
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, partition, Offset::Offset(effective_start))?;
    consumer
        .assign(&tpl)
        .context("Failed to assign partition")?;

    let order_desc = query
        .order
        .as_ref()
        .map(|o| matches!(o.dir, OrderDir::Desc))
        .unwrap_or(true);
    let limit_global = query_limit.or(query.limit);
    let window_size = limit_global.map(|n| n as i64).unwrap_or(256).max(1);

    if order_desc {
        let mut scan_end_exclusive = effective_end_exclusive;
        'outer: loop {
            if scan_end_exclusive <= effective_start {
                break;
            }
            let remaining = scan_end_exclusive - effective_start;
            let window = remaining.min(window_size);
            let window_start = scan_end_exclusive - window;

            consumer
                .seek(
                    &topic,
                    partition,
                    Offset::Offset(window_start),
                    Duration::from_secs(5),
                )
                .context("seek window")?;

            loop {
                match consumer.recv().await {
                    Ok(msg) => {
                        if is_partition_eof(&msg) {
                            break;
                        }
                        if msg.offset() >= scan_end_exclusive {
                            break;
                        }

                        if let Some(env) = build_query_envelope(&args, partition, &query, &msg) {
                            if tx.send(env).await.is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Err(KafkaError::PartitionEOF(_)) => {
                        break;
                    }
                    Err(e) => {
                        log_partition_error(partition, &format!("{}", e));
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }

            scan_end_exclusive = window_start;
        }
    } else {
        'asc: loop {
            match consumer.recv().await {
                Ok(msg) => {
                    if is_partition_eof(&msg) {
                        break 'asc;
                    }
                    if msg.offset() >= effective_end_exclusive {
                        break 'asc;
                    }

                    if let Some(env) = build_query_envelope(&args, partition, &query, &msg) {
                        if tx.send(env).await.is_err() {
                            break 'asc;
                        }
                    }
                }
                Err(KafkaError::PartitionEOF(_)) => break 'asc,
                Err(e) => {
                    log_partition_error(partition, &format!("{}", e));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    Ok(())
}

fn build_query_envelope(
    args: &RunArgs,
    partition: i32,
    query: &SelectQuery,
    msg: &BorrowedMessage<'_>,
) -> Option<MessageEnvelope> {
    let key = decode_key(msg);
    let (payload_str, payload_json, payload_valid) = decode_payload(msg);
    let timestamp_ms = msg.timestamp().to_millis().unwrap_or(0);
    let matches = query
        .r#where
        .as_ref()
        .map(|expr| expr.matches(&key, &payload_json, payload_str.as_deref(), timestamp_ms))
        .unwrap_or(true);
    if !matches {
        return None;
    }
    let value_print =
        format_value_column(args.keys_only, &payload_str, &payload_json, payload_valid);
    Some(MessageEnvelope {
        partition,
        offset: msg.offset(),
        timestamp_ms,
        key,
        value: value_print,
    })
}

fn decode_key(msg: &BorrowedMessage<'_>) -> String {
    msg.key()
        .map(|k| String::from_utf8_lossy(k).to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn decode_payload(msg: &BorrowedMessage<'_>) -> (Option<String>, Value, bool) {
    if let Some(payload) = msg.payload() {
        let s = String::from_utf8_lossy(payload).to_string();
        if let Ok(json) = serde_json::from_str::<Value>(&s) {
            (Some(s), json, true)
        } else {
            (Some(s), Value::Null, false)
        }
    } else {
        (None, Value::Null, false)
    }
}

fn format_value_column(
    keys_only: bool,
    payload_str: &Option<String>,
    payload_json: &Value,
    payload_valid: bool,
) -> Option<String> {
    if keys_only {
        return None;
    }
    if let Some(s) = payload_str {
        if payload_valid {
            Some(serde_json::to_string_pretty(payload_json).unwrap_or_else(|_| s.clone()))
        } else {
            Some(s.clone())
        }
    } else {
        Some("null".to_string())
    }
}

fn is_partition_eof(msg: &BorrowedMessage<'_>) -> bool {
    msg.payload().is_none() && msg.key().is_none() && msg.timestamp().to_millis().is_none()
}

fn log_partition_error(partition: i32, message: &str) {
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
            let _ = writeln!(f, "{} [partition {}] {}", ts, partition, message);
        }
    }
}

fn offset_for_timestamp(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    ts_ms: i64,
    low_watermark: i64,
    high_watermark: i64,
) -> Result<i64> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(ts_ms))?;
    let tpl = match consumer.offsets_for_times(tpl, Duration::from_secs(5)) {
        Ok(list) => list,
        Err(e) => {
            log_partition_error(partition, &format!("offsets_for_times error: {}", e));
            return Ok(high_watermark);
        }
    };
    let offset = tpl
        .elements()
        .iter()
        .find(|elem| elem.partition() == partition)
        .and_then(|elem| match elem.offset() {
            Offset::Offset(pos) => Some(pos),
            Offset::Beginning => Some(low_watermark),
            Offset::End => Some(high_watermark),
            _ => None,
        })
        .unwrap_or(high_watermark);
    Ok(offset.clamp(low_watermark, high_watermark))
}
