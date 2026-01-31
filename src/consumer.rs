use crate::args::RunArgs;
use crate::models::{MessageEnvelope, OffsetSpec, SslConfig};
use crate::query::{CompiledExpr, EvalContext, OrderDir, OrderField, OrderSpec, SelectQuery};
use anyhow::{Context, Result};
use memchr::memmem;
use rayon::prelude::*;
use rdkafka::Offset;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const JSON_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone)]
struct OwnedMessage {
    offset: i64,
    timestamp_ms: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

impl OwnedMessage {
    fn from_borrowed(msg: &BorrowedMessage<'_>) -> Self {
        Self {
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
            key: msg.key().map(|k| k.to_vec()),
            value: msg.payload().map(|v| v.to_vec()),
        }
    }
}

#[derive(Clone)]
pub struct GlobalLimit {
    max: usize,
    counter: Arc<AtomicUsize>,
}

impl GlobalLimit {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reached(&self) -> bool {
        self.counter.load(Ordering::Relaxed) >= self.max
    }

    fn incr(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

pub async fn spawn_partition_consumer(
    args: RunArgs,
    partition: i32,
    offset_spec: OffsetSpec,
    tx: Sender<MessageEnvelope>,
    query: Option<std::sync::Arc<SelectQuery>>,
    global_limit: Option<GlobalLimit>,
    ssl: Option<SslConfig>,
    query_scan_multiplier: usize,
) -> Result<()> {
    // unique group id (we never commit)
    let group_id = format!("rkl-{}-p{}", uuid::Uuid::new_v4(), partition);

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &args.broker)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("fetch.wait.max.ms", "25")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.bytes", "104857600")
        .set("max.partition.fetch.bytes", "20971520");
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
        run_query_partition_consumer(
            args,
            partition,
            consumer,
            topic,
            query,
            global_limit,
            tx,
            query_scan_multiplier,
        )
        .await
    } else {
        run_search_partition_consumer(
            args,
            partition,
            offset_spec,
            consumer,
            topic,
            tx,
            global_limit,
        )
        .await
    }
}

async fn run_search_partition_consumer(
    args: RunArgs,
    partition: i32,
    offset_spec: OffsetSpec,
    consumer: StreamConsumer,
    topic: String,
    tx: Sender<MessageEnvelope>,
    global_limit: Option<GlobalLimit>,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, partition, offset_spec.to_rdkafka())?;
    consumer
        .assign(&tpl)
        .context("Failed to assign partition")?;

    let needle = args.search.as_deref();

    loop {
        if global_limit.as_ref().is_some_and(|limit| limit.reached()) {
            break;
        }
        match consumer.recv().await {
            Ok(msg) => {
                if is_partition_eof(&msg) {
                    continue;
                }

                let key_bytes = msg.key();
                let payload_bytes = msg.payload();
                let matches = if let Some(needle) = needle {
                    let needle_bytes = needle.as_bytes();
                    let key_match = key_bytes
                        .map(|k| bytes_contains(k, needle_bytes))
                        .unwrap_or_else(|| "null".contains(needle));
                    let value_match = payload_bytes
                        .map(|v| bytes_contains(v, needle_bytes))
                        .unwrap_or(false);
                    key_match || value_match
                } else {
                    true
                };

                if matches {
                    let key = decode_key_bytes(key_bytes);
                    let value_print = format_value_column(args.keys_only, payload_bytes);

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
                    if let Some(limit) = global_limit.as_ref() {
                        if limit.incr() >= limit.max {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanKind {
    TimestampAsc,
    TimestampDesc,
    PoffsetAsc,
    PoffsetDesc,
    PoffsetTsAsc,
    PoffsetTsDesc,
}

fn infer_scan_kind(query: &SelectQuery) -> ScanKind {
    match query.order {
        Some(OrderSpec {
            field: OrderField::Timestamp,
            dir: OrderDir::Asc,
        }) => ScanKind::TimestampAsc,
        Some(OrderSpec {
            field: OrderField::Timestamp,
            dir: OrderDir::Desc,
        }) => ScanKind::TimestampDesc,
        Some(OrderSpec {
            field: OrderField::Poffset,
            dir: OrderDir::Asc,
        }) => ScanKind::PoffsetAsc,
        Some(OrderSpec {
            field: OrderField::Poffset,
            dir: OrderDir::Desc,
        }) => ScanKind::PoffsetDesc,
        Some(OrderSpec {
            field: OrderField::PoffsetTs,
            dir: OrderDir::Asc,
        }) => ScanKind::PoffsetTsAsc,
        Some(OrderSpec {
            field: OrderField::PoffsetTs,
            dir: OrderDir::Desc,
        }) => ScanKind::PoffsetTsDesc,
        None => ScanKind::PoffsetDesc,
    }
}

async fn run_query_partition_consumer(
    args: RunArgs,
    partition: i32,
    consumer: StreamConsumer,
    topic: String,
    query: std::sync::Arc<SelectQuery>,
    global_limit: Option<GlobalLimit>,
    tx: Sender<MessageEnvelope>,
    query_scan_multiplier: usize,
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

    let compiled = query.r#where.as_ref().map(CompiledExpr::compile);
    let needs_json = compiled.as_ref().is_some_and(|expr| expr.needs_json());
    let scan_kind = infer_scan_kind(&query);

    match scan_kind {
        ScanKind::TimestampDesc
        | ScanKind::TimestampAsc
        | ScanKind::PoffsetTsAsc
        | ScanKind::PoffsetTsDesc => {
            if needs_json {
                let mut batch: Vec<OwnedMessage> = Vec::with_capacity(JSON_BATCH_SIZE);
                let mut stop = false;
                'asc: loop {
                    match consumer.recv().await {
                        Ok(msg) => {
                            if is_partition_eof(&msg) {
                                break 'asc;
                            }
                            if msg.offset() >= effective_end_exclusive {
                                break 'asc;
                            }
                            batch.push(OwnedMessage::from_borrowed(&msg));
                            if batch.len() >= JSON_BATCH_SIZE {
                                let out = drain_batch_to_vec(
                                    &args,
                                    partition,
                                    compiled.as_ref(),
                                    &mut batch,
                                );
                                for env in out {
                                    if tx.send(env).await.is_err() {
                                        stop = true;
                                        break 'asc;
                                    }
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
                if !batch.is_empty() {
                    if !stop {
                        let out =
                            drain_batch_to_vec(&args, partition, compiled.as_ref(), &mut batch);
                        for env in out {
                            if tx.send(env).await.is_err() {
                                break;
                            }
                        }
                    }
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

                            if let Some(env) =
                                build_query_envelope(&args, partition, compiled.as_ref(), &msg)
                            {
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
        }
        ScanKind::PoffsetAsc => {
            if needs_json {
                let mut batch: Vec<OwnedMessage> = Vec::with_capacity(JSON_BATCH_SIZE);
                let mut stop = false;
                'asc: loop {
                    if global_limit
                        .as_ref()
                        .is_some_and(|limit| limit.reached())
                    {
                        break 'asc;
                    }
                    match consumer.recv().await {
                        Ok(msg) => {
                            if is_partition_eof(&msg) {
                                break 'asc;
                            }
                            if msg.offset() >= effective_end_exclusive {
                                break 'asc;
                            }
                            batch.push(OwnedMessage::from_borrowed(&msg));
                            if batch.len() >= JSON_BATCH_SIZE {
                                let out = drain_batch_to_vec(
                                    &args,
                                    partition,
                                    compiled.as_ref(),
                                    &mut batch,
                                );
                                for env in out {
                                    if tx.send(env).await.is_err() {
                                        stop = true;
                                        break 'asc;
                                    }
                                    if let Some(limit) = global_limit.as_ref() {
                                        if limit.incr() >= limit.max {
                                            stop = true;
                                            break 'asc;
                                        }
                                    }
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
                if !batch.is_empty() {
                    if !stop {
                        let out =
                            drain_batch_to_vec(&args, partition, compiled.as_ref(), &mut batch);
                        for env in out {
                            if tx.send(env).await.is_err() {
                                break;
                            }
                            if let Some(limit) = global_limit.as_ref() {
                                if limit.incr() >= limit.max {
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                'asc: loop {
                    if global_limit
                        .as_ref()
                        .is_some_and(|limit| limit.reached())
                    {
                        break 'asc;
                    }
                    match consumer.recv().await {
                        Ok(msg) => {
                            if is_partition_eof(&msg) {
                                break 'asc;
                            }
                            if msg.offset() >= effective_end_exclusive {
                                break 'asc;
                            }

                            if let Some(env) =
                                build_query_envelope(&args, partition, compiled.as_ref(), &msg)
                            {
                                if tx.send(env).await.is_err() {
                                    break 'asc;
                                }
                                if let Some(limit) = global_limit.as_ref() {
                                    if limit.incr() >= limit.max {
                                        break 'asc;
                                    }
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
        }
        ScanKind::PoffsetDesc => {
            let mut scan_end_exclusive = effective_end_exclusive;
            let total_span = (effective_end_exclusive - effective_start).max(1);
            let mut window_size = 64i64.min(total_span).max(64);
            let max_window = ((query_scan_multiplier.max(1) as i64) * 8192)
                .min(50_000i64)
                .min(total_span);
            let mut empty_windows = 0usize;
            let mut dense_windows = 0usize;

            'outer: loop {
                if scan_end_exclusive <= effective_start {
                    break 'outer;
                }
                if global_limit
                    .as_ref()
                    .is_some_and(|limit| limit.reached())
                {
                    break 'outer;
                }
                let remaining = scan_end_exclusive - effective_start;
                let window = remaining.min(window_size);
                if window <= 0 {
                    break 'outer;
                }
                let window_start = scan_end_exclusive - window;

                consumer
                    .seek(
                        &topic,
                        partition,
                        Offset::Offset(window_start),
                        Duration::from_secs(5),
                    )
                    .context("seek window")?;

                let mut local: Vec<MessageEnvelope> = Vec::new();
                if needs_json {
                    let mut batch: Vec<OwnedMessage> = Vec::with_capacity(JSON_BATCH_SIZE);
                    loop {
                        if global_limit
                            .as_ref()
                            .is_some_and(|limit| limit.reached())
                        {
                            break;
                        }
                        match consumer.recv().await {
                            Ok(msg) => {
                                if is_partition_eof(&msg) {
                                    break;
                                }
                                if msg.offset() >= scan_end_exclusive {
                                    break;
                                }

                                batch.push(OwnedMessage::from_borrowed(&msg));
                                if batch.len() >= JSON_BATCH_SIZE {
                                    let out = drain_batch_to_vec(
                                        &args,
                                        partition,
                                        compiled.as_ref(),
                                        &mut batch,
                                    );
                                    local.extend(out);
                                }
                            }
                            Err(KafkaError::PartitionEOF(_)) => break,
                            Err(e) => {
                                log_partition_error(partition, &format!("{}", e));
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                    if !batch.is_empty() {
                        let out =
                            drain_batch_to_vec(&args, partition, compiled.as_ref(), &mut batch);
                        local.extend(out);
                    }
                } else {
                    loop {
                        if global_limit
                            .as_ref()
                            .is_some_and(|limit| limit.reached())
                        {
                            break;
                        }
                        match consumer.recv().await {
                            Ok(msg) => {
                                if is_partition_eof(&msg) {
                                    break;
                                }
                                if msg.offset() >= scan_end_exclusive {
                                    break;
                                }

                                if let Some(env) =
                                    build_query_envelope(&args, partition, compiled.as_ref(), &msg)
                                {
                                    local.push(env);
                                }
                            }
                            Err(KafkaError::PartitionEOF(_)) => break,
                            Err(e) => {
                                log_partition_error(partition, &format!("{}", e));
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }

                let local_len = local.len();
                for env in local.into_iter().rev() {
                    if tx.send(env).await.is_err() {
                        break 'outer;
                    }
                    if let Some(limit) = global_limit.as_ref() {
                        if limit.incr() >= limit.max {
                            break 'outer;
                        }
                    }
                }

                if local_len == 0 {
                    empty_windows += 1;
                    if empty_windows >= 8 && window_size < max_window {
                        window_size = (window_size * 2).min(max_window);
                        empty_windows = 0;
                    }
                    dense_windows = 0;
                } else {
                    empty_windows = 0;
                    dense_windows += 1;
                    if dense_windows >= 2 && window_size < max_window {
                        window_size = (window_size * 2).min(max_window);
                        dense_windows = 0;
                    }
                }

                scan_end_exclusive = window_start;
            }
        }
    }

    Ok(())
}

fn build_query_envelope(
    args: &RunArgs,
    partition: i32,
    compiled: Option<&CompiledExpr>,
    msg: &BorrowedMessage<'_>,
) -> Option<MessageEnvelope> {
    let timestamp_ms = msg.timestamp().to_millis().unwrap_or(0);
    let mut ctx = EvalContext::new(msg.key(), msg.payload(), timestamp_ms);
    if let Some(expr) = compiled {
        if !expr.matches(&mut ctx) {
            return None;
        }
    }
    let key = ctx.take_key_string();
    let value_print = if args.keys_only {
        None
    } else {
        ctx.take_value_string()
    };
    Some(MessageEnvelope {
        partition,
        offset: msg.offset(),
        timestamp_ms,
        key,
        value: value_print,
    })
}

fn build_query_envelope_owned(
    args: &RunArgs,
    partition: i32,
    compiled: Option<&CompiledExpr>,
    msg: &OwnedMessage,
) -> Option<MessageEnvelope> {
    let mut ctx = EvalContext::new(msg.key.as_deref(), msg.value.as_deref(), msg.timestamp_ms);
    if let Some(expr) = compiled {
        if !expr.matches(&mut ctx) {
            return None;
        }
    }
    let key = ctx.take_key_string();
    let value_print = if args.keys_only {
        None
    } else {
        ctx.take_value_string()
    };
    Some(MessageEnvelope {
        partition,
        offset: msg.offset,
        timestamp_ms: msg.timestamp_ms,
        key,
        value: value_print,
    })
}

fn drain_batch_to_vec(
    args: &RunArgs,
    partition: i32,
    compiled: Option<&CompiledExpr>,
    batch: &mut Vec<OwnedMessage>,
) -> Vec<MessageEnvelope> {
    if batch.is_empty() {
        return Vec::new();
    }
    let rows: Vec<Option<MessageEnvelope>> = batch
        .par_iter()
        .map(|msg| build_query_envelope_owned(args, partition, compiled, msg))
        .collect();
    batch.clear();
    rows.into_iter().flatten().collect()
}

fn decode_key_bytes(key: Option<&[u8]>) -> String {
    key.map(|k| String::from_utf8_lossy(k).into_owned())
        .unwrap_or_else(|| "null".to_string())
}

fn format_value_column(keys_only: bool, payload: Option<&[u8]>) -> Option<String> {
    if keys_only {
        return None;
    }
    let out = payload
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_else(|| "null".to_string());
    Some(out)
}

fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    memmem::find(haystack, needle).is_some()
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
