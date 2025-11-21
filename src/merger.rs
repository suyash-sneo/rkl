use crate::models::{MessageEnvelope, SortableEnvelope};
use crate::output::OutputSink;
use anyhow::Result;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, interval};

/// Receives envelopes from all partitions and emits globally ordered rows.
/// When a LIMIT is present we retain up to `limit * partitions` rows to reduce
/// churn while still bounding memory. The merger always drains the channel to
/// completion so partition tasks can finish their per-partition scans.
#[allow(clippy::too_many_arguments)]
pub async fn run_merger<S: OutputSink + Send>(
    rx: Receiver<MessageEnvelope>,
    out: &mut S,
    watermark: usize,
    flush_interval_ms: u64,
    max_messages: Option<usize>,
    order_desc: bool,
    partition_count: usize,
    interactive: bool,
    global_sort_by_timestamp: bool,
) -> Result<()> {
    if global_sort_by_timestamp {
        if let Some(limit) = max_messages {
            run_merger_bounded(
                rx,
                out,
                limit,
                order_desc,
                partition_count,
                flush_interval_ms,
                interactive,
            )
            .await
        } else {
            run_merger_streaming(rx, out, watermark, flush_interval_ms, order_desc).await
        }
    } else {
        run_merger_passthrough(rx, out, max_messages, flush_interval_ms, interactive).await
    }
}

async fn run_merger_passthrough<S: OutputSink + Send>(
    mut rx: Receiver<MessageEnvelope>,
    out: &mut S,
    max_messages: Option<usize>,
    flush_interval_ms: u64,
    interactive: bool,
) -> Result<()> {
    let limit = max_messages.unwrap_or(usize::MAX);
    let mut sent = 0usize;

    if interactive {
        let mut buf: Vec<MessageEnvelope> = Vec::new();
        let mut tick = interval(Duration::from_millis(flush_interval_ms.max(1)));
        loop {
            tokio::select! {
                biased;
                _ = tick.tick() => {
                    if !buf.is_empty() {
                        for env in buf.drain(..) {
                            out.push(&env);
                        }
                        out.flush_block();
                    }
                }
                maybe_env = rx.recv() => {
                    match maybe_env {
                        Some(env) => {
                            if sent < limit {
                                buf.push(env);
                                sent += 1;
                            }
                        }
                        None => {
                            if !buf.is_empty() {
                                for env in buf.drain(..) {
                                    out.push(&env);
                                }
                                out.flush_block();
                            }
                            break;
                        }
                    }
                }
            }
        }
    } else {
        while let Some(env) = rx.recv().await {
            if sent < limit {
                out.push(&env);
                sent += 1;
            }
        }
        out.flush_block();
    }

    Ok(())
}

async fn run_merger_bounded<S: OutputSink + Send>(
    mut rx: Receiver<MessageEnvelope>,
    out: &mut S,
    limit: usize,
    order_desc: bool,
    partition_count: usize,
    _flush_interval_ms: u64,
    interactive: bool,
) -> Result<()> {
    let partitions = partition_count.max(1);
    let capacity = limit.saturating_mul(partitions);
    if capacity == 0 {
        while rx.recv().await.is_some() {}
        return Ok(());
    }

    let mut heap = BoundedHeap::new(order_desc);
    if interactive {
        let mut tick = interval(Duration::from_millis(200));
        let mut done_rx = false;
        while !done_rx {
            tokio::select! {
                biased;

                _ = tick.tick() => {
                    if heap.len() > 0 {
                        let snapshot = heap.snapshot(limit, order_desc);
                        if !snapshot.is_empty() {
                            for env in &snapshot {
                                out.push(env);
                            }
                            out.flush_block();
                        }
                    }
                }

                maybe_env = rx.recv() => {
                    match maybe_env {
                        Some(env) => heap.push(env, capacity),
                        None => done_rx = true,
                    }
                }
            }
        }
    } else {
        while let Some(env) = rx.recv().await {
            heap.push(env, capacity);
        }
    }

    let mut rows = heap.into_vec();
    if rows.is_empty() {
        return Ok(());
    }

    rows.sort_unstable_by(cmp_envelopes);
    if order_desc {
        rows.reverse();
    }

    let take = limit.min(rows.len());
    for env in rows.into_iter().take(take) {
        out.push(&env);
    }
    out.flush_block();

    Ok(())
}

async fn run_merger_streaming<S: OutputSink + Send>(
    mut rx: Receiver<MessageEnvelope>,
    out: &mut S,
    watermark: usize,
    flush_interval_ms: u64,
    order_desc: bool,
) -> Result<()> {
    let mut heap = HeapKind::new(order_desc);
    let mut tick = interval(Duration::from_millis(flush_interval_ms));

    loop {
        tokio::select! {
            biased;

            _ = tick.tick() => {
                drain_heap(&mut heap, out, usize::MAX);
            }

            maybe_msg = rx.recv() => {
                if let Some(env) = maybe_msg {
                    heap.push(env);
                    if heap.len() >= watermark {
                        let target = (heap.len() / 2).max(1);
                        drain_heap(&mut heap, out, target);
                    }
                } else {
                    drain_heap(&mut heap, out, usize::MAX);
                    break;
                }
            }
        }
    }

    Ok(())
}

fn cmp_envelopes(a: &MessageEnvelope, b: &MessageEnvelope) -> Ordering {
    a.timestamp_ms
        .cmp(&b.timestamp_ms)
        .then_with(|| a.partition.cmp(&b.partition))
        .then_with(|| a.offset.cmp(&b.offset))
}

enum HeapKind {
    Asc(BinaryHeap<Reverse<SortableEnvelope>>),
    Desc(BinaryHeap<SortableEnvelope>),
}

impl HeapKind {
    fn new(desc: bool) -> Self {
        if desc {
            HeapKind::Desc(BinaryHeap::new())
        } else {
            HeapKind::Asc(BinaryHeap::new())
        }
    }
    fn len(&self) -> usize {
        match self {
            HeapKind::Asc(h) => h.len(),
            HeapKind::Desc(h) => h.len(),
        }
    }
    fn push(&mut self, env: MessageEnvelope) {
        match self {
            HeapKind::Asc(h) => h.push(Reverse(SortableEnvelope(env))),
            HeapKind::Desc(h) => h.push(SortableEnvelope(env)),
        }
    }
    fn pop(&mut self) -> Option<MessageEnvelope> {
        match self {
            HeapKind::Asc(h) => h.pop().map(|Reverse(se)| se.0),
            HeapKind::Desc(h) => h.pop().map(|se| se.0),
        }
    }
}

fn drain_heap<S: OutputSink>(heap: &mut HeapKind, out: &mut S, max_rows: usize) {
    let mut n = 0usize;
    while let Some(env) = heap.pop() {
        out.push(&env);
        n += 1;
        if n >= max_rows {
            break;
        }
    }
    if n > 0 {
        out.flush_block();
    }
}

enum BoundedHeap {
    Desc(BinaryHeap<Reverse<SortableEnvelope>>),
    Asc(BinaryHeap<SortableEnvelope>),
}

impl BoundedHeap {
    fn new(desc: bool) -> Self {
        if desc {
            BoundedHeap::Desc(BinaryHeap::new())
        } else {
            BoundedHeap::Asc(BinaryHeap::new())
        }
    }

    fn len(&self) -> usize {
        match self {
            BoundedHeap::Desc(h) => h.len(),
            BoundedHeap::Asc(h) => h.len(),
        }
    }

    fn push(&mut self, env: MessageEnvelope, capacity: usize) {
        match self {
            BoundedHeap::Desc(h) => {
                h.push(Reverse(SortableEnvelope(env)));
                if h.len() > capacity {
                    h.pop();
                }
            }
            BoundedHeap::Asc(h) => {
                h.push(SortableEnvelope(env));
                if h.len() > capacity {
                    h.pop();
                }
            }
        }
    }

    fn into_vec(self) -> Vec<MessageEnvelope> {
        match self {
            BoundedHeap::Desc(h) => h.into_iter().map(|Reverse(se)| se.0).collect(),
            BoundedHeap::Asc(h) => h.into_iter().map(|se| se.0).collect(),
        }
    }

    fn snapshot(&self, limit: usize, order_desc: bool) -> Vec<MessageEnvelope> {
        let mut rows: Vec<MessageEnvelope> = match self {
            BoundedHeap::Desc(h) => h.iter().map(|Reverse(se)| se.0.clone()).collect(),
            BoundedHeap::Asc(h) => h.iter().map(|se| se.0.clone()).collect(),
        };
        if rows.is_empty() {
            return rows;
        }
        rows.sort_unstable_by(cmp_envelopes);
        if order_desc {
            rows.reverse();
        }
        if rows.len() > limit {
            rows.truncate(limit);
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[derive(Default)]
    struct TestSink {
        rows: Vec<MessageEnvelope>,
        flushes: usize,
    }

    impl OutputSink for TestSink {
        fn push(&mut self, env: &MessageEnvelope) {
            self.rows.push(env.clone());
        }

        fn flush_block(&mut self) {
            self.flushes += 1;
        }
    }

    fn env(partition: i32, offset: i64, timestamp_ms: i64) -> MessageEnvelope {
        MessageEnvelope {
            partition,
            offset,
            timestamp_ms,
            key: format!("k-{partition}-{offset}"),
            value: Some(format!("v-{timestamp_ms}")),
        }
    }

    #[tokio::test]
    async fn desc_queries_emit_global_top_n() {
        let (tx, rx) = mpsc::channel(16);
        let inputs = vec![
            env(0, 1, 1_000),
            env(1, 5, 2_000),
            env(0, 2, 1_500),
            env(2, 1, 1_800),
            env(1, 6, 2_500),
            env(3, 0, 1_200),
        ];
        for env in inputs {
            tx.send(env).await.unwrap();
        }
        drop(tx);

        let mut sink = TestSink::default();
        run_merger(rx, &mut sink, 4, 50, Some(3), true, 4, false, true)
            .await
            .unwrap();

        assert_eq!(sink.rows.len(), 3);
        assert_eq!(sink.flushes, 1);
        let timestamps: Vec<i64> = sink.rows.iter().map(|e| e.timestamp_ms).collect();
        assert_eq!(timestamps, vec![2_500, 2_000, 1_800]);
    }

    #[tokio::test]
    async fn asc_queries_respect_limit_and_ties() {
        let (tx, rx) = mpsc::channel(16);
        let inputs = vec![
            env(1, 5, 1_050),
            env(0, 2, 1_000),
            env(2, 9, 900),
            env(0, 1, 1_000),
            env(1, 1, 1_000),
            env(1, 2, 1_000),
            env(3, 0, 1_100),
        ];
        for env in inputs {
            tx.send(env).await.unwrap();
        }
        drop(tx);

        let mut sink = TestSink::default();
        run_merger(rx, &mut sink, 4, 50, Some(4), false, 4, false, true)
            .await
            .unwrap();

        assert_eq!(sink.rows.len(), 4);
        assert_eq!(sink.flushes, 1);
        let triples: Vec<(i32, i64, i64)> = sink
            .rows
            .iter()
            .map(|e| (e.partition, e.offset, e.timestamp_ms))
            .collect();
        assert_eq!(
            triples,
            vec![(2, 9, 900), (0, 1, 1_000), (0, 2, 1_000), (1, 1, 1_000),]
        );
    }
}
