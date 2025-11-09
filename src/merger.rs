use crate::models::{MessageEnvelope, SortableEnvelope};
use crate::output::OutputSink;
use anyhow::Result;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, interval};

enum HeapKind {
    Asc(BinaryHeap<Reverse<SortableEnvelope>>),
    Desc(BinaryHeap<SortableEnvelope>),
}

impl HeapKind {
    fn new(desc: bool) -> Self {
        if desc { HeapKind::Desc(BinaryHeap::new()) } else { HeapKind::Asc(BinaryHeap::new()) }
    }
    fn len(&self) -> usize {
        match self { HeapKind::Asc(h) => h.len(), HeapKind::Desc(h) => h.len() }
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

/// Receives envelopes from all partitions, maintains a min-heap by timestamp,
/// and periodically flushes in-order rows to the output sink.
pub async fn run_merger<S: OutputSink + Send>(
    mut rx: Receiver<MessageEnvelope>,
    out: &mut S,
    watermark: usize,
    flush_interval_ms: u64,
    max_messages: Option<usize>,
    order_desc: bool,
) -> Result<()> {
    let mut heap = HeapKind::new(order_desc);
    let mut tick = interval(Duration::from_millis(flush_interval_ms));
    let mut emitted: usize = 0;

    loop {
        tokio::select! {
            biased;

            _ = tick.tick() => {
                // periodic flush
                drain_heap(&mut heap, out, usize::MAX, &mut emitted, max_messages);
                if done(emitted, max_messages) { break; }
            }

            maybe_msg = rx.recv() => {
                if let Some(env) = maybe_msg {
                    heap.push(env);
                    if heap.len() >= watermark {
                        // flush oldest ~half to keep latency low
                        let target = heap.len() / 2;
                        drain_heap(&mut heap, out, target, &mut emitted, max_messages);
                        if done(emitted, max_messages) { break; }
                    }
                } else {
                    // producers finished; drain all remaining
                    drain_heap(&mut heap, out, usize::MAX, &mut emitted, max_messages);
                    break;
                }
            }
        }
    }

    Ok(())
}

fn drain_heap<S: OutputSink>(
    heap: &mut HeapKind,
    out: &mut S,
    max_rows: usize,
    emitted: &mut usize,
    max_messages: Option<usize>,
) {
    let mut n = 0usize;
    while let Some(env) = heap.pop() {
        out.push(&env);
        *emitted += 1;
        n += 1;
        if n >= max_rows || done(*emitted, max_messages) {
            break;
        }
    }
    if n > 0 {
        out.flush_block();
    }
}

#[inline]
fn done(emitted: usize, max: Option<usize>) -> bool {
    max.map(|m| emitted >= m).unwrap_or(false)
}
