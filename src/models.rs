use rdkafka::Offset;
use serde::Serialize;
use std::cmp::Ordering;

/// What to assign for each partition.
#[derive(Debug, Copy, Clone)]
pub enum OffsetSpec {
    Beginning,
    End,
    Absolute(i64),
}

impl OffsetSpec {
    pub fn to_rdkafka(self) -> Offset {
        match self {
            OffsetSpec::Beginning => Offset::Beginning,
            OffsetSpec::End => Offset::End,
            OffsetSpec::Absolute(n) => Offset::Offset(n),
        }
    }

    pub fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "beginning" => Ok(Self::Beginning),
            "end" => Ok(Self::End),
            _ => s.parse::<i64>().map(Self::Absolute).map_err(|_| ()),
        }
    }
}

/// Data sent from partition tasks to the merger.
#[derive(Debug, Clone, Serialize)]
pub struct MessageEnvelope {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64, // 0 if unknown
    pub key: String,
    pub value: Option<String>, // None if keys_only
}

/// Wrapper that gives us total ordering by (timestamp, partition, offset)
#[derive(Debug, Clone)]
pub struct SortableEnvelope(pub MessageEnvelope);

impl PartialEq for SortableEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.0.timestamp_ms == other.0.timestamp_ms
            && self.0.partition == other.0.partition
            && self.0.offset == other.0.offset
    }
}
impl Eq for SortableEnvelope {}

impl PartialOrd for SortableEnvelope {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SortableEnvelope {
    fn cmp(&self, other: &Self) -> Ordering {
        // natural ordering: smaller timestamp first
        match self.0.timestamp_ms.cmp(&other.0.timestamp_ms) {
            Ordering::Equal => match self.0.partition.cmp(&other.0.partition) {
                Ordering::Equal => self.0.offset.cmp(&other.0.offset),
                x => x,
            },
            x => x,
        }
    }
}

