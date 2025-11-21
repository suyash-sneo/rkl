use std::sync::OnceLock;
use time::format_description::FormatItem;
use time::{OffsetDateTime, UtcOffset};

pub(super) fn fmt_ts(ms: i64, use_utc: bool) -> String {
    let offset = if use_utc {
        Some(UtcOffset::UTC)
    } else {
        UtcOffset::current_local_offset().ok()
    };
    fmt_with_offset(ms, offset)
}

fn fmt_with_offset(ms: i64, offset: Option<UtcOffset>) -> String {
    if ms <= 0 {
        return "0".to_string();
    }
    let nanos = (ms as i128) * 1_000_000;
    let base = match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => dt,
        Err(_) => return ms.to_string(),
    };
    let tm = match offset {
        Some(off) => base.to_offset(off),
        None => base,
    };
    tm.format(ts_format()).unwrap_or_else(|_| ms.to_string())
}

fn ts_format() -> &'static [FormatItem<'static>] {
    static FMT: OnceLock<Vec<FormatItem<'static>>> = OnceLock::new();
    FMT.get_or_init(|| {
        time::format_description::parse(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory]:[offset_minute]",
        )
        .expect("valid timestamp format")
    })
    .as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_millisecond_precision() {
        let s = fmt_ts(1_700_000_000_123, true);
        assert!(
            s.contains(".123"),
            "timestamp should include milliseconds: {}",
            s
        );
        assert!(s.ends_with("+00:00"));
    }

    #[test]
    fn respects_custom_offset() {
        let offset = UtcOffset::from_hms(5, 30, 0).unwrap();
        let s = fmt_with_offset(1_700_000_000_000, Some(offset));
        assert!(s.ends_with("+05:30"));
    }
}
