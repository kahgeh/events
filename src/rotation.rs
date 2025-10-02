use crate::{EsError, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum RotationPolicy {
    TimeWindow {
        window: std::time::Duration,
        max_bytes: Option<u64>,
    },
}

impl RotationPolicy {
    pub fn window(&self) -> std::time::Duration {
        match self {
            RotationPolicy::TimeWindow { window, .. } => *window,
        }
    }

    pub fn max_bytes(&self) -> Option<u64> {
        match self {
            RotationPolicy::TimeWindow { max_bytes, .. } => *max_bytes,
        }
    }
}

/// Floors a timestamp to the start of its time window.
///
/// # Examples
///
/// ```
/// use events::rotation::floor_to_window_ms;
/// use std::time::Duration;
///
/// let window = Duration::from_secs(3600); // 1 hour
/// let ms = 1727888400000; // 2024-10-02 17:00:00 UTC
/// let floored = floor_to_window_ms(ms, window); // 2024-10-02 16:00:00 UTC
/// ```
pub fn floor_to_window_ms(now_ms: i64, window: std::time::Duration) -> i64 {
    let w_ms = window.as_millis() as i64;
    (now_ms / w_ms) * w_ms
}

/// Generates a human-readable label for a time window.
///
/// # Errors
///
/// Returns an error if:
/// - The timestamp is invalid
///
/// # Examples
///
/// ```
/// use events::rotation::label_for;
/// use std::time::Duration;
///
/// let window = Duration::from_secs(3600); // 1 hour
/// let ms = 1727884800000; // 2024-10-02 16:00:00 UTC
/// let label = label_for(ms, window)?; // "20241002T16"
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn label_for(start_ms: i64, window: std::time::Duration) -> Result<String> {
    use time::{OffsetDateTime, UtcOffset};
    let dt = OffsetDateTime::from_unix_timestamp_nanos((start_ms as i128) * 1_000_000)
        .map_err(|_| EsError::Migration("Invalid timestamp for label generation".to_string()))?
        .to_offset(UtcOffset::UTC);

    let secs = window.as_secs();
    match secs {
        secs if secs >= 86400 => Ok(format!(
            "{:04}{:02}{:02}",
            dt.year(),
            dt.month() as u8,
            dt.day()
        )),
        secs if secs >= 3600 => Ok(format!(
            "{:04}{:02}{:02}T{:02}",
            dt.year(),
            dt.month() as u8,
            dt.day(),
            dt.hour()
        )),
        _ => Ok(format!(
            "{:04}{:02}{:02}T{:02}{:02}",
            dt.year(),
            dt.month() as u8,
            dt.day(),
            dt.hour(),
            dt.minute()
        )),
    }
}

pub fn generate_partition_name(
    start_ms: i64,
    window: std::time::Duration,
    suffix: Option<char>,
) -> Result<String> {
    let base_label = label_for(start_ms, window)?;
    Ok(suffix
        .map(|s| format!("events_{}_{}.db", base_label, s))
        .unwrap_or_else(|| format!("events_{}.db", base_label)))
}

/// Parses date components from a YYYYMMDD string.
///
/// # Errors
///
/// Returns an error if the date components are invalid.
fn parse_date_components(date_part: &str) -> Result<(i32, u8, u8)> {
    let year = date_part[0..4]
        .parse::<i32>()
        .map_err(|_| EsError::InvalidPartition("Invalid year in partition name".to_string()))?;
    let month = date_part[4..6]
        .parse::<u8>()
        .map_err(|_| EsError::InvalidPartition("Invalid month in partition name".to_string()))?;
    let day = date_part[6..8]
        .parse::<u8>()
        .map_err(|_| EsError::InvalidPartition("Invalid day in partition name".to_string()))?;

    Ok((year, month, day))
}

/// Parses time component from a HH or HHMM string.
///
/// # Errors
///
/// Returns an error if the time component is invalid.
fn parse_time_component(time_part: &str) -> Result<(u8, Option<u8>)> {
    match time_part.len() {
        2 => {
            let hour = time_part[0..2].parse::<u8>().map_err(|_| {
                EsError::InvalidPartition("Invalid hour in partition name".to_string())
            })?;
            Ok((hour, None))
        }
        4 => {
            let hour = time_part[0..2].parse::<u8>().map_err(|_| {
                EsError::InvalidPartition("Invalid hour in partition name".to_string())
            })?;
            let minute = time_part[2..4].parse::<u8>().map_err(|_| {
                EsError::InvalidPartition("Invalid minute in partition name".to_string())
            })?;
            Ok((hour, Some(minute)))
        }
        _ => Err(EsError::InvalidPartition(format!(
            "Invalid time format: {}",
            time_part
        ))),
    }
}

/// Creates a datetime with time components.
///
/// # Errors
///
/// Returns an error if the time components are invalid.
fn create_datetime_with_time(
    date: time::Date,
    hour: u8,
    minute: Option<u8>,
) -> Result<time::OffsetDateTime> {
    let datetime = match minute {
        Some(min) => date.with_hms(hour, min, 0),
        None => date.with_hms(hour, 0, 0),
    }
    .map_err(|_| EsError::InvalidPartition("Invalid time in partition name".to_string()))?;

    Ok(datetime.assume_utc())
}

/// Creates a datetime from parsed components.
///
/// # Errors
///
/// Returns an error if the datetime components are invalid.
fn create_datetime(
    year: i32,
    month: u8,
    day: u8,
    time_part: Option<&str>,
) -> Result<time::OffsetDateTime> {
    let date = time::Date::from_calendar_date(
        year,
        time::Month::try_from(month).map_err(|_| {
            EsError::InvalidPartition("Invalid month in partition name".to_string())
        })?,
        day,
    )
    .map_err(|_| EsError::InvalidPartition("Invalid date in partition name".to_string()))?;

    match time_part {
        Some(time) => {
            let (hour, minute) = parse_time_component(time)?;
            create_datetime_with_time(date, hour, minute)
        }
        None => create_datetime_with_time(date, 0, None),
    }
}

pub fn parse_partition_name(name: &str) -> Result<(i64, Option<char>)> {
    use regex::Regex;

    // Parse patterns: events_YYYYMMDD.db, events_YYYYMMDDTXX.db, events_YYYYMMDDTXXXX.db
    // Also handle suffixes: events_YYYYMMDD_a.db, etc.
    let re = Regex::new(r"^events_(\d{8})(?:T(\d{2,4}))?(?:_([a-z]))?\.db$").map_err(|_| {
        EsError::Migration("Invalid regex pattern for partition name parsing".to_string())
    })?;

    let caps = re
        .captures(name)
        .ok_or_else(|| EsError::InvalidPartition(format!("Invalid partition name: {}", name)))?;

    let date_part = &caps[1];
    let time_part = caps.get(2).map(|m| m.as_str());
    let suffix = caps.get(3).and_then(|m| m.as_str().chars().next());

    let (year, month, day) = parse_date_components(date_part)?;
    let dt = create_datetime(year, month, day, time_part)?;

    let start_ms = dt.unix_timestamp() * 1000;
    Ok((start_ms, suffix))
}

pub async fn get_file_size(path: &PathBuf) -> Result<u64> {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.len())
        .map_err(Into::into)
}

pub fn get_next_suffix(current_suffix: Option<char>) -> Option<char> {
    match current_suffix {
        None => Some('a'),
        Some('z') => None, // End of alphabet
        Some(c) => Some((c as u8 + 1) as char),
    }
}

/// Determines if rotation should occur based on file size and available suffixes.
///
/// # Errors
///
/// Returns an error if the partition name cannot be parsed.
fn should_rotate_by_size(
    rotation: &RotationPolicy,
    current_file_size: u64,
    current_name: &str,
) -> Result<bool> {
    let max_bytes = match rotation.max_bytes() {
        Some(bytes) => bytes,
        None => return Ok(false),
    };

    if current_file_size < max_bytes {
        return Ok(false);
    }

    let (_, suffix) = parse_partition_name(current_name)?;
    Ok(get_next_suffix(suffix).is_some())
}

pub fn should_rotate(
    current_name: &str,
    rotation: &RotationPolicy,
    current_file_size: u64,
    current_start_ms: i64,
) -> Result<bool> {
    let now_ms = time::OffsetDateTime::now_utc().unix_timestamp() * 1000; // Convert to ms

    // Check time window rotation
    let window_start_ms = floor_to_window_ms(now_ms, rotation.window());
    if window_start_ms != current_start_ms {
        return Ok(true);
    }

    // Check size rotation
    should_rotate_by_size(rotation, current_file_size, current_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_floor_to_window_ms() {
        // Daily window
        let window = std::time::Duration::from_secs(24 * 3600);
        let ms = 1727884800000; // 2024-10-02 12:00:00 UTC
        let floored = floor_to_window_ms(ms, window);
        assert_eq!(floored, 1727827200000); // 2024-10-02 00:00:00 UTC

        // Hourly window
        let window = std::time::Duration::from_secs(3600);
        let ms = 1727884800000; // 2024-10-02 16:00:00 UTC
        let floored = floor_to_window_ms(ms, window);
        assert_eq!(floored, 1727884800000); // 2024-10-02 16:00:00 UTC (already floored)
    }

    #[test]
    fn test_label_for() -> crate::Result<()> {
        // Daily
        let window = std::time::Duration::from_secs(24 * 3600);
        let ms = 1727827200000; // 2024-10-02 00:00:00 UTC
        assert_eq!(label_for(ms, window)?, "20241002");

        // Hourly
        let window = std::time::Duration::from_secs(3600);
        let ms = 1727884800000; // 2024-10-02 16:00:00 UTC
        assert_eq!(label_for(ms, window)?, "20241002T16");

        // 15-minute
        let window = std::time::Duration::from_secs(15 * 60);
        let ms = 1727872200000; // 2024-10-02 12:30:00 UTC
        assert_eq!(label_for(ms, window)?, "20241002T1230");
        Ok(())
    }

    #[test]
    fn test_generate_partition_name() -> crate::Result<()> {
        let window = std::time::Duration::from_secs(24 * 3600);
        let ms = 1727827200000; // 2024-10-02 00:00:00 UTC

        assert_eq!(
            generate_partition_name(ms, window, None)?,
            "events_20241002.db"
        );
        assert_eq!(
            generate_partition_name(ms, window, Some('a'))?,
            "events_20241002_a.db"
        );
        Ok(())
    }

    #[test]
    fn test_parse_partition_name() -> crate::Result<()> {
        let (start_ms, suffix) = parse_partition_name("events_20241002.db")?;
        assert_eq!(start_ms, 1727827200000); // 2024-10-02 00:00:00 UTC
        assert_eq!(suffix, None);

        let (start_ms, suffix) = parse_partition_name("events_20241002T16.db")?;
        assert_eq!(start_ms, 1727884800000); // 2024-10-02 16:00:00 UTC
        assert_eq!(suffix, None);

        let (start_ms, suffix) = parse_partition_name("events_20241002T1230.db")?;
        assert_eq!(start_ms, 1727872200000); // 2024-10-02 12:30:00 UTC
        assert_eq!(suffix, None);

        let (start_ms, suffix) = parse_partition_name("events_20241002_a.db")?;
        assert_eq!(start_ms, 1727827200000); // 2024-10-02 00:00:00 UTC
        assert_eq!(suffix, Some('a'));
        Ok(())
    }

    #[test]
    fn test_get_next_suffix() {
        assert_eq!(get_next_suffix(None), Some('a'));
        assert_eq!(get_next_suffix(Some('a')), Some('b'));
        assert_eq!(get_next_suffix(Some('y')), Some('z'));
        assert_eq!(get_next_suffix(Some('z')), None);
    }
}
