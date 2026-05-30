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
