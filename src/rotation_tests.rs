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
        generate_partition_name(ms, window, Some(1))?,
        "events_20241002_000001.db"
    );
    assert_eq!(
        generate_partition_name(ms, window, Some(27))?,
        "events_20241002_000027.db"
    );
    assert_eq!(
        generate_partition_name(ms, window, Some(999_999))?,
        "events_20241002_999999.db"
    );
    Ok(())
}

#[test]
fn test_parse_partition_name() -> crate::Result<()> {
    let (start_ms, ordinal) = parse_partition_name("events_20241002.db")?;
    assert_eq!(start_ms, 1727827200000); // 2024-10-02 00:00:00 UTC
    assert_eq!(ordinal, None);

    let (start_ms, ordinal) = parse_partition_name("events_20241002T16.db")?;
    assert_eq!(start_ms, 1727884800000); // 2024-10-02 16:00:00 UTC
    assert_eq!(ordinal, None);

    let (start_ms, ordinal) = parse_partition_name("events_20241002T1230.db")?;
    assert_eq!(start_ms, 1727872200000); // 2024-10-02 12:30:00 UTC
    assert_eq!(ordinal, None);

    let (start_ms, ordinal) = parse_partition_name("events_20241002_000027.db")?;
    assert_eq!(start_ms, 1727827200000); // 2024-10-02 00:00:00 UTC
    assert_eq!(ordinal, Some(27));
    Ok(())
}

#[test]
fn test_parse_partition_name_rejects_invalid_overflow_ordinals() {
    for name in [
        "events_20241002_a.db",
        "events_20241002_1.db",
        "events_20241002_000000.db",
        "events_20241002_1000000.db",
        "events_20241002_-00001.db",
        "events_20241002_00a001.db",
    ] {
        assert!(
            matches!(
                parse_partition_name(name),
                Err(crate::EsError::InvalidPartition(_))
            ),
            "{name} must be rejected"
        );
    }
}

#[test]
fn test_get_next_ordinal() {
    assert_eq!(get_next_ordinal(None), Some(1));
    assert_eq!(get_next_ordinal(Some(1)), Some(2));
    assert_eq!(get_next_ordinal(Some(26)), Some(27));
    assert_eq!(get_next_ordinal(Some(999_998)), Some(999_999));
    assert_eq!(get_next_ordinal(Some(999_999)), None);
}
