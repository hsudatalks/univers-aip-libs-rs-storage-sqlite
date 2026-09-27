use super::*;

#[test]
fn checkpoint_waits_for_write_quiescence_and_does_not_repeat_without_new_writes() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let active_write = now - Duration::from_secs(5);
    let idle_write = now - PASSIVE_CHECKPOINT_IDLE_AFTER;

    assert!(!passive_checkpoint_due(4096, active_write, None, now));
    assert!(passive_checkpoint_due(4096, idle_write, None, now));
    assert!(!passive_checkpoint_due(
        4096,
        idle_write,
        Some(idle_write),
        now
    ));
}

#[test]
fn checkpoint_bounds_wal_growth_during_continuous_writes() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let active_write = now - Duration::from_secs(1);

    assert!(passive_checkpoint_due(
        PASSIVE_CHECKPOINT_FORCE_BYTES,
        active_write,
        None,
        now
    ));
}
