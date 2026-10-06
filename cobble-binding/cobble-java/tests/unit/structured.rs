use super::{DIRECT_SCAN_BATCH_INITIAL_CAPACITY, initialize_direct_scan_batch};

#[test]
fn direct_scan_batch_initial_capacity_is_bounded() {
    for io_capacity in [0, 1, 3, 4, 64, 2048, 4096, usize::MAX] {
        let mut encoded = Vec::new();
        initialize_direct_scan_batch(&mut encoded, io_capacity);
        assert_eq!(encoded.as_slice(), &0u32.to_be_bytes());
        assert!(encoded.capacity() >= io_capacity.min(DIRECT_SCAN_BATCH_INITIAL_CAPACITY));
        assert!(encoded.capacity() <= DIRECT_SCAN_BATCH_INITIAL_CAPACITY);
    }
}

#[test]
fn direct_scan_batch_retains_capacity_after_growth() {
    let mut stored = Vec::new();
    initialize_direct_scan_batch(&mut stored, 2048);
    let initial_ptr = stored.as_ptr();
    stored.resize(1024, 0);
    assert_eq!(stored.as_ptr(), initial_ptr);
    stored.resize(8192, 0);
    let capacity = stored.capacity();
    let ptr = stored.as_ptr();
    for io_capacity in [1, 2048, usize::MAX] {
        let mut encoded = std::mem::take(&mut stored);
        encoded.clear();
        initialize_direct_scan_batch(&mut encoded, io_capacity);
        encoded.resize(64, 0);
        stored = encoded;
        assert_eq!(stored.capacity(), capacity);
        assert_eq!(stored.as_ptr(), ptr);
    }
}
