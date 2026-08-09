use super::*;
use crate::time::ManualTimeProvider;

#[test]
fn test_ttl_provider() {
    let time_provider = Arc::new(ManualTimeProvider::new(1000));
    let ttl_provider = TTLProvider::new(
        &TtlConfig {
            enabled: true,
            default_ttl_seconds: Some(500),
        },
        time_provider.clone(),
    );

    // Test default TTL
    let expiration = ttl_provider.get_expiration_timestamp(None);
    assert_eq!(expiration, Some(1500));

    // Test custom TTL
    let expiration = ttl_provider.get_expiration_timestamp(Some(300));
    assert_eq!(expiration, Some(1300));

    // Test expiration check
    assert!(!ttl_provider.expired(&Some(1500)));
    time_provider.set_time(1600);
    assert!(ttl_provider.expired(&Some(1500)));
}
