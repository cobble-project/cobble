use super::*;

#[test]
fn test_bloom_filter_roundtrip() {
    let mut builder = BloomFilterBuilder::new(10);
    builder.add(b"alpha");
    builder.add(b"beta");
    builder.add(b"gamma");
    let filter = builder.finish();
    let encoded = filter.encode();
    let decoded = BloomFilter::decode(encoded).unwrap();
    assert!(decoded.may_contain(b"alpha"));
    assert!(decoded.may_contain(b"beta"));
    assert!(decoded.may_contain(b"gamma"));
}
