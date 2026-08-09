use super::*;

#[test]
fn parquet_meta_roundtrip_v1() {
    let meta = ParquetMeta::new(
        2,
        vec![ParquetRowGroupRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
        }],
    )
    .encode();
    let decoded = decode_meta(Some(meta)).unwrap().unwrap();
    assert_eq!(decoded.version(), PARQUET_META_VERSION_CURRENT);
    assert_eq!(decoded.row_count(), 2);
    assert_eq!(decoded.row_groups().len(), 1);
}
