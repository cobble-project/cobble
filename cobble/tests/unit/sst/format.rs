use super::*;

const FOOTER_VERSION_OFFSET: usize = 4 * std::mem::size_of::<u64>() + std::mem::size_of::<u32>();

fn footer_version(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(
        encoded[FOOTER_VERSION_OFFSET..FOOTER_VERSION_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

fn set_footer_version(encoded: &mut [u8], version: u32) {
    encoded[FOOTER_VERSION_OFFSET..FOOTER_VERSION_OFFSET + 4]
        .copy_from_slice(&version.to_le_bytes());
}

#[test]
fn test_footer_encode_decode() {
    let footer = Footer {
        index_block_offset: 100,
        index_block_size: 200,
        filter_block_offset: 300,
        filter_block_size: 400,
        filter_present: true,
        partitioned_index: false,
        value_has_ttl: true,
        block_checksums: true,
    };
    let encoded = footer.encode();
    assert_eq!(encoded.len(), FOOTER_SIZE);
    assert_eq!(footer_version(&encoded), SST_FOOTER_VERSION_CURRENT);

    let decoded = Footer::decode(&encoded).unwrap();
    assert_eq!(decoded.index_block_offset, 100);
    assert_eq!(decoded.index_block_size, 200);
    assert_eq!(decoded.filter_block_offset, 300);
    assert_eq!(decoded.filter_block_size, 400);
    assert!(decoded.filter_present);
    assert!(!decoded.partitioned_index);
    assert!(decoded.value_has_ttl);
    assert!(decoded.block_checksums);
}

#[test]
fn test_sst_read_metadata_rejects_zero_sized_unpartitioned_index() {
    let mut builder = BlockBuilder::new(1024);
    builder.add(b"key", b"value");
    let footer = Footer {
        index_block_offset: 0,
        index_block_size: 0,
        filter_block_offset: 0,
        filter_block_size: 0,
        filter_present: false,
        partitioned_index: false,
        value_has_ttl: true,
        block_checksums: false,
    };

    assert!(SstReadMetadata::from_index_block(footer, &builder.build()).is_err());
}

#[test]
fn test_footer_without_checksums_still_uses_current_version() {
    let footer = Footer {
        index_block_offset: 10,
        index_block_size: 20,
        filter_block_offset: 30,
        filter_block_size: 40,
        filter_present: true,
        partitioned_index: true,
        value_has_ttl: false,
        block_checksums: false,
    };
    let encoded = footer.encode();
    assert_eq!(footer_version(&encoded), SST_FOOTER_VERSION_CURRENT);
    let decoded = Footer::decode(&encoded).unwrap();
    assert_eq!(decoded.index_block_offset, 10);
    assert_eq!(decoded.index_block_size, 20);
    assert_eq!(decoded.filter_block_offset, 30);
    assert_eq!(decoded.filter_block_size, 40);
    assert!(decoded.filter_present);
    assert!(decoded.partitioned_index);
    assert!(!decoded.value_has_ttl);
    assert!(!decoded.block_checksums);
}

#[test]
fn test_legacy_footer_without_checksums_is_supported() {
    let footer = Footer {
        index_block_offset: 10,
        index_block_size: 20,
        filter_block_offset: 30,
        filter_block_size: 40,
        filter_present: false,
        partitioned_index: false,
        value_has_ttl: true,
        block_checksums: false,
    };
    let mut encoded = footer.encode().to_vec();
    set_footer_version(&mut encoded, SST_FOOTER_VERSION_LEGACY);

    let decoded = Footer::decode(&encoded).unwrap();
    assert!(!decoded.block_checksums);
}

#[test]
fn test_legacy_footer_rejects_checksum_flag() {
    let footer = Footer {
        index_block_offset: 10,
        index_block_size: 20,
        filter_block_offset: 30,
        filter_block_size: 40,
        filter_present: false,
        partitioned_index: false,
        value_has_ttl: true,
        block_checksums: true,
    };
    let mut encoded = footer.encode().to_vec();
    set_footer_version(&mut encoded, SST_FOOTER_VERSION_LEGACY);

    assert!(Footer::decode(&encoded).is_err());
}

#[test]
fn test_block_encode_decode() {
    let mut builder = BlockBuilder::new(4096);
    builder.add(b"key1", b"value1");
    builder.add(b"key2", b"value2");
    builder.add(b"key3", b"value3");

    let block = builder.build();
    assert_eq!(block.offsets_len(), 3);

    let encoded = block.encode();
    let decoded = Block::decode(encoded).unwrap();

    assert_eq!(decoded.offsets_len(), 3);

    let (key, value) = decoded.get(0).unwrap();
    assert_eq!(&key[..], b"key1");
    assert_eq!(&value[..], b"value1");

    let (key, value) = decoded.get(1).unwrap();
    assert_eq!(&key[..], b"key2");
    assert_eq!(&value[..], b"value2");

    let (key, value) = decoded.get(2).unwrap();
    assert_eq!(&key[..], b"key3");
    assert_eq!(&value[..], b"value3");
}

#[test]
fn test_block_builder_encoded_matches_block_encoding() {
    for prefix_compressed in [false, true] {
        let populate = |builder: &mut BlockBuilder| {
            builder.add(b"map:key:0001", b"value1");
            builder.add(b"map:key:0002", b"value2");
            builder.add(b"map:key:0010", b"value3");
        };
        let mut block_builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
        let mut encoded_builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
        populate(&mut block_builder);
        populate(&mut encoded_builder);

        let expected = block_builder.build().encode();
        let actual = encoded_builder.build_encoded();
        assert_eq!(actual, expected);

        let decoded = Block::decode(actual).unwrap();
        assert_eq!(decoded.offsets_len(), 3);
        assert_eq!(decoded.get(1).unwrap().0.as_ref(), b"map:key:0002");
        assert_eq!(decoded.get(1).unwrap().1.as_ref(), b"value2");
    }
}

#[test]
fn test_empty_block_builder_encoded_round_trip() {
    let builder = BlockBuilder::new(32);
    assert!(builder.is_empty());
    assert_eq!(builder.estimated_size(), BLOCK_HEADER_SIZE);

    let encoded = builder.build_encoded();
    assert_eq!(encoded.len(), BLOCK_HEADER_SIZE);
    let decoded = Block::decode(encoded).unwrap();
    assert_eq!(decoded.offsets_len(), 0);
    assert_eq!(decoded.size_in_bytes(), BLOCK_HEADER_SIZE);
}

#[test]
fn test_block_builder_size_boundary_includes_header_and_offsets() {
    let entry_size = 4 + b"key".len() + 4 + b"value".len();
    let encoded_size = BLOCK_HEADER_SIZE + entry_size + 4;
    let mut builder = BlockBuilder::new(encoded_size);
    assert_eq!(builder.estimated_size(), BLOCK_HEADER_SIZE);
    assert!(!builder.should_finish());

    builder.add(b"key", b"value");
    assert_eq!(builder.estimated_size(), encoded_size);
    assert!(builder.should_finish());
    assert_eq!(builder.build_encoded().len(), encoded_size);
}

#[test]
fn test_block_builder_should_finish() {
    let mut builder = BlockBuilder::new(100);
    assert!(!builder.should_finish());

    // Add enough data to exceed target size
    builder.add(b"key1", b"value1_with_long_data");
    builder.add(b"key2", b"value2_with_long_data");
    builder.add(b"key3", b"value3_with_long_data");

    assert!(builder.should_finish());
}

#[test]
fn test_block_prefix_encode_decode() {
    let mut builder = BlockBuilder::new_with_prefix(4096, 2, true);
    builder.add(b"map:key:0001", b"v1");
    builder.add(b"map:key:0002", b"v2");
    builder.add(b"map:key:0003", b"v3");

    let encoded = builder.build().encode();
    let decoded = Block::decode(encoded).unwrap();
    assert_eq!(&decoded.key(0).unwrap()[..], b"map:key:0001");
    assert_eq!(&decoded.key(1).unwrap()[..], b"map:key:0002");
    assert_eq!(&decoded.key(2).unwrap()[..], b"map:key:0003");
    assert_eq!(&decoded.value(1).unwrap()[..], b"v2");
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("map:key:0002"))
            .unwrap(),
        1
    );
}

#[test]
fn test_block_exact_lookup() {
    for prefix_compressed in [false, true] {
        let mut builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
        builder.add(b"map:key:0001", b"v1");
        builder.add(b"map:key:0002", b"v2");
        builder.add(b"map:key:0003", b"v3");
        let long_key = vec![b'z'; PREFIX_SEARCH_STACK_BYTES + 1];
        builder.add(long_key.as_slice(), b"long");
        let block = Block::decode(builder.build().encode()).unwrap();

        assert_eq!(
            block.get_exact(b"map:key:0002").unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        assert!(block.get_exact(b"map:key:0002a").unwrap().is_none());
        assert!(block.get_exact(b"map:key:9999").unwrap().is_none());
        assert_eq!(
            block.get_exact(long_key.as_slice()).unwrap().as_deref(),
            Some(b"long".as_slice())
        );
    }
}

#[test]
fn test_block_prefix_restart_interval_repeats() {
    let mut builder = BlockBuilder::new_with_prefix(4096, 2, true);
    builder.add(b"map:key:0001", b"v1");
    builder.add(b"map:key:0002", b"v2");
    builder.add(b"map:key:0003", b"v3");
    builder.add(b"map:key:0004", b"v4");
    let decoded = Block::decode(builder.build().encode()).unwrap();

    fn shared_len(block: &Block, idx: usize) -> u16 {
        let offset = block.offsets[idx] as usize;
        let data = block.data.as_ref();
        u16::from_le_bytes(
            data[offset..offset + 2]
                .try_into()
                .expect("prefix entry header exists"),
        )
    }

    assert_eq!(shared_len(&decoded, 0), 0);
    assert!(shared_len(&decoded, 1) > 0);
    assert_eq!(shared_len(&decoded, 2), 0);
    assert!(shared_len(&decoded, 3) > 0);
}

#[test]
fn test_block_prefix_seek_binary_then_linear_interval() {
    let mut builder = BlockBuilder::new_with_prefix(4096, 3, true);
    for i in 1..=8 {
        let key = format!("map:key:{i:04}");
        let value = format!("v{i}");
        builder.add(key.as_bytes(), value.as_bytes());
    }
    let decoded = Block::decode(builder.build().encode()).unwrap();

    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("map:key:0000"))
            .unwrap(),
        0
    );
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("map:key:0004"))
            .unwrap(),
        3
    );
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("map:key:0005"))
            .unwrap(),
        4
    );
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("map:key:9999"))
            .unwrap(),
        decoded.offsets_len()
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("map:key:0000"))
            .unwrap(),
        0
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("map:key:0004"))
            .unwrap(),
        3
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("map:key:0004a"))
            .unwrap(),
        3
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("map:key:0005"))
            .unwrap(),
        4
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("map:key:9999"))
            .unwrap(),
        7
    );
}

#[test]
fn test_block_prefix_seek_uses_target_prefix_and_key_len() {
    let mut builder = BlockBuilder::new_with_prefix(4096, 4, true);
    builder.add(b"abc", b"v1");
    builder.add(b"abcx", b"v2");
    builder.add(b"abcxy", b"v3");
    builder.add(b"abcz", b"v4");
    let decoded = Block::decode(builder.build().encode()).unwrap();

    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("abc"))
            .unwrap(),
        0
    );
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("abcd"))
            .unwrap(),
        1
    );
    assert_eq!(
        decoded
            .find_equal_or_greater_idx(&Bytes::from("abcxz"))
            .unwrap(),
        3
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("abc"))
            .unwrap(),
        0
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("abcd"))
            .unwrap(),
        0
    );
    assert_eq!(
        decoded
            .find_lower_or_equal_idx(&Bytes::from("abcxz"))
            .unwrap(),
        2
    );
}
