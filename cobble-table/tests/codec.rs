use bytes::Bytes;
use cobble_table::{
    BucketHash, DataField, ExtensionType, KeyCodec, LogicalType, TimestampKind, Value, ValueCodec,
};
use serde_json::Value as JsonValue;

#[test]
fn codec_contract_vectors_round_trip_and_reject_invalid_input() {
    let key_types = key_types();
    let key_values = key_values();
    let key = KeyCodec::encode_row(&key_types, &key_values).unwrap();
    let fixture: JsonValue = serde_json::from_str(include_str!(
        "../../spec/table/fixtures/cobble_table_v1_codec.json"
    ))
    .unwrap();
    assert_eq!(hex(&key), fixture["key_hex"].as_str().unwrap());
    assert_eq!(KeyCodec::decode_row(&key_types, &key).unwrap(), key_values);
    assert!(KeyCodec::encode_row(&[LogicalType::int32()], &[Value::Null]).is_err());
    assert!(
        KeyCodec::encode_row(&[LogicalType::int32()], &[Value::Int32(-1)]).unwrap()
            < KeyCodec::encode_row(&[LogicalType::int32()], &[Value::Int32(0)]).unwrap()
    );

    let value_type = value_type();
    let value = value();
    let encoded = ValueCodec::encode(&value_type, &value).unwrap();
    assert_eq!(hex(&encoded), fixture["value_hex"].as_str().unwrap());
    assert_eq!(
        ValueCodec::encoded_size(&value_type, &value).unwrap(),
        encoded.len()
    );
    assert_eq!(ValueCodec::decode(&value_type, &encoded).unwrap(), value);
    let owned = Bytes::from(encoded.clone());
    let decoded = ValueCodec::decode_bytes(&value_type, owned.clone()).unwrap();
    assert_eq!(decoded, value);
    let Value::Struct(fields) = decoded else {
        unreachable!()
    };
    let Value::Binary(binary) = &fields[12] else {
        unreachable!()
    };
    assert!(binary.as_ptr() >= owned.as_ptr() && binary.as_ptr() < owned.as_ptr_range().end);
    assert!(ValueCodec::decode(&LogicalType::boolean(), &[2]).is_err());
    assert!(
        ValueCodec::decode(
            &LogicalType::list(LogicalType::int32()),
            &[255, 255, 255, 255]
        )
        .is_err()
    );
    let mut output = vec![7];
    assert!(
        ValueCodec::encode_into(
            &LogicalType::int32(),
            &Value::String("bad".to_string()),
            &mut output
        )
        .is_err()
    );
    assert_eq!(output, vec![7]);
    assert!(
        KeyCodec::encode_row_into(&[LogicalType::int32()], &[Value::Null], &mut output).is_err()
    );
    assert_eq!(output, vec![7]);

    let bucket_hash = BucketHash::new(fixture["bucket_count"].as_u64().unwrap() as u32).unwrap();
    assert_eq!(
        bucket_hash.bucket(&key[..fixture["bucket_prefix_length"].as_u64().unwrap() as usize]),
        fixture["bucket"].as_u64().unwrap() as u16
    );
    assert!(BucketHash::new(0).is_err());
    assert!(BucketHash::new(65_537).is_err());
    assert_eq!(BucketHash::new(1).unwrap().bucket(&key), 0);
    assert_eq!(BucketHash::new(65_536).unwrap().bucket(&[0xc5]), 65_508);
}

fn key_types() -> Vec<LogicalType> {
    vec![
        LogicalType::boolean(),
        LogicalType::int8(),
        LogicalType::int16(),
        LogicalType::int32(),
        LogicalType::int64(),
        LogicalType::decimal(20, 2),
        LogicalType::date(),
        LogicalType::time(6),
        LogicalType::timestamp(3, TimestampKind::WithoutTimeZone),
        LogicalType::string(),
        LogicalType::binary(),
    ]
}

fn key_values() -> Vec<Value> {
    vec![
        Value::Boolean(false),
        Value::Int8(-1),
        Value::Int16(0),
        Value::Int32(1),
        Value::Int64(-2),
        Value::Decimal {
            precision: 20,
            scale: 2,
            unscaled: -42,
        },
        Value::Date(-1),
        Value::Time(123_000_000),
        Value::Timestamp {
            precision: 3,
            timestamp_kind: TimestampKind::WithoutTimeZone,
            seconds: -3,
            nanos: 123_000_000,
        },
        Value::String("x\0é".to_string()),
        Value::Binary(Bytes::from_static(&[0, 0xff])),
    ]
}

fn value_type() -> LogicalType {
    LogicalType::struct_type(vec![
        DataField::new(1, "bool", LogicalType::boolean()).unwrap(),
        DataField::new(2, "i8", LogicalType::int8()).unwrap(),
        DataField::new(3, "i16", LogicalType::int16()).unwrap(),
        DataField::new(4, "i32", LogicalType::int32()).unwrap(),
        DataField::new(5, "i64", LogicalType::int64()).unwrap(),
        DataField::new(6, "f32", LogicalType::float32()).unwrap(),
        DataField::new(7, "f64", LogicalType::float64()).unwrap(),
        DataField::new(8, "decimal", LogicalType::decimal(20, 2)).unwrap(),
        DataField::new(9, "date", LogicalType::date()).unwrap(),
        DataField::new(10, "time", LogicalType::time(6)).unwrap(),
        DataField::new(
            11,
            "timestamp",
            LogicalType::timestamp(3, TimestampKind::WithLocalTimeZone),
        )
        .unwrap(),
        DataField::new(12, "string", LogicalType::string()).unwrap(),
        DataField::new(13, "binary", LogicalType::binary()).unwrap(),
        DataField::new(
            14,
            "list",
            LogicalType::list(LogicalType::binary().nullable()),
        )
        .unwrap(),
        DataField::new(
            15,
            "map",
            LogicalType::map(LogicalType::string(), LogicalType::int32().nullable()),
        )
        .unwrap(),
        DataField::new(
            16,
            "nested",
            LogicalType::struct_type(vec![
                DataField::new(17, "n", LogicalType::string().nullable()).unwrap(),
            ]),
        )
        .unwrap(),
        DataField::new(
            18,
            "extension",
            LogicalType::extension(
                ExtensionType::new("test.extension", JsonValue::Null, LogicalType::int16())
                    .unwrap(),
            ),
        )
        .unwrap(),
    ])
}

fn value() -> Value {
    Value::Struct(vec![
        Value::Boolean(true),
        Value::Int8(-1),
        Value::Int16(-2),
        Value::Int32(-3),
        Value::Int64(-4),
        Value::Float32(1.5),
        Value::Float64(-2.5),
        Value::Decimal {
            precision: 20,
            scale: 2,
            unscaled: -42,
        },
        Value::Date(-1),
        Value::Time(123_000_000),
        Value::Timestamp {
            precision: 3,
            timestamp_kind: TimestampKind::WithLocalTimeZone,
            seconds: -3,
            nanos: 123_000_000,
        },
        Value::String("hi é".to_string()),
        Value::Binary(Bytes::from_static(&[0, 0xff])),
        Value::List(vec![Value::Binary(Bytes::from_static(b"a")), Value::Null]),
        Value::Map(vec![(Value::String("k".to_string()), Value::Null)]),
        Value::Struct(vec![Value::String("nested".to_string())]),
        Value::Extension {
            type_id: "test.extension".to_string(),
            value: Box::new(Value::Int16(7)),
        },
    ])
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
