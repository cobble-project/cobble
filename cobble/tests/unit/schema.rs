use super::*;
use crate::merge_operator::{MergeOperator, U32CounterMergeOperator};
use crate::row_merge::{SchemaMergePlan, SchemaValue, merge_schema_values};
use crate::r#type::{Column, Value, ValueType};
use crate::vlog::VlogPointer;
use bytes::Bytes;

struct BracketMergeOperator;

impl MergeOperator for BracketMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn crate::TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let existing = existing_value.as_ref();
        Ok((
            format!(
                "[{}+{}]",
                String::from_utf8_lossy(existing),
                String::from_utf8_lossy(value.as_ref())
            )
            .into_bytes()
            .into(),
            None,
        ))
    }
}

#[test]
fn test_schema_aware_merge_materializes_transform_boundaries() {
    let definition_manager = Arc::new(SchemaManager::new(1));
    let mut builder = definition_manager.builder();
    builder
        .add_column(1, None, Some(Bytes::from_static(b"default")), None)
        .unwrap();
    builder.add_column(2, None, None, None).unwrap();
    let schema1 = builder.commit();
    let mut schema1_file = schema_to_file(&schema1);
    let transition = schema1_file.column_families[0].transition.as_mut().unwrap();
    let ColumnEvolutionFile::Source { transform_id, .. } = &mut transition[0] else {
        panic!("first target column should preserve the source");
    };
    *transform_id = Some("decorate".to_string());

    let registry = Arc::new(SchemaTransformRegistry::default());
    registry
        .register("decorate", |value: Option<Bytes>| {
            Ok(value.map(|value| [value.as_ref(), b"-x"].concat().into()))
        })
        .unwrap();
    let manager = SchemaManager::new_with_transform_registry(1, registry);
    manager
        .register_schema_from_def(&schema1_file, None)
        .unwrap();
    let latest = manager.latest_schema();
    let mut merge_plan = SchemaMergePlan::new(latest.as_ref(), &manager, DEFAULT_COLUMN_FAMILY_ID);

    let pointer = VlogPointer::new(1, 9).to_bytes();
    let merged = merge_schema_values(
        vec![
            SchemaValue {
                schema_id: 0,
                value: Value::new(vec![Some(Column::new(
                    ValueType::PutSeparated,
                    pointer.to_vec(),
                ))]),
            },
            SchemaValue {
                schema_id: 0,
                value: Value::new(vec![Some(Column::new(
                    ValueType::Merge,
                    Bytes::from_static(b"b"),
                ))]),
            },
            SchemaValue {
                schema_id: 1,
                value: Value::new(vec![
                    Some(Column::new(ValueType::Merge, Bytes::from_static(b"c"))),
                    None,
                    None,
                ]),
            },
        ],
        &mut merge_plan,
        None,
        |pointer| {
            assert_eq!(pointer, VlogPointer::new(1, 9));
            Ok(Bytes::from_static(b"a"))
        },
    )
    .unwrap()
    .unwrap();
    let columns = crate::row_merge::value_to_vec_of_columns_with_vlog(
        merged,
        |_| unreachable!("source VLOG pointer must be materialized at the transition"),
        latest.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(columns[0].as_deref(), Some(&b"ab-xc"[..]));
    assert_eq!(columns[1].as_deref(), Some(&b"default"[..]));
    assert!(columns[2].is_none());

    // A custom transform boundary cannot be crossed backward. Compaction must
    // close it before an older physical schema can appear above a newer one.
    let fallback_error = match merge_schema_values(
        vec![
            SchemaValue {
                schema_id: 1,
                value: Value::new(vec![
                    Some(Column::new(ValueType::Put, Bytes::from_static(b"a-x"))),
                    Some(Column::new(ValueType::Put, Bytes::from_static(b"default"))),
                    None,
                ]),
            },
            SchemaValue {
                schema_id: 0,
                value: Value::new(vec![Some(Column::new(
                    ValueType::Merge,
                    Bytes::from_static(b"b"),
                ))]),
            },
        ],
        &mut merge_plan,
        None,
        |_| unreachable!("this scenario has no VLOG pointers"),
    ) {
        Ok(_) => panic!("custom transform boundary must require compaction closure"),
        Err(error) => error,
    };
    assert!(
        fallback_error
            .to_string()
            .contains("compaction must close incompatible schema transitions")
    );

    let compatible_manager = Arc::new(SchemaManager::new(1));
    let compatible_schema = compatible_manager.builder().commit();
    let mut compatible_plan = SchemaMergePlan::new(
        compatible_schema.as_ref(),
        compatible_manager.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    );
    let compatible_fallback = merge_schema_values(
        vec![
            SchemaValue {
                schema_id: compatible_schema.version(),
                value: Value::new(vec![Some(Column::new(
                    ValueType::Put,
                    Bytes::from_static(b"a"),
                ))]),
            },
            SchemaValue {
                schema_id: 0,
                value: Value::new(vec![Some(Column::new(
                    ValueType::Merge,
                    Bytes::from_static(b"b"),
                ))]),
            },
        ],
        &mut compatible_plan,
        None,
        |_| unreachable!("this scenario has no VLOG pointers"),
    )
    .unwrap()
    .unwrap();
    let columns = crate::row_merge::value_to_vec_of_columns_with_vlog(
        compatible_fallback,
        |_| unreachable!("this scenario has no VLOG pointers"),
        compatible_schema.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(columns[0].as_deref(), Some(&b"ab"[..]));

    let deleted = merge_schema_values(
        vec![SchemaValue {
            schema_id: 0,
            value: Value::new(vec![Some(Column::new(ValueType::Delete, Bytes::new()))]),
        }],
        &mut merge_plan,
        None,
        |_| unreachable!("whole-row delete must not resolve VLOG"),
    )
    .unwrap()
    .unwrap();
    assert!(deleted.columns().iter().all(|column| {
        column
            .as_ref()
            .is_some_and(|column| *column.value_type() == ValueType::Delete)
    }));
}

#[test]
fn test_schema_builder_set_column_operator() {
    let manager = Arc::new(SchemaManager::new(2));
    let mut builder = manager.builder();
    builder
        .set_column_operator(None, 1, Arc::new(BracketMergeOperator))
        .unwrap();
    let schema = builder.commit();
    assert_eq!(schema.version(), 1);
    let op = schema.operator(1);
    let (merged, _) = op
        .merge(Bytes::from_static(b"x"), Bytes::from_static(b"y"), None)
        .unwrap();
    assert_eq!(merged.as_ref(), b"[x+y]");
}

#[test]
fn test_schema_falls_back_to_default_for_missing_column() {
    let manager = SchemaManager::new(1);
    let schema = manager.latest_schema();
    let op = schema.operator(10);
    let (merged, _) = op
        .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
        .unwrap();
    assert_eq!(merged.as_ref(), b"ab");
}

#[test]
fn test_schema_builder_extends_columns() {
    let manager = Arc::new(SchemaManager::new(1));
    let base = manager.latest_schema();
    let mut builder = manager.builder_from(base);
    builder
        .set_column_operator(None, 3, Arc::new(BracketMergeOperator))
        .unwrap();
    let schema = builder.commit();
    assert_eq!(schema.num_columns(), 4);
    let (merged0, _) = schema
        .operator(0)
        .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
        .unwrap();
    let (merged3, _) = schema
        .operator(3)
        .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
        .unwrap();
    assert_eq!(merged0.as_ref(), b"ab");
    assert_eq!(merged3.as_ref(), b"[a+b]");
}

#[test]
fn test_schema_evolution_add_column() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder.add_column(1, None, None, None).unwrap();
    let schema = builder.commit();
    assert_eq!(schema.version(), 1);
    assert_eq!(schema.num_columns(), 2);
    assert!(
        schema_to_file(&schema).column_families[0]
            .transition
            .is_some()
    );
    assert_eq!(
        schema
            .transition_compatibility_in_family(DEFAULT_COLUMN_FAMILY_ID)
            .unwrap(),
        TransitionCompatibility::Incompatible
    );

    let value = Value::new(vec![Some(Column::new(
        ValueType::Put,
        Bytes::from_static(b"v"),
    ))]);
    let evolved = manager
        .evolve_value_in_family(value, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert_eq!(evolved.columns().len(), 2);
    assert!(evolved.columns()[1].is_none());
}

#[test]
fn test_schema_evolution_add_column_with_default() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .add_column(1, None, Some(Bytes::from_static(b"default")), None)
        .unwrap();
    let schema = builder.commit();
    assert_eq!(schema.version(), 1);
    assert_eq!(schema.num_columns(), 2);

    let value = Value::new(vec![Some(Column::new(
        ValueType::Put,
        Bytes::from_static(b"v"),
    ))]);
    let evolved = manager
        .evolve_value_in_family(value, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert_eq!(evolved.columns().len(), 2);
    let default_column = evolved.columns()[1].as_ref().unwrap();
    assert_eq!(*default_column.value_type(), ValueType::Put);
    assert_eq!(default_column.data().as_ref(), b"default");

    let deleted = Value::new(vec![Some(Column::new(ValueType::Delete, Bytes::new()))]);
    let evolved = manager
        .evolve_value_in_family(deleted, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert!(evolved.columns().iter().all(|column| {
        column
            .as_ref()
            .is_some_and(|column| *column.value_type() == ValueType::Delete)
    }));
}

#[test]
fn test_schema_evolution_delete_column() {
    let manager = Arc::new(SchemaManager::new(2));
    let mut builder = manager.builder();
    builder.delete_column(None, 0).unwrap();
    let schema = builder.commit();
    assert_eq!(schema.version(), 1);
    assert_eq!(schema.num_columns(), 1);
    assert!(
        schema_to_file(&schema).column_families[0]
            .transition
            .is_some()
    );
    assert_eq!(
        schema
            .transition_compatibility_in_family(DEFAULT_COLUMN_FAMILY_ID)
            .unwrap(),
        TransitionCompatibility::Compatible
    );

    let value = Value::new(vec![
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v0"))),
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v1"))),
    ]);
    let evolved = manager
        .evolve_value_in_family(value, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert_eq!(evolved.columns().len(), 1);
    assert_eq!(
        evolved.columns()[0].as_ref().unwrap().data().as_ref(),
        b"v1"
    );
}

#[test]
fn test_schema_evolution_mixes_add_delete_and_replace() {
    let manager = Arc::new(SchemaManager::new(3));
    let mut builder = manager.builder();
    builder.add_column(1, None, None, None).unwrap();
    builder.delete_column(None, 2).unwrap();
    builder
        .replace_column(None, 1, Arc::new(U32CounterMergeOperator))
        .unwrap();
    let schema = builder.commit();

    assert_eq!(schema.num_columns(), 3);
    assert_eq!(schema.operator(1).id(), U32CounterMergeOperator.id());

    let schema_file = schema_to_file(&schema);
    assert!(schema_file.column_families[0].transition.is_some());
    assert_eq!(
        schema
            .transition_compatibility_in_family(DEFAULT_COLUMN_FAMILY_ID)
            .unwrap(),
        TransitionCompatibility::Incompatible
    );
    let restored = schema_from_file(&schema_file, None).unwrap();
    assert_eq!(restored.num_columns(), 3);
    assert_eq!(
        restored.operator(1).id(),
        U32CounterMergeOperator.id(),
        "replacement survives schema persistence"
    );
    manager
        .schemas
        .write()
        .unwrap()
        .insert(1, Arc::new(restored));

    let value = Value::new(vec![
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v0"))),
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v1"))),
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v2"))),
    ]);
    let evolved = manager
        .evolve_value_in_family(value, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert_eq!(evolved.columns().len(), 3);
    assert_eq!(
        evolved.columns()[0].as_ref().unwrap().data().as_ref(),
        b"v0"
    );
    assert!(evolved.columns()[1].is_none());
    assert_eq!(
        evolved.columns()[2].as_ref().unwrap().data().as_ref(),
        b"v2"
    );

    let deleted = Value::new(vec![
        Some(Column::new(ValueType::Delete, Bytes::new())),
        Some(Column::new(ValueType::Delete, Bytes::new())),
        Some(Column::new(ValueType::Delete, Bytes::new())),
    ]);
    let evolved = manager
        .evolve_value_in_family(deleted, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert!(evolved.columns().iter().all(|column| {
        column
            .as_ref()
            .is_some_and(|column| *column.value_type() == ValueType::Delete)
    }));

    let mut schema_file = schema_to_file(&schema);
    let transition = schema_file.column_families[0]
        .transition
        .as_mut()
        .expect("mixed transition");
    let ColumnEvolutionFile::Source { transform_id, .. } = &mut transition[0] else {
        panic!("first target column should use a source");
    };
    *transform_id = Some("test-transform".to_string());
    let payload = serde_json::to_vec(&schema_file).unwrap();
    let restored_file: SchemaFile = serde_json::from_slice(&payload).unwrap();
    let restored = schema_from_file(&restored_file, None).unwrap();
    manager
        .schemas
        .write()
        .unwrap()
        .insert(1, Arc::new(restored));
    let error = match manager.evolve_value_in_family(
        Value::new(vec![
            Some(Column::new(ValueType::Put, Bytes::from_static(b"v0"))),
            Some(Column::new(ValueType::Put, Bytes::from_static(b"v1"))),
            Some(Column::new(ValueType::Put, Bytes::from_static(b"v2"))),
        ]),
        0,
        1,
        DEFAULT_COLUMN_FAMILY_ID,
    ) {
        Ok(_) => panic!("transform transition must reject per-value execution"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("test-transform' requires materialized row execution")
    );

    let transforms = Arc::new(SchemaTransformRegistry::default());
    transforms
        .register("first", |value: Option<Bytes>| {
            let mut value = value.expect("first transform source").to_vec();
            value.extend_from_slice(b"-first");
            Ok(Some(value.into()))
        })
        .unwrap();
    let duplicate = transforms
        .register("first", |value: Option<Bytes>| Ok(value))
        .unwrap_err();
    assert!(duplicate.to_string().contains("already registered"));
    transforms
        .register("second", |value: Option<Bytes>| {
            let mut value = value
                .unwrap_or_else(|| Bytes::from_static(b"null"))
                .to_vec();
            value.extend_from_slice(b"-second");
            Ok(Some(value.into()))
        })
        .unwrap();

    let source_manager = Arc::new(SchemaManager::new(1));
    let mut builder = source_manager.builder();
    builder
        .add_column(1, None, Some(Bytes::from_static(b"default")), None)
        .unwrap();
    builder.add_column(2, None, None, None).unwrap();
    let schema1 = builder.commit();
    let mut builder = source_manager.builder_from(schema1.clone());
    builder
        .replace_column(None, 0, Arc::new(U32CounterMergeOperator))
        .unwrap();
    let schema2 = builder.commit();
    let mut schema1_file = schema_to_file(&schema1);
    let schema1_transition = schema1_file.column_families[0].transition.as_mut().unwrap();
    let ColumnEvolutionFile::Source { transform_id, .. } = &mut schema1_transition[0] else {
        panic!("first schema should preserve its source column");
    };
    *transform_id = Some("first".to_string());
    let mut schema2_file = schema_to_file(&schema2);
    for evolution in schema2_file.column_families[0].transition.as_mut().unwrap() {
        let ColumnEvolutionFile::Source { transform_id, .. } = evolution else {
            panic!("second schema should preserve every source column");
        };
        *transform_id = Some("second".to_string());
    }

    let out_of_order = SchemaManager::new_with_transform_registry(1, Arc::clone(&transforms));
    out_of_order
        .register_schema_from_def(&schema2_file, None)
        .unwrap();
    assert_eq!(
        out_of_order
            .schema(2)
            .unwrap()
            .transition_compatibility_in_family(DEFAULT_COLUMN_FAMILY_ID)
            .unwrap(),
        TransitionCompatibility::Unknown
    );
    out_of_order
        .register_schema_from_def(&schema1_file, None)
        .unwrap();
    assert_eq!(
        out_of_order
            .schema(2)
            .unwrap()
            .transition_compatibility_in_family(DEFAULT_COLUMN_FAMILY_ID)
            .unwrap(),
        TransitionCompatibility::Incompatible
    );
    let route = out_of_order
        .compile_projection_route(0, 2, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert_eq!(
        route.apply(&[Some(Bytes::from_static(b"value"))]).unwrap(),
        vec![
            Some(Bytes::from_static(b"value-first-second")),
            Some(Bytes::from_static(b"default-second")),
            Some(Bytes::from_static(b"null-second")),
        ]
    );

    let missing_registry = Arc::new(SchemaTransformRegistry::default());
    missing_registry
        .register("second", |value: Option<Bytes>| Ok(value))
        .unwrap();
    let missing = SchemaManager::new_with_transform_registry(1, missing_registry);
    missing
        .register_schema_from_def(&schema1_file, None)
        .unwrap();
    missing
        .register_schema_from_def(&schema2_file, None)
        .unwrap();
    let error = match missing.compile_projection_route(0, 2, DEFAULT_COLUMN_FAMILY_ID) {
        Ok(_) => panic!("missing transform must reject route compilation"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("'first' is not registered"));
}

#[test]
fn test_schema_evolution_remaps_columns_atomically() {
    let manager = Arc::new(SchemaManager::new(3));
    let mut builder = manager.builder();
    builder
        .remap_columns(
            None,
            vec![
                ColumnEvolution::Source {
                    source_index: 2,
                    transform_id: None,
                },
                ColumnEvolution::Default {
                    value: Bytes::from_static(b"new"),
                },
                ColumnEvolution::Source {
                    source_index: 0,
                    transform_id: None,
                },
            ],
        )
        .unwrap();
    let schema = builder.commit();
    let schema_file = schema_to_file(&schema);
    let restored = schema_from_file(&schema_file, None).unwrap();
    manager
        .schemas
        .write()
        .unwrap()
        .insert(1, Arc::new(restored));

    let value = Value::new(vec![
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v0"))),
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v1"))),
        Some(Column::new(ValueType::Put, Bytes::from_static(b"v2"))),
    ]);
    let evolved = manager
        .evolve_value_in_family(value, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    let columns = evolved.columns();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].as_ref().unwrap().data().as_ref(), b"v2");
    assert_eq!(columns[1].as_ref().unwrap().data().as_ref(), b"new");
    assert_eq!(columns[2].as_ref().unwrap().data().as_ref(), b"v0");
    let deleted = Value::new(vec![
        Some(Column::new(ValueType::Delete, Bytes::new())),
        Some(Column::new(ValueType::Delete, Bytes::new())),
        Some(Column::new(ValueType::Delete, Bytes::new())),
    ]);
    let evolved = manager
        .evolve_value_in_family(deleted, 0, 1, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    assert!(evolved.columns().iter().all(|column| {
        column
            .as_ref()
            .is_some_and(|column| *column.value_type() == ValueType::Delete)
    }));
    assert!(
        Arc::new(SchemaManager::new(2))
            .builder()
            .remap_columns(
                None,
                vec![
                    ColumnEvolution::Source {
                        source_index: 0,
                        transform_id: None,
                    },
                    ColumnEvolution::Source {
                        source_index: 0,
                        transform_id: None,
                    },
                ],
            )
            .is_err()
    );
}

#[test]
fn test_schema_default_column_family() {
    let manager = Arc::new(SchemaManager::new(2));
    let schema = manager.latest_schema();
    let families = schema.column_families();
    assert_eq!(families.len(), 1);
    assert_eq!(
        families[0],
        (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 2usize)
    );
}

#[test]
fn test_schema_builder_assign_new_column_to_new_column_family() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .expect("add column with column family");
    let schema = builder.commit();
    let families = schema.column_families();
    assert_eq!(families.len(), 2);
    assert_eq!(
        families[0],
        (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 1usize)
    );
    assert_eq!(families[1], ("metrics".to_string(), 1usize));
    assert_eq!(
        schema
            .transition_compatibility_in_family(1)
            .expect("new column family compatibility"),
        TransitionCompatibility::Compatible
    );
}

#[test]
fn test_schema_builder_family_local_column_indexing() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    builder
        .add_column(0, None, None, Some("tags".to_string()))
        .unwrap();
    builder
        .add_column(1, None, None, Some("metrics".to_string()))
        .unwrap();
    let schema = builder.commit();
    assert_eq!(schema.num_columns(), 1);
    let families = schema.column_families();
    assert_eq!(
        families,
        vec![
            (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 1usize),
            ("metrics".to_string(), 2usize),
            ("tags".to_string(), 1usize)
        ]
    );
}

#[test]
fn test_schema_builder_column_family_count_limit() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    for i in 1..MAX_COLUMN_FAMILY_COUNT {
        builder
            .add_column(0, None, None, Some(format!("cf{}", i)))
            .expect("define column family within limit");
    }
    let err = builder
        .add_column(0, None, None, Some("overflow".to_string()))
        .expect_err("overflow should fail");
    assert!(err.to_string().contains("exceeds max"));
}

#[test]
fn test_schema_public_column_family_metadata() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    builder
        .set_column_operator(
            Some("metrics".to_string()),
            0,
            Arc::new(BracketMergeOperator),
        )
        .unwrap();
    let schema = builder.commit();

    assert_eq!(
        schema.column_family_ids(),
        BTreeMap::from([("default".to_string(), 0), ("metrics".to_string(), 1),])
    );
    assert_eq!(
        schema.operator_ids_in_family("metrics").unwrap(),
        vec![BracketMergeOperator.id().to_string()]
    );
}

#[test]
fn test_schema_builder_column_family_value_ttl_option() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    builder
        .set_column_family_options(
            Some("metrics".to_string()),
            ColumnFamilyOptions {
                value_has_ttl: false,
                ..ColumnFamilyOptions::default()
            },
        )
        .unwrap();
    let schema = builder.commit();
    let metrics_cf = schema.resolve_column_family_id(Some("metrics")).unwrap();
    assert!(!schema.value_has_ttl_in_family(metrics_cf));
    assert!(schema.value_has_ttl_in_family(DEFAULT_COLUMN_FAMILY_ID));
}

#[test]
fn test_set_column_family_options_creates_missing_family() {
    let manager = Arc::new(SchemaManager::new(1));
    let mut builder = manager.builder();
    builder
        .set_column_family_options(
            Some("metrics".to_string()),
            ColumnFamilyOptions {
                value_has_ttl: false,
                ..ColumnFamilyOptions::default()
            },
        )
        .unwrap();
    let schema = builder.commit();
    let metrics_cf = schema.resolve_column_family_id(Some("metrics")).unwrap();
    assert_eq!(schema.num_columns_in_family(metrics_cf), Some(0));
    assert!(!schema.value_has_ttl_in_family(metrics_cf));
}
