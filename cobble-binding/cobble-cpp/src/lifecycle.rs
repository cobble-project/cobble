use crate::{
    BridgeResult, database::NativeDatabase, error::format_cobble_error, error::input_error,
};

pub(crate) fn native_database_now_seconds(db: &NativeDatabase) -> u32 {
    db.db.now_seconds()
}

pub(crate) fn native_database_switch_memtable_type(
    db: &NativeDatabase,
    kind: u8,
    flush_current: bool,
) -> BridgeResult<()> {
    let memtable_type = match kind {
        0 => cobble_binding::MemtableType::Hash,
        1 => cobble_binding::MemtableType::Skiplist,
        2 => cobble_binding::MemtableType::Vec,
        3 => cobble_binding::MemtableType::Adaptive,
        _ => return Err(input_error("unknown memtable type")),
    };
    db.db
        .switch_memtable_type(memtable_type, flush_current)
        .map_err(format_cobble_error)
}

pub(crate) fn native_database_load_readonly_files_to_primary(
    db: &NativeDatabase,
) -> BridgeResult<u64> {
    db.db
        .load_readonly_files_to_primary()
        .and_then(|count| {
            u64::try_from(count).map_err(|_| {
                cobble_binding::Error::InvalidState(
                    "readonly file count does not fit in u64".to_string(),
                )
            })
        })
        .map_err(format_cobble_error)
}
