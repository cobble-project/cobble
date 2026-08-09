use super::*;
use crate::runtime_manifest::{RUNTIME_MANIFEST_VERSION_CURRENT, RuntimeIncrementalManifest};

fn manifest(generation: u64) -> RuntimeManifest {
    RuntimeManifest {
        generation,
        seq_id: generation,
        topology_epoch: 0,
        latest_schema_id: 0,
        bucket_ranges: Vec::new(),
        lsm_tree_bucket_ranges: Vec::new(),
        tree_scopes: Vec::new(),
        tree_levels: Vec::new(),
        vlog_files: Vec::new(),
        truncation_cursors: Vec::new(),
    }
}

fn incremental_envelope(generation: u64, base_generation: u64) -> RuntimeManifestEnvelope {
    RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
            generation,
            base_generation,
            seq_id: generation,
            topology_epoch: 0,
            latest_schema_id: 0,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }),
    }
}

#[test]
fn continuous_incremental_publishes_advance_local_chain_depth() {
    let initial_manifest = manifest(1);
    let initial = LoadedRuntimeManifest {
        generation: 1,
        base_generation: None,
        chain_depth: 1,
        manifest: initial_manifest,
    };
    let second =
        published_manifest_state(manifest(2), &incremental_envelope(2, 1), Some(&initial)).unwrap();
    assert_eq!(second.base_generation, Some(1));
    assert_eq!(second.chain_depth, 2);

    let third =
        published_manifest_state(manifest(3), &incremental_envelope(3, 2), Some(&second)).unwrap();
    assert_eq!(third.base_generation, Some(2));
    assert_eq!(third.chain_depth, 3);
}
