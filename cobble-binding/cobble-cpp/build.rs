fn main() {
    cxx_build::bridges(["src/lib.rs", "src/structured_bridge.rs"])
        .std("c++20")
        .compile("cobble-cpp-bridge");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/structured_bridge.rs");
    println!("cargo:rerun-if-changed=src/structured/mod.rs");
    for source in [
        "src/structured/conversion.rs",
        "src/structured/database.rs",
        "src/structured/encoding.rs",
        "src/structured/lifecycle.rs",
        "src/structured/multi_get.rs",
        "src/structured/options.rs",
        "src/structured/row.rs",
        "src/structured/scan.rs",
        "src/structured/scan_plan.rs",
        "src/structured/schema.rs",
        "src/structured/single_db.rs",
        "src/structured/types.rs",
        "src/structured/write_batch.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
    for source in [
        "src/database.rs",
        "src/coordinator.rs",
        "src/distributed_scan.rs",
        "src/encoding.rs",
        "src/error.rs",
        "src/lifecycle.rs",
        "src/metrics.rs",
        "src/multi_get.rs",
        "src/options.rs",
        "src/read_only_db.rs",
        "src/reader.rs",
        "src/scan.rs",
        "src/schema.rs",
        "src/sharded_db.rs",
        "src/snapshot.rs",
        "src/write_batch.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
}
