fn main() {
    cxx_build::bridge("src/lib.rs")
        .std("c++20")
        .compile("cobble-cpp-bridge");
    println!("cargo:rerun-if-changed=src/lib.rs");
    for source in [
        "src/database.rs",
        "src/encoding.rs",
        "src/error.rs",
        "src/options.rs",
        "src/scan.rs",
        "src/write_batch.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
}
