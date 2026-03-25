use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-env-changed=COBBLE_BUILD_COMMIT");

    if let Ok(explicit) = std::env::var("COBBLE_BUILD_COMMIT")
        && !explicit.trim().is_empty()
    {
        println!(
            "cargo:rustc-env=COBBLE_BUILD_COMMIT_GENERATED={}",
            explicit.trim()
        );
        return;
    }

    let short = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=COBBLE_BUILD_COMMIT_GENERATED={short}");
}
