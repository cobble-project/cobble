use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let ui_dir = manifest_dir.join("web-ui");
    let ui_work_dir = out_dir.join("web-ui-build");
    let dist_dir = out_dir.join("web-ui-dist");
    let package_json = ui_dir.join("package.json");
    let package_lock = ui_dir.join("package-lock.json");
    let build_version = format!(
        "v{}",
        env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string())
    );
    let build_commit = resolve_build_commit();

    println!("cargo:rerun-if-env-changed=NPM");
    println!("cargo:rerun-if-env-changed=COBBLE_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=COBBLE_WEB_MONITOR_SKIP_UI_BUILD");
    println!("cargo:rerun-if-changed={}", package_json.display());
    println!("cargo:rerun-if-changed={}", package_lock.display());
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("../.git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("index.html").display()
    );
    println!("cargo:rerun-if-changed={}", ui_dir.join("src").display());
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("vite.config.js").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("tailwind.config.js").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("postcss.config.js").display()
    );

    if env::var("COBBLE_WEB_MONITOR_SKIP_UI_BUILD").as_deref() == Ok("1") {
        write_fallback_dist(&dist_dir, &build_version, &build_commit);
        emit_ui_dist_env(&dist_dir);
        println!("cargo:warning=COBBLE_WEB_MONITOR_SKIP_UI_BUILD=1; using fallback embedded UI");
        return;
    }

    if !package_json.exists() {
        write_fallback_dist(&dist_dir, &build_version, &build_commit);
        emit_ui_dist_env(&dist_dir);
        println!("cargo:warning=web-ui/package.json not found; using fallback embedded UI");
        return;
    }

    let npm = env::var("NPM").unwrap_or_else(|_| "npm".to_string());
    prepare_ui_workspace(&ui_dir, &ui_work_dir);
    if package_lock.exists() {
        run_npm(
            &npm,
            &ui_work_dir,
            &["ci", "--no-fund", "--no-audit", "--include=dev"],
            &[],
        );
    } else {
        run_npm(
            &npm,
            &ui_work_dir,
            &["install", "--no-fund", "--no-audit", "--include=dev"],
            &[],
        );
    }
    run_npm_with_out_dir(
        &npm,
        &ui_work_dir,
        &dist_dir,
        &[
            ("VITE_COBBLE_VERSION", &build_version),
            ("VITE_COBBLE_COMMIT", &build_commit),
        ],
    );
    emit_ui_dist_env(&dist_dir);
}

fn resolve_build_commit() -> String {
    if let Ok(explicit) = env::var("COBBLE_BUILD_COMMIT")
        && !explicit.trim().is_empty()
    {
        return explicit.trim().to_string();
    }
    Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .current_dir(
            env::var("CARGO_MANIFEST_DIR")
                .ok()
                .map(PathBuf::from)
                .and_then(|path| path.parent().map(|v| v.to_path_buf()))
                .unwrap_or_else(|| PathBuf::from(".")),
        )
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn run_npm(npm: &str, cwd: &Path, args: &[&str], extra_env: &[(&str, &str)]) {
    let mut cmd = Command::new(npm);
    cmd.args(args).current_dir(cwd);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    let status = cmd.status().unwrap_or_else(|err| {
        panic!(
            "failed to execute `{}` in {}: {}. Install Node.js/npm or set NPM=<path>. \
             You can set COBBLE_WEB_MONITOR_SKIP_UI_BUILD=1 for backend-only local builds.",
            npm,
            cwd.display(),
            err
        )
    });
    assert!(
        status.success(),
        "`{} {}` failed in {} with status {}",
        npm,
        args.join(" "),
        cwd.display(),
        status
    );
}

fn run_npm_with_out_dir(npm: &str, cwd: &Path, dist_dir: &Path, extra_env: &[(&str, &str)]) {
    let dist_dir_str = dist_dir.to_string_lossy().into_owned();
    run_npm(
        npm,
        cwd,
        &["run", "build", "--", "--outDir", dist_dir_str.as_str()],
        extra_env,
    );
}

fn prepare_ui_workspace(src_dir: &Path, dst_dir: &Path) {
    if dst_dir.exists() {
        fs::remove_dir_all(dst_dir).expect("remove old web-ui build workspace");
    }
    copy_ui_tree(src_dir, dst_dir);
}

fn copy_ui_tree(src_dir: &Path, dst_dir: &Path) {
    fs::create_dir_all(dst_dir).expect("create web-ui build workspace");
    for entry in fs::read_dir(src_dir).expect("read web-ui source directory") {
        let entry = entry.expect("read web-ui source directory entry");
        let file_type = entry.file_type().expect("read web-ui source file type");
        let file_name = entry.file_name();
        if file_name == "node_modules" || file_name == "dist" {
            continue;
        }
        let src_path = entry.path();
        let dst_path = dst_dir.join(&file_name);
        if file_type.is_dir() {
            copy_ui_tree(&src_path, &dst_path);
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path).unwrap_or_else(|err| {
                panic!(
                    "copy web-ui file {} -> {} failed: {}",
                    src_path.display(),
                    dst_path.display(),
                    err
                )
            });
        }
    }
}

fn emit_ui_dist_env(dist_dir: &Path) {
    let dist_path = dist_dir.to_string_lossy().replace('\\', "/");
    println!("cargo:rustc-env=COBBLE_WEB_MONITOR_UI_DIST={dist_path}");
}

fn write_fallback_dist(dist_dir: &Path, version: &str, commit: &str) {
    fs::create_dir_all(dist_dir).expect("create fallback dist dir");
    let index_path = dist_dir.join("index.html");
    let html_template = r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Cobble Web Monitor</title>
    <style>
      body { margin: 0; font-family: ui-sans-serif, system-ui, sans-serif; background: #17120f; color: #fff; }
      main { max-width: 760px; margin: 48px auto; padding: 20px; border: 1px solid #3a2c22; border-radius: 10px; background: #211812; }
      code { background: #2b211b; border-radius: 6px; padding: 2px 6px; }
      pre { background: #2b211b; border-radius: 8px; padding: 12px; }
    </style>
  </head>
  <body>
    <main>
      <h1>Frontend build skipped</h1>
      <p>Build metadata: version={{VERSION}}, commit={{COMMIT}}</p>
      <p>Set up Node.js/npm and rebuild to enable Vue SPA:</p>
      <pre><code>cd cobble-web-monitor/web-ui
npm install
npm run build
cargo run -p cobble-cli -- web-monitor --config &lt;path&gt;</code></pre>
    </main>
  </body>
</html>
"#;
    let html = html_template
        .replace("{{VERSION}}", version)
        .replace("{{COMMIT}}", commit);
    fs::write(index_path, html).expect("write fallback dist/index.html");
}
