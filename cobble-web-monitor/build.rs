use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let ui_dir = manifest_dir.join("web-ui");
    let dist_dir = ui_dir.join("dist");
    let package_json = ui_dir.join("package.json");
    let package_lock = ui_dir.join("package-lock.json");

    println!("cargo:rerun-if-env-changed=NPM");
    println!("cargo:rerun-if-env-changed=COBBLE_WEB_MONITOR_SKIP_UI_BUILD");
    println!("cargo:rerun-if-changed={}", package_json.display());
    println!("cargo:rerun-if-changed={}", package_lock.display());
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
        write_fallback_dist(&dist_dir);
        println!("cargo:warning=COBBLE_WEB_MONITOR_SKIP_UI_BUILD=1; using fallback embedded UI");
        return;
    }

    if !package_json.exists() {
        write_fallback_dist(&dist_dir);
        println!("cargo:warning=web-ui/package.json not found; using fallback embedded UI");
        return;
    }

    let npm = env::var("NPM").unwrap_or_else(|_| "npm".to_string());
    if package_lock.exists() {
        run_npm(
            &npm,
            &ui_dir,
            &["ci", "--no-fund", "--no-audit", "--include=dev"],
        );
    } else {
        run_npm(
            &npm,
            &ui_dir,
            &["install", "--no-fund", "--no-audit", "--include=dev"],
        );
    }
    run_npm(&npm, &ui_dir, &["run", "build"]);
}

fn run_npm(npm: &str, cwd: &Path, args: &[&str]) {
    let status = Command::new(npm)
        .args(args)
        .current_dir(cwd)
        .status()
        .unwrap_or_else(|err| {
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

fn write_fallback_dist(dist_dir: &Path) {
    fs::create_dir_all(dist_dir).expect("create fallback dist dir");
    let index_path = dist_dir.join("index.html");
    let html = r#"<!doctype html>
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
      <p>Set up Node.js/npm and rebuild to enable Vue SPA:</p>
      <pre><code>cd cobble-web-monitor/web-ui
npm install
npm run build
cargo run -p cobble-cli -- web-monitor --config &lt;path&gt;</code></pre>
    </main>
  </body>
</html>
"#;
    fs::write(index_path, html).expect("write fallback dist/index.html");
}
