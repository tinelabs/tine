use std::fs;
use std::path::{Path, PathBuf};

fn sync_png(source_path: &Path, output_path: &Path) {
    let source = fs::read(source_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", source_path.display(), e));

    let needs_write = match fs::read(output_path) {
        Ok(existing) => existing != source,
        Err(_) => true,
    };
    if needs_write {
        fs::write(output_path, source).unwrap_or_else(|e| {
            panic!("failed to write {}: {}", output_path.display(), e)
        });
    }
}

fn main() {
    // Regenerate tauri.conf.json from the template so the bundle version
    // always matches the workspace version (Cargo.toml → CARGO_PKG_VERSION).
    //
    // Single source of truth: `version.workspace = true` in Cargo.toml.
    // tauri.conf.json is a build artifact and must not be edited by hand.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let template_path = manifest_dir.join("tauri.conf.template.json");
    let config_path = manifest_dir.join("tauri.conf.json");
    let runtime_dir = manifest_dir.join("resources").join("runtime");
    let icons_dir = manifest_dir.join("icons");
    let source_png_path = icons_dir.join("128x128@2x.png");
    let icns_icon_path = icons_dir.join("icon.icns");
    let png_icon_path = icons_dir.join("icon.png");
    let ico_icon_path = icons_dir.join("icon.ico");

    fs::create_dir_all(&runtime_dir).unwrap_or_else(|e| {
        panic!(
            "failed to create {}: {}",
            runtime_dir.display(),
            e
        )
    });

    sync_png(&source_png_path, &png_icon_path);
    if !icns_icon_path.exists() {
        panic!("expected macOS icon at {}", icns_icon_path.display());
    }
    if !ico_icon_path.exists() {
        panic!("expected Windows icon at {}", ico_icon_path.display());
    }

    let version = env!("CARGO_PKG_VERSION");
    let template = fs::read_to_string(&template_path).unwrap_or_else(|e| {
        panic!(
            "failed to read {}: {}",
            template_path.display(),
            e
        )
    });
    let rendered = template.replace("{{VERSION}}", version);

    // Avoid unnecessary writes so tauri-build does not see a spurious change.
    let needs_write = match fs::read_to_string(&config_path) {
        Ok(existing) => existing != rendered,
        Err(_) => true,
    };
    if needs_write {
        fs::write(&config_path, rendered).unwrap_or_else(|e| {
            panic!("failed to write {}: {}", config_path.display(), e)
        });
    }

    println!("cargo:rerun-if-changed=tauri.conf.template.json");
    println!("cargo:rerun-if-changed={}", source_png_path.display());
    println!("cargo:rerun-if-env-changed=CARGO_PKG_VERSION");

    tauri_build::build()
}
