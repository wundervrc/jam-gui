mod app;
mod binarypack;
mod jam;
mod peerjs;
mod player;

use jam::UiCmd;
use player::Backend;

fn main() -> eframe::Result {
    // rustls 0.23 needs an explicit crypto provider before the first TLS handshake
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args: Vec<String> = std::env::args().collect();
    let has = |f: &str| args.iter().any(|a| a == f);
    let val = |f: &str| {
        args.iter()
            .position(|a| a == f)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    if has("--headless") {
        return run_headless(&args);
    }

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([420.0, 640.0])
            .with_min_inner_size([360.0, 520.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Jam",
        native_options,
        Box::new(|cc| Ok(Box::new(app::JamApp::new(cc)))),
    )
}

fn run_headless(args: &[String]) -> eframe::Result {
    let has = |f: &str| args.iter().any(|a| a == f);
    let val = |f: &str| {
        args.iter()
            .position(|a| a == f)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let dry_run = has("--dry-run");
    let bus_suffix = val("--mpris")
        .or_else(|| std::env::var("JAM_MPRIS").ok())
        .unwrap_or_else(|| "fastpotify".into());
    let backend = match val("--backend").as_deref() {
        Some("cliamp") => Backend::Cliamp,
        Some(other) => Backend::Spotifast { bus_suffix: other.into() },
        None => Backend::Spotifast { bus_suffix },
    };
    let name = val("--name")
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "jam".into());

    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (shared, cmd_tx) = jam::JamCore::spawn(backend, dry_run, running.clone());

    let host = has("--host");
    let gc = has("--gc");
    let code = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--") && a.len() <= 8 && a.chars().all(|c| c.is_ascii_alphanumeric()))
        .cloned()
        .unwrap_or_default();

    if host {
        cmd_tx
            .send(UiCmd::Host { name, gc, code: if code.is_empty() { None } else { Some(code) } })
            .expect("core alive");
    } else if code.is_empty() {
        eprintln!("usage: jam-gui --headless <CODE> [--backend spotifast|cliamp] [--dry-run] | jam-gui --headless --host [--gc]");
        std::process::exit(1);
    } else {
        cmd_tx.send(UiCmd::Join { code, name }).expect("core alive");
    }

    let mut printed = 0usize;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(200));
        if let Ok(s) = shared.lock() {
            while printed < s.logs.len() {
                println!("{}", s.logs[printed]);
                printed += 1;
            }
        }
    }
}
