//! The egui front-end: join/host screens + live session panel.

use eframe::egui;

use crate::jam::{Mode, Shared, UiCmd};
use crate::player::Backend;

pub struct JamApp {
    shared: Shared,
    cmd_tx: std::sync::mpsc::Sender<UiCmd>,
    active_backend: Backend,
    code_input: String,
    name_input: String,
    gc: bool,
    add_input: String,
    drift_preset: String,
}

impl JamApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        // Windows has no MPRIS — default to the cliamp backend there
        let backend = if cfg!(windows) {
            Backend::Cliamp
        } else {
            Backend::Spotifast {
                bus_suffix: std::env::var("JAM_MPRIS").unwrap_or_else(|_| "fastpotify".into()),
            }
        };
        let (shared, cmd_tx) = crate::jam::JamCore::spawn(backend.clone(), false, running);
        let name = std::env::var("USER").unwrap_or_else(|_| "wunder".into());
        Self {
            shared,
            cmd_tx,
            active_backend: backend,
            code_input: String::new(),
            name_input: name,
            gc: true,
            add_input: String::new(),
            drift_preset: "Tight".into(),
        }
    }

    /// send a command on the current core (backend changes hot-swap via SetBackend)
    fn send_with_backend(&mut self, cmd: UiCmd) {
        let _ = self.cmd_tx.send(cmd);
    }

    fn send_set_backend(&mut self, b: Backend) {
        if self.active_backend == b {
            return;
        }
        self.active_backend = b.clone();
        let _ = self.cmd_tx.send(UiCmd::SetBackend(b));
    }
}

impl eframe::App for JamApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let snapshot = self.shared.lock().unwrap().clone();
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                let (dot, txt): (&str, String) = match snapshot.mode {
                    Mode::Idle => ("○", "idle".into()),
                    Mode::Joining => ("◌", "connecting…".into()),
                    Mode::Guest => ("●", format!("in a Jam with {}", snapshot.host_name)),
                    Mode::Hosting => ("●", format!("hosting · {}", snapshot.jam_id)),
                };
                ui.heading("Jam");
                ui.label(
                    egui::RichText::new(dot)
                        .color(if snapshot.connected {
                            egui::Color32::from_rgb(46, 204, 113)
                        } else {
                            egui::Color32::GRAY
                        })
                        .size(18.0),
                );
                ui.strong(txt);
                if snapshot.ping_ms >= 0 {
                    ui.weak(format!("{}ms", snapshot.ping_ms));
                }
                // hot-swappable player picker (works mid-session)
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.menu_button(format!("♫ {}", snapshot.backend), |ui| {
                        #[cfg(not(target_os = "windows"))]
                        {
                            let spot = Backend::Spotifast {
                                bus_suffix: std::env::var("JAM_MPRIS").unwrap_or_else(|_| "fastpotify".into()),
                            };
                            if ui.button(spot.label()).clicked() {
                                self.send_set_backend(spot);
                                ui.close_menu();
                            }
                        }
                        #[cfg(target_os = "windows")]
                        {
                            if ui.button(Backend::SpotifastWin.label()).clicked() {
                                self.send_set_backend(Backend::SpotifastWin);
                                ui.close_menu();
                            }
                        }
                        if ui.button(Backend::Cliamp.label()).clicked() {
                            self.send_set_backend(Backend::Cliamp);
                            ui.close_menu();
                        }
                        ui.weak(
                            egui::RichText::new("switches instantly —\nsessions keep running").size(10.0),
                        );
                    });
                });
            });
            if let Some(err) = &snapshot.error {
                ui.add_space(2.0);
                ui.colored_label(egui::Color32::from_rgb(231, 76, 60), format!("⚠ {err}"));
            }
            ui.add_space(4.0);
        });

        egui::TopBottomPanel::bottom("compat").show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(3.0);
                ui.label(
                    egui::RichText::new("♪ compatible with Kyzen's Spotify Jam (spicetify-jam)")
                        .weak()
                        .size(11.0),
                );
                ui.add_space(2.0);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| match snapshot.mode {
            Mode::Idle | Mode::Joining => self.idle_screen(ui, &snapshot),
            Mode::Guest => self.session_screen(ui, &snapshot),
            Mode::Hosting => self.session_screen(ui, &snapshot),
        });

        ctx.request_repaint_after(std::time::Duration::from_millis(250));
    }
}

impl JamApp {
    fn idle_screen(&mut self, ui: &mut egui::Ui, s: &crate::jam::SharedState) {
        let joining = s.mode == Mode::Joining;
        ui.add_space(10.0);
        ui.vertical_centered(|ui| {
            ui.heading("Listen together");
            ui.label(
                egui::RichText::new("play your side natively — spotifast or cliamp")
                    .weak()
                    .size(13.0),
            );
        });
        ui.add_space(12.0);

        egui::Grid::new("setup").num_columns(2).spacing([8.0, 8.0]).show(ui, |ui| {
            ui.label("Your name");
            ui.text_edit_singleline(&mut self.name_input)
                .on_hover_text("the name other listeners see.\nRecommended: your Spotify display name — that's what the real\nextension would send. Anything works though :3");
            ui.end_row();
        });

        // ---- join ----
        ui.add_space(10.0);
        ui.separator();
        ui.add_space(10.0);

        ui.horizontal(|ui| {
            let code = self.code_input.trim().to_uppercase();
            let enabled = !joining && code.len() == 6 && !self.name_input.trim().is_empty();
            if ui
                .add_enabled(enabled, egui::Button::new("Join a Jam").min_size(egui::vec2(120.0, 32.0)))
                .clicked()
            {
                self.send_with_backend(UiCmd::Join {
                    code,
                    name: self.name_input.trim().to_string(),
                });
            }
            ui.text_edit_singleline(&mut self.code_input)
                .on_hover_text("the 6-character code from the host");
        });
        ui.weak(
            egui::RichText::new("joining someone's Jam? they decide if guests can control playback")
                .size(11.0),
        );

        // ---- host ----
        ui.add_space(10.0);
        ui.separator();
        ui.add_space(10.0);

        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!joining, egui::Button::new("Host a Jam").min_size(egui::vec2(120.0, 32.0)))
                    .clicked()
                {
                    self.send_with_backend(UiCmd::Host {
                        name: self.name_input.trim().to_string(),
                        gc: self.gc,
                        code: None,
                    });
                }
                ui.label(
                    egui::RichText::new("you get a code to share").weak().size(11.0),
                );
            });
            ui.add_space(2.0);
            ui.checkbox(&mut self.gc, "let the guest control playback")
                .on_hover_text("hosting only — lets listeners use play/pause/next/seek\nand add songs to the shared queue. When you join someone\nelse's Jam, the host sets this on their side.");
        });

        ui.add_space(16.0);
        ui.separator();
        ui.add_space(6.0);
        self.log_view(ui, s, 6);
    }

    fn session_screen(&mut self, ui: &mut egui::Ui, s: &crate::jam::SharedState) {
        if s.mode == Mode::Hosting {
            ui.horizontal(|ui| {
                ui.label("Jam code");
                ui.monospace(
                    egui::RichText::new(&s.jam_id)
                        .size(26.0)
                        .color(egui::Color32::from_rgb(46, 204, 113))
                        .strong(),
                );
                if ui.small_button("📋 copy").clicked() {
                    ui.output_mut(|o| o.copied_text = s.jam_id.clone());
                }
            });
            ui.add_space(4.0);
        }

        // now playing
        ui.add_space(6.0);
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.set_width(ui.available_width());
            if let Some(np) = &s.now_playing {
                ui.horizontal(|ui| {
                    // album art over HTTP is a follow-up; tinted placeholder for now
                    let art = egui::RichText::new("♪").size(34.0).color(egui::Color32::from_rgb(46, 204, 113));
                    ui.add(egui::Label::new(art).selectable(false));
                    ui.vertical(|ui| {
                        ui.strong(&np.title);
                        ui.weak(&np.artist);
                    });
                });
            } else {
                ui.weak("nothing playing yet");
            }
            let dur = s.duration_ms.max(1.0);
            let prog = ((s.progress_ms / dur) as f32).clamp(0.0, 1.0);
            ui.add(
                egui::ProgressBar::new(prog)
                    .text(format!("{} / {}", ms_fmt(s.progress_ms), ms_fmt(s.duration_ms))),
            );
            ui.horizontal(|ui| {
                let can_control = s.mode == Mode::Hosting || (s.mode == Mode::Guest && s.gc);
                let label = if s.playing { "⏸" } else { "▶" };
                if ui.add_enabled(can_control, egui::Button::new(label).min_size(egui::vec2(36.0, 26.0))).clicked() {
                    if s.playing {
                        let _ = self.cmd_tx.send(UiCmd::Pause);
                    } else {
                        let _ = self.cmd_tx.send(UiCmd::Play);
                    }
                }
                let next = egui::Button::new("⏭");
                if ui.add_enabled(can_control, next).clicked() {
                    let _ = self.cmd_tx.send(UiCmd::Next);
                }
                if s.mode == Mode::Guest {
                    if ui.button("↻ sync").clicked() {
                        let _ = self.cmd_tx.send(UiCmd::SyncNow);
                    }
                }
            });

            // drift fine-tuning (guest side — this is where corrections happen)
            if s.mode == Mode::Guest {
                ui.add_space(2.0);
                ui.collapsing(
                    egui::RichText::new("🎚 drift correction").size(12.0),
                    |ui| {
                        ui.set_width(ui.available_width());
                        ui.label(
                            egui::RichText::new(format!(
                                "deadband {}ms · jump {}ms",
                                s.drift_deadband_ms as u64,
                                s.drift_jump_ms as u64
                            ))
                            .weak()
                            .size(10.5),
                        );
                        let presets: [(&str, bool, f64, f64, &str); 4] = [
                            ("Tight", true, 160.0, 650.0, "snaps quickly — best on solid connections"),
                            ("Normal", true, 300.0, 1200.0, "balanced"),
                            ("Relaxed", true, 600.0, 3000.0, "fewer jumps for spotty internet / slow machines"),
                            ("Manual only", false, 0.0, 0.0, "never auto-seeks — use the ↻ sync button"),
                        ];
                        for (label, enabled, deadband, jump, hint) in presets {
                            let selected = s.drift_enabled == enabled
                                && (!enabled
                                    || (s.drift_deadband_ms == deadband
                                        && s.drift_jump_ms == jump));
                            if ui.selectable_label(selected, label).clicked() {
                                self.drift_preset = label.to_string();
                                let _ = self.cmd_tx.send(UiCmd::SetDrift {
                                    enabled,
                                    deadband_ms: deadband,
                                    jump_ms: jump,
                                });
                            }
                            if selected {
                                ui.label(egui::RichText::new(hint).weak().size(10.5));
                            }
                        }
                    },
                );
            }        });

        ui.add_space(6.0);
        // members
        if !s.members.is_empty() {
            ui.collapsing(
                egui::RichText::new(format!("👥 Listeners · {}", s.members.len())).size(13.0),
                |ui| {
                    for m in &s.members {
                        ui.horizontal(|ui| {
                            ui.label(if m.is_host { "●" } else { "○" });
                            ui.label(&m.name);
                            if m.is_host {
                                ui.weak("host");
                            }
                        });
                    }
                },
            );
        }

        // queue
        if !s.queue.is_empty() {
            ui.collapsing(
                egui::RichText::new(format!("♪ Up next · {}", s.queue.len())).size(13.0),
                |ui| {
                    egui::ScrollArea::vertical().max_height(140.0).show(ui, |ui| {
                        for (i, t) in s.queue.iter().enumerate() {
                            ui.horizontal(|ui| {
                                ui.weak(format!("{}.", i + 1));
                                ui.label(&t.title);
                                ui.weak(&t.artist);
                            });
                        }
                    });
                },
            );
        }

        // guest: send a song to the host's queue
        if s.mode == Mode::Guest {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let parsed = parse_track_uri(&self.add_input);
                let can = parsed.is_some() && s.connected;
                ui.label(
                    egui::RichText::new("➕").size(13.0),
                );
                let resp = ui.add_enabled(
                    s.connected,
                    egui::TextEdit::singleline(&mut self.add_input)
                        .hint_text("paste a song link to add to the host's queue…")
                        .desired_width(ui.available_width() - 90.0),
                );
                let clicked = ui.add_enabled(can, egui::Button::new("add")).clicked()
                    || (resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) && can);
                if clicked {
                    if let Some(uri) = parsed {
                        let _ = self.cmd_tx.send(UiCmd::AddToQueue { uri });
                        self.add_input.clear();
                    }
                }
            });
        }

        // host-only live toggle: matches the extension's Session Settings
        if s.mode == Mode::Hosting {
            ui.add_space(2.0);
            let mut v = s.gc;
            if ui
                .checkbox(&mut v, "let the guest control playback")
                .on_hover_text("hosting only — lets listeners play/pause/skip and add songs.\nApplies instantly; the guest's UI updates live.")
                .changed()
            {
                let _ = self.cmd_tx.send(UiCmd::SetGc(v));
            }
        }

        ui.add_space(6.0);
        ui.vertical_centered(|ui| {
            let label = if s.mode == Mode::Hosting { "End Jam" } else { "Leave Jam" };
            if ui.button(egui::RichText::new(label).color(egui::Color32::from_rgb(231, 76, 60))).clicked() {
                let _ = self.cmd_tx.send(UiCmd::Leave);
            }
        });

        ui.add_space(8.0);
        ui.separator();
        self.log_view(ui, s, 10);
    }

    fn log_view(&self, ui: &mut egui::Ui, s: &crate::jam::SharedState, max: usize) {
        if s.logs.is_empty() {
            return;
        }
        let start = s.logs.len().saturating_sub(max);
        for line in s.logs.iter().skip(start) {
            ui.monospace(egui::RichText::new(line.as_str()).size(10.5).weak());
        }
    }
}

fn ms_fmt(ms: f64) -> String {
    let s = (ms / 1000.0) as u64;
    format!("{}:{:02}", s / 60, s % 60)
}

/// accept spotify:track:… URIs or open.spotify.com track links
fn parse_track_uri(input: &str) -> Option<String> {
    let t = input.trim();
    if t.starts_with("spotify:track:") {
        return Some(t.split('?').next()?.to_string());
    }
    // https://open.spotify.com/track/ID?...
    let idx = t.find("open.spotify.com/track/")?;
    let rest = &t[idx + "open.spotify.com/track/".len()..];
    let id = rest.split(['?', '/', '&']).next()?;
    if id.len() == 22 && id.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(format!("spotify:track:{id}"))
    } else {
        None
    }
}
