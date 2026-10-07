use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::time::{Duration, Instant};

use base64::Engine as _;
use eframe::egui;
use serde::{Deserialize, Serialize};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
const ATTACH_MAX_B64: usize = 4_000_000;

// ---------- Protocol ----------
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReplyRef {
    id: i64,
    from: String,
    text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reaction {
    emoji: String,
    count: i64,
    mine: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NetMessage {
    id: i64,
    chat_id: i64,
    from: String,
    text: String,
    ts: i64,
    #[serde(default)]
    reply_to: Option<ReplyRef>,
    #[serde(default)]
    forward_from: Option<String>,
    #[serde(default)]
    edited: bool,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    reactions: Vec<Reaction>,
    #[serde(default)]
    attach: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct Chat {
    id: i64,
    name: String,
    #[serde(default)]
    is_dm: bool,
    last: Option<String>,
    #[serde(default)]
    unread: i64,
    #[serde(default)]
    online: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AuthResp {
    token: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum NetEvent {
    #[serde(rename = "message")]
    Message(NetMessage),
    #[serde(rename = "typing")]
    Typing { chat_id: i64, from: String },
    #[serde(rename = "read")]
    Read { chat_id: i64, by: String, msg_id: i64 },
    #[serde(rename = "presence")]
    Presence { user: String, online: bool },
}

#[derive(Debug, Deserialize)]
struct MessagesResp {
    messages: Vec<NetMessage>,
    #[serde(default)]
    other_read: i64,
    #[serde(default)]
    pinned: Option<NetMessage>,
}

#[derive(Clone, Debug, Deserialize)]
struct Contact {
    username: String,
    #[serde(default)]
    online: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct AvatarDto {
    username: String,
    data: String,
}

// ---------- Session file (auto-login) ----------
fn session_path() -> std::path::PathBuf {
    let dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|q| q.to_path_buf()))
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    dir.join("my_messenger_session.txt")
}

fn save_session_local(server: &str, username: &str, token: &str) {
    let _ = std::fs::write(session_path(), format!("{}\n{}\n{}", server, username, token));
}

fn load_session_local() -> Option<(String, String, String)> {
    let data = std::fs::read_to_string(session_path()).ok()?;
    let mut l = data.lines();
    let s = l.next()?.to_string();
    let u = l.next()?.to_string();
    let t = l.next()?.to_string();
    if t.is_empty() { None } else { Some((s, u, t)) }
}

fn clear_session_local() {
    let _ = std::fs::remove_file(session_path());
}

// ---------- HTTP ----------
fn request_json<Req: Serialize, Res: serde::de::DeserializeOwned + Send + 'static>(
    ctx: &egui::Context,
    method: &str,
    url: String,
    token: Option<String>,
    body: Option<&Req>,
) -> Receiver<Result<Res, String>> {
    let (tx, rx) = channel();
    let mut req = ehttp::Request::get(url);
    req.method = method.to_string();
    if let Some(b) = body {
        req.body = serde_json::to_vec(b).unwrap_or_default();
    }
    req.headers.headers.push(("Content-Type".into(), "application/json".into()));
    if let Some(t) = token {
        req.headers.headers.push(("Authorization".into(), format!("Bearer {}", t)));
    }
    let ctx = ctx.clone();
    ehttp::fetch(req, move |res| {
        let out = res.map_err(|e| e.to_string()).and_then(|r| {
            if r.ok {
                serde_json::from_slice(&r.bytes).map_err(|e| e.to_string())
            } else {
                Err(String::from_utf8_lossy(&r.bytes).to_string())
            }
        });
        let _ = tx.send(out);
        ctx.request_repaint();
    });
    rx
}

// ---------- Helpers ----------
fn hsv_to_rgb(h: f32, s: f32, v: f32) -> egui::Color32 {
    let h = h.rem_euclid(360.0) / 60.0;
    let i = h.floor();
    let f = h - i;
    let p = v * (1.0 - s);
    let q = v * (1.0 - s * f);
    let t = v * (1.0 - s * (1.0 - f));
    let (r, g, b) = match (i as i32).rem_euclid(6) {
        0 => (v, t, p),
        1 => (q, v, p),
        2 => (p, v, t),
        3 => (p, q, v),
        4 => (t, p, v),
        _ => (v, p, q),
    };
    egui::Color32::from_rgb((r * 255.0) as u8, (g * 255.0) as u8, (b * 255.0) as u8)
}

fn text_on(c: egui::Color32) -> egui::Color32 {
    let lum = 0.299 * c.r() as f32 + 0.587 * c.g() as f32 + 0.114 * c.b() as f32;
    if lum > 140.0 { egui::Color32::BLACK } else { egui::Color32::WHITE }
}

const MONTHS_RU: [&str; 12] = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
];

fn days_to_ymd(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn day_label(ts: i64, today_days: i64) -> String {
    let d = ts.div_euclid(86_400);
    if d == today_days {
        "Сегодня".into()
    } else if d == today_days - 1 {
        "Вчера".into()
    } else {
        let (y, m, dd) = days_to_ymd(d);
        format!("{} {} {}", dd, MONTHS_RU[(m - 1) as usize], y)
    }
}

fn name_hue(name: &str) -> f32 {
    let h = name.bytes().fold(0u32, |a, b| a.wrapping_mul(31).wrapping_add(b as u32));
    (h % 360) as f32
}

fn time_str(ts: i64) -> String {
    let s = ts.rem_euclid(86_400);
    format!("{:02}:{:02}", s / 3_600, (s % 3_600) / 60)
}

fn paint_ticks(p: &egui::Painter, rect: egui::Rect, col: egui::Color32, double: bool) {
    let h = rect.height().min(8.0);
    let w = h * 0.9;
    let y = rect.center().y;
    let x0 = rect.left();
    let n = if double { 2 } else { 1 };
    for k in 0..n {
        let x = x0 + k as f32 * (w * 0.65);
        p.line_segment([egui::pos2(x, y), egui::pos2(x + w * 0.35, y + h * 0.4)], egui::Stroke::new(1.4_f32, col));
        p.line_segment([egui::pos2(x + w * 0.35, y + h * 0.4), egui::pos2(x + w, y - h * 0.4)], egui::Stroke::new(1.4_f32, col));
    }
}

fn icon_menu(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(3.0_f32, col);
    for i in 0..4 {
        let y = c.y - r * 0.6 + i as f32 * (r * 0.4);
        p.line_segment([egui::pos2(c.x - r * 0.75, y), egui::pos2(c.x + r * 0.75, y)], s);
    }
}

fn icon_chats(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(2.0_f32, col);
    let a = egui::Rect::from_center_size(c + egui::vec2(-r * 0.25, -r * 0.2), egui::vec2(r * 1.3, r * 0.95));
    p.rect_stroke(a, 6.0_f32, s);
    let b = egui::Rect::from_center_size(c + egui::vec2(r * 0.35, r * 0.3), egui::vec2(r * 1.1, r * 0.8));
    p.rect_stroke(b, 6.0_f32, s);
}

fn icon_folder(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let pts = vec![
        c + egui::vec2(-r * 0.75, -r * 0.55),
        c + egui::vec2(-r * 0.15, -r * 0.55),
        c + egui::vec2(r * 0.1, -r * 0.25),
        c + egui::vec2(r * 0.75, -r * 0.25),
        c + egui::vec2(r * 0.75, r * 0.55),
        c + egui::vec2(-r * 0.75, r * 0.55),
    ];
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: pts,
        closed: true,
        fill: egui::Color32::TRANSPARENT,
        stroke: egui::Stroke::new(2.0_f32, col).into(),
    }));
}

fn icon_search(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(2.2_f32, col);
    p.circle_stroke(c + egui::vec2(-r * 0.15, -r * 0.15), r * 0.5, s);
    p.line_segment([c + egui::vec2(r * 0.2, r * 0.2), c + egui::vec2(r * 0.7, r * 0.7)], s);
}

fn icon_bookmark(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let pts = vec![
        c + egui::vec2(-r * 0.45, -r * 0.7),
        c + egui::vec2(r * 0.45, -r * 0.7),
        c + egui::vec2(r * 0.45, r * 0.7),
        c + egui::vec2(0.0, r * 0.25),
        c + egui::vec2(-r * 0.45, r * 0.7),
    ];
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: pts,
        closed: true,
        fill: egui::Color32::TRANSPARENT,
        stroke: egui::Stroke::new(2.2_f32, col).into(),
    }));
}

fn icon_phone(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let pts = vec![
        c + egui::vec2(-r * 0.7, -r * 0.25),
        c + egui::vec2(-r * 0.45, -r * 0.7),
        c + egui::vec2(-r * 0.1, -r * 0.5),
        c + egui::vec2(-r * 0.2, -r * 0.15),
        c + egui::vec2(r * 0.15, r * 0.2),
        c + egui::vec2(r * 0.5, r * 0.1),
        c + egui::vec2(r * 0.7, r * 0.45),
        c + egui::vec2(r * 0.25, r * 0.7),
        c + egui::vec2(-r * 0.3, r * 0.35),
        c + egui::vec2(-r * 0.65, 0.0),
    ];
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: pts,
        closed: true,
        fill: col,
        stroke: egui::Stroke::NONE.into(),
    }));
}

fn icon_clip(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(2.2_f32, col);
    let pts = vec![
        c + egui::vec2(r * 0.25, -r * 0.55),
        c + egui::vec2(-r * 0.35, r * 0.05),
        c + egui::vec2(-r * 0.1, r * 0.35),
        c + egui::vec2(r * 0.45, -r * 0.2),
        c + egui::vec2(r * 0.1, -r * 0.6),
        c + egui::vec2(-r * 0.6, r * 0.1),
        c + egui::vec2(-r * 0.2, r * 0.6),
        c + egui::vec2(r * 0.6, -r * 0.15),
    ];
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: pts,
        closed: false,
        fill: egui::Color32::TRANSPARENT,
        stroke: s.into(),
    }));
}

fn icon_smile(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(2.0_f32, col);
    p.circle_stroke(c, r * 0.7, s);
    p.circle_filled(c + egui::vec2(-r * 0.25, -r * 0.2), 1.8, col);
    p.circle_filled(c + egui::vec2(r * 0.25, -r * 0.2), 1.8, col);
    let smile: Vec<egui::Pos2> = (-3..=3)
        .map(|i| {
            let x = i as f32 / 3.0;
            c + egui::vec2(x * r * 0.35, r * 0.15 + x * x * r * 0.25)
        })
        .collect();
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: smile,
        closed: false,
        fill: egui::Color32::TRANSPARENT,
        stroke: s.into(),
    }));
}

fn icon_mic(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let cap = egui::Rect::from_center_size(c + egui::vec2(0.0, -r * 0.25), egui::vec2(r * 0.5, r * 0.9));
    p.rect_filled(cap, r * 0.25, col);
    let arc: Vec<egui::Pos2> = (-4..=4)
        .map(|i| {
            let a = std::f32::consts::PI * (0.15 + 0.7 * (i as f32 + 4.0) / 8.0);
            c + egui::vec2(a.cos() * r * 0.65, a.sin() * r * 0.65 - r * 0.1)
        })
        .collect();
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: arc,
        closed: false,
        fill: egui::Color32::TRANSPARENT,
        stroke: egui::Stroke::new(2.0_f32, col).into(),
    }));
    p.line_segment([c + egui::vec2(0.0, r * 0.55), c + egui::vec2(0.0, r * 0.75)], egui::Stroke::new(2.0_f32, col));
    p.line_segment([c + egui::vec2(-r * 0.3, r * 0.75), c + egui::vec2(r * 0.3, r * 0.75)], egui::Stroke::new(2.0_f32, col));
}

fn icon_send(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let pts = vec![
        c + egui::vec2(-r * 0.35, -r * 0.5),
        c + egui::vec2(-r * 0.35, r * 0.5),
        c + egui::vec2(r * 0.6, 0.0),
    ];
    p.add(egui::Shape::Path(egui::epaint::PathShape {
        points: pts,
        closed: true,
        fill: col,
        stroke: egui::Stroke::NONE.into(),
    }));
}

fn icon_check(p: &egui::Painter, c: egui::Pos2, r: f32, col: egui::Color32) {
    let s = egui::Stroke::new(2.6_f32, col);
    p.line_segment([c + egui::vec2(-r * 0.5, 0.0), c + egui::vec2(-r * 0.1, r * 0.4)], s);
    p.line_segment([c + egui::vec2(-r * 0.1, r * 0.4), c + egui::vec2(r * 0.55, -r * 0.4)], s);
}

const EMOJIS: [&str; 40] = [
    "😀", "😂", "😍", "😎", "🤔", "😢", "😡", "👍",
    "👎", "", "👏", "🔥", "✨", "💪", "❤️", "💔",
    "⭐", "💯", "🤝", "✌️", "🥳", "🤯", "😱", "🙄",
    "😇", "🥺", "💩", "🍕", "", "🎮", "🎵", "🚀",
    "🌙", "☀️", "🐱", "🐶", "🌸", "🌈", "", "🔔",
];

// ---------- Images ----------
fn decode_avatar(data: &str) -> Option<egui::ColorImage> {
    let bytes = B64.decode(data).ok()?;
    let dyn_img = image::load_from_memory(&bytes).ok()?;
    let rgba = image::imageops::resize(&dyn_img, 128, 128, image::imageops::FilterType::Triangle);
    let size = [rgba.width() as usize, rgba.height() as usize];
    Some(egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw()))
}

fn encode_avatar(path: &std::path::Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let img = image::load_from_memory(&bytes).ok()?;
    let img = image::imageops::resize(&img, 128, 128, image::imageops::FilterType::Triangle);
    let mut buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut buf, image::ImageFormat::Png).ok()?;
    Some(B64.encode(buf.into_inner()))
}

fn decode_image(data: &str) -> Option<egui::ColorImage> {
    let bytes = B64.decode(data).ok()?;
    let dyn_img = image::load_from_memory(&bytes).ok()?;
    let rgba = dyn_img.to_rgba8();
    let size = [rgba.width() as usize, rgba.height() as usize];
    Some(egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw()))
}

fn encode_attachment(path: &std::path::Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let img = image::load_from_memory(&bytes).ok()?;
    let img = img.resize(1280, 1280, image::imageops::FilterType::Triangle).to_rgb8();
    let mut buf = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(img)
        .write_to(&mut buf, image::ImageFormat::Jpeg)
        .ok()?;
    Some(B64.encode(buf.into_inner()))
}

fn image_aspect(img: &egui::ColorImage) -> f32 {
    (img.size[0] as f32 / (img.size[1] as f32).max(1.0)).clamp(0.2, 5.0)
}

// ---------- App ----------
#[derive(Clone, Copy, PartialEq, Default)]
enum Screen {
    #[default]
    Auth,
    Main,
}

#[derive(Clone, Copy, PartialEq, Default)]
enum AuthMode {
    #[default]
    Welcome,
    Register,
}

#[derive(Clone, Copy, PartialEq)]
enum LeftTab {
    Chats,
    Contacts,
}

enum CtxAction {
    Reply(NetMessage),
    Edit(NetMessage),
    Delete(i64),
}

struct MyMessenger {
    screen: Screen,
    server: String,
    username: String,
    password: String,
    email: String,
    auth_mode: AuthMode,
    error: String,
    error_shown: String,
    error_at: Option<Instant>,
    offline: bool,
    info: String,
    info_shown: String,
    info_at: Option<Instant>,
    token: Option<String>,
    my_name: String,
    rt: Option<tokio::runtime::Runtime>,
    auth_rx: Option<Receiver<Result<AuthResp, String>>>,
    dm_rx: Option<Receiver<Result<serde_json::Value, String>>>,
    chats_rx: Option<Receiver<Result<Vec<Chat>, String>>>,
    msg_rx: Option<Receiver<Result<MessagesResp, String>>>,
    ack_rx: Option<Receiver<Result<serde_json::Value, String>>>,
    send_rx: Option<Receiver<Result<NetMessage, String>>>,
    contacts_rx: Option<Receiver<Result<Vec<Contact>, String>>>,
    avatars_rx: Option<Receiver<Result<Vec<AvatarDto>, String>>>,
    need_chats: bool,
    need_contacts: bool,
    need_avatars: bool,
    chats: Vec<Chat>,
    contacts: Vec<Contact>,
    open_chat: Option<i64>,
    open_name: String,
    open_is_dm: bool,
    messages: Vec<NetMessage>,
    other_read: i64,
    pinned: Option<NetMessage>,
    draft: String,
    reply_to: Option<ReplyRef>,
    editing: Option<i64>,
    dm_win: bool,
    dm_name: String,
    group_open: bool,
    group_name: String,
    group_members: String,
    pending_chat_id: Option<i64>,
    forward_pending: Option<(String, String)>,
    ws_send: Option<ewebsock::WsSender>,
    ws_recv: Option<ewebsock::WsReceiver>,
    ws_status: String,
    last_ws_attempt: Instant,
    ws_backoff: Duration,
    pending_send_text: Option<String>,
    online: HashMap<String, bool>,
    typing: Option<(i64, String, Instant)>,
    last_typing_sent: Instant,
    last_chat_poll: Instant,
    avatar_data: HashMap<String, String>,
    avatar_tex: HashMap<String, egui::TextureHandle>,
    pending_attach: Option<String>,
    attach_preview: Option<(egui::TextureHandle, f32)>,
    attach_tex: HashMap<i64, (egui::TextureHandle, f32)>,
    viewer_open: bool,
    viewer_tex: Option<egui::TextureHandle>,
    viewer_zoom: f32,
    viewer_pan: egui::Vec2,
    dark: bool,
    hue: f32,
    sat: f32,
    val: f32,
    left_tab: LeftTab,
    favorites: Vec<i64>,
    show_favorites: bool,
    search_open: bool,
    search_q: String,
    chat_search_open: bool,
    chat_search: String,
    emoji_open: bool,
    menu_open: bool,
}

impl MyMessenger {
    fn accent(&self) -> egui::Color32 {
        hsv_to_rgb(self.hue, self.sat, self.val)
    }
    fn base_url(&self) -> String {
        self.server.trim_end_matches('/').to_string()
    }
    fn ws_url(&self) -> String {
        format!("{}/ws?token={}", self.base_url().replacen("http", "ws", 1), self.token.as_deref().unwrap_or(""))
    }
    fn notice(&mut self, t: &str) {
        self.info = t.to_string();
        self.info_shown.clear();
        self.info_at = None;
    }
    fn handle_error(&mut self, e: String) {
        if e.contains("Connection Failed") || e.contains("Connect error") || e.contains("os error") || e.contains("error sending request") {
            self.error.clear();
            self.error_shown.clear();
            self.offline = true;
        } else if e.contains("unauthorized") {
            self.token = None;
            self.ws_send = None;
            self.ws_recv = None;
            self.chats.clear();
            self.messages.clear();
            self.open_chat = None;
            self.screen = Screen::Auth;
            self.error = "Сессия истекла. Войдите снова.".into();
            self.error_shown.clear();
            self.error_at = None;
            clear_session_local();
        } else {
            self.error = e;
            self.error_shown.clear();
            self.error_at = None;
        }
    }
    fn pal_tile(&self) -> egui::Color32 {
        if self.dark { egui::Color32::from_gray(150) } else { egui::Color32::from_gray(215) }
    }
    fn pal_tile_hover(&self) -> egui::Color32 {
        if self.dark { egui::Color32::from_gray(175) } else { egui::Color32::from_gray(200) }
    }
    fn pal_icon(&self) -> egui::Color32 {
        if self.dark { egui::Color32::BLACK } else { egui::Color32::from_gray(40) }
    }
    fn apply_theme(&self, ctx: &egui::Context) {
        let mut style = (*ctx.style()).clone();
        let accent = self.accent();
        let mut v = if self.dark { egui::Visuals::dark() } else { egui::Visuals::light() };
        v.override_text_color = Some(if self.dark { egui::Color32::from_rgb(235, 235, 240) } else { egui::Color32::from_rgb(35, 35, 40) });
        v.selection.bg_fill = accent;
        v.hyperlink_color = accent;
        if self.dark {
            v.panel_fill = egui::Color32::from_gray(43);
            v.window_fill = egui::Color32::from_gray(100);
            v.extreme_bg_color = egui::Color32::from_gray(70);
            v.faint_bg_color = egui::Color32::from_gray(150);
        } else {
            v.panel_fill = egui::Color32::from_gray(235);
            v.window_fill = egui::Color32::from_gray(250);
            v.extreme_bg_color = egui::Color32::WHITE;
            v.faint_bg_color = egui::Color32::from_gray(215);
        }
        v.window_stroke = egui::Stroke::NONE;
        v.widgets.noninteractive.bg_stroke = egui::Stroke::NONE;
        v.widgets.inactive.bg_stroke = egui::Stroke::NONE;
        v.widgets.inactive.weak_bg_fill = self.pal_tile();
        v.widgets.inactive.rounding = 10.0_f32.into();
        v.widgets.hovered.bg_fill = self.pal_tile_hover();
        v.widgets.hovered.bg_stroke = egui::Stroke::NONE;
        v.widgets.hovered.rounding = 10.0_f32.into();
        v.widgets.active.bg_fill = accent;
        v.widgets.active.bg_stroke = egui::Stroke::NONE;
        v.widgets.active.rounding = 10.0_f32.into();
        style.visuals = v;
        use egui::TextStyle::*;
        style.text_styles.insert(Body, egui::FontId::proportional(14.0));
        style.text_styles.insert(Button, egui::FontId::proportional(14.0));
        style.text_styles.insert(Small, egui::FontId::proportional(11.0));
        style.text_styles.insert(Heading, egui::FontId::proportional(18.0));
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        style.spacing.window_margin = egui::Margin::same(14.0);
        style.spacing.button_padding = egui::vec2(12.0, 6.0);
        ctx.set_style(style);
    }

    // ---------- network ----------
    fn do_auth(&mut self, ctx: &egui::Context, endpoint: &str) {
        self.error.clear();
        #[derive(Serialize)]
        struct AuthReq {
            username: String,
            password: String,
        }
        self.auth_rx = Some(request_json::<AuthReq, AuthResp>(
            ctx,
            "POST",
            format!("{}/api/{}", self.base_url(), endpoint),
            None,
            Some(&AuthReq { username: self.username.trim().to_string(), password: self.password.clone() }),
        ));
    }
    fn fetch_chats(&mut self, ctx: &egui::Context) {
        if self.chats_rx.is_none() {
            self.chats_rx = Some(request_json::<serde_json::Value, Vec<Chat>>(
                ctx, "GET", format!("{}/api/chats", self.base_url()), self.token.clone(), None::<&serde_json::Value>));
        }
    }
    fn fetch_contacts(&mut self, ctx: &egui::Context) {
        if self.contacts_rx.is_none() {
            self.contacts_rx = Some(request_json::<serde_json::Value, Vec<Contact>>(
                ctx, "GET", format!("{}/api/users", self.base_url()), self.token.clone(), None::<&serde_json::Value>));
        }
    }
    fn fetch_avatars(&mut self, ctx: &egui::Context) {
        if self.avatars_rx.is_none() {
            self.avatars_rx = Some(request_json::<serde_json::Value, Vec<AvatarDto>>(
                ctx, "GET", format!("{}/api/avatars", self.base_url()), self.token.clone(), None::<&serde_json::Value>));
        }
    }
    fn fetch_messages(&mut self, ctx: &egui::Context, chat_id: i64) {
        self.msg_rx = Some(request_json::<serde_json::Value, MessagesResp>(
            ctx, "GET", format!("{}/api/chats/{}/messages", self.base_url(), chat_id), self.token.clone(), None::<&serde_json::Value>));
    }
    fn open_chat(&mut self, ctx: &egui::Context, id: i64, name: String, is_dm: bool) {
        self.open_chat = Some(id);
        self.open_name = name;
        self.open_is_dm = is_dm;
        self.messages.clear();
        self.other_read = 0;
        self.pinned = None;
        self.reply_to = None;
        self.editing = None;
        self.typing = None;
        self.chat_search.clear();
        self.chat_search_open = false;
        self.attach_tex.clear();
        self.pending_attach = None;
        self.attach_preview = None;
        self.fetch_messages(ctx, id);
    }
    fn mark_read(&mut self, ctx: &egui::Context, msg_id: i64) {
        if let Some(cid) = self.open_chat {
            #[derive(Serialize)]
            struct ReadReq {
                msg_id: i64,
            }
            self.ack_rx = Some(request_json::<ReadReq, serde_json::Value>(
                ctx, "POST", format!("{}/api/chats/{}/read", self.base_url(), cid), self.token.clone(), Some(&ReadReq { msg_id })));
        }
    }
    fn send_current(&mut self, ctx: &egui::Context) {
        let Some(cid) = self.open_chat else { return };
        if self.send_rx.is_some() { return; }
        let text = std::mem::take(&mut self.draft);
        if let Some(mid) = self.editing.take() {
            if text.trim().is_empty() {
                return;
            }
            #[derive(Serialize)]
            struct EditReq {
                text: String,
            }
            self.ack_rx = Some(request_json::<EditReq, serde_json::Value>(
                ctx, "PATCH", format!("{}/api/chats/{}/messages/{}", self.base_url(), cid, mid), self.token.clone(), Some(&EditReq { text })));
            return;
        }
        let attach = self.pending_attach.clone();
        if text.trim().is_empty() && attach.is_none() {
            return;
        }
        #[derive(Serialize)]
        struct SendReq {
            text: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            reply_to: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            attach: Option<String>,
        }
        self.pending_send_text = Some(text.clone());
        self.send_rx = Some(request_json::<SendReq, NetMessage>(
            ctx,
            "POST",
            format!("{}/api/chats/{}/messages", self.base_url(), cid),
            self.token.clone(),
            Some(&SendReq { text, reply_to: self.reply_to.take().map(|r| r.id), attach }),
        ));
        self.typing = None;
    }
    fn send_forward(&mut self, ctx: &egui::Context, cid: i64, text: String, from: String) {
        #[derive(Serialize)]
        struct SendReq {
            text: String,
            reply_to: Option<i64>,
            forward_from: String,
        }
        self.ack_rx = Some(request_json::<SendReq, serde_json::Value>(
            ctx,
            "POST",
            format!("{}/api/chats/{}/messages", self.base_url(), cid),
            self.token.clone(),
            Some(&SendReq { text, reply_to: None, forward_from: from }),
        ));
        self.need_chats = true;
    }
    fn toggle_reaction(&mut self, ctx: &egui::Context, mid: i64, emoji: String) {
        if let Some(cid) = self.open_chat {
            #[derive(Serialize)]
            struct ReactionReq {
                emoji: String,
            }
            self.ack_rx = Some(request_json::<ReactionReq, serde_json::Value>(
                ctx,
                "POST",
                format!("{}/api/chats/{}/messages/{}/reaction", self.base_url(), cid, mid),
                self.token.clone(),
                Some(&ReactionReq { emoji }),
            ));
        }
    }
    fn toggle_pin(&mut self, ctx: &egui::Context, mid: i64) {
        if let Some(cid) = self.open_chat {
            self.ack_rx = Some(request_json::<serde_json::Value, serde_json::Value>(
                ctx, "POST", format!("{}/api/chats/{}/messages/{}/pin", self.base_url(), cid, mid), self.token.clone(), None::<&serde_json::Value>));
        }
    }
    fn delete_message(&mut self, ctx: &egui::Context, mid: i64) {
        if let Some(cid) = self.open_chat {
            self.ack_rx = Some(request_json::<serde_json::Value, serde_json::Value>(
                ctx, "DELETE", format!("{}/api/chats/{}/messages/{}", self.base_url(), cid, mid), self.token.clone(), None::<&serde_json::Value>));
        }
    }
    fn create_dm_with(&mut self, ctx: &egui::Context, name: String) {
        #[derive(Serialize)]
        struct DmReq {
            username: String,
        }
        self.dm_rx = Some(request_json::<DmReq, serde_json::Value>(
            ctx, "POST", format!("{}/api/dm", self.base_url()), self.token.clone(), Some(&DmReq { username: name })));
    }
    fn create_group(&mut self, ctx: &egui::Context) {
        let name = self.group_name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let members: Vec<String> = self
            .group_members
            .split(',')
            .map(|s| s.trim().trim_start_matches('@').to_string())
            .filter(|s| !s.is_empty() && s != &self.my_name)
            .collect();
        self.group_open = false;
        self.group_name.clear();
        self.group_members.clear();
        #[derive(Serialize)]
        struct GroupReq {
            name: String,
            members: Vec<String>,
        }
        self.dm_rx = Some(request_json::<GroupReq, serde_json::Value>(
            ctx, "POST", format!("{}/api/groups", self.base_url()), self.token.clone(), Some(&GroupReq { name, members })));
    }
    fn leave_chat(&mut self, ctx: &egui::Context, chat_id: i64) {
        self.ack_rx = Some(request_json::<serde_json::Value, serde_json::Value>(
            ctx, "DELETE", format!("{}/api/chats/{}/leave", self.base_url(), chat_id), self.token.clone(), None::<&serde_json::Value>));
        if self.open_chat == Some(chat_id) {
            self.open_chat = None;
            self.open_name.clear();
            self.messages.clear();
            self.pinned = None;
        }
        self.need_chats = true;
    }
    fn upload_avatar(&mut self, ctx: &egui::Context) {
        let path = rfd::FileDialog::new().add_filter("Image", &["png", "jpg", "jpeg"]).pick_file();
        let Some(path) = path else { return };
        let Some(data) = encode_avatar(&path) else {
            self.notice("Не удалось прочитать картинку");
            return;
        };
        #[derive(Serialize)]
        struct AvatarReq {
            data: String,
        }
        self.ack_rx = Some(request_json::<AvatarReq, serde_json::Value>(
            ctx, "POST", format!("{}/api/avatar", self.base_url()), self.token.clone(), Some(&AvatarReq { data: data.clone() })));
        self.avatar_data.insert(self.my_name.clone(), data);
        self.avatar_tex.clear();
    }
    fn clear_avatar(&mut self, ctx: &egui::Context) {
        #[derive(Serialize)]
        struct AvatarReq {
            data: String,
        }
        self.ack_rx = Some(request_json::<AvatarReq, serde_json::Value>(
            ctx, "POST", format!("{}/api/avatar", self.base_url()), self.token.clone(), Some(&AvatarReq { data: String::new() })));
        self.avatar_data.remove(&self.my_name);
        self.avatar_tex.clear();
    }
    fn pick_attach(&mut self, ctx: &egui::Context) {
        let path = rfd::FileDialog::new()
            .add_filter("Изображения", &["png", "jpg", "jpeg", "gif", "webp", "bmp"])
            .pick_file();
        let Some(path) = path else { return };
        let Some(data) = encode_attachment(&path) else {
            self.notice("Не удалось прочитать картинку");
            return;
        };
        if data.len() > ATTACH_MAX_B64 {
            self.notice("Картинка слишком большая");
            return;
        }
        if let Some(img) = decode_image(&data) {
            let aspect = image_aspect(&img);
            let tex = ctx.load_texture("attach_preview", img, egui::TextureOptions::LINEAR);
            self.attach_preview = Some((tex, aspect));
        }
        self.pending_attach = Some(data);
    }
    fn attach_tex(&mut self, ctx: &egui::Context, id: i64, data: &str) -> Option<(egui::TextureHandle, f32)> {
        if let Some(t) = self.attach_tex.get(&id) {
            return Some(t.clone());
        }
        let img = decode_image(data)?;
        let aspect = image_aspect(&img);
        let tex = ctx.load_texture(format!("attach_{}", id), img, egui::TextureOptions::LINEAR);
        let pair = (tex, aspect);
        self.attach_tex.insert(id, pair.clone());
        Some(pair)
    }
    fn cache_attach(&mut self, ctx: &egui::Context, m: &NetMessage) {
        if let Some(data) = &m.attach {
            if !self.attach_tex.contains_key(&m.id) {
                self.attach_tex(ctx, m.id, data);
            }
        }
    }
    fn logout(&mut self, ctx: &egui::Context) {
        if let Some(t) = self.token.clone() {
            let _rx = request_json::<serde_json::Value, serde_json::Value>(
                ctx, "POST", format!("{}/api/logout", self.base_url()), Some(t), None::<&serde_json::Value>);
        }
        clear_session_local();
        self.token = None;
        self.ws_send = None;
        self.ws_recv = None;
        self.chats.clear();
        self.messages.clear();
        self.contacts.clear();
        self.pinned = None;
        self.open_chat = None;
        self.online.clear();
        self.password.clear();
        self.error.clear();
        self.error_shown.clear();
        self.info.clear();
        self.info_shown.clear();
        self.info_at = None;
        self.menu_open = false;
        self.emoji_open = false;
        self.pending_attach = None;
        self.attach_preview = None;
        self.attach_tex.clear();
        self.viewer_open = false;
        self.viewer_tex = None;
        self.screen = Screen::Auth;
        self.auth_mode = AuthMode::Welcome;
    }
    fn ws_send_text(&mut self, text: String) {
        let _guard = self.rt.as_ref().map(|rt| rt.enter());
        if let Some(ws) = &mut self.ws_send {
            ws.send(ewebsock::WsMessage::Text(text));
        }
    }
    fn avatar_tex(&mut self, ctx: &egui::Context, name: &str) -> Option<egui::TextureHandle> {
        if let Some(t) = self.avatar_tex.get(name) {
            return Some(t.clone());
        }
        let data = self.avatar_data.get(name)?.clone();
        let color = decode_avatar(&data)?;
        let tex = ctx.load_texture(format!("avatar_{}", name), color, egui::TextureOptions::LINEAR);
        self.avatar_tex.insert(name.to_string(), tex.clone());
        Some(tex)
    }
    fn draw_avatar(&mut self, ui: &mut egui::Ui, rect: egui::Rect, name: &str, is_dm: bool, rounding: f32) {
        let ctx = ui.ctx().clone();
        let tex = self.avatar_tex(&ctx, name);
        if let Some(t) = tex {
            ui.painter().image(
                t.id(),
                rect,
                egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                egui::Color32::WHITE,
            );
        } else {
            let bg = if is_dm { hsv_to_rgb(name_hue(name), 0.45, 0.55) } else { egui::Color32::from_gray(95) };
            ui.painter().rect_filled(rect, rounding, bg);
            let initial: String = name.trim_start_matches('@').chars().take(1).collect::<String>().to_uppercase();
            let font = (rect.height() * 0.42).max(10.0);
            ui.painter().text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                initial,
                egui::FontId::proportional(font),
                text_on(bg),
            );
        }
    }
    fn draw_image_viewer(&mut self, ctx: &egui::Context) {
        if !self.viewer_open {
            return;
        }
        let Some(tex) = self.viewer_tex.clone() else {
            self.viewer_open = false;
            return;
        };
        let screen = ctx.screen_rect();
        let tex_size = tex.size_vec2();
        let fit = ((screen.width() - 80.0) / tex_size.x)
            .min((screen.height() - 80.0) / tex_size.y)
            .min(1.0)
            .max(0.05);
        let zoom = fit * self.viewer_zoom;
        let disp = tex_size * zoom;
        let center = screen.center() + self.viewer_pan;
        let rect = egui::Rect::from_center_size(center, disp);
        egui::Area::new(egui::Id::new("image_viewer"))
            .order(egui::Order::Foreground)
            .fixed_pos(screen.min)
            .show(ctx, |ui| {
                ui.set_min_size(screen.size());
                let resp = ui.allocate_response(screen.size(), egui::Sense::click_and_drag());
                ui.painter().rect_filled(screen, 0.0_f32, egui::Color32::from_black_alpha(200));
                if resp.dragged() {
                    self.viewer_pan += ui.input(|i| i.pointer.delta());
                }
                if resp.double_clicked() {
                    self.viewer_zoom = 1.0_f32;
                    self.viewer_pan = egui::Vec2::ZERO;
                }
                ui.painter().image(
                    tex.id(),
                    rect,
                    egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                    egui::Color32::WHITE,
                );
                ui.painter().rect_stroke(
                    rect,
                    2.0_f32,
                    egui::Stroke::new(1.0_f32, egui::Color32::from_white_alpha(80)),
                );
                let btn_rect = egui::Rect::from_center_size(
                    screen.right_top() + egui::vec2(-28.0, 28.0),
                    egui::vec2(36.0, 36.0),
                );
                let bresp = ui.allocate_rect(btn_rect, egui::Sense::click());
                ui.painter().rect_filled(btn_rect, 8.0_f32, egui::Color32::from_white_alpha(40));
                ui.painter().line_segment(
                    [btn_rect.left_top() + egui::vec2(10.0, 10.0), btn_rect.right_bottom() + egui::vec2(-10.0, -10.0)],
                    egui::Stroke::new(2.5_f32, egui::Color32::WHITE),
                );
                ui.painter().line_segment(
                    [btn_rect.right_top() + egui::vec2(-10.0, 10.0), btn_rect.left_bottom() + egui::vec2(10.0, -10.0)],
                    egui::Stroke::new(2.5_f32, egui::Color32::WHITE),
                );
                if bresp.clicked() {
                    self.viewer_open = false;
                }
                ui.painter().text(
                    egui::pos2(screen.center().x, screen.max.y - 16.0),
                    egui::Align2::CENTER_CENTER,
                    "Колесо — масштаб · перетаскивание — перемещение · двойной клик — сброс · Esc / X — закрыть",
                    egui::FontId::proportional(12.0),
                    egui::Color32::from_white_alpha(160),
                );
                if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                    self.viewer_open = false;
                }
            });
    }
    fn poll(&mut self, ctx: &egui::Context) {
        if self.screen == Screen::Main && self.token.is_some() && self.ws_send.is_none()
            && self.last_ws_attempt.elapsed() >= self.ws_backoff
        {
            self.last_ws_attempt = Instant::now();
            let _guard = self.rt.as_ref().map(|rt| rt.enter());
            match ewebsock::connect(self.ws_url()) {
                Ok((send, recv)) => {
                    self.ws_send = Some(send);
                    self.ws_recv = Some(recv);
                    self.ws_status = "в сети".into();
                    self.ws_backoff = Duration::from_secs(1);
                    self.offline = false;
                }
                Err(e) => {
                    self.ws_status = format!("ошибка: {}", e);
                    self.ws_backoff = (self.ws_backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
        if self.ws_recv.is_some() {
            let mut closed = false;
            let mut events: Vec<NetEvent> = Vec::new();
            if let Some(recv) = &mut self.ws_recv {
                while let Some(ev) = recv.try_recv() {
                    match ev {
                        ewebsock::WsEvent::Message(ewebsock::WsMessage::Text(text)) => {
                            if let Ok(e) = serde_json::from_str::<NetEvent>(&text) {
                                events.push(e);
                            }
                        }
                        ewebsock::WsEvent::Closed | ewebsock::WsEvent::Error(_) => {
                            closed = true;
                            break;
                        }
                        _ => {}
                    }
                }
            }
            for e in events {
                match e {
                    NetEvent::Message(m) => {
                        self.cache_attach(ctx, &m);
                        if Some(m.chat_id) == self.open_chat {
                            if m.deleted {
                                self.messages.retain(|x| x.id != m.id);
                            } else if let Some(pos) = self.messages.iter().position(|x| x.id == m.id) {
                                self.messages[pos] = m.clone();
                            } else {
                                self.messages.push(m.clone());
                            }
                            if m.pinned {
                                self.pinned = Some(m.clone());
                            }
                            if !m.deleted {
                                self.mark_read(ctx, m.id);
                            }
                        }
                        self.need_chats = true;
                    }
                    NetEvent::Typing { chat_id, from } => {
                        if from != self.my_name {
                            self.typing = Some((chat_id, from, Instant::now()));
                        }
                    }
                    NetEvent::Read { chat_id, by, msg_id } => {
                        if by != self.my_name && Some(chat_id) == self.open_chat {
                            self.other_read = self.other_read.max(msg_id);
                        }
                    }
                    NetEvent::Presence { user, online } => {
                        self.online.insert(user, online);
                    }
                }
            }
            if closed {
                self.ws_send = None;
                self.ws_recv = None;
                self.ws_status = "переподключение…".into();
                self.last_ws_attempt = Instant::now();
                self.ws_backoff = (self.ws_backoff * 2).min(Duration::from_secs(10));
                if let Some(cid) = self.open_chat {
                    self.fetch_messages(ctx, cid);
                }
                self.need_chats = true;
            }
        }
        if let Some(rx) = &self.auth_rx {
            if let Ok(res) = rx.try_recv() {
                self.auth_rx = None;
                match res {
                    Ok(resp) => {
                        let srv = self.server.clone();
                        let usr = self.username.trim().to_string();
                        save_session_local(&srv, &usr, &resp.token);
                        self.token = Some(resp.token);
                        self.my_name = usr;
                        self.screen = Screen::Main;
                        self.need_chats = true;
                        self.need_avatars = true;
                        self.offline = false;
                        self.last_chat_poll = Instant::now();
                        self.error.clear();
                        self.error_shown.clear();
                    }
                    Err(e) => self.handle_error(e),
                }
            }
        }
        if let Some(rx) = &self.dm_rx {
            if let Ok(res) = rx.try_recv() {
                self.dm_rx = None;
                match res {
                    Ok(v) => {
                        self.need_chats = true;
                        if let Some(cid) = v.get("chat_id").and_then(|x| x.as_i64()) {
                            self.pending_chat_id = Some(cid);
                        }
                    }
                    Err(e) => self.handle_error(e),
                }
            }
        }
        if self.screen == Screen::Main && self.need_chats && self.chats_rx.is_none() {
            self.need_chats = false;
            self.fetch_chats(ctx);
        }
        if let Some(rx) = &self.chats_rx {
            if let Ok(res) = rx.try_recv() {
                self.chats_rx = None;
                match res {
                    Ok(c) => {
                        self.offline = false;
                        if let Some(pid) = self.pending_chat_id {
                            if let Some(ch) = c.iter().find(|x| x.id == pid) {
                                self.pending_chat_id = None;
                                self.open_chat(ctx, ch.id, ch.name.clone(), ch.is_dm);
                            }
                        }
                        self.chats = c;
                    }
                    Err(e) => self.handle_error(e),
                }
            }
        }
        if self.screen == Screen::Main && self.need_contacts && self.contacts_rx.is_none() {
            self.need_contacts = false;
            self.fetch_contacts(ctx);
        }
        if let Some(rx) = &self.contacts_rx {
            if let Ok(res) = rx.try_recv() {
                self.contacts_rx = None;
                match res {
                    Ok(list) => {
                        let me = self.my_name.clone();
                        self.contacts = list.into_iter().filter(|c| c.username != me).collect();
                    }
                    Err(e) => self.handle_error(e),
                }
            }
        }
        if self.screen == Screen::Main && self.need_avatars && self.avatars_rx.is_none() {
            self.need_avatars = false;
            self.fetch_avatars(ctx);
        }
        if let Some(rx) = &self.avatars_rx {
            if let Ok(res) = rx.try_recv() {
                self.avatars_rx = None;
                if let Ok(list) = res {
                    self.avatar_data = list.into_iter().map(|a| (a.username, a.data)).collect();
                    self.avatar_tex.clear();
                }
            }
        }
        if let Some(rx) = &self.msg_rx {
            if let Ok(res) = rx.try_recv() {
                self.msg_rx = None;
                match res {
                    Ok(r) => {
                        self.messages = r.messages.into_iter().filter(|m| !m.deleted).collect();
                        self.other_read = r.other_read;
                        self.pinned = r.pinned;
                        let snapshot = self.messages.clone();
                        for m in &snapshot {
                            self.cache_attach(ctx, m);
                        }
                        if let Some(last) = self.messages.last() {
                            self.mark_read(ctx, last.id);
                        }
                        self.need_chats = true;
                    }
                    Err(e) => self.handle_error(e),
                }
            }
        }
        if let Some(rx) = &self.send_rx {
            if let Ok(res) = rx.try_recv() {
                self.send_rx = None;
                match res {
                    Ok(m) => {
                        self.pending_send_text = None;
                        self.pending_attach = None;
                        self.attach_preview = None;
                        self.offline = false;
                        self.cache_attach(ctx, &m);
                        if Some(m.chat_id) == self.open_chat {
                            if let Some(pos) = self.messages.iter().position(|x| x.id == m.id) {
                                self.messages[pos] = m.clone();
                            } else {
                                self.messages.push(m.clone());
                            }
                            self.fetch_messages(ctx, m.chat_id);
                        }
                        self.need_chats = true;
                    }
                    Err(e) => {
                        if let Some(t) = self.pending_send_text.take() {
                            if self.draft.is_empty() {
                                self.draft = t;
                            }
                        }
                        self.handle_error(e);
                    }
                }
            }
        }
        if let Some(rx) = self.ack_rx.take() {
            match rx.try_recv() {
                Ok(res) => match res {
                    Ok(_v) => {
                        self.offline = false;
                        if let Some(cid) = self.open_chat {
                            self.fetch_messages(ctx, cid);
                        }
                        self.need_chats = true;
                    }
                    Err(e) => self.handle_error(e),
                },
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    self.ack_rx = Some(rx);
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {}
            }
        }
    }
    fn chat_status(&self) -> String {
        if let Some((cid, from, t)) = &self.typing {
            if *cid == self.open_chat.unwrap_or(-1) && t.elapsed() < Duration::from_secs(4) && from != &self.my_name {
                return format!("{} печатает…", from);
            }
        }
        if self.open_is_dm {
            let other = self.open_name.trim_start_matches('@');
            if self.online.get(other).copied().unwrap_or(false) {
                "В сети".into()
            } else {
                "Был(а) недавно".into()
            }
        } else {
            String::new()
        }
    }
    fn pill_edit(
        ui: &mut egui::Ui,
        width: f32,
        height: f32,
        fill: egui::Color32,
        text_col: egui::Color32,
        text: &mut String,
        hint: &str,
        password: bool,
    ) -> egui::Response {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(width, height), egui::Sense::hover());
        ui.painter().rect_filled(rect, height * 0.5, fill);
        let inner = rect.shrink2(egui::vec2(14.0, 2.0));
        ui.scope(|ui| {
            ui.style_mut().visuals.extreme_bg_color = egui::Color32::TRANSPARENT;
            ui.style_mut().visuals.widgets.inactive.bg_stroke = egui::Stroke::NONE;
            ui.style_mut().visuals.widgets.hovered.bg_stroke = egui::Stroke::NONE;
            ui.style_mut().visuals.widgets.active.bg_stroke = egui::Stroke::NONE;
            ui.style_mut().visuals.override_text_color = Some(text_col);
            let te = egui::TextEdit::singleline(text).hint_text(hint).desired_width(f32::INFINITY);
            let te = if password { te.password(true) } else { te };
            ui.put(inner, te)
        })
            .inner
    }
    fn tile(
        ui: &mut egui::Ui,
        size: egui::Vec2,
        rounding: f32,
        fill: egui::Color32,
        hover: egui::Color32,
        active: bool,
        accent: egui::Color32,
    ) -> (egui::Rect, egui::Response) {
        let (rect, resp) = ui.allocate_exact_size(size, egui::Sense::click());
        let f = if resp.hovered() { hover } else { fill };
        ui.painter().rect_filled(rect, rounding, f);
        if active {
            ui.painter().rect_stroke(rect.shrink(2.0), rounding, egui::Stroke::new(2.0_f32, accent));
        }
        (rect, resp)
    }

    // ---------- screens ----------
    fn ui_auth(&mut self, ctx: &egui::Context) {
        let mut do_login = false;
        let mut do_register = false;
        let mut switch_mode: Option<AuthMode> = None;
        let card_w = 320.0_f32;
        let card_h = match self.auth_mode {
            AuthMode::Welcome => 430.0_f32,
            AuthMode::Register => 440.0_f32,
        };
        egui::CentralPanel::default().show(ctx, |ui| {
            let screen = ui.max_rect();
            let card_rect = egui::Rect::from_center_size(screen.center(), egui::vec2(card_w, card_h));
            for k in (1..=16).rev() {
                let alpha = (k as f32 * 1.4) as u8;
                ui.painter().rect_stroke(
                    card_rect.expand(k as f32 * 1.7),
                    26.0_f32 + k as f32,
                    egui::Stroke::new(1.7_f32, egui::Color32::from_white_alpha(alpha)),
                );
            }
            ui.painter().rect_filled(card_rect, 26.0_f32, if self.dark { egui::Color32::from_gray(100) } else { egui::Color32::from_gray(250) });
            ui.allocate_new_ui(egui::UiBuilder::new().max_rect(card_rect.shrink(28.0)), |ui| {
                ui.vertical_centered(|ui| {
                    let tile = self.pal_tile();
                    let text_col = if self.dark { egui::Color32::from_gray(60) } else { egui::Color32::from_gray(40) };
                    let (hrect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), 40.0), egui::Sense::hover());
                    ui.painter().rect_filled(hrect, 12.0_f32, tile);
                    ui.painter().text(
                        hrect.center(),
                        egui::Align2::CENTER_CENTER,
                        match self.auth_mode {
                            AuthMode::Welcome => "Welcome",
                            AuthMode::Register => "Registration",
                        },
                        egui::FontId::proportional(18.0),
                        egui::Color32::WHITE,
                    );
                    ui.add_space(46.0);
                    let w = ui.available_width();
                    let login_resp = Self::pill_edit(ui, w, 38.0, tile, text_col, &mut self.username, "Login", false);
                    ui.add_space(8.0);
                    let pass_resp = Self::pill_edit(ui, w, 38.0, tile, text_col, &mut self.password, "Password", true);
                    if self.auth_mode == AuthMode::Register {
                        ui.add_space(8.0);
                        Self::pill_edit(ui, w, 38.0, tile, text_col, &mut self.email, "Email/phone number", false);
                    } else {
                        ui.add_space(10.0);
                        if ui.label(egui::RichText::new("Register").strong().color(egui::Color32::WHITE)).clicked() {
                            switch_mode = Some(AuthMode::Register);
                        }
                    }
                    if !self.error.is_empty() {
                        ui.add_space(8.0);
                        ui.colored_label(egui::Color32::LIGHT_RED, &self.error);
                    }
                    ui.add_space(60.0);
                    let (brect, bresp) = ui.allocate_exact_size(egui::vec2(w, 42.0), egui::Sense::click());
                    let bfill = if bresp.hovered() { self.pal_tile_hover() } else { tile };
                    ui.painter().rect_filled(brect, 12.0_f32, bfill);
                    let label = match self.auth_mode {
                        AuthMode::Welcome => "Entrance",
                        AuthMode::Register => "Register",
                    };
                    ui.painter().text(brect.center(), egui::Align2::CENTER_CENTER, label, egui::FontId::proportional(17.0), egui::Color32::WHITE);
                    let enter = ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if bresp.clicked() || (enter && (login_resp.has_focus() || pass_resp.has_focus())) {
                        match self.auth_mode {
                            AuthMode::Welcome => do_login = true,
                            AuthMode::Register => do_register = true,
                        }
                    }
                });
            });
            if !self.error.is_empty() {
                ui.vertical_centered(|ui| {
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("Сервер:").small().weak());
                        ui.add(egui::TextEdit::singleline(&mut self.server).desired_width(200.0));
                    });
                });
            }
        });
        if let Some(m) = switch_mode {
            self.auth_mode = m;
            self.error.clear();
        }
        if do_login {
            self.do_auth(ctx, "login");
        }
        if do_register {
            self.do_auth(ctx, "register");
        }
    }

    fn ui_main(&mut self, ctx: &egui::Context) {
        let accent = self.accent();
        let dark = self.dark;
        let tile = self.pal_tile();
        let tile_hover = self.pal_tile_hover();
        let icon = self.pal_icon();
        let status_text = self.chat_status();
        let mut do_send = false;
        let mut do_attach = false;
        let mut ctx_action: Option<CtxAction> = None;
        let mut tab_req: Option<LeftTab> = None;
        let mut do_logout = false;
        let mut do_reset_theme = false;
        let mut do_dm_with: Option<String> = None;
        let mut open_group_win = false;
        let mut open_dm_win = false;
        let mut delete_req: Option<i64> = None;
        let mut open_req: Option<(i64, String, bool)> = None;
        let mut fav_req: Option<(i64, bool)> = None;
        let mut notice_text: Option<String> = None;
        let mut react_req: Option<(i64, String)> = None;
        let mut pin_req: Option<i64> = None;
        let mut forward_req: Option<(String, String)> = None;
        let mut do_upload_avatar = false;
        let mut do_clear_avatar = false;
        let mut open_viewer: Option<egui::TextureHandle> = None;
        let open_id = self.open_chat;
        let search_q = self.search_q.trim().to_lowercase();
        let chats_snapshot: Vec<Chat> = self
            .chats
            .iter()
            .filter(|c| search_q.is_empty() || c.name.to_lowercase().contains(&search_q))
            .cloned()
            .collect();
        let contacts_snapshot: Vec<Contact> = self
            .contacts
            .iter()
            .filter(|c| search_q.is_empty() || c.username.to_lowercase().contains(&search_q))
            .cloned()
            .collect();

        // ---- rail ----
        egui::SidePanel::left("rail")
            .exact_width(88.0)
            .frame(egui::Frame::none().fill(tile))
            .show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(10.0);
                    let (r, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile_hover, false, accent);
                    icon_menu(ui.painter(), r.center(), 18.0, icon);
                    if resp.clicked() {
                        self.menu_open = true;
                    }
                    resp.on_hover_text("Меню");
                    ui.add_space(14.0);
                    let (r, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile_hover, self.left_tab == LeftTab::Chats, accent);
                    icon_chats(ui.painter(), r.center(), 18.0, icon);
                    ui.label(egui::RichText::new("Все чаты").size(10.0).color(icon));
                    if resp.clicked() {
                        tab_req = Some(LeftTab::Chats);
                    }
                    ui.add_space(10.0);
                    let (r, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile_hover, self.left_tab == LeftTab::Contacts, accent);
                    icon_folder(ui.painter(), r.center(), 18.0, icon);
                    ui.label(egui::RichText::new("Папка").size(10.0).color(icon));
                    if resp.clicked() {
                        tab_req = Some(LeftTab::Contacts);
                    }
                });
            });

        // ---- column ----
        egui::SidePanel::left("col")
            .exact_width(88.0)
            .frame(egui::Frame::none().fill(tile_hover))
            .show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(10.0);
                    let (r, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile, false, accent);
                    icon_search(ui.painter(), r.center(), 16.0, icon);
                    if resp.clicked() {
                        self.search_open = !self.search_open;
                    }
                    resp.on_hover_text("Поиск");
                    ui.add_space(10.0);
                    let (r, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile, self.show_favorites, accent);
                    icon_bookmark(ui.painter(), r.center(), 16.0, icon);
                    if resp.clicked() {
                        self.show_favorites = !self.show_favorites;
                    }
                    resp.on_hover_text("Закладки");
                    ui.add_space(12.0);
                    let bg_rect = egui::Rect::from_min_max(
                        egui::pos2(ui.min_rect().left(), ui.cursor().top()),
                        ui.min_rect().right_bottom(),
                    );
                    let bg = ui.interact(bg_rect, ui.id().with("col_bg"), egui::Sense::click());
                    egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                        ui.vertical_centered(|ui| {
                            match self.left_tab {
                                LeftTab::Chats => {
                                    for c in &chats_snapshot {
                                        if self.show_favorites && !self.favorites.contains(&c.id) {
                                            continue;
                                        }
                                        let selected = open_id == Some(c.id);
                                        let cid = c.id;
                                        let cname = c.name.clone();
                                        let is_dm = c.is_dm;
                                        let unread = c.unread;
                                        let online = c.online.unwrap_or(false);
                                        let in_fav = self.favorites.contains(&cid);
                                        let (rect, resp) = Self::tile(
                                            ui,
                                            egui::vec2(64.0, 64.0),
                                            14.0,
                                            if selected { accent.gamma_multiply(0.6) } else { tile },
                                            tile_hover,
                                            selected,
                                            accent,
                                        );
                                        self.draw_avatar(ui, rect.shrink(5.0), &cname, is_dm, 11.0);
                                        if is_dm {
                                            let dot = rect.right_bottom() + egui::vec2(-9.0, -9.0);
                                            ui.painter().circle_filled(dot, 6.0, tile_hover);
                                            ui.painter().circle_filled(
                                                dot,
                                                4.5,
                                                if online { egui::Color32::from_rgb(86, 205, 128) } else { egui::Color32::from_gray(130) },
                                            );
                                        }
                                        if unread > 0 {
                                            let b = rect.right_top() + egui::vec2(-10.0, 10.0);
                                            ui.painter().circle_filled(b, 9.0, accent);
                                            ui.painter().text(
                                                b,
                                                egui::Align2::CENTER_CENTER,
                                                if unread > 9 { "9+".to_string() } else { unread.to_string() },
                                                egui::FontId::proportional(10.0),
                                                text_on(accent),
                                            );
                                        }
                                        if resp.clicked() {
                                            open_req = Some((cid, cname, is_dm));
                                        }
                                        resp.context_menu(|ui| {
                                            if ui.button(if in_fav { "Убрать из закладок" } else { "В закладки" }).clicked() {
                                                fav_req = Some((cid, !in_fav));
                                                ui.close_menu();
                                            }
                                            if ui.button("Удалить чат").clicked() {
                                                delete_req = Some(cid);
                                                ui.close_menu();
                                            }
                                        });
                                        ui.add_space(8.0);
                                    }
                                }
                                LeftTab::Contacts => {
                                    for ct in &contacts_snapshot {
                                        let uname = ct.username.clone();
                                        let online = ct.online;
                                        let (rect, resp) = Self::tile(ui, egui::vec2(64.0, 64.0), 14.0, tile, tile_hover, false, accent);
                                        self.draw_avatar(ui, rect.shrink(5.0), &uname, true, 11.0);
                                        let dot = rect.right_bottom() + egui::vec2(-9.0, -9.0);
                                        ui.painter().circle_filled(dot, 6.0, tile_hover);
                                        ui.painter().circle_filled(
                                            dot,
                                            4.5,
                                            if online { egui::Color32::from_rgb(86, 205, 128) } else { egui::Color32::from_gray(130) },
                                        );
                                        if resp.clicked() {
                                            do_dm_with = Some(uname);
                                        }
                                        ui.add_space(8.0);
                                    }
                                }
                            }
                        });
                    });
                    bg.context_menu(|ui| {
                        if ui.button("Новое сообщение").clicked() {
                            open_dm_win = true;
                            ui.close_menu();
                        }
                        if ui.button("Создать группу").clicked() {
                            open_group_win = true;
                            ui.close_menu();
                        }
                    });
                });
            });

        // ---- central ----
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.open_chat.is_none() {
                ui.centered_and_justified(|ui| {
                    ui.label(egui::RichText::new("Выберите чат слева").size(18.0).weak());
                });
                return;
            }
            let avail = ui.available_rect_before_wrap();
            let header_h = 68.0_f32;
            let banner_h = if self.pinned.is_some() { 34.0_f32 } else { 0.0_f32 };
            let emoji_h = if self.emoji_open { 176.0_f32 } else { 0.0_f32 };
            let preview_h = if self.pending_attach.is_some() { 84.0_f32 } else { 0.0_f32 };
            let input_h = (64.0_f32 + emoji_h + preview_h)
                .min((avail.height() - header_h - banner_h - 40.0_f32).max(64.0_f32))
                .max(64.0_f32);
            let header_rect = egui::Rect::from_min_size(avail.min, egui::vec2(avail.width(), header_h));
            let banner_rect = egui::Rect::from_min_size(
                egui::pos2(avail.min.x, header_rect.max.y),
                egui::vec2(avail.width(), banner_h),
            );
            let input_rect = egui::Rect::from_min_size(
                egui::pos2(avail.min.x, avail.max.y - input_h),
                egui::vec2(avail.width(), input_h),
            );
            let msgs_rect = egui::Rect::from_min_max(
                egui::pos2(avail.min.x, banner_rect.max.y),
                egui::pos2(avail.max.x, (input_rect.min.y - 4.0).max(banner_rect.max.y + 40.0)),
            );

            // header
            ui.allocate_new_ui(egui::UiBuilder::new().max_rect(header_rect), |ui| {
                egui::Frame::none()
                    .fill(if dark { egui::Color32::from_gray(120) } else { egui::Color32::from_gray(210) })
                    .inner_margin(egui::Margin::symmetric(10.0, 8.0))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            let (arect, _) = ui.allocate_exact_size(egui::vec2(52.0, 52.0), egui::Sense::hover());
                            let name_clone = self.open_name.clone();
                            let is_dm = self.open_is_dm;
                            self.draw_avatar(ui, arect, &name_clone, is_dm, 12.0);
                            ui.add_space(10.0);
                            ui.vertical(|ui| {
                                ui.label(egui::RichText::new(&self.open_name).strong().color(egui::Color32::WHITE));
                                if !status_text.is_empty() {
                                    let typing_now = status_text.ends_with('…');
                                    ui.label(egui::RichText::new(&status_text).small().color(
                                        if typing_now { accent } else { egui::Color32::from_gray(230) },
                                    ));
                                }
                            });
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                let hf = if dark { egui::Color32::from_gray(120) } else { egui::Color32::from_gray(210) };
                                let (pr, presp) = Self::tile(ui, egui::vec2(44.0, 44.0), 12.0, hf, hf, false, accent);
                                icon_phone(ui.painter(), pr.center(), 14.0, egui::Color32::BLACK);
                                if presp.clicked() {
                                    notice_text = Some("Звонки пока в разработке".to_string());
                                }
                                let (sr, sresp) = Self::tile(ui, egui::vec2(44.0, 44.0), 12.0, hf, hf, self.chat_search_open, accent);
                                icon_search(ui.painter(), sr.center(), 13.0, egui::Color32::BLACK);
                                if sresp.clicked() {
                                    self.chat_search_open = !self.chat_search_open;
                                }
                                if self.chat_search_open {
                                    ui.add(egui::TextEdit::singleline(&mut self.chat_search).desired_width(160.0).hint_text("Поиск в чате"));
                                }
                            });
                        });
                    });
            });

            // pinned banner
            if banner_h > 0.0 {
                ui.allocate_new_ui(egui::UiBuilder::new().max_rect(banner_rect), |ui| {
                    egui::Frame::none()
                        .fill(if dark { egui::Color32::from_gray(60) } else { egui::Color32::from_gray(225) })
                        .inner_margin(egui::Margin::symmetric(10.0, 6.0))
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                if let Some(p) = &self.pinned {
                                    let preview: String = p.text.chars().take(60).collect();
                                    ui.label(egui::RichText::new(format!("Закреплено · {}: {}", p.from, preview)).small().color(accent));
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        if ui.small_button("x").clicked() {
                                            pin_req = Some(p.id);
                                        }
                                    });
                                }
                            });
                        });
                });
            }

            // messages
            ui.allocate_new_ui(egui::UiBuilder::new().max_rect(msgs_rect), |ui| {
                egui::ScrollArea::vertical()
                    .max_height(ui.available_height().max(1.0))
                    .stick_to_bottom(true)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let filter = self.chat_search.trim().to_lowercase();
                        let msgs: Vec<NetMessage> = self
                            .messages
                            .iter()
                            .filter(|m| filter.is_empty() || m.text.to_lowercase().contains(&filter))
                            .cloned()
                            .collect();
                        let other_read = self.other_read;
                        let my_name = self.my_name.clone();
                        let mut last_day: Option<i64> = None;
                        let today_days = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64 / 86_400)
                            .unwrap_or(0);
                        for m in &msgs {
                            let mday = m.ts.div_euclid(86_400);
                            if last_day != Some(mday) {
                                last_day = Some(mday);
                                let label = day_label(m.ts, today_days);
                                let pill_fill = if dark { egui::Color32::from_gray(60) } else { egui::Color32::from_gray(220) };
                                ui.add_space(6.0);
                                ui.vertical_centered(|ui| {
                                    egui::Frame::none()
                                        .rounding(10.0_f32)
                                        .fill(pill_fill)
                                        .inner_margin(egui::Margin::symmetric(10.0, 3.0))
                                        .show(ui, |ui| {
                                            ui.label(egui::RichText::new(label).small().weak());
                                        });
                                });
                                ui.add_space(2.0);
                            }
                            let mine = m.from == my_name;
                            let fill = if mine { accent } else if dark { egui::Color32::from_gray(70) } else { egui::Color32::from_gray(235) };
                            let text_col = if mine { text_on(fill) } else { ui.visuals().text_color() };
                            let time_col = text_col.gamma_multiply(0.65);
                            let body_font = egui::FontId::proportional(14.0);
                            let small_font = egui::FontId::proportional(11.0);
                            let body_text: String = if m.deleted { "сообщение удалено".into() } else { m.text.clone() };
                            let text_size = ui.fonts(|f| f.layout_no_wrap(body_text.clone(), body_font, egui::Color32::WHITE)).size();
                            let mut meta = time_str(m.ts);
                            if m.edited && !m.deleted {
                                meta = format!("ред. · {}", meta);
                            }
                            let meta_measure = if mine { format!("00  {}", meta) } else { meta.clone() };
                            let meta_size = ui.fonts(|f| f.layout_no_wrap(meta_measure, small_font.clone(), egui::Color32::WHITE)).size();
                            let reply_w = m
                                .reply_to
                                .as_ref()
                                .map(|r| {
                                    let rt: String = r.text.chars().take(40).collect();
                                    let a = ui.fonts(|f| f.layout_no_wrap(r.from.clone(), small_font.clone(), egui::Color32::WHITE)).size();
                                    let b = ui.fonts(|f| f.layout_no_wrap(rt, small_font.clone(), egui::Color32::WHITE)).size();
                                    a.x.max(b.x)
                                })
                                .unwrap_or(0.0);
                            let fwd_w = m
                                .forward_from
                                .as_ref()
                                .map(|f| {
                                    ui.fonts(|x| x.layout_no_wrap(format!("Переслано от {}", f), small_font.clone(), egui::Color32::WHITE)).size().x
                                })
                                .unwrap_or(0.0);
                            let react_h = if m.reactions.is_empty() { 0.0_f32 } else { 24.0_f32 };
                            let (img_w, img_h) = match &m.attach {
                                Some(data) => match self.attach_tex(ui.ctx(), m.id, data) {
                                    Some((_, aspect)) => {
                                        let mut w = 300.0_f32;
                                        let mut h = w / aspect;
                                        if h > 300.0 {
                                            h = 300.0;
                                            w = h * aspect;
                                        }
                                        (w, h)
                                    }
                                    None => (0.0, 0.0),
                                },
                                None => (0.0, 0.0),
                            };
                            let mut bubble_w = (text_size.x.max(meta_size.x).max(reply_w).max(fwd_w).max(img_w) + 28.0).min(460.0);
                            let est_lines = if body_text.is_empty() {
                                0.0_f32
                            } else if text_size.x + 24.0 <= bubble_w {
                                1.0_f32
                            } else {
                                ((text_size.x + 24.0) / bubble_w).ceil()
                            };
                            let mut bubble_h = est_lines * 19.0 + 30.0 + react_h
                                + if m.reply_to.is_some() { 48.0 } else { 0.0 }
                                + if m.forward_from.is_some() { 18.0 } else { 0.0 };
                            if img_h > 0.0 {
                                bubble_h += img_h + 6.0;
                            }
                            let row_w = ui.available_width();
                            let (row_rect, _) = ui.allocate_exact_size(egui::vec2(row_w, bubble_h), egui::Sense::hover());
                            let bubble_rect = if mine {
                                egui::Rect::from_min_size(row_rect.right_top() - egui::vec2(bubble_w, 0.0), egui::vec2(bubble_w, bubble_h))
                            } else {
                                egui::Rect::from_min_size(row_rect.left_top(), egui::vec2(bubble_w, bubble_h))
                            };
                            ui.painter().rect_filled(bubble_rect, 14.0_f32, fill);
                            let bubble_resp = ui.interact(bubble_rect, ui.id().with(m.id), egui::Sense::click());
                            if !m.deleted {
                                bubble_resp.context_menu(|ui| {
                                    ui.menu_button("Реакция", |ui| {
                                        for e in ["👍", "❤️", "😂", "🔥", "😮"] {
                                            if ui.button(e).clicked() {
                                                react_req = Some((m.id, e.to_string()));
                                                ui.close_menu();
                                            }
                                        }
                                    });
                                    if ui.button("Ответить").clicked() {
                                        ctx_action = Some(CtxAction::Reply(m.clone()));
                                        ui.close_menu();
                                    }
                                    if ui.button("Переслать").clicked() {
                                        forward_req = Some((m.text.clone(), m.from.clone()));
                                        ui.close_menu();
                                    }
                                    if ui.button(if m.pinned { "Открепить" } else { "Закрепить" }).clicked() {
                                        pin_req = Some(m.id);
                                        ui.close_menu();
                                    }
                                    if mine {
                                        if ui.button("Изменить").clicked() {
                                            ctx_action = Some(CtxAction::Edit(m.clone()));
                                            ui.close_menu();
                                        }
                                        if ui.button("Удалить").clicked() {
                                            ctx_action = Some(CtxAction::Delete(m.id));
                                            ui.close_menu();
                                        }
                                    }
                                });
                            }
                            ui.allocate_new_ui(egui::UiBuilder::new().max_rect(bubble_rect.shrink2(egui::vec2(10.0, 6.0))), |ui| {
                                ui.scope(|ui| {
                                    ui.style_mut().visuals.override_text_color = Some(text_col);
                                    if let Some(r) = &m.reply_to {
                                        egui::Frame::none()
                                            .rounding(8.0_f32)
                                            .fill(fill.gamma_multiply(0.8))
                                            .stroke(egui::Stroke::new(2.0_f32, accent.gamma_multiply(0.9)))
                                            .inner_margin(egui::Margin::symmetric(8.0, 4.0))
                                            .show(ui, |ui| {
                                                ui.label(egui::RichText::new(&r.from).small().strong().color(accent));
                                                let rt: String = r.text.chars().take(40).collect();
                                                ui.label(egui::RichText::new(rt).small());
                                            });
                                        ui.add_space(4.0);
                                    }
                                    if let Some(f) = &m.forward_from {
                                        ui.label(egui::RichText::new(format!("Переслано от {}", f)).small().italics().color(time_col));
                                    }
                                    if let Some(data) = &m.attach {
                                        if let Some((tex, _)) = self.attach_tex(ui.ctx(), m.id, data) {
                                            let (r, resp) = ui.allocate_exact_size(egui::vec2(img_w, img_h), egui::Sense::click());
                                            ui.painter().image(
                                                tex.id(),
                                                r,
                                                egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                                                egui::Color32::WHITE,
                                            );
                                            ui.painter().rect_stroke(
                                                r,
                                                8.0_f32,
                                                egui::Stroke::new(1.0_f32, egui::Color32::from_white_alpha(45)),
                                            );
                                            let resp = resp
                                                .on_hover_cursor(egui::CursorIcon::ZoomIn)
                                                .on_hover_text("Нажмите, чтобы открыть изображение");
                                            if resp.clicked() {
                                                open_viewer = Some(tex.clone());
                                            }
                                            if !m.text.is_empty() {
                                                ui.add_space(4.0);
                                            }
                                        }
                                    }
                                    if m.deleted {
                                        ui.label(egui::RichText::new("сообщение удалено").italics().color(time_col));
                                    } else if !m.text.is_empty() {
                                        ui.label(&m.text);
                                    }
                                    if !m.reactions.is_empty() {
                                        ui.horizontal(|ui| {
                                            for r in &m.reactions {
                                                let label = format!("{} {}", r.emoji, r.count);
                                                let btn = egui::Button::new(egui::RichText::new(label).size(11.0));
                                                let resp = ui.add(btn);
                                                if r.mine {
                                                    ui.painter().rect_stroke(resp.rect.expand(1.0), 8.0_f32, egui::Stroke::new(1.5_f32, accent));
                                                }
                                                if resp.clicked() {
                                                    react_req = Some((m.id, r.emoji.clone()));
                                                }
                                            }
                                        });
                                    }
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::BOTTOM), |ui| {
                                        if mine {
                                            let read = m.id <= other_read;
                                            let (trect, _) = ui.allocate_exact_size(egui::vec2(if read { 17.0 } else { 10.0 }, 11.0), egui::Sense::hover());
                                            paint_ticks(ui.painter(), trect, if read { accent } else { time_col }, read);
                                        }
                                        let mut meta = time_str(m.ts);
                                        if m.edited && !m.deleted {
                                            meta = format!("ред. · {}", meta);
                                        }
                                        ui.label(egui::RichText::new(meta).size(11.0).color(time_col));
                                    });
                                });
                            });
                            ui.add_space(4.0);
                        }
                    });
            });

            // input zone
            ui.allocate_new_ui(egui::UiBuilder::new().max_rect(input_rect), |ui| {
                egui::Frame::none()
                    .fill(if dark { egui::Color32::from_gray(50) } else { egui::Color32::from_gray(225) })
                    .inner_margin(egui::Margin::symmetric(8.0, 8.0))
                    .show(ui, |ui| {
                        if self.emoji_open {
                            egui::Frame::none()
                                .fill(if dark { egui::Color32::from_gray(70) } else { egui::Color32::WHITE })
                                .rounding(12.0_f32)
                                .inner_margin(egui::Margin::symmetric(8.0, 8.0))
                                .show(ui, |ui| {
                                    egui::ScrollArea::vertical().max_height(120.0).auto_shrink([false, false]).show(ui, |ui| {
                                        ui.horizontal_wrapped(|ui| {
                                            ui.spacing_mut().item_spacing = egui::vec2(4.0, 4.0);
                                            for e in EMOJIS {
                                                if ui.add_sized([34.0, 34.0], egui::Button::new(egui::RichText::new(e.to_string()).size(20.0))).clicked() {
                                                    self.draft.push_str(e);
                                                }
                                            }
                                        });
                                    });
                                });
                            ui.add_space(6.0);
                        }
                        if let Some((tex, aspect)) = &self.attach_preview {
                            let tex = tex.clone();
                            let w = (72.0_f32 * *aspect).clamp(40.0, 140.0);
                            let mut clear = false;
                            ui.horizontal(|ui| {
                                let (r, _) = ui.allocate_exact_size(egui::vec2(w, 72.0), egui::Sense::hover());
                                ui.painter().image(
                                    tex.id(),
                                    r,
                                    egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                                    egui::Color32::WHITE,
                                );
                                ui.vertical(|ui| {
                                    ui.label(egui::RichText::new("📎 Фото прикреплено").small().color(accent));
                                    if ui.small_button("Убрать ✕").clicked() {
                                        clear = true;
                                    }
                                });
                            });
                            if clear {
                                self.pending_attach = None;
                                self.attach_preview = None;
                            }
                        }
                        if self.editing.is_some() {
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new("Редактирование сообщения").small().color(accent));
                                if ui.small_button("x").clicked() {
                                    self.editing = None;
                                    self.draft.clear();
                                }
                            });
                        }
                        if self.reply_to.is_some() {
                            let r = self.reply_to.clone().unwrap();
                            ui.horizontal(|ui| {
                                let preview: String = r.text.chars().take(40).collect();
                                ui.label(egui::RichText::new(format!("Ответ {}: {}", r.from, preview)).small().color(accent));
                                if ui.small_button("x").clicked() {
                                    self.reply_to = None;
                                }
                            });
                        }
                        ui.horizontal(|ui| {
                            let bar_tile = if dark { egui::Color32::from_gray(150) } else { egui::Color32::from_gray(200) };
                            let (cr, cresp) = Self::tile(ui, egui::vec2(48.0, 48.0), 12.0, bar_tile, tile_hover, false, accent);
                            icon_clip(ui.painter(), cr.center(), 14.0, icon);
                            if cresp.clicked() {
                                do_attach = true;
                            }
                            let pill_fill = if dark { egui::Color32::from_gray(200) } else { egui::Color32::WHITE };
                            let editing = self.editing.is_some();
                            let gap = ui.spacing().item_spacing.x;
                            let reserved = 48.0 * 4.0 + gap * 5.0 + 8.0;
                            let text_width = (ui.available_width() - reserved).max(120.0);
                            let resp = Self::pill_edit(
                                ui,
                                text_width,
                                48.0,
                                pill_fill,
                                egui::Color32::from_gray(40),
                                &mut self.draft,
                                if editing { "Отредактируйте сообщение..." } else { "Сообщение..." },
                                false,
                            );
                            let enter_send = resp.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            let (snr, sresp) = Self::tile(ui, egui::vec2(48.0, 48.0), 12.0, accent, accent.gamma_multiply(0.8), false, accent);
                            if editing {
                                icon_check(ui.painter(), snr.center(), 14.0, text_on(accent));
                            } else {
                                icon_send(ui.painter(), snr.center(), 14.0, text_on(accent));
                            }
                            if (sresp.clicked() || enter_send) && (!self.draft.trim().is_empty() || self.pending_attach.is_some()) {
                                do_send = true;
                            }
                            let (er, eresp) = Self::tile(ui, egui::vec2(48.0, 48.0), 12.0, bar_tile, tile_hover, self.emoji_open, accent);
                            icon_smile(ui.painter(), er.center(), 15.0, icon);
                            if eresp.clicked() {
                                self.emoji_open = !self.emoji_open;
                            }
                            let (mr, mresp) = Self::tile(ui, egui::vec2(48.0, 48.0), 12.0, bar_tile, tile_hover, false, accent);
                            icon_mic(ui.painter(), mr.center(), 15.0, icon);
                            if mresp.clicked() {
                                notice_text = Some("Голосовые сообщения пока в разработке".to_string());
                            }
                        });
                    });
            });
        });

        // ---- deferred ----
        if let Some(tex) = open_viewer {
            self.viewer_tex = Some(tex);
            self.viewer_zoom = 1.0_f32;
            self.viewer_pan = egui::Vec2::ZERO;
            self.viewer_open = true;
        }
        if let Some((cid, add)) = fav_req {
            if add {
                if !self.favorites.contains(&cid) {
                    self.favorites.push(cid);
                }
            } else {
                self.favorites.retain(|x| *x != cid);
            }
        }
        if let Some(a) = ctx_action {
            match a {
                CtxAction::Reply(m) => {
                    self.reply_to = Some(ReplyRef { id: m.id, from: m.from, text: m.text });
                }
                CtxAction::Edit(m) => {
                    self.editing = Some(m.id);
                    self.draft = m.text.clone();
                    self.reply_to = None;
                }
                CtxAction::Delete(mid) => self.delete_message(ctx, mid),
            }
        }
        if do_send {
            self.send_current(ctx);
        }
        if do_attach {
            self.pick_attach(ctx);
        }
        if let Some((mid, emoji)) = react_req {
            self.toggle_reaction(ctx, mid, emoji);
        }
        if let Some(mid) = pin_req {
            self.toggle_pin(ctx, mid);
        }
        if let Some((text, from)) = forward_req {
            self.forward_pending = Some((text, from));
        }
        if let Some(t) = notice_text {
            self.notice(&t);
        }
        if let Some(tab) = tab_req {
            self.left_tab = tab;
            match tab {
                LeftTab::Chats => self.need_chats = true,
                LeftTab::Contacts => self.need_contacts = true,
            }
        }
        if let Some(ref name) = do_dm_with {
            self.create_dm_with(ctx, name.clone());
        }
        if open_group_win {
            self.group_open = true;
        }
        if open_dm_win {
            self.dm_win = true;
        }
        if let Some(id) = delete_req {
            self.leave_chat(ctx, id);
        }
        if let Some((id, ref name, is_dm)) = open_req {
            self.open_chat(ctx, id, name.clone(), is_dm);
        }
        if do_upload_avatar {
            self.upload_avatar(ctx);
        }
        if do_clear_avatar {
            self.clear_avatar(ctx);
        }

        // ---- windows ----
        if self.menu_open {
            let mut open = self.menu_open;
            egui::Window::new("Меню").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_max_width(300.0);
                ui.label(egui::RichText::new(format!("@{}", self.my_name)).strong());
                ui.add_space(4.0);
                ui.label(egui::RichText::new("Сервер").small().weak());
                ui.text_edit_singleline(&mut self.server);
                ui.label(egui::RichText::new("Соединение").small().weak());
                ui.label(egui::RichText::new(&self.ws_status).small().weak());
                ui.add_space(6.0);
                if ui.button("Загрузить аватар").clicked() {
                    do_upload_avatar = true;
                }
                if ui.button("Убрать аватар").clicked() {
                    do_clear_avatar = true;
                }
                ui.add_space(8.0);
                ui.separator();
                if ui.button("Новое сообщение").clicked() {
                    self.dm_win = true;
                }
                if ui.button("Создать группу").clicked() {
                    self.group_open = true;
                }
                ui.add_space(8.0);
                ui.separator();
                ui.label(egui::RichText::new("Тема оформления").small().weak());
                ui.horizontal(|ui| {
                    if ui.selectable_label(self.dark, "Тёмная").clicked() {
                        self.dark = true;
                    }
                    if ui.selectable_label(!self.dark, "Светлая").clicked() {
                        self.dark = false;
                    }
                });
                ui.label(egui::RichText::new("Оттенок").small().weak());
                ui.add(egui::Slider::new(&mut self.hue, 0.0..=360.0));
                ui.label(egui::RichText::new("Насыщенность").small().weak());
                ui.add(egui::Slider::new(&mut self.sat, 0.0..=1.0));
                ui.label(egui::RichText::new("Яркость").small().weak());
                ui.add(egui::Slider::new(&mut self.val, 0.2..=1.0));
                ui.add_space(6.0);
                if ui.button("Сбросить тему").clicked() {
                    do_reset_theme = true;
                }
                ui.add_space(8.0);
                if ui.button("Выйти из аккаунта").clicked() {
                    do_logout = true;
                }
            });
            self.menu_open = open;
        }
        if self.search_open {
            let mut open = self.search_open;
            egui::Window::new("Поиск").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_max_width(300.0);
                ui.text_edit_singleline(&mut self.search_q);
                ui.add_space(6.0);
                ui.separator();
                let q = self.search_q.trim().to_lowercase();
                egui::ScrollArea::vertical().max_height(260.0).auto_shrink([false, false]).show(ui, |ui| {
                    if q.is_empty() {
                        ui.label(egui::RichText::new("Начните печатать…").weak());
                        return;
                    }
                    for c in self.chats.iter().filter(|c| c.name.to_lowercase().contains(&q)).cloned().collect::<Vec<Chat>>() {
                        if ui.button(format!("Чат: {}", c.name)).clicked() {
                            open_req = Some((c.id, c.name.clone(), c.is_dm));
                            self.search_open = false;
                        }
                    }
                    for ct in self.contacts.iter().filter(|c| c.username.to_lowercase().contains(&q)).cloned().collect::<Vec<Contact>>() {
                        if ui.button(format!("@{}", ct.username)).clicked() {
                            do_dm_with = Some(ct.username.clone());
                            self.search_open = false;
                        }
                    }
                });
            });
            self.search_open = open;
        }
        if self.dm_win {
            let mut open = self.dm_win;
            let mut do_open = false;
            egui::Window::new("Новое сообщение").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_max_width(280.0);
                ui.label(egui::RichText::new("Имя пользователя").small().weak());
                ui.text_edit_singleline(&mut self.dm_name);
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Открыть").clicked() {
                        do_open = true;
                    }
                    if ui.button("Отмена").clicked() {
                        self.dm_win = false;
                    }
                });
            });
            self.dm_win = open;
            if do_open {
                let name = self.dm_name.trim().trim_start_matches('@').to_string();
                self.dm_name.clear();
                self.dm_win = false;
                if !name.is_empty() && name != self.my_name {
                    self.create_dm_with(ctx, name);
                }
            }
        }
        if self.group_open {
            let mut open = self.group_open;
            let mut do_create = false;
            egui::Window::new("Новая группа").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_max_width(300.0);
                ui.label(egui::RichText::new("Название группы").small().weak());
                ui.text_edit_singleline(&mut self.group_name);
                ui.add_space(4.0);
                ui.label(egui::RichText::new("Участники (имена через запятую)").small().weak());
                ui.text_edit_singleline(&mut self.group_members);
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui.button("Создать").clicked() {
                        do_create = true;
                    }
                    if ui.button("Отмена").clicked() {
                        self.group_open = false;
                    }
                });
            });
            self.group_open = open;
            if do_create {
                self.create_group(ctx);
            }
        }
        if let Some((text, from)) = self.forward_pending.clone() {
            let mut open = true;
            let mut send_fwd: Option<i64> = None;
            egui::Window::new("Переслать в…").open(&mut open).collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_max_width(260.0);
                egui::ScrollArea::vertical().max_height(240.0).auto_shrink([false, false]).show(ui, |ui| {
                    for c in &self.chats {
                        if ui.button(c.name.clone()).clicked() {
                            send_fwd = Some(c.id);
                        }
                    }
                });
            });
            if !open {
                self.forward_pending = None;
            }
            if let Some(cid) = send_fwd {
                self.send_forward(ctx, cid, text, from);
                self.forward_pending = None;
            }
        }
        if do_logout {
            self.logout(ctx);
        }
        if do_reset_theme {
            self.hue = 210.0;
            self.sat = 0.65;
            self.val = 0.9;
        }
    }
}

impl Default for MyMessenger {
    fn default() -> Self {
        Self {
            screen: Screen::Auth,
            server: "http://127.0.0.1:3001".into(),
            username: String::new(),
            password: String::new(),
            email: String::new(),
            auth_mode: AuthMode::Welcome,
            error: String::new(),
            error_shown: String::new(),
            error_at: None,
            offline: false,
            info: String::new(),
            info_shown: String::new(),
            info_at: None,
            token: None,
            my_name: String::new(),
            rt: None,
            auth_rx: None,
            dm_rx: None,
            chats_rx: None,
            msg_rx: None,
            ack_rx: None,
            send_rx: None,
            contacts_rx: None,
            avatars_rx: None,
            need_chats: true,
            need_contacts: true,
            need_avatars: false,
            chats: Vec::new(),
            contacts: Vec::new(),
            open_chat: None,
            open_name: String::new(),
            open_is_dm: false,
            messages: Vec::new(),
            other_read: 0,
            pinned: None,
            draft: String::new(),
            reply_to: None,
            editing: None,
            dm_win: false,
            dm_name: String::new(),
            group_open: false,
            group_name: String::new(),
            group_members: String::new(),
            pending_chat_id: None,
            forward_pending: None,
            ws_send: None,
            ws_recv: None,
            ws_status: String::new(),
            last_ws_attempt: Instant::now(),
            ws_backoff: Duration::from_secs(1),
            pending_send_text: None,
            online: HashMap::new(),
            typing: None,
            last_typing_sent: Instant::now(),
            last_chat_poll: Instant::now(),
            avatar_data: HashMap::new(),
            avatar_tex: HashMap::new(),
            pending_attach: None,
            attach_preview: None,
            attach_tex: HashMap::new(),
            viewer_open: false,
            viewer_tex: None,
            viewer_zoom: 1.0,
            viewer_pan: egui::Vec2::ZERO,
            dark: true,
            hue: 210.0,
            sat: 0.65,
            val: 0.9,
            left_tab: LeftTab::Chats,
            favorites: Vec::new(),
            show_favorites: false,
            search_open: false,
            search_q: String::new(),
            chat_search_open: false,
            chat_search: String::new(),
            emoji_open: false,
            menu_open: false,
        }
    }
}

impl eframe::App for MyMessenger {
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        storage.set_string("server", self.server.clone());
        storage.set_string("username", self.username.clone());
        storage.set_string("token", self.token.clone().unwrap_or_default());
        storage.set_string("dark", self.dark.to_string());
        storage.set_string("hue", self.hue.to_string());
        storage.set_string("sat", self.sat.to_string());
        storage.set_string("val", self.val.to_string());
        let favs: Vec<String> = self.favorites.iter().map(|x| x.to_string()).collect();
        storage.set_string("favs", favs.join(","));
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.apply_theme(ctx);
        self.poll(ctx);
        if self.viewer_open {
            let wheel = ctx.input(|i| i.smooth_scroll_delta.y);
            if wheel != 0.0 {
                self.viewer_zoom = (self.viewer_zoom * (1.0_f32 + wheel * 0.0015_f32)).clamp(0.2_f32, 12.0_f32);
            }
            ctx.input_mut(|i| i.smooth_scroll_delta = egui::Vec2::ZERO);
        }
        if self.screen == Screen::Main && self.last_chat_poll.elapsed() > Duration::from_secs(5) {
            self.last_chat_poll = Instant::now();
            match self.left_tab {
                LeftTab::Contacts => self.fetch_contacts(ctx),
                LeftTab::Chats => self.fetch_chats(ctx),
            }
        }
        if let Some(cid) = self.open_chat {
            if !self.draft.is_empty() && self.last_typing_sent.elapsed() > Duration::from_secs(2) {
                self.last_typing_sent = Instant::now();
                self.ws_send_text(format!("{{\"type\":\"typing\",\"chat_id\":{}}}", cid));
            }
        }
        match self.screen {
            Screen::Auth => self.ui_auth(ctx),
            Screen::Main => self.ui_main(ctx),
        }
        self.draw_image_viewer(ctx);
        if !self.error.is_empty() && self.error != self.error_shown {
            if self.error_at.is_none() {
                self.error_at = Some(Instant::now());
            }
            let show = self.error_at.map(|t| t.elapsed().as_secs() < 5).unwrap_or(false);
            if show {
                egui::Area::new(egui::Id::new("err_toast"))
                    .anchor(egui::Align2::LEFT_BOTTOM, egui::vec2(12.0, -12.0))
                    .show(ctx, |ui| {
                        egui::Frame::none()
                            .fill(egui::Color32::from_rgb(60, 20, 20))
                            .rounding(10.0_f32)
                            .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                            .show(ui, |ui| {
                                ui.colored_label(egui::Color32::from_rgb(255, 150, 150), &self.error);
                            });
                    });
            } else {
                self.error_shown = self.error.clone();
                self.error_at = None;
            }
        }
        if !self.info.is_empty() && self.info != self.info_shown {
            if self.info_at.is_none() {
                self.info_at = Some(Instant::now());
            }
            let show = self.info_at.map(|t| t.elapsed().as_secs() < 4).unwrap_or(false);
            if show {
                egui::Area::new(egui::Id::new("info_toast"))
                    .anchor(egui::Align2::LEFT_BOTTOM, egui::vec2(12.0, -56.0))
                    .show(ctx, |ui| {
                        egui::Frame::none()
                            .fill(egui::Color32::from_gray(40))
                            .rounding(10.0_f32)
                            .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                            .show(ui, |ui| {
                                ui.colored_label(egui::Color32::from_gray(230), &self.info);
                            });
                    });
            } else {
                self.info_shown = self.info.clone();
                self.info_at = None;
            }
        }
        if self.offline {
            egui::Area::new(egui::Id::new("offline_banner"))
                .anchor(egui::Align2::CENTER_TOP, egui::vec2(0.0, 4.0))
                .show(ctx, |ui| {
                    egui::Frame::none()
                        .fill(egui::Color32::from_gray(60))
                        .rounding(8.0_f32)
                        .inner_margin(egui::Margin::symmetric(10.0, 4.0))
                        .show(ui, |ui| {
                            ui.label(
                                egui::RichText::new("Нет связи с сервером — переподключаюсь…")
                                    .small()
                                    .color(egui::Color32::from_gray(200)),
                            );
                        });
                });
        }
        ctx.request_repaint_after(Duration::from_millis(150));
    }
}

fn main() -> eframe::Result<()> {
    let mut app = MyMessenger::default();
    app.rt = Some(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime"),
    );
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1040.0, 680.0])
            .with_min_inner_size([760.0, 500.0]),
        ..Default::default()
    };
    eframe::run_native(
        "My Messenger",
        options,
        Box::new(move |cc| {
            if let Some(storage) = cc.storage {
                app.server = storage.get_string("server").unwrap_or_else(|| "http://127.0.0.1:3001".into());
                app.username = storage.get_string("username").unwrap_or_default();
                app.token = storage.get_string("token").filter(|t| !t.is_empty());
                app.dark = storage.get_string("dark").and_then(|v| v.parse().ok()).unwrap_or(true);
                app.hue = storage.get_string("hue").and_then(|v| v.parse().ok()).unwrap_or(210.0);
                app.sat = storage.get_string("sat").and_then(|v| v.parse().ok()).unwrap_or(0.65);
                app.val = storage.get_string("val").and_then(|v| v.parse().ok()).unwrap_or(0.9);
                app.favorites = storage
                    .get_string("favs")
                    .unwrap_or_default()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| s.parse().ok())
                    .collect();
            }
            for p in [
                "C:\\Windows\\Fonts\\seguiemj.ttf",
                "/usr/share/fonts/truetype/noto/NotoEmoji-Regular.ttf",
                "/usr/share/fonts/noto/NotoEmoji-Regular.ttf",
            ] {
                if let Ok(bytes) = std::fs::read(p) {
                    let mut fonts = egui::FontDefinitions::default();
                    fonts.font_data.insert("emoji".to_owned(), egui::FontData::from_owned(bytes));
                    if let Some(v) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
                        v.push("emoji".to_owned());
                    }
                    cc.egui_ctx.set_fonts(fonts);
                    break;
                }
            }
            let mut server = app.server.clone();
            let mut username = app.username.clone();
            let mut token = app.token.clone();
            if let Some((s, u, t)) = load_session_local() {
                server = s;
                username = u;
                token = Some(t);
            }
            app.server = server;
            app.username = username;
            app.token = token;
            if app.token.is_some() {
                app.my_name = app.username.clone();
                app.screen = Screen::Main;
                app.need_chats = true;
                app.need_avatars = true;
            }
            Ok(Box::new(app))
        }),
    )
}