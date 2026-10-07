use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

const MAX_MESSAGE_LEN: usize = 4096;
const MAX_ATTACH_LEN: usize = 4_000_000;

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
struct OutMessage {
    id: i64,
    chat_id: i64,
    from: String,
    text: String,
    ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply_to: Option<ReplyRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    forward_from: Option<String>,
    #[serde(default)]
    edited: bool,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    reactions: Vec<Reaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attach: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type")]
enum Event {
    #[serde(rename = "message")]
    Message(OutMessage),
    #[serde(rename = "typing")]
    Typing { chat_id: i64, from: String },
    #[serde(rename = "read")]
    Read { chat_id: i64, by: String, msg_id: i64 },
    #[serde(rename = "presence")]
    Presence { user: String, online: bool },
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ClientEvent {
    #[serde(rename = "typing")]
    Typing { chat_id: i64 },
}

#[derive(Serialize)]
struct ChatDto {
    id: i64,
    name: String,
    is_dm: bool,
    last: Option<String>,
    unread: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    online: Option<bool>,
}

#[derive(Serialize)]
struct UserDto {
    username: String,
    online: bool,
    has_avatar: bool,
}

#[derive(Serialize)]
struct AvatarDto {
    username: String,
    data: String,
}

#[derive(Deserialize)]
struct AuthReq {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct SendReq {
    text: String,
    #[serde(default)]
    reply_to: Option<i64>,
    #[serde(default)]
    forward_from: Option<String>,
    #[serde(default)]
    attach: Option<String>,
}

#[derive(Deserialize)]
struct DmReq {
    username: String,
}

#[derive(Deserialize)]
struct GroupReq {
    name: String,
    #[serde(default)]
    members: Vec<String>,
}

#[derive(Deserialize)]
struct ReadReq {
    msg_id: i64,
}

#[derive(Deserialize)]
struct WsQuery {
    token: String,
}

#[derive(Deserialize)]
struct ReactionReq {
    emoji: String,
}

#[derive(Deserialize)]
struct AvatarReq {
    data: String,
}

// ---------- State ----------
struct AppState {
    db: Mutex<Connection>,
    connections: Mutex<HashMap<i64, usize>>,
    tx: broadcast::Sender<Event>,
}

fn data_dir() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|q| q.to_path_buf()))
        .unwrap_or_else(|| std::path::PathBuf::from("."))
}

fn init_db() -> Connection {
    let path = data_dir().join("my_messenger.db");
    let db = Connection::open(&path).expect("open db");

    db.execute_batch(
        "PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            pass_hash TEXT NOT NULL,
            avatar TEXT
        );

        CREATE TABLE IF NOT EXISTS chats(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            is_dm INTEGER NOT NULL DEFAULT 0,
            created_by INTEGER
        );

        CREATE TABLE IF NOT EXISTS chat_members(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            ts INTEGER NOT NULL,
            reply_to INTEGER,
            edited INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            forward_from TEXT,
            pinned INTEGER NOT NULL DEFAULT 0,
            attach TEXT
        );

        CREATE TABLE IF NOT EXISTS last_read(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            msg_id INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS sessions(
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS reactions(
            message_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            PRIMARY KEY(message_id, user_id, emoji)
        );",
    )
        .expect("init db");

    db.execute("ALTER TABLE messages ADD COLUMN reply_to INTEGER", params![]).ok();
    db.execute("ALTER TABLE messages ADD COLUMN edited INTEGER NOT NULL DEFAULT 0", params![]).ok();
    db.execute("ALTER TABLE messages ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0", params![]).ok();
    db.execute("ALTER TABLE chats ADD COLUMN created_by INTEGER", params![]).ok();
    db.execute("ALTER TABLE users ADD COLUMN avatar TEXT", params![]).ok();
    db.execute("ALTER TABLE messages ADD COLUMN forward_from TEXT", params![]).ok();
    db.execute("ALTER TABLE messages ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0", params![]).ok();
    db.execute("ALTER TABLE messages ADD COLUMN attach TEXT", params![]).ok();

    db.execute("INSERT OR IGNORE INTO chats(id, name) VALUES(1, 'Global')", params![]).ok();

    db
}

const MSG_SELECT: &str = "
SELECT m.id, m.chat_id, u.username, m.text, m.ts, m.edited, m.deleted, m.reply_to,
ru.username, rm.text, rm.deleted, m.forward_from, m.pinned, m.attach
FROM messages m
JOIN users u ON u.id = m.user_id
LEFT JOIN messages rm ON rm.id = m.reply_to
LEFT JOIN users ru ON ru.id = rm.user_id
";

fn row_to_message(r: &rusqlite::Row) -> rusqlite::Result<OutMessage> {
    let reply_to: Option<i64> = r.get(7)?;
    let rfrom: Option<String> = r.get(8)?;
    let rtext: Option<String> = r.get(9)?;
    let rdel: Option<i64> = r.get(10)?;
    let forward_from: Option<String> = r.get(11)?;
    let pinned: i64 = r.get(12)?;

    Ok(OutMessage {
        id: r.get(0)?,
        chat_id: r.get(1)?,
        from: r.get(2)?,
        text: r.get(3)?,
        ts: r.get(4)?,
        edited: r.get(5)?,
        deleted: r.get(6)?,
        reply_to: reply_to.map(|id| ReplyRef {
            id,
            from: rfrom.unwrap_or_default(),
            text: if rdel.unwrap_or(0) == 1 {
                "сообщение удалено".into()
            } else {
                rtext.unwrap_or_default()
            },
        }),
        forward_from,
        pinned: pinned == 1,
        reactions: Vec::new(),
        attach: r.get(13)?,
    })
}

fn attach_reactions(db: &Connection, msgs: &mut [OutMessage], uid: i64) {
    if msgs.is_empty() {
        return;
    }

    let ph: String = msgs.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "SELECT r.message_id, r.emoji, COUNT(*),
         SUM(CASE WHEN r.user_id = ?1 THEN 1 ELSE 0 END)
         FROM reactions r WHERE r.message_id IN ({})
         GROUP BY r.message_id, r.emoji
         ORDER BY r.message_id, COUNT(*) DESC",
        ph
    );

    let mut args: Vec<i64> = vec![uid];
    args.extend(msgs.iter().map(|m| m.id));

    if let Ok(mut stmt) = db.prepare(&sql) {
        if let Ok(rows) = stmt.query_map(params_from_iter(args), |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        }) {
            for row in rows.flatten() {
                let (mid, emoji, count, mine) = row;
                if let Some(m) = msgs.iter_mut().find(|m| m.id == mid) {
                    m.reactions.push(Reaction {
                        emoji,
                        count,
                        mine: mine > 0,
                    });
                }
            }
        }
    }
}

fn load_message(db: &Connection, mid: i64, uid: i64) -> Option<OutMessage> {
    let mut m = db
        .query_row(&format!("{} WHERE m.id = ?1", MSG_SELECT), params![mid], row_to_message)
        .ok()?;
    attach_reactions(db, std::slice::from_mut(&mut m), uid);
    Some(m)
}

fn hash_password(pw: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(pw.as_bytes(), &salt)
        .expect("hash")
        .to_string()
}

fn verify_password(hash: &str, pw: &str) -> bool {
    match PasswordHash::new(hash) {
        Ok(parsed) => Argon2::default().verify_password(pw.as_bytes(), &parsed).is_ok(),
        Err(_) => false,
    }
}

fn auth_user(st: &AppState, headers: &HeaderMap) -> Option<i64> {
    let h = headers.get("authorization")?.to_str().ok()?;
    let token = h.strip_prefix("Bearer ")?;
    let db = st.db.lock().ok()?;
    db.query_row(
        "SELECT user_id FROM sessions WHERE token=?1",
        params![token],
        |r| r.get(0),
    )
        .ok()
}

fn create_session(db: &Connection, uid: i64) -> String {
    let token = Uuid::new_v4().to_string();
    let ts = now_ts();
    db.execute(
        "INSERT INTO sessions(token, user_id, created_at) VALUES(?1, ?2, ?3)",
        params![token, uid, ts],
    )
        .ok();
    token
}

fn is_member(db: &Connection, chat_id: i64, uid: i64) -> bool {
    db.query_row(
        "SELECT 1 FROM chat_members WHERE chat_id=?1 AND user_id=?2",
        params![chat_id, uid],
        |_| Ok(()),
    )
        .is_ok()
}

fn username_of(db: &Connection, uid: i64) -> String {
    db.query_row("SELECT username FROM users WHERE id=?1", params![uid], |r| r.get(0))
        .unwrap_or_else(|_| "unknown".into())
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

// ---------- Handlers ----------
async fn register(State(st): State<Arc<AppState>>, Json(req): Json<AuthReq>) -> Response {
    let username = req.username.trim().to_string();
    if username.is_empty() || req.password.len() < 4 {
        return (StatusCode::BAD_REQUEST, "username required, password >= 4 chars").into_response();
    }

    let pass_hash = hash_password(&req.password);
    let db = st.db.lock().unwrap();

    match db.execute(
        "INSERT INTO users(username, pass_hash) VALUES(?1, ?2)",
        params![username, pass_hash],
    ) {
        Ok(_) => {
            let uid = db.last_insert_rowid();
            db.execute(
                "INSERT OR IGNORE INTO chat_members(chat_id, user_id) VALUES(1, ?1)",
                params![uid],
            )
                .ok();

            let token = create_session(&db, uid);
            Json(json!({
                "token": token,
                "id": uid,
                "username": username
            }))
                .into_response()
        }
        Err(_) => (StatusCode::CONFLICT, "username already taken").into_response(),
    }
}

async fn login(State(st): State<Arc<AppState>>, Json(req): Json<AuthReq>) -> Response {
    let username = req.username.trim().to_string();

    let row: Option<(i64, String)> = {
        let db = st.db.lock().unwrap();
        db.query_row(
            "SELECT id, pass_hash FROM users WHERE username=?1",
            params![username],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
            .ok()
    };

    match row {
        Some((uid, hash)) if verify_password(&hash, &req.password) => {
            let token = {
                let db = st.db.lock().unwrap();
                create_session(&db, uid)
            };

            Json(json!({
                "token": token,
                "id": uid,
                "username": username
            }))
                .into_response()
        }
        _ => (StatusCode::UNAUTHORIZED, "invalid credentials").into_response(),
    }
}

async fn logout(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let h = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = h.strip_prefix("Bearer ").unwrap_or("");

    let db = st.db.lock().unwrap();
    db.execute("DELETE FROM sessions WHERE token=?1", params![token]).ok();

    Json(json!({ "ok": true })).into_response()
}

async fn me(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();
    Json(json!({
        "id": uid,
        "username": username_of(&db, uid)
    }))
        .into_response()
}

async fn users(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(_uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();
    let conns = st.connections.lock().unwrap();

    let mut stmt = db
        .prepare(
            "SELECT id, username, (avatar IS NOT NULL AND avatar != '') FROM users ORDER BY username",
        )
        .unwrap();

    let list: Vec<UserDto> = stmt
        .query_map(params![], |r| {
            let id: i64 = r.get(0)?;
            Ok(UserDto {
                username: r.get(1)?,
                online: conns.contains_key(&id),
                has_avatar: r.get::<_, i64>(2)? == 1,
            })
        })
        .unwrap()
        .filter_map(|u| u.ok())
        .collect();

    Json(list).into_response()
}

async fn set_avatar(State(st): State<Arc<AppState>>, headers: HeaderMap, Json(req): Json<AvatarReq>) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    if req.data.len() > 2_000_000 {
        return (StatusCode::BAD_REQUEST, "avatar too big").into_response();
    }

    let db = st.db.lock().unwrap();
    db.execute("UPDATE users SET avatar=?1 WHERE id=?2", params![req.data, uid]).ok();

    Json(json!({ "ok": true })).into_response()
}

async fn get_avatars(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(_uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();

    let mut stmt = db
        .prepare("SELECT username, avatar FROM users WHERE avatar IS NOT NULL AND avatar != ''")
        .unwrap();

    let list: Vec<AvatarDto> = stmt
        .query_map(params![], |r| {
            Ok(AvatarDto {
                username: r.get(0)?,
                data: r.get(1)?,
            })
        })
        .unwrap()
        .filter_map(|a| a.ok())
        .collect();

    Json(list).into_response()
}

async fn chats(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();

    let mut stmt = db
        .prepare(
            "SELECT c.id, c.name, c.is_dm,
             (SELECT CASE WHEN m.text = '' AND m.attach IS NOT NULL THEN '📷 Фото' ELSE m.text END
                FROM messages m WHERE m.chat_id = c.id AND m.deleted = 0
                ORDER BY m.ts DESC, m.id DESC LIMIT 1),
             (SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.id AND m.deleted = 0
                AND m.user_id != ?1 AND m.id > COALESCE((SELECT msg_id FROM last_read lr
                  WHERE lr.chat_id = c.id AND lr.user_id = ?1), 0)),
             (SELECT cm2.user_id FROM chat_members cm2 WHERE cm2.chat_id = c.id AND cm2.user_id != ?1 LIMIT 1)
             FROM chats c JOIN chat_members cm ON cm.chat_id = c.id
             WHERE cm.user_id = ?1
             ORDER BY COALESCE((SELECT MAX(ts) FROM messages WHERE chat_id = c.id), 0) DESC, c.id",
        )
        .unwrap();

    let conns = st.connections.lock().unwrap();

    let list: Vec<ChatDto> = stmt
        .query_map(params![uid], |r| {
            let is_dm: i64 = r.get(2)?;
            let other: Option<i64> = r.get(5)?;

            Ok(ChatDto {
                id: r.get(0)?,
                name: r.get(1)?,
                is_dm: is_dm == 1,
                last: r.get(3)?,
                unread: r.get(4)?,
                online: if is_dm == 1 {
                    Some(other.map(|o| conns.contains_key(&o)).unwrap_or(false))
                } else {
                    None
                },
            })
        })
        .unwrap()
        .filter_map(|c| c.ok())
        .collect();

    Json(list).into_response()
}

async fn get_messages(State(st): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();

    if !is_member(&db, chat_id, uid) {
        return (StatusCode::FORBIDDEN, "not a member of this chat").into_response();
    }

    let mut stmt = db
        .prepare(&format!(
            "{} WHERE m.chat_id = ?1 ORDER BY m.ts, m.id",
            MSG_SELECT
        ))
        .unwrap();

    let mut msgs: Vec<OutMessage> = stmt
        .query_map(params![chat_id], row_to_message)
        .unwrap()
        .filter_map(|m| m.ok())
        .collect();

    attach_reactions(&db, &mut msgs, uid);

    let other_read: i64 = db
        .query_row(
            "SELECT COALESCE(MAX(msg_id),0) FROM last_read WHERE chat_id=?1 AND user_id != ?2",
            params![chat_id, uid],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let pinned: Option<OutMessage> = db
        .query_row(
            &format!(
                "{} WHERE m.chat_id = ?1 AND m.pinned = 1 ORDER BY m.id DESC LIMIT 1",
                MSG_SELECT
            ),
            params![chat_id],
            row_to_message,
        )
        .ok();

    let mut pv = pinned.map(|p| vec![p]).unwrap_or_default();
    attach_reactions(&db, &mut pv, uid);
    let pinned = pv.into_iter().next();

    Json(json!({
        "messages": msgs,
        "other_read": other_read,
        "pinned": pinned
    }))
        .into_response()
}

async fn send_message(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(chat_id): Path<i64>,
    Json(req): Json<SendReq>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let text = req.text.trim().to_string();

    if text.is_empty() && req.attach.is_none() {
        return (StatusCode::BAD_REQUEST, "empty message").into_response();
    }

    if text.len() > MAX_MESSAGE_LEN {
        return (StatusCode::BAD_REQUEST, "message too long").into_response();
    }

    if let Some(a) = &req.attach {
        if a.len() > MAX_ATTACH_LEN {
            return (StatusCode::BAD_REQUEST, "attachment too big").into_response();
        }
    }

    let msg = {
        let db = st.db.lock().unwrap();

        if !is_member(&db, chat_id, uid) {
            return (StatusCode::FORBIDDEN, "not a member of this chat").into_response();
        }

        if let Some(rid) = req.reply_to {
            let valid = db
                .query_row(
                    "SELECT 1 FROM messages WHERE id=?1 AND chat_id=?2",
                    params![rid, chat_id],
                    |_| Ok(()),
                )
                .is_ok();

            if !valid {
                return (StatusCode::BAD_REQUEST, "bad reply_to").into_response();
            }
        }

        let ts = now_ts();

        db.execute(
            "INSERT INTO messages(chat_id, user_id, text, ts, reply_to, forward_from, attach)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![chat_id, uid, text, ts, req.reply_to, req.forward_from, req.attach],
        )
            .unwrap();

        let mid = db.last_insert_rowid();
        load_message(&db, mid, uid)
    };

    match msg {
        Some(m) => {
            let _ = st.tx.send(Event::Message(m.clone()));
            Json(m).into_response()
        }
        None => (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
    }
}

async fn edit_message(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, mid)): Path<(i64, i64)>,
    Json(req): Json<SendReq>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let text = req.text.trim().to_string();

    if text.is_empty() || text.len() > MAX_MESSAGE_LEN {
        return (StatusCode::BAD_REQUEST, "bad text").into_response();
    }

    let msg = {
        let db = st.db.lock().unwrap();

        let owner: Option<i64> = db
            .query_row(
                "SELECT user_id FROM messages WHERE id=?1 AND chat_id=?2",
                params![mid, chat_id],
                |r| r.get(0),
            )
            .ok();

        if owner != Some(uid) {
            return (StatusCode::FORBIDDEN, "not your message").into_response();
        }

        db.execute(
            "UPDATE messages SET text=?1, edited=1 WHERE id=?2",
            params![text, mid],
        )
            .ok();

        load_message(&db, mid, uid)
    };

    match msg {
        Some(m) => {
            let _ = st.tx.send(Event::Message(m.clone()));
            Json(m).into_response()
        }
        None => (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
    }
}

async fn delete_message(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, mid)): Path<(i64, i64)>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let msg = {
        let db = st.db.lock().unwrap();

        let owner: Option<i64> = db
            .query_row(
                "SELECT user_id FROM messages WHERE id=?1 AND chat_id=?2",
                params![mid, chat_id],
                |r| r.get(0),
            )
            .ok();

        if owner != Some(uid) {
            return (StatusCode::FORBIDDEN, "not your message").into_response();
        }

        db.execute(
            "UPDATE messages SET deleted=1, text='', attach=NULL WHERE id=?1",
            params![mid],
        )
            .ok();

        load_message(&db, mid, uid)
    };

    match msg {
        Some(m) => {
            let _ = st.tx.send(Event::Message(m.clone()));
            Json(m).into_response()
        }
        None => (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
    }
}

async fn toggle_reaction(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, mid)): Path<(i64, i64)>,
    Json(req): Json<ReactionReq>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let emoji = req.emoji.trim().to_string();

    if emoji.is_empty() || emoji.chars().count() > 8 {
        return (StatusCode::BAD_REQUEST, "bad emoji").into_response();
    }

    let msg = {
        let db = st.db.lock().unwrap();

        if !is_member(&db, chat_id, uid) {
            return (StatusCode::FORBIDDEN, "not a member of this chat").into_response();
        }

        let exists = db
            .query_row(
                "SELECT 1 FROM reactions WHERE message_id=?1 AND user_id=?2 AND emoji=?3",
                params![mid, uid, emoji],
                |_| Ok(()),
            )
            .is_ok();

        if exists {
            db.execute(
                "DELETE FROM reactions WHERE message_id=?1 AND user_id=?2 AND emoji=?3",
                params![mid, uid, emoji],
            )
                .ok();
        } else {
            db.execute(
                "INSERT INTO reactions(message_id, user_id, emoji) VALUES(?1, ?2, ?3)",
                params![mid, uid, emoji],
            )
                .ok();
        }

        load_message(&db, mid, uid)
    };

    match msg {
        Some(m) => {
            let _ = st.tx.send(Event::Message(m.clone()));
            Json(m).into_response()
        }
        None => (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
    }
}

async fn toggle_pin(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, mid)): Path<(i64, i64)>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let msg = {
        let db = st.db.lock().unwrap();

        if !is_member(&db, chat_id, uid) {
            return (StatusCode::FORBIDDEN, "not a member of this chat").into_response();
        }

        db.execute(
            "UPDATE messages SET pinned = CASE WHEN pinned = 1 THEN 0 ELSE 1 END WHERE id=?1",
            params![mid],
        )
            .ok();

        load_message(&db, mid, uid)
    };

    match msg {
        Some(m) => {
            let _ = st.tx.send(Event::Message(m.clone()));
            Json(m).into_response()
        }
        None => (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
    }
}

async fn mark_read(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(chat_id): Path<i64>,
    Json(req): Json<ReadReq>,
) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let by = {
        let db = st.db.lock().unwrap();

        if !is_member(&db, chat_id, uid) {
            return (StatusCode::FORBIDDEN, "not a member of this chat").into_response();
        }

        db.execute(
            "INSERT INTO last_read(chat_id, user_id, msg_id) VALUES(?1, ?2, ?3)
             ON CONFLICT(chat_id, user_id) DO UPDATE SET msg_id = excluded.msg_id",
            params![chat_id, uid, req.msg_id],
        )
            .ok();

        username_of(&db, uid)
    };

    let _ = st.tx.send(Event::Read {
        chat_id,
        by,
        msg_id: req.msg_id,
    });

    Json(json!({ "ok": true })).into_response()
}

async fn leave_chat(State(st): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let db = st.db.lock().unwrap();

    if !is_member(&db, chat_id, uid) {
        return (StatusCode::NOT_FOUND, "chat not found").into_response();
    }

    if db
        .execute(
            "DELETE FROM chat_members WHERE chat_id=?1 AND user_id=?2",
            params![chat_id, uid],
        )
        .is_err()
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to leave chat").into_response();
    }

    Json(json!({ "ok": true })).into_response()
}

async fn create_dm(State(st): State<Arc<AppState>>, headers: HeaderMap, Json(req): Json<DmReq>) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let other_name = req.username.trim().trim_start_matches('@').to_string();
    let db = st.db.lock().unwrap();

    let oid: Option<i64> = db
        .query_row("SELECT id FROM users WHERE username=?1", params![other_name], |r| r.get(0))
        .ok();

    let Some(oid) = oid else {
        return (StatusCode::NOT_FOUND, "user not found").into_response();
    };

    if oid == uid {
        return (StatusCode::BAD_REQUEST, "cannot create chat with yourself").into_response();
    }

    let existing: Option<i64> = db
        .query_row(
            "SELECT c.id FROM chats c
             JOIN chat_members a ON a.chat_id = c.id AND a.user_id = ?1
             JOIN chat_members b ON b.chat_id = c.id AND b.user_id = ?2
             WHERE c.is_dm = 1",
            params![uid, oid],
            |r| r.get(0),
        )
        .ok();

    if let Some(id) = existing {
        return Json(json!({ "chat_id": id })).into_response();
    }

    db.execute(
        "INSERT INTO chats(name, is_dm) VALUES(?1, 1)",
        params![format!("@{}", other_name)],
    )
        .unwrap();

    let cid = db.last_insert_rowid();

    db.execute(
        "INSERT INTO chat_members(chat_id, user_id) VALUES(?1, ?2)",
        params![cid, uid],
    )
        .ok();

    db.execute(
        "INSERT INTO chat_members(chat_id, user_id) VALUES(?1, ?2)",
        params![cid, oid],
    )
        .ok();

    Json(json!({ "chat_id": cid })).into_response()
}

async fn create_group(State(st): State<Arc<AppState>>, headers: HeaderMap, Json(req): Json<GroupReq>) -> Response {
    let Some(uid) = auth_user(&st, &headers) else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };

    let name = req.name.trim().to_string();

    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "group name required").into_response();
    }

    let db = st.db.lock().unwrap();

    db.execute(
        "INSERT INTO chats(name, is_dm, created_by) VALUES(?1, 0, ?2)",
        params![name, uid],
    )
        .unwrap();

    let cid = db.last_insert_rowid();

    db.execute(
        "INSERT INTO chat_members(chat_id, user_id) VALUES(?1, ?2)",
        params![cid, uid],
    )
        .ok();

    for m in &req.members {
        let m = m.trim().trim_start_matches('@').to_string();
        if m.is_empty() {
            continue;
        }

        if let Ok(oid) = db.query_row("SELECT id FROM users WHERE username=?1", params![m], |r| r.get::<_, i64>(0)) {
            db.execute(
                "INSERT OR IGNORE INTO chat_members(chat_id, user_id) VALUES(?1, ?2)",
                params![cid, oid],
            )
                .ok();
        }
    }

    Json(json!({ "chat_id": cid })).into_response()
}

// ---------- WebSocket ----------
async fn ws_handler(State(st): State<Arc<AppState>>, Query(q): Query<WsQuery>, ws: WebSocketUpgrade) -> Response {
    let uid = {
        let db = st.db.lock().unwrap();
        db.query_row("SELECT user_id FROM sessions WHERE token=?1", params![q.token], |r| r.get(0))
            .ok()
    };

    match uid {
        Some(uid) => ws.on_upgrade(move |socket| handle_socket(socket, st, uid)),
        None => (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
    }
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, uid: i64) {
    let uname = {
        let db = state.db.lock().unwrap();
        username_of(&db, uid)
    };

    let became_online = {
        let mut c = state.connections.lock().unwrap();
        let entry = c.entry(uid).or_insert(0);
        *entry += 1;
        *entry == 1
    };

    if became_online {
        let _ = state.tx.send(Event::Presence {
            user: uname.clone(),
            online: true,
        });
    }

    let mut rx = state.tx.subscribe();
    let (mut sender, mut receiver) = socket.split();

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(ev) => {
                        let text = serde_json::to_string(&ev).unwrap();
                        if sender.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(ClientEvent::Typing { chat_id }) = serde_json::from_str(&text) {
                            let member = {
                                let db = state.db.lock().unwrap();
                                is_member(&db, chat_id, uid)
                            };

                            if member {
                                let _ = state.tx.send(Event::Typing {
                                    chat_id,
                                    from: uname.clone(),
                                });
                            }
                        }
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    _ => break,
                }
            }
        }
    }

    let became_offline = {
        let mut c = state.connections.lock().unwrap();
        match c.get_mut(&uid) {
            Some(n) => {
                *n -= 1;
                if *n == 0 {
                    c.remove(&uid);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    };

    if became_offline {
        let _ = state.tx.send(Event::Presence {
            user: uname,
            online: false,
        });
    }
}

// ---------- Main ----------
#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel(1024);

    let state = Arc::new(AppState {
        db: Mutex::new(init_db()),
        connections: Mutex::new(HashMap::new()),
        tx,
    });

    let app = Router::new()
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/chats", get(chats))
        .route("/api/users", get(users))
        .route("/api/avatar", post(set_avatar))
        .route("/api/avatars", get(get_avatars))
        .route("/api/chats/:id/messages", get(get_messages).post(send_message))
        .route("/api/chats/:id/read", post(mark_read))
        .route("/api/chats/:id/leave", delete(leave_chat))
        .route(
            "/api/chats/:id/messages/:mid",
            patch(edit_message).delete(delete_message),
        )
        .route("/api/chats/:id/messages/:mid/reaction", post(toggle_reaction))
        .route("/api/chats/:id/messages/:mid/pin", post(toggle_pin))
        .route("/api/dm", post(create_dm))
        .route("/api/groups", post(create_group))
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port: u16 = std::env::var("MESSENGER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001);

    let addr = format!("127.0.0.1:{}", port);

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!();
            eprintln!("========================================");
            eprintln!("  НЕ УДАЛОСЬ ЗАНЯТЬ АДРЕС {}", addr);
            eprintln!("========================================");
            eprintln!("Ошибка: {}", e);
            eprintln!("  1. Уже запущен сервер: taskkill /F /IM my_messenger_server.exe");
            eprintln!("  2. Порт зарезервирован Windows: (админ) net stop winnat && net start winnat");
            eprintln!("  3. Другой порт: set MESSENGER_PORT=8787");
            eprintln!("========================================");
            std::process::exit(1);
        }
    };

    println!("My Messenger server on http://{}", addr);
    axum::serve(listener, app).await.expect("serve");
}