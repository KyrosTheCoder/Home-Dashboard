#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
smarthome_all_in_one.py
========================
Smart Home Dashboard - KOMPLETT in einer einzigen Datei:
PV-Anlage (Fronius), Bitcoin-Miner (Braiins OS + Bitmain), Personen-
erkennung/Kameras (eingebettete Heimüberwachung mit YOLO), Strompreise
(aWATTar), Bitcoin-Kurs, Wetter, Familien-Organisation - inklusive der
kompletten Web-Oberfläche (HTML/CSS/JS als Text in dieser Datei).

Diese Datei wurde automatisch aus den einzelnen Quell-Dateien des
Projekts zusammengesetzt. Jedes ehemalige Modul (database.py, fronius.py,
miners.py, external_apis.py, automation.py, savings.py,
energy_analytics.py, heimueberwachung_engine.py, surveillance.py,
scheduler.py) läuft weiterhin in seinem EIGENEN, isolierten Namensraum
(wie ein echter Python-Import) - nur eben als Text in dieser Datei statt
als eigene .py-Datei. Das verhindert, dass gleichnamige Variablen
verschiedener Module sich gegenseitig überschreiben (z.B. haben mehrere
Module ein eigenes "TIMEOUT" oder "log" - die bleiben garantiert getrennt).

Start:
    pip install -r requirements.txt
    python3 smarthome_all_in_one.py

Läuft dann auf http://<ip-dieses-rechners>:5000

Optionale Zusatzfunktion "Browser-Push-Benachrichtigungen" (siehe
Heimüberwachung -> Einstellungen): braucht zusätzlich
    pip install pywebpush
Ist das Paket nicht installiert, bleibt diese eine Funktion einfach
deaktiviert (klar erkennbar in der Oberfläche) - der Rest des Dashboards
läuft davon vollkommen unberührt.
"""
import os as _os

_PROJECT_BASE_DIR = _os.path.dirname(_os.path.abspath(__file__))

DATABASE_SOURCE = r'''
"""
database.py
SQLite-Datenbankschicht für das SmartHome Dashboard.
Alles läuft über eine einzige Datei (instance/smarthome.db), keine externen
Datenbank-Server nötig.

Performance-Hinweise:
  - Jeder Thread bekommt genau eine wiederverwendete Connection (statt bei
    jedem Call neu zu öffnen/schließen) - das spart messbar Zeit bei den
    häufigen Scheduler-Polls (alle 10-20s) und parallelen Web-Requests.
  - journal_mode=WAL erlaubt nebenläufige Reads während ein Write läuft.
    Reine Lesezugriffe (get_db_read) nehmen daher KEINEN globalen Lock;
    nur Schreibzugriffe (get_db, via Default-Verhalten) werden serialisiert,
    damit konkurrierende Writes sich nicht in die Quere kommen.
"""
import sqlite3
import threading
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta

DB_PATH = os.path.join(_PROJECT_BASE_DIR, "instance", "smarthome.db")

_write_lock = threading.Lock()
_thread_local = threading.local()


def _get_thread_connection():
    """Liefert die wiederverwendbare SQLite-Connection für den aktuellen Thread."""
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")  # WAL + NORMAL ist sicher genug und deutlich schneller als FULL
        _thread_local.conn = conn
    return conn


@contextmanager
def get_db():
    """Context manager für SCHREIBENDE Zugriffe - serialisiert über alle Threads,
    damit konkurrierende INSERT/UPDATE/DELETE sich nicht überschneiden."""
    with _write_lock:
        conn = _get_thread_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


@contextmanager
def get_db_read():
    """Context manager für LESENDE Zugriffe - nimmt bewusst KEINEN globalen Lock.
    Dank WAL-Modus sind parallele Reads sicher, auch während ein anderer Thread
    schreibt. Das beschleunigt häufige parallele GET-Endpunkte deutlich."""
    conn = _get_thread_connection()
    yield conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS miners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    ip TEXT NOT NULL,
    firmware TEXT NOT NULL DEFAULT 'braiins',      -- 'braiins' | 'bitmain'
    api_port INTEGER NOT NULL DEFAULT 4028,        -- cgminer/braiins TCP api port
    web_port INTEGER NOT NULL DEFAULT 80,          -- web ui port (für bitmain start/stop)
    braiins_user TEXT DEFAULT 'admin',
    braiins_pass TEXT DEFAULT '',
    power_watts INTEGER NOT NULL DEFAULT 3500,
    trigger_source TEXT NOT NULL DEFAULT 'pv_surplus',  -- 'pv_surplus' | 'grid_import' | 'pv_production' | 'battery_soc'
    threshold_on REAL NOT NULL DEFAULT 500,
    threshold_off REAL NOT NULL DEFAULT 400,
    priority INTEGER NOT NULL DEFAULT 1,
    min_runtime INTEGER NOT NULL DEFAULT 300,
    min_offtime INTEGER NOT NULL DEFAULT 300,
    automation_enabled INTEGER NOT NULL DEFAULT 1,
    note TEXT DEFAULT '',
    last_status TEXT DEFAULT 'offline',
    last_state_change TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS miner_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT DEFAULT (datetime('now')),
    total_hashrate REAL,
    total_power REAL,
    active_count INTEGER,
    surplus REAL
);

CREATE TABLE IF NOT EXISTS miner_stats_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    miner_id INTEGER NOT NULL,
    timestamp TEXT DEFAULT (datetime('now')),
    hashrate REAL,
    temperature REAL,
    power_watts REAL,
    status TEXT,
    FOREIGN KEY (miner_id) REFERENCES miners(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_miner_stats_history_miner_id ON miner_stats_history(miner_id, timestamp);

CREATE TABLE IF NOT EXISTS miner_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    miner_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,           -- 'started' | 'stopped' | 'auto_started' | 'auto_stopped' | 'created' | 'updated' | 'error'
    message TEXT NOT NULL,
    timestamp TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (miner_id) REFERENCES miners(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_miner_events_miner_id ON miner_events(miner_id, timestamp);

CREATE TABLE IF NOT EXISTS cameras (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cam_type TEXT NOT NULL DEFAULT 'mjpeg',         -- 'mjpeg' | 'rtsp'
    url TEXT NOT NULL,                              -- voll qualifizierte URL (inkl. Login bei rtsp)
    enabled INTEGER NOT NULL DEFAULT 1,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS shopping_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    added_by TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS calendar_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT DEFAULT '',
    start_time TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS notes (
    category TEXT PRIMARY KEY,
    content TEXT DEFAULT '',
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT DEFAULT 'info',                       -- info | warning | error
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    read INTEGER NOT NULL DEFAULT 0,
    timestamp TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS energy_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT DEFAULT (datetime('now')),
    pv_power REAL,
    house_load REAL,
    grid_import REAL,
    battery_soc REAL
);

CREATE TABLE IF NOT EXISTS btc_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT DEFAULT (datetime('now')),
    price_eur REAL
);

CREATE TABLE IF NOT EXISTS daily_energy_summary (
    day TEXT PRIMARY KEY,                    -- 'YYYY-MM-DD' (lokale Kalendertag)
    pv_kwh REAL NOT NULL DEFAULT 0,
    self_consumed_kwh REAL NOT NULL DEFAULT 0,
    exported_kwh REAL NOT NULL DEFAULT 0,
    imported_kwh REAL NOT NULL DEFAULT 0,
    house_kwh REAL NOT NULL DEFAULT 0,
    avg_battery_soc REAL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS push_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT UNIQUE NOT NULL,
    p256dh TEXT NOT NULL,
    auth TEXT NOT NULL,
    user_agent TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

DEFAULT_SETTINGS = {
    "fronius_ip": "192.168.178.100",
    "notification_battery_low": "20",
    "notification_battery_full": "85",
    "notification_high_import": "2000",
    "price_notification_threshold": "0.20",
    "miner_automation_enabled": "1",
    "miner_surplus_threshold": "2000",
    "miner_draw_threshold": "500",
    "miner_battery_threshold": "20",
    "miner_price_threshold": "0.30",
    "miner_start_time": "08:00",
    "miner_end_time": "18:00",
    "electricity_buyback_price": "0.07",
    "pv_install_cost": "12000",
    "pv_installed_kwp": "5",
    "background_mode": "weather",   # 'weather' | 'time' | 'static' | 'off'
    "background_accent": "amber",   # Farbthema für 'static' Modus
    # Hinweis: Die frühere Server-Einstellung "overview_top_stat" (fixierte
    # Wahl zwischen Verbrauch/Ereignissen für EINE Kachel) wurde durch die
    # freiere, rein clientseitige Schnellübersicht-Widget-Personalisierung
    # ersetzt (Einstellungen → Persönlich, siehe WIDGET_DEFS im Frontend) -
    # dort lässt sich pro Kachel aus deutlich mehr Datentypen wählen.
    # Personenerkennung (Heimüberwachung) - läuft eingebettet im selben
    # Prozess/Port wie dieses Dashboard (kein zweiter Server, keine
    # Server-Adresse/API-Key nötig). "1" aktiviert das Laden des YOLO-Modells
    # und das Verbinden der Kameras beim nächsten Neustart des Dashboards.
    "surveillance_enabled": "0",
}


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as conn:
        conn.executescript(SCHEMA)
        for k, v in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v)
            )
        conn.execute(
            "INSERT OR IGNORE INTO notes (category, content) VALUES ('general', '')"
        )
    _migrate_schema()
    cleanup_old_history()
    if os.environ.get("DEMO_MODE") == "1":
        seed_demo_data()


def _migrate_schema():
    """
    Leichtgewichtige Migration für Datenbanken, die mit einer älteren Version
    dieses Dashboards angelegt wurden: CREATE TABLE IF NOT EXISTS legt bei
    bereits existierenden Tabellen keine neuen Spalten an, daher werden hier
    fehlende Spalten gezielt nachgetragen (ALTER TABLE ... ADD COLUMN).
    """
    migrations = [
        ("miners", "trigger_source", "TEXT NOT NULL DEFAULT 'pv_surplus'"),
    ]
    with get_db() as conn:
        for table, column, coltype in migrations:
            existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
                log_migration(table, column)


def log_migration(table, column):
    import logging
    logging.getLogger("smarthome.database").info(
        "Datenbank-Migration: Spalte '%s' zu Tabelle '%s' hinzugefügt", column, table
    )


def cleanup_old_history(days=2):
    """Hält die History-Tabellen klein (rollende 48h für Charts).
    Bevor energy_history-Zeilen gelöscht werden, werden sie zuerst in
    daily_energy_summary aufsummiert (für Wochen-/Monatsvergleiche)."""
    rollup_energy_history_to_daily()
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("DELETE FROM energy_history WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM miner_history WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM btc_history WHERE timestamp < ?", (cutoff,))
    cleanup_old_miner_stats_history()
    cleanup_old_miner_events()


def row_to_dict(row):
    return dict(row) if row else None


def rows_to_list(rows):
    return [dict(r) for r in rows]


# ── Settings ──────────────────────────────────────────────────────
def get_all_settings():
    with get_db_read() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


def get_setting(key, default=None):
    with get_db_read() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_settings(d):
    with get_db() as conn:
        for k, v in d.items():
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (k, str(v)),
            )


# ── Browser-Push-Abos ────────────────────────────────────────────────
def add_push_subscription(endpoint, p256dh, auth, user_agent=None):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO push_subscriptions (endpoint, p256dh, auth, user_agent) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(endpoint) DO UPDATE SET p256dh=excluded.p256dh, auth=excluded.auth, user_agent=excluded.user_agent",
            (endpoint, p256dh, auth, user_agent),
        )


def remove_push_subscription(endpoint):
    with get_db() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint=?", (endpoint,))


def get_push_subscriptions():
    with get_db_read() as conn:
        rows = conn.execute("SELECT * FROM push_subscriptions").fetchall()
    return rows_to_list(rows)


def count_push_subscriptions():
    with get_db_read() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM push_subscriptions").fetchone()
    return row["c"] if row else 0


# ── Miners ────────────────────────────────────────────────────────
def get_miners():
    with get_db_read() as conn:
        rows = conn.execute("SELECT * FROM miners ORDER BY priority ASC, id ASC").fetchall()
    return rows_to_list(rows)


def get_miner(miner_id):
    with get_db_read() as conn:
        row = conn.execute("SELECT * FROM miners WHERE id=?", (miner_id,)).fetchone()
    return row_to_dict(row)


def add_miner(data):
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO miners
            (name, ip, firmware, api_port, web_port, braiins_user, braiins_pass,
             power_watts, trigger_source, threshold_on, threshold_off, priority, min_runtime,
             min_offtime, automation_enabled, note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data["name"], data["ip"], data.get("firmware", "braiins"),
                data.get("api_port", 4028), data.get("web_port", 80),
                data.get("braiins_user", "admin"), data.get("braiins_pass", ""),
                data.get("power_watts", 3500), data.get("trigger_source", "pv_surplus"),
                data.get("threshold_on", 500),
                data.get("threshold_off", 400), data.get("priority", 1),
                data.get("min_runtime", 300), data.get("min_offtime", 300),
                1 if data.get("automation_enabled", True) else 0,
                data.get("note", ""),
            ),
        )
        return cur.lastrowid


def update_miner(miner_id, data):
    fields = []
    values = []
    allowed = [
        "name", "ip", "firmware", "api_port", "web_port", "braiins_user",
        "braiins_pass", "power_watts", "trigger_source", "threshold_on", "threshold_off",
        "priority", "min_runtime", "min_offtime", "automation_enabled", "note",
        "last_status", "last_state_change",
    ]
    for k in allowed:
        if k in data:
            fields.append(f"{k}=?")
            values.append(data[k])
    if not fields:
        return
    values.append(miner_id)
    with get_db() as conn:
        conn.execute(f"UPDATE miners SET {', '.join(fields)} WHERE id=?", values)


def delete_miner(miner_id):
    with get_db() as conn:
        conn.execute("DELETE FROM miners WHERE id=?", (miner_id,))


def add_miner_history(total_hashrate, total_power, active_count, surplus):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO miner_history (total_hashrate, total_power, active_count, surplus) VALUES (?,?,?,?)",
            (total_hashrate, total_power, active_count, surplus),
        )


def get_miner_history(hours=24):
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM miner_history WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff,)
        ).fetchall()
    return rows_to_list(rows)


# ── Cameras ───────────────────────────────────────────────────────
def get_cameras():
    with get_db_read() as conn:
        rows = conn.execute("SELECT * FROM cameras ORDER BY sort_order ASC, id ASC").fetchall()
    return rows_to_list(rows)


def get_camera(cam_id):
    with get_db_read() as conn:
        row = conn.execute("SELECT * FROM cameras WHERE id=?", (cam_id,)).fetchone()
    return row_to_dict(row)


def add_camera(data):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO cameras (name, cam_type, url, enabled, sort_order) VALUES (?,?,?,?,?)",
            (
                data["name"], data.get("cam_type", "mjpeg"), data["url"],
                1 if data.get("enabled", True) else 0, data.get("sort_order", 0),
            ),
        )
        return cur.lastrowid


def update_camera(cam_id, data):
    fields, values = [], []
    for k in ["name", "cam_type", "url", "enabled", "sort_order"]:
        if k in data:
            fields.append(f"{k}=?")
            values.append(data[k])
    if not fields:
        return
    values.append(cam_id)
    with get_db() as conn:
        conn.execute(f"UPDATE cameras SET {', '.join(fields)} WHERE id=?", values)


def delete_camera(cam_id):
    with get_db() as conn:
        conn.execute("DELETE FROM cameras WHERE id=?", (cam_id,))


# ── Shopping list ─────────────────────────────────────────────────
def get_shopping_list():
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM shopping_list ORDER BY completed ASC, created_at DESC"
        ).fetchall()
    return rows_to_list(rows)


def add_shopping_item(item, added_by=""):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO shopping_list (item, added_by) VALUES (?, ?)", (item, added_by)
        )
        return cur.lastrowid


def complete_shopping_item(item_id):
    with get_db() as conn:
        row = conn.execute("SELECT completed FROM shopping_list WHERE id=?", (item_id,)).fetchone()
        if row is None:
            return
        new_val = 0 if row["completed"] else 1
        conn.execute("UPDATE shopping_list SET completed=? WHERE id=?", (new_val, item_id))


def delete_shopping_item(item_id):
    with get_db() as conn:
        conn.execute("DELETE FROM shopping_list WHERE id=?", (item_id,))


def clear_completed_shopping():
    with get_db() as conn:
        conn.execute("DELETE FROM shopping_list WHERE completed=1")


# ── Calendar ──────────────────────────────────────────────────────
def get_calendar_events():
    with get_db_read() as conn:
        rows = conn.execute("SELECT * FROM calendar_events ORDER BY start_time ASC").fetchall()
    return rows_to_list(rows)


def add_calendar_event(title, description, start_time):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO calendar_events (title, description, start_time) VALUES (?,?,?)",
            (title, description, start_time),
        )
        return cur.lastrowid


def delete_calendar_event(event_id):
    with get_db() as conn:
        conn.execute("DELETE FROM calendar_events WHERE id=?", (event_id,))


# ── Notes ─────────────────────────────────────────────────────────
def get_notes():
    with get_db_read() as conn:
        rows = conn.execute("SELECT * FROM notes").fetchall()
    return {r["category"]: r["content"] for r in rows}


def set_note(category, content):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO notes (category, content, updated_at) VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(category) DO UPDATE SET content=excluded.content, updated_at=datetime('now')",
            (category, content),
        )


# ── Notifications ─────────────────────────────────────────────────
def get_notifications(limit=50):
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM notifications ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
    return rows_to_list(rows)


def add_notification(title, message, type_="info"):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO notifications (type, title, message) VALUES (?,?,?)",
            (type_, title, message),
        )
        return cur.lastrowid


def mark_all_notifications_read():
    with get_db() as conn:
        conn.execute("UPDATE notifications SET read=1 WHERE read=0")


def has_recent_notification(title, minutes=30):
    """Verhindert Spam: prüft ob es diese Notification-Überschrift in den letzten X Minuten schon gab."""
    cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        row = conn.execute(
            "SELECT id FROM notifications WHERE title=? AND timestamp >= ? LIMIT 1",
            (title, cutoff),
        ).fetchone()
    return row is not None


# ── Energy history (für Charts) ──────────────────────────────────
def add_energy_history(pv_power, house_load, grid_import, battery_soc):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO energy_history (pv_power, house_load, grid_import, battery_soc) VALUES (?,?,?,?)",
            (pv_power, house_load, grid_import, battery_soc),
        )


def get_energy_history(hours=24):
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM energy_history WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff,)
        ).fetchall()
    return rows_to_list(rows)


def add_btc_history(price_eur):
    with get_db() as conn:
        conn.execute("INSERT INTO btc_history (price_eur) VALUES (?)", (price_eur,))


def get_btc_history(hours=24):
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM btc_history WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff,)
        ).fetchall()
    return rows_to_list(rows)


# ── Daily energy summary (Wochen-/Monatsvergleich, PV-Historie) ──────────
def rollup_energy_history_to_daily():
    """
    Fasst alle energy_history-Zeilen, die älter als der heutige Kalendertag
    sind, pro Tag zusammen (Trapez-Integration über Leistungswerte -> kWh)
    und schreibt das Ergebnis nach daily_energy_summary. Bereits aggregierte
    Tage werden beim erneuten Aufruf überschrieben (idempotent), damit
    mehrfaches Ausführen pro Tag nicht zu Duplikaten führt.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM energy_history WHERE timestamp < ? ORDER BY timestamp ASC",
            (today_str,),
        ).fetchall()

    if not rows:
        return

    by_day = {}
    for r in rows:
        day = r["timestamp"][:10]
        by_day.setdefault(day, []).append(dict(r))

    for day, day_rows in by_day.items():
        summary = _integrate_day(day_rows)
        with get_db() as conn:
            conn.execute(
                """INSERT INTO daily_energy_summary
                   (day, pv_kwh, self_consumed_kwh, exported_kwh, imported_kwh, house_kwh, avg_battery_soc, sample_count, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,datetime('now'))
                   ON CONFLICT(day) DO UPDATE SET
                     pv_kwh=excluded.pv_kwh, self_consumed_kwh=excluded.self_consumed_kwh,
                     exported_kwh=excluded.exported_kwh, imported_kwh=excluded.imported_kwh,
                     house_kwh=excluded.house_kwh, avg_battery_soc=excluded.avg_battery_soc,
                     sample_count=excluded.sample_count, updated_at=datetime('now')""",
                (day, summary["pv_kwh"], summary["self_consumed_kwh"], summary["exported_kwh"],
                 summary["imported_kwh"], summary["house_kwh"], summary["avg_battery_soc"], len(day_rows)),
            )


def _integrate_day(day_rows):
    """Integriert eine Liste von energy_history-dicts (ein Kalendertag) zu kWh-Summen."""
    pv_wh = self_wh = export_wh = import_wh = house_wh = 0.0
    soc_sum, soc_n = 0.0, 0

    for i in range(1, len(day_rows)):
        prev, cur = day_rows[i - 1], day_rows[i]
        try:
            t0 = datetime.strptime(prev["timestamp"], "%Y-%m-%d %H:%M:%S")
            t1 = datetime.strptime(cur["timestamp"], "%Y-%m-%d %H:%M:%S")
            dt_hours = (t1 - t0).total_seconds() / 3600.0
        except Exception:
            dt_hours = 30 / 3600.0
        if dt_hours <= 0 or dt_hours > 1:
            continue

        pv = cur.get("pv_power") or 0
        house = cur.get("house_load") or 0
        grid = cur.get("grid_import") or 0

        pv_wh += pv * dt_hours
        house_wh += house * dt_hours
        if grid < 0:
            export_wh += abs(grid) * dt_hours
        else:
            import_wh += grid * dt_hours
        self_wh += max(0, pv - max(0, -grid)) * dt_hours

    for r in day_rows:
        if r.get("battery_soc") is not None:
            soc_sum += r["battery_soc"]
            soc_n += 1

    return {
        "pv_kwh": round(pv_wh / 1000.0, 3),
        "self_consumed_kwh": round(self_wh / 1000.0, 3),
        "exported_kwh": round(export_wh / 1000.0, 3),
        "imported_kwh": round(import_wh / 1000.0, 3),
        "house_kwh": round(house_wh / 1000.0, 3),
        "avg_battery_soc": round(soc_sum / soc_n, 1) if soc_n else None,
    }


def get_daily_summaries(days=35):
    """Liefert die letzten N Tage aus daily_energy_summary, älteste zuerst."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM daily_energy_summary WHERE day >= ? ORDER BY day ASC", (cutoff,)
        ).fetchall()
    return rows_to_list(rows)


def get_today_partial_summary():
    """
    Aggregiert den NOCH NICHT abgeschlossenen heutigen Tag direkt aus
    energy_history (live), damit Wochen-/Monatscharts auch den laufenden
    Tag anzeigen können, ohne auf den nächtlichen Rollup zu warten.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM energy_history WHERE timestamp >= ? ORDER BY timestamp ASC",
            (today_str,),
        ).fetchall()
    rows = [dict(r) for r in rows]
    if not rows:
        return {"day": today_str, "pv_kwh": 0, "self_consumed_kwh": 0, "exported_kwh": 0,
                "imported_kwh": 0, "house_kwh": 0, "avg_battery_soc": None, "sample_count": 0}
    summary = _integrate_day(rows)
    summary["day"] = today_str
    summary["sample_count"] = len(rows)
    return summary


def get_hourly_profile(days=14):
    """
    Durchschnittliches PV- und Hausverbrauchsprofil je Uhrzeit (0-23 Uhr),
    gemittelt über die letzten `days` Tage Rohdaten (energy_history).
    Liefert eine Liste von 24 dicts: {hour, avg_pv, avg_house, avg_grid}.
    Da energy_history nur 48h rollend gehalten wird, deckt dies praktisch
    die letzten ein bis zwei Tage ab - ausreichend für ein Tagesprofil,
    das sich an "wann verbraucht/erzeugt das Haus typischerweise" orientiert.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT timestamp, pv_power, house_load, grid_import FROM energy_history WHERE timestamp >= ?",
            (cutoff,),
        ).fetchall()

    buckets = {h: {"pv": [], "house": [], "grid": []} for h in range(24)}
    for r in rows:
        try:
            hour = int(r["timestamp"][11:13])
        except Exception:
            continue
        buckets[hour]["pv"].append(r["pv_power"] or 0)
        buckets[hour]["house"].append(r["house_load"] or 0)
        buckets[hour]["grid"].append(r["grid_import"] or 0)

    result = []
    for h in range(24):
        b = buckets[h]
        result.append({
            "hour": h,
            "avg_pv": round(sum(b["pv"]) / len(b["pv"]), 1) if b["pv"] else 0,
            "avg_house": round(sum(b["house"]) / len(b["house"]), 1) if b["house"] else 0,
            "avg_grid": round(sum(b["grid"]) / len(b["grid"]), 1) if b["grid"] else 0,
        })
    return result


# ── Pro-Miner Historie (für die Miner-Detailansicht) ─────────────────────
def add_miner_stats_history(miner_id, hashrate, temperature, power_watts, status):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO miner_stats_history (miner_id, hashrate, temperature, power_watts, status) VALUES (?,?,?,?,?)",
            (miner_id, hashrate, temperature, power_watts, status),
        )


def get_miner_stats_history(miner_id, hours=24):
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM miner_stats_history WHERE miner_id=? AND timestamp >= ? ORDER BY timestamp ASC",
            (miner_id, cutoff),
        ).fetchall()
    return rows_to_list(rows)


def cleanup_old_miner_stats_history(days=8):
    """Pro-Miner-Historie wird länger gehalten als die Roh-Energiedaten (für Wochenblick im Miner-Detail)."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("DELETE FROM miner_stats_history WHERE timestamp < ?", (cutoff,))


# ── Miner Events (Log für die Miner-Detailansicht) ───────────────────────
def add_miner_event(miner_id, event_type, message):
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO miner_events (miner_id, event_type, message) VALUES (?,?,?)",
            (miner_id, event_type, message),
        )
        return cur.lastrowid


def get_miner_events(miner_id, limit=50):
    with get_db_read() as conn:
        rows = conn.execute(
            "SELECT * FROM miner_events WHERE miner_id=? ORDER BY timestamp DESC LIMIT ?",
            (miner_id, limit),
        ).fetchall()
    return rows_to_list(rows)


def cleanup_old_miner_events(days=30):
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("DELETE FROM miner_events WHERE timestamp < ?", (cutoff,))


# ── Demo-Modus ────────────────────────────────────────────────────────────
# Erzeugt realistisch aussehende Beispieldaten, damit sich das Dashboard
# auch ohne eigene Hardware (Fronius/Miner/Kameras) ausprobieren lässt -
# praktisch für einen ersten Eindruck oder für Screenshots. Wird nur
# aktiv, wenn die Umgebungsvariable DEMO_MODE=1 gesetzt ist UND die
# betroffenen Tabellen noch leer sind (überschreibt also nie echte Daten).
def seed_demo_data():
    import math
    import random

    with get_db_read() as conn:
        already_has_data = conn.execute("SELECT COUNT(*) AS c FROM energy_history").fetchone()["c"] > 0

    if already_has_data:
        return

    now = datetime.utcnow()
    rng = random.Random(42)  # fester Seed -> reproduzierbare Demo-Kurve

    with get_db() as conn:
        # 48h Energie-Verlauf als sanfte Tageskurve (PV folgt der Sonne)
        for i in range(48 * 12, 0, -1):  # alle 5 Minuten
            ts = now - timedelta(minutes=5 * i)
            hour = ts.hour + ts.minute / 60.0
            daylight = max(0.0, math.sin((hour - 6) / 12 * math.pi))
            pv_power = round(daylight ** 1.3 * 4800 + rng.uniform(-80, 80), 1)
            pv_power = max(0.0, pv_power)
            house_load = round(500 + 700 * math.sin(hour / 24 * 2 * math.pi + 2) ** 2 + rng.uniform(-40, 40), 1)
            house_load = max(150.0, house_load)
            grid_import = round(house_load - pv_power, 1)
            battery_soc = round(35 + 55 * max(0.0, math.sin((hour - 5) / 14 * math.pi)) + rng.uniform(-3, 3), 1)
            battery_soc = min(100.0, max(5.0, battery_soc))
            conn.execute(
                "INSERT INTO energy_history (timestamp, pv_power, house_load, grid_import, battery_soc) "
                "VALUES (?,?,?,?,?)",
                (ts.strftime("%Y-%m-%d %H:%M:%S"), pv_power, house_load, grid_import, battery_soc),
            )

        # Bitcoin-Kurs, leicht schwankender Random Walk um einen Demo-Wert
        price = 58000.0
        for i in range(48 * 6, 0, -1):  # alle 10 Minuten
            ts = now - timedelta(minutes=10 * i)
            price += rng.uniform(-150, 150)
            price = max(20000.0, price)
            conn.execute(
                "INSERT INTO btc_history (timestamp, price_eur) VALUES (?,?)",
                (ts.strftime("%Y-%m-%d %H:%M:%S"), round(price, 2)),
            )

        # Einkaufsliste
        for item, done in [("Milch", 0), ("Kaffee", 0), ("Batterien AA", 1), ("Spülmaschinentabs", 0)]:
            conn.execute(
                "INSERT INTO shopping_list (item, completed, added_by) VALUES (?,?,?)",
                (item, done, "Demo"),
            )

        # Kalender
        conn.execute(
            "INSERT INTO calendar_events (title, description, start_time) VALUES (?,?,?)",
            ("Wartungstermin Wechselrichter", "Jährliche Sichtprüfung durch den Installateur",
             (now + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")),
        )

        # Notiz
        conn.execute(
            "INSERT OR REPLACE INTO notes (category, content, updated_at) VALUES ('general', ?, datetime('now'))",
            ("Willkommen im Demo-Modus! Diese Beispieldaten sind frei erfunden - "
             "unter Einstellungen kannst du echte Geräte (Fronius, Miner, Kameras) verbinden.",),
        )

        # Beispiel-Benachrichtigung
        conn.execute(
            "INSERT INTO notifications (type, title, message) VALUES (?,?,?)",
            ("info", "Demo-Modus aktiv",
             "Dieses Dashboard läuft mit Beispieldaten (DEMO_MODE=1). "
             "Verbinde eigene Geräte in den Einstellungen, um echte Werte zu sehen."),
        )

'''

def _make_database():
    import types
    ns = {"__name__": "smarthome.database", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.database")
    mod.__dict__.update(ns)
    exec(compile(DATABASE_SOURCE, "<database>", "exec"), mod.__dict__)
    return mod

FRONIUS_SOURCE = r'''
"""
fronius.py
Anbindung an die Fronius Solar API (lokal, kein Cloud-Login nötig).
Nutzt GetPowerFlowRealtimeData für PV-Leistung, Hausverbrauch, Netz und
Batterie in einem Aufruf - das ist der Standard-Endpoint aller Fronius
Symo/Gen24-Wechselrichter mit aktivierter Solar API (Menü > Netzwerk > Solar API).
"""
import requests
import logging

log = logging.getLogger("smarthome.fronius")

TIMEOUT = 5


class FroniusError(Exception):
    pass


def get_power_flow(ip):
    """
    Liest aktuelle Leistungsdaten vom Fronius Wechselrichter.
    Returns dict mit pv_power, house_load, grid_import, battery_soc, battery_power, autonomy, self_consumption
    oder None bei Fehler.
    """
    url = f"http://{ip}/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        site = data["Body"]["Data"]["Site"]

        pv_power = float(site.get("P_PV") or 0)            # Erzeugung (immer >= 0)
        load = float(site.get("P_Load") or 0)               # negativ = Verbrauch in Fronius-Konvention
        grid = float(site.get("P_Grid") or 0)                # positiv = Bezug, negativ = Einspeisung
        battery = site.get("P_Akku")
        battery_power = float(battery) if battery is not None else 0.0
        autonomy = site.get("rel_Autonomy")
        self_consumption = site.get("rel_SelfConsumption")

        house_load = abs(load)

        # SoC kommt meist über Inverters-Block, separat abfragen
        battery_soc = _get_battery_soc(ip)

        return {
            "pv_power": round(pv_power, 1),
            "house_load": round(house_load, 1),
            "grid_import": round(grid, 1),   # >0 Bezug, <0 Einspeisung
            "battery_power": round(battery_power, 1),
            "battery_soc": battery_soc,
            "autonomy": round(float(autonomy), 1) if autonomy is not None else 0,
            "self_consumption": round(float(self_consumption), 1) if self_consumption is not None else 0,
        }
    except Exception as e:
        log.warning("Fronius nicht erreichbar (%s): %s", ip, e)
        return None


def _get_battery_soc(ip):
    """Batterie-Ladezustand über GetStorageRealtimeData (Gen24/Symo Hybrid mit Akku)."""
    try:
        url = f"http://{ip}/solar_api/v1/GetStorageRealtimeData.cgi?Scope=Device&DeviceId=0"
        r = requests.get(url, timeout=TIMEOUT)
        if r.ok:
            data = r.json()
            controller = data.get("Body", {}).get("Data", {}).get("Controller", {})
            soc = controller.get("StateOfCharge_Relative")
            if soc is not None:
                return round(float(soc), 1)
    except Exception:
        pass
    return 0.0


def get_daily_energy(ip):
    """Heutige PV-Erzeugung in Wh über GetInverterRealtimeData (CumulationInverterData)."""
    try:
        url = f"http://{ip}/solar_api/v1/GetInverterRealtimeData.cgi?Scope=System&DataCollection=CumulationInverterData"
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        day_energy = data["Body"]["Data"]["DAY_ENERGY"]["Values"]
        # Values ist ein dict {inverter_id: wh}; alle Inverter summieren
        total_wh = sum(float(v) for v in day_energy.values())
        return round(total_wh, 1)
    except Exception as e:
        log.debug("Fronius Tagesenergie nicht verfügbar: %s", e)
        return 0.0


def get_full_solar_data(ip):
    """Kombiniert PowerFlow + Tagesenergie zu einem fertigen dict für das Dashboard."""
    flow = get_power_flow(ip)
    if flow is None:
        return None
    flow["pv_day"] = get_daily_energy(ip)
    return flow

'''

def _make_fronius():
    import types
    ns = {"__name__": "smarthome.fronius", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.fronius")
    mod.__dict__.update(ns)
    exec(compile(FRONIUS_SOURCE, "<fronius>", "exec"), mod.__dict__)
    return mod

MINERS_SOURCE = r'''
"""
miners.py
Steuerung und Statusabfrage für Bitcoin-Miner.
Unterstützt zwei Firmware-Typen:

  - braiins: Braiins OS / Braiins OS+ mit der modernen REST API (Port 80,
    Bearer-Token-Login) UND als Fallback der klassischen CGMiner-API (4028).
  - bitmain: Stock Bitmain/Antminer Firmware - Stats über die CGMiner-kompatible
    TCP-API (Port 4028, JSON-Lines-Protokoll), Start/Stop über die Web-UI
    (Digest-Auth, /cgi-bin/...).

Alle Funktionen geben bei Fehlern None/False zurück statt zu werfen, damit der
Scheduler robust weiterläuft, auch wenn ein Miner offline ist.
"""
import socket
import json
import logging
import requests
from requests.auth import HTTPDigestAuth

log = logging.getLogger("smarthome.miners")

SOCKET_TIMEOUT = 4
HTTP_TIMEOUT = 5


# ── Low-level CGMiner TCP API (funktioniert für Braiins UND Bitmain stock) ──
def _cgminer_command(ip, port, command, parameter=None):
    """Sendet einen Befehl über die CGMiner-API (JSON über rohes TCP)."""
    payload = {"command": command}
    if parameter is not None:
        payload["parameter"] = parameter
    try:
        with socket.create_connection((ip, port), timeout=SOCKET_TIMEOUT) as s:
            s.sendall(json.dumps(payload).encode("utf-8"))
            chunks = []
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            raw = b"".join(chunks).decode("utf-8", errors="ignore")
            raw = raw.replace("\x00", "").strip()
            if not raw:
                return None
            return json.loads(raw)
    except Exception as e:
        log.debug("CGMiner API Fehler %s:%s %s -> %s", ip, port, command, e)
        return None


def get_cgminer_stats(ip, port=4028):
    """
    Liefert vereinheitlichte Stats (hashrate TH/s, temperature C, status) ueber
    die CGMiner-kompatible API. Funktioniert fuer Braiins OS UND Bitmain stock.
    """
    summary = _cgminer_command(ip, port, "summary")
    stats = _cgminer_command(ip, port, "stats")

    if summary is None:
        return None

    try:
        summary_data = summary.get("SUMMARY", [{}])[0]
        ghs = summary_data.get("GHS 5s") or summary_data.get("GHS av") or 0
        hashrate_ths = round(float(ghs) / 1000.0, 2)

        temperature = 0
        if stats and stats.get("STATS"):
            for stat_block in stats["STATS"]:
                for key in ("temp2", "temp1", "temp", "Temperature"):
                    if key in stat_block and stat_block[key]:
                        try:
                            t = float(stat_block[key])
                            if t > 0:
                                temperature = max(temperature, t)
                        except (TypeError, ValueError):
                            pass

        elapsed = summary_data.get("Elapsed", 0)
        is_mining = hashrate_ths > 0

        return {
            "hashrate": hashrate_ths,
            "temperature": round(temperature, 1) if temperature else None,
            "status": "running" if is_mining else "paused",
            "elapsed": elapsed,
        }
    except Exception as e:
        log.debug("Konnte CGMiner Stats nicht parsen fuer %s: %s", ip, e)
        return None


# ── Braiins OS: moderne REST API (für Start/Stop, mit Fallback) ──────────────
def _braiins_login(ip, username, password):
    """Loggt sich bei Braiins OS+ REST API ein, liefert Bearer Token."""
    try:
        r = requests.post(
            f"http://{ip}/api/v1/auth/login",
            json={"username": username, "password": password},
            timeout=HTTP_TIMEOUT,
        )
        if r.ok:
            return r.json().get("token")
    except Exception as e:
        log.debug("Braiins Login fehlgeschlagen %s: %s", ip, e)
    return None


def braiins_set_power(ip, username, password, turn_on):
    """
    Schaltet einen Braiins-Miner ueber die REST-API ein/aus (pause/resume mining).
    Faellt auf CGMiner 'ascset'-Befehle zurueck falls REST nicht erreichbar.
    """
    token = _braiins_login(ip, username, password)
    if token:
        try:
            endpoint = "resume" if turn_on else "pause"
            r = requests.put(
                f"http://{ip}/api/v1/actions/{endpoint}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=HTTP_TIMEOUT,
            )
            if r.ok:
                return True
        except Exception as e:
            log.debug("Braiins REST pause/resume Fehler %s: %s", ip, e)

    # Fallback: CGMiner curtail-Befehle (aeltere Braiins OS Versionen)
    cmd = "curtail,resume" if turn_on else "curtail,sleep"
    result = _cgminer_command(ip, 4028, "ascset", parameter=f"0,{cmd}")
    return result is not None


# ── Bitmain stock: Web-UI Start/Stop ─────────────────────────────────────────
def bitmain_set_power(ip, web_port, username, password, turn_on):
    """
    Bitmain/Antminer stock Firmware hat kein offenes API fuer Start/Stop.
    Wir nutzen die bekannten cgi-bin Endpoints der stock Web-UI (Digest-Auth).
    'set_miner_conf' mit miner-mode 0=normal/1=sleep ist das gaengigste Verfahren
    bei neueren Antminer-Firmwares; aeltere nutzen /cgi-bin/miner_pause.cgi.
    """
    auth = HTTPDigestAuth(username or "root", password or "root")
    base = f"http://{ip}:{web_port}"
    try:
        mode = 0 if turn_on else 1
        r = requests.get(f"{base}/cgi-bin/set_miner_conf.cgi?miner-mode={mode}", auth=auth, timeout=HTTP_TIMEOUT)
        if r.ok:
            return True
    except Exception as e:
        log.debug("Bitmain Web-UI set_miner_conf Fehler %s: %s", ip, e)

    try:
        action = "miner_resume" if turn_on else "miner_pause"
        r = requests.get(f"{base}/cgi-bin/{action}.cgi", auth=auth, timeout=HTTP_TIMEOUT)
        return r.ok
    except Exception as e:
        log.debug("Bitmain Web-UI Fallback Fehler %s: %s", ip, e)
        return False


# ── Unified Interface (wird vom Scheduler / API benutzt) ────────────────────
def get_miner_stats(miner):
    """
    miner: dict aus der Datenbank (firmware, ip, api_port, ...).
    Returns dict mit hashrate, temperature, status ('running'|'paused'|'offline').
    """
    stats = get_cgminer_stats(miner["ip"], miner.get("api_port", 4028))
    if stats is None:
        return {"hashrate": 0, "temperature": None, "status": "offline", "elapsed": 0}
    return stats


def set_miner_power(miner, turn_on):
    """
    miner: dict aus der Datenbank.
    Schaltet den Miner ein (turn_on=True) oder aus (turn_on=False),
    je nach Firmware-Typ ueber den passenden Mechanismus.
    Returns True bei (vermutlichem) Erfolg, False bei Fehler.
    """
    firmware = miner.get("firmware", "braiins")
    if firmware == "bitmain":
        return bitmain_set_power(
            miner["ip"], miner.get("web_port", 80),
            miner.get("braiins_user", "root"), miner.get("braiins_pass", "root"),
            turn_on,
        )
    else:
        return braiins_set_power(
            miner["ip"], miner.get("braiins_user", "admin"), miner.get("braiins_pass", ""),
            turn_on,
        )

'''

def _make_miners():
    import types
    ns = {"__name__": "smarthome.miners", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.miners")
    mod.__dict__.update(ns)
    exec(compile(MINERS_SOURCE, "<miners>", "exec"), mod.__dict__)
    return mod

EXTERNAL_APIS_SOURCE = r'''
"""
external_apis.py
Anbindung an externe, frei zugaengliche APIs (alle ohne API-Key):

  - aWATTar (Stundenpreise Strom, Marktgebiet Oesterreich)
  - CoinGecko (Bitcoin-Kurs EUR/USD)
  - Open-Meteo (Wetter, Geo per Lat/Lon, keine Registrierung notwendig)
"""
import requests
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("smarthome.external")
TIMEOUT = 8

# Ruprechtshofen, Niederoesterreich (Standardkoordinaten, im Code anpassbar)
# Standard-Koordinaten (Wien) - bitte in den Einstellungen auf den eigenen Standort anpassen
DEFAULT_LAT = 48.2082
DEFAULT_LON = 16.3738

AWATTAR_URL = "https://api.awattar.at/v1/marketdata"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

WEATHER_CODE_MAP = {
    0: "Sonnig", 1: "Sonnig", 2: "Teilweise bewoelkt", 3: "Bewoelkt",
    45: "Nebel", 48: "Nebel", 51: "Leichter Regen", 53: "Regen", 55: "Regen",
    61: "Regen", 63: "Regen", 65: "Starker Regen", 71: "Schnee", 73: "Schnee",
    75: "Starker Schnee", 80: "Regenschauer", 81: "Regenschauer", 82: "Starker Schauer",
    95: "Gewitter", 96: "Gewitter mit Hagel", 99: "Gewitter mit Hagel",
}

# Grobkategorie je WMO weather_code - wird vom Frontend genutzt, um Hintergrund-
# Effekte (Regen/Schnee/Sonne/Wolken/Gewitter) und Vorhersage-Icons zu wählen.
WEATHER_CATEGORY_MAP = {
    0: "clear", 1: "clear", 2: "cloudy", 3: "cloudy",
    45: "fog", 48: "fog",
    51: "rain", 53: "rain", 55: "rain", 61: "rain", 63: "rain", 65: "rain",
    71: "snow", 73: "snow", 75: "snow",
    80: "rain", 81: "rain", 82: "rain",
    95: "thunder", 96: "thunder", 99: "thunder",
}

WEEKDAY_LABELS_DE = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]


def get_electricity_prices():
    """
    Holt aWATTar Stundenpreise fuer heute (inkl. Steuern/Netzentgelt NICHT enthalten -
    das ist der reine Boersenpreis, in €/kWh umgerechnet).
    Returns dict mit current_price, prices_today (liste), cheapest_today oder None.
    """
    now = datetime.now(timezone.utc)
    start = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end = int((now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
    try:
        r = requests.get(AWATTAR_URL, params={"start": start, "end": end}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return None

        prices_today = []
        current_price = None
        now_ts = now.timestamp() * 1000

        local_tz_offset = datetime.now().astimezone().utcoffset()

        for entry in data:
            price_eur_kwh = entry["marketprice"] / 1000.0  # API liefert €/MWh
            start_dt = datetime.fromtimestamp(entry["start_timestamp"] / 1000, tz=timezone.utc)
            local_dt = start_dt + local_tz_offset
            is_current = entry["start_timestamp"] <= now_ts < entry["end_timestamp"]
            if is_current:
                current_price = price_eur_kwh
            prices_today.append({
                "hour": local_dt.hour,
                "price": round(price_eur_kwh, 4),
                "is_current": is_current,
            })

        if current_price is None and prices_today:
            current_price = prices_today[-1]["price"]

        cheapest = min(prices_today, key=lambda p: p["price"]) if prices_today else None

        return {
            "current_price": round(current_price, 4) if current_price is not None else 0,
            "prices_today": prices_today,
            "cheapest_today": cheapest,
            "updated": datetime.now().isoformat(),
        }
    except Exception as e:
        log.warning("aWATTar Abfrage fehlgeschlagen: %s", e)
        return None


def get_bitcoin_price():
    """Holt aktuellen BTC-Preis in EUR und USD + 24h Änderung von CoinGecko."""
    try:
        r = requests.get(
            COINGECKO_URL,
            params={"ids": "bitcoin", "vs_currencies": "eur,usd", "include_24hr_change": "true"},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()["bitcoin"]
        return {
            "price_eur": round(data["eur"], 2),
            "price_usd": round(data["usd"], 2),
            "change_24h": round(data.get("eur_24h_change", 0), 2),
        }
    except Exception as e:
        log.warning("CoinGecko Abfrage fehlgeschlagen: %s", e)
        return None


def get_weather(lat=DEFAULT_LAT, lon=DEFAULT_LON):
    """
    Holt aktuelles Wetter UND Vorhersage von Open-Meteo in einem Request:
      - current: aktuelle Bedingungen
      - hourly: stündliche Vorhersage für die nächsten 48h
      - daily: tägliche Vorhersage für die nächsten 7 Tage
    Alles gratis, ohne API-Key, ohne Registrierung.
    """
    try:
        r = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover,weather_code,is_day",
                "hourly": "temperature_2m,precipitation_probability,weather_code,shortwave_radiation,cloud_cover",
                "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max,sunrise,sunset",
                "timezone": "auto",
                "forecast_days": 7,
            },
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        payload = r.json()
        cur = payload["current"]
        code = cur.get("weather_code", 0)

        result = {
            "temperature": round(cur.get("temperature_2m", 0)),
            "humidity": round(cur.get("relative_humidity_2m", 0)),
            "windspeed": round(cur.get("wind_speed_10m", 0)),
            "clouds": round(cur.get("cloud_cover", 0)),
            "conditions": WEATHER_CODE_MAP.get(code, "Unbekannt"),
            "category": WEATHER_CATEGORY_MAP.get(code, "cloudy"),
            "is_day": bool(cur.get("is_day", 1)),
        }
        result["hourly"] = _parse_hourly_forecast(payload.get("hourly", {}), hours=32)
        result["daily"] = _parse_daily_forecast(payload.get("daily", {}))
        return result
    except Exception as e:
        log.warning("Open-Meteo Abfrage fehlgeschlagen: %s", e)
        return None


def _parse_hourly_forecast(hourly, hours=24):
    """Wandelt die stündliche Open-Meteo-Antwort in eine kompakte Liste um (nächste `hours` Stunden ab jetzt)."""
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation_probability", [])
    codes = hourly.get("weather_code", [])
    radiation = hourly.get("shortwave_radiation", [])
    clouds = hourly.get("cloud_cover", [])
    if not times:
        return []

    now_str = datetime.now().strftime("%Y-%m-%dT%H:00")
    start_idx = 0
    for i, t in enumerate(times):
        if t >= now_str:
            start_idx = i
            break

    result = []
    for i in range(start_idx, min(start_idx + hours, len(times))):
        code = codes[i] if i < len(codes) else 0
        hour_label = times[i][11:16]
        result.append({
            "time": times[i],
            "hour": hour_label,
            "temperature": round(temps[i]) if i < len(temps) else None,
            "precipitation_probability": precip[i] if i < len(precip) else 0,
            "conditions": WEATHER_CODE_MAP.get(code, "Unbekannt"),
            "category": WEATHER_CATEGORY_MAP.get(code, "cloudy"),
            "radiation": round(radiation[i]) if i < len(radiation) and radiation[i] is not None else 0,
            "clouds": round(clouds[i]) if i < len(clouds) and clouds[i] is not None else None,
        })
    return result


def _parse_daily_forecast(daily):
    """Wandelt die tägliche Open-Meteo-Antwort in eine kompakte 7-Tage-Liste um."""
    days = daily.get("time", [])
    codes = daily.get("weather_code", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_probability_max", [])
    sunrise = daily.get("sunrise", [])
    sunset = daily.get("sunset", [])
    if not days:
        return []

    result = []
    for i, day_str in enumerate(days):
        try:
            day_date = datetime.strptime(day_str, "%Y-%m-%d")
            weekday = WEEKDAY_LABELS_DE[day_date.weekday()]
            label = f"{day_date.day}.{day_date.month}."
        except Exception:
            weekday, label = "", day_str
        code = codes[i] if i < len(codes) else 0
        result.append({
            "day": day_str,
            "weekday": weekday,
            "label": label,
            "temp_max": round(tmax[i]) if i < len(tmax) else None,
            "temp_min": round(tmin[i]) if i < len(tmin) else None,
            "precipitation_probability": precip[i] if i < len(precip) else 0,
            "conditions": WEATHER_CODE_MAP.get(code, "Unbekannt"),
            "category": WEATHER_CATEGORY_MAP.get(code, "cloudy"),
            "sunrise": sunrise[i][11:16] if i < len(sunrise) else None,
            "sunset": sunset[i][11:16] if i < len(sunset) else None,
        })
    return result


# ── Solarprognose ─────────────────────────────────────────────────
# Grobe, aber brauchbare Schätzung der PV-Erzeugung anhand der Globalstrahlung
# (shortwave_radiation, W/m²) aus der Open-Meteo-Vorhersage. Kein Ersatz für
# eine echte PVGIS/Anlagen-Simulation (Ausrichtung, Verschattung, Neigung
# werden nicht berücksichtigt), aber ohne zusätzliche Konfiguration/API-Key
# eine realistische Größenordnung für "wie viel PV-Strom bringt der morgige
# Tag ungefähr" - Systemwirkungsgrad (Wechselrichter, Verkabelung, Temperatur,
# Verschmutzung, nicht perfekte Ausrichtung) wird pauschal mit 78% angenommen.
SOLAR_SYSTEM_EFFICIENCY = 0.78
STC_IRRADIANCE = 1000.0  # W/m², Referenzbestrahlungsstärke für die kWp-Angabe eines Moduls


def compute_solar_forecast(hourly_forecast, installed_kwp, efficiency=SOLAR_SYSTEM_EFFICIENCY):
    """
    hourly_forecast: Liste von Stunden-Einträgen wie von _parse_hourly_forecast
    (braucht das Feld "radiation" in W/m²).
    installed_kwp: installierte PV-Peakleistung in kWp (0/None = keine Anlage
    konfiguriert -> leeres Ergebnis).

    Returns dict mit "hours" (stündliche kW-Schätzung) sowie "today_kwh" und
    "tomorrow_kwh" (aufsummiert je Kalendertag).
    """
    if not installed_kwp or installed_kwp <= 0 or not hourly_forecast:
        return {"hours": [], "today_kwh": 0, "tomorrow_kwh": 0, "installed_kwp": installed_kwp or 0}

    today_str = datetime.now().strftime("%Y-%m-%d")
    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    hours_out = []
    today_kwh = 0.0
    tomorrow_kwh = 0.0
    for entry in hourly_forecast:
        radiation = entry.get("radiation") or 0
        est_kw = round(installed_kwp * (radiation / STC_IRRADIANCE) * efficiency, 2)
        day_str = entry["time"][:10]
        if day_str == today_str:
            today_kwh += est_kw  # 1 Werteintrag == 1 Stunde, kW * 1h = kWh
        elif day_str == tomorrow_str:
            tomorrow_kwh += est_kw
        hours_out.append({
            "hour": entry["hour"], "time": entry["time"],
            "radiation": radiation, "estimated_kw": est_kw,
            "conditions": entry.get("conditions"), "category": entry.get("category"),
        })

    return {
        "hours": hours_out,
        "today_kwh": round(today_kwh, 1),
        "tomorrow_kwh": round(tomorrow_kwh, 1),
        "installed_kwp": installed_kwp,
    }

'''

def _make_external_apis():
    import types
    ns = {"__name__": "smarthome.external_apis", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.external_apis")
    mod.__dict__.update(ns)
    exec(compile(EXTERNAL_APIS_SOURCE, "<external_apis>", "exec"), mod.__dict__)
    return mod

AUTOMATION_SOURCE = r'''
"""
automation.py
Entscheidet, welche Miner ein- oder ausgeschaltet werden sollen.

Globale Sperren (gelten für alle automatisierten Miner gleich):
  - Tageszeit-Fenster (Start/Endzeit)
  - Mindest-Batterie-Ladezustand (verhindert Miner-Betrieb, wenn die Batterie
    geschont werden muss)
  - aktueller Strompreis (Miner aus, wenn Strom zu teuer ist)

Pro-Miner-Trigger (jeder Miner wählt eine Datenquelle, die seine eigene
Ein/Aus-Schwellwert-Logik steuert):
  - pv_surplus    (Standard): PV-Überschuss/Einspeisung in Watt
                   ein ab threshold_on W Überschuss, aus ab threshold_off W Netzbezug
  - grid_import   : reiner Netzbezug in Watt
                   ein, wenn Netzbezug UNTER threshold_on W bleibt
                   aus, wenn Netzbezug ÜBER threshold_off W steigt
  - pv_production : PV-Rohleistung (Erzeugung, unabhängig vom Hausverbrauch) in Watt
                   ein ab threshold_on W Erzeugung, aus unter threshold_off W
  - battery_soc   : Batterie-Ladezustand in %
                   ein ab threshold_on % SoC, aus unter threshold_off % SoC

Zusätzlich pro Miner: Priorität (niedrigere Zahl = wird zuerst eingeschaltet,
zuletzt ausgeschaltet, nur relevant für pv_surplus-Miner, die sich die
verfügbare Leistung teilen), Mindestlaufzeit / Mindest-Aus-Zeit (verhindert
Kurzzyklen-Flattern).

Diese Funktion ist reine Logik (keine I/O) und daher gut testbar.
"""
import logging
from datetime import datetime, time as dtime

log = logging.getLogger("smarthome.automation")

VALID_TRIGGER_SOURCES = ("pv_surplus", "grid_import", "pv_production", "battery_soc")


def _parse_time(s):
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(0, 0)


def _within_time_window(now_time, start_str, end_str):
    start = _parse_time(start_str)
    end = _parse_time(end_str)
    if start <= end:
        return start <= now_time <= end
    return now_time >= start or now_time <= end  # Fenster ueber Mitternacht


def decide_miner_actions(miners, solar_data, current_price, settings, now=None):
    """
    miners: Liste von Miner-dicts (inkl. last_status, last_state_change als ISO-String oder None,
            trigger_source, threshold_on, threshold_off, priority, power_watts,
            min_runtime, min_offtime, automation_enabled)
    solar_data: dict mit grid_import (>0 Bezug, <0 Einspeisung), battery_soc, pv_power
    current_price: aktueller Strompreis in EUR/kWh (oder None)
    settings: dict mit miner_battery_threshold, miner_price_threshold,
              miner_start_time, miner_end_time, miner_automation_enabled

    Returns: dict {miner_id: 'resume'|'pause'}  (fehlender Eintrag = keine Aktion)
    """
    now = now or datetime.now()
    actions = {}

    if str(settings.get("miner_automation_enabled", "1")) != "1":
        return actions

    if not _within_time_window(now.time(), settings.get("miner_start_time", "08:00"), settings.get("miner_end_time", "18:00")):
        for m in miners:
            if m.get("automation_enabled") and m.get("last_status") == "running":
                actions[m["id"]] = "pause"
        return actions

    if solar_data is None:
        return actions

    battery_soc = solar_data.get("battery_soc", 0) or 0
    grid_import = solar_data.get("grid_import", 0) or 0
    pv_power = solar_data.get("pv_power", 0) or 0
    # net_pv_balance: positiv = Einspeisung/Ueberschuss, negativ = Netzbezug
    net_pv_balance = -grid_import

    battery_threshold = float(settings.get("miner_battery_threshold", 20))
    price_threshold = float(settings.get("miner_price_threshold", 0.30))

    price_too_high = current_price is not None and current_price > price_threshold
    battery_too_low = battery_soc < battery_threshold
    hard_block = battery_too_low or price_too_high

    automated = [m for m in miners if m.get("automation_enabled")]
    automated.sort(key=lambda m: m.get("priority", 1))

    # available_power wird nur für trigger_source='pv_surplus' gebraucht: die
    # Miner mit dieser Quelle teilen sich den PV-Überschuss nach Prioritaet.
    running_surplus_miners = [
        m for m in automated
        if m.get("last_status") == "running" and _trigger_source(m) == "pv_surplus"
    ]
    available_power = net_pv_balance + sum(m.get("power_watts", 0) for m in running_surplus_miners)

    for m in automated:
        is_running = m.get("last_status") == "running"
        seconds_in_state = _seconds_since(m.get("last_state_change"), now)
        source = _trigger_source(m)

        if hard_block:
            if is_running and seconds_in_state >= m.get("min_runtime", 300):
                actions[m["id"]] = "pause"
            continue

        if source == "pv_surplus":
            available_power = _decide_pv_surplus(m, is_running, seconds_in_state, available_power, actions)
        elif source == "grid_import":
            _decide_threshold_lower_is_on(m, is_running, seconds_in_state, grid_import, actions)
        elif source == "pv_production":
            _decide_threshold_higher_is_on(m, is_running, seconds_in_state, pv_power, actions)
        elif source == "battery_soc":
            _decide_threshold_higher_is_on(m, is_running, seconds_in_state, battery_soc, actions)

    return actions


def _trigger_source(miner):
    source = miner.get("trigger_source") or "pv_surplus"
    return source if source in VALID_TRIGGER_SOURCES else "pv_surplus"


def _decide_pv_surplus(m, is_running, seconds_in_state, available_power, actions):
    """Bisheriges Standardverhalten: Miner teilen sich den PV-Überschuss nach Priorität."""
    power = m.get("power_watts", 3500)
    threshold_on = m.get("threshold_on", 500)
    threshold_off = m.get("threshold_off", 400)

    if is_running:
        still_enough = available_power >= power - threshold_off
        if not still_enough and seconds_in_state >= m.get("min_runtime", 300):
            actions[m["id"]] = "pause"
            available_power -= power
    else:
        if available_power >= max(power, threshold_on) and seconds_in_state >= m.get("min_offtime", 300):
            actions[m["id"]] = "resume"
            available_power -= power
    return available_power


def _decide_threshold_higher_is_on(m, is_running, seconds_in_state, value, actions):
    """Für pv_production und battery_soc: HOHER Wert schaltet ein, NIEDRIGER schaltet aus.
    threshold_on: ab diesem Wert einschalten. threshold_off: darunter ausschalten
    (threshold_off sollte <= threshold_on sein, sonst flattert es ohne Hysterese-Lücke)."""
    threshold_on = m.get("threshold_on", 0)
    threshold_off = m.get("threshold_off", 0)
    if is_running:
        if value < threshold_off and seconds_in_state >= m.get("min_runtime", 300):
            actions[m["id"]] = "pause"
    else:
        if value >= threshold_on and seconds_in_state >= m.get("min_offtime", 300):
            actions[m["id"]] = "resume"


def _decide_threshold_lower_is_on(m, is_running, seconds_in_state, value, actions):
    """Für grid_import: NIEDRIGER Wert (wenig/kein Netzbezug) schaltet ein, HÖHERER schaltet aus.
    threshold_on: einschalten, solange Bezug darunter bleibt. threshold_off: ausschalten,
    wenn Bezug darüber steigt (threshold_off sollte >= threshold_on sein für eine Hysterese-Lücke)."""
    threshold_on = m.get("threshold_on", 500)
    threshold_off = m.get("threshold_off", 1000)
    if is_running:
        if value > threshold_off and seconds_in_state >= m.get("min_runtime", 300):
            actions[m["id"]] = "pause"
    else:
        if value <= threshold_on and seconds_in_state >= m.get("min_offtime", 300):
            actions[m["id"]] = "resume"


def _seconds_since(iso_str, now):
    if not iso_str:
        return 999999
    try:
        dt = datetime.fromisoformat(iso_str)
        return max(0, (now - dt).total_seconds())
    except Exception:
        return 999999

'''

def _make_automation():
    import types
    ns = {"__name__": "smarthome.automation", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.automation")
    mod.__dict__.update(ns)
    exec(compile(AUTOMATION_SOURCE, "<automation>", "exec"), mod.__dict__)
    return mod

# database-Modul wird von savings/energy_analytics/scheduler gebraucht -> zuerst erzeugen
_database_module = _make_database()

_fronius_module = _make_fronius()
_miners_module = _make_miners()
_external_apis_module = _make_external_apis()
_automation_module = _make_automation()

SAVINGS_SOURCE = r'''
"""
savings.py
Berechnet Kosteneinsparungen durch PV-Eigenverbrauch.
Nutzt die rollende Energy-History (energy_history Tabelle) um taegliche,
monatliche und jaehrliche Werte zu schaetzen - daher ist die Genauigkeit
abhaengig davon, wie lange das System schon laeuft (die ersten Tage sind
Hochrechnungen).
"""
import logging
from datetime import datetime

log = logging.getLogger("smarthome.savings")


def compute_savings(grid_price=0.25):
    """
    grid_price: aktueller/durchschnittlicher Netzbezugspreis in EUR/kWh
    (vereinfachte Rechnung: jede kWh PV, die selbst verbraucht statt aus dem
    Netz bezogen wird, spart grid_price - buyback_price).

    Nutzt für "heute" die Live-Rohdaten und für Monat/Jahr die bereits
    abgeschlossenen Tagesaggregate (daily_energy_summary) plus den
    laufenden Tag - so bleiben die Werte auch korrekt, wenn die
    48h-Rohdaten-History schon aufgeräumt wurde.
    """
    buyback = float(db.get_setting("electricity_buyback_price", "0.07"))
    install_cost = float(db.get_setting("pv_install_cost", "12000"))

    today = db.get_today_partial_summary()
    month_days = db.get_daily_summaries(days=30)
    year_days = db.get_daily_summaries(days=365)

    self_kwh_today = today["self_consumed_kwh"]
    export_kwh_today = today["exported_kwh"]

    self_kwh_month = sum(d["self_consumed_kwh"] for d in month_days) + self_kwh_today
    export_kwh_month = sum(d["exported_kwh"] for d in month_days) + export_kwh_today
    self_kwh_year = sum(d["self_consumed_kwh"] for d in year_days) + self_kwh_today
    export_kwh_year = sum(d["exported_kwh"] for d in year_days) + export_kwh_today

    daily_savings = self_kwh_today * grid_price + export_kwh_today * buyback
    monthly_savings = self_kwh_month * grid_price + export_kwh_month * buyback
    yearly_savings = self_kwh_year * grid_price + export_kwh_year * buyback

    roi_years = None
    if yearly_savings > 0:
        roi_years = round(install_cost / yearly_savings, 1)

    return {
        "daily_savings": round(daily_savings, 2),
        "monthly_savings": round(monthly_savings, 2),
        "yearly_savings": round(yearly_savings, 2),
        "roi_years": roi_years,
        "self_consumed_kwh": round(self_kwh_today, 2),
        "exported_kwh": round(export_kwh_today, 2),
    }


def _self_and_export_kwh(history):
    """
    Integriert Leistungswerte (W) ueber die Zeit zu Energie (kWh).
    Nutzt die tatsaechliche Zeitdifferenz zwischen Messpunkten fuer korrekte
    Integration (Trapezregel-naeherung ueber Rechtecke).
    """
    if len(history) < 2:
        return 0.0, 0.0

    self_wh = 0.0
    export_wh = 0.0
    for i in range(1, len(history)):
        prev = history[i - 1]
        cur = history[i]
        try:
            t0 = datetime.fromisoformat(prev["timestamp"])
            t1 = datetime.fromisoformat(cur["timestamp"])
            dt_hours = (t1 - t0).total_seconds() / 3600.0
        except Exception:
            dt_hours = 30 / 3600.0

        if dt_hours <= 0 or dt_hours > 1:
            continue

        pv = cur.get("pv_power") or 0
        grid = cur.get("grid_import") or 0

        if grid < 0:
            export_wh += abs(grid) * dt_hours
        self_consumed_power = max(0, pv - max(0, -grid))
        self_wh += self_consumed_power * dt_hours

    return self_wh / 1000.0, export_wh / 1000.0

'''

def _make_savings():
    import types
    ns = {"__name__": "smarthome.savings", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    ns.update({"db": _database_module})
    mod = types.ModuleType("smarthome.savings")
    mod.__dict__.update(ns)
    exec(compile(SAVINGS_SOURCE, "<savings>", "exec"), mod.__dict__)
    return mod

_savings_module = _make_savings()

ENERGY_ANALYTICS_SOURCE = r'''
"""
energy_analytics.py
Aufbereitete Analysen für die Energie-Detailseite des Dashboards:
  - Wochenvergleich (diese Woche vs. letzte Woche, Tag für Tag)
  - Monatsübersicht (Tagesbalken über die letzten 30 Tage)
  - Tagesprofil (Ø PV/Verbrauch je Uhrzeit)
  - PV-Ertrags-Historie (alle bisher abgeschlossenen Tage)

Reine Aufbereitungs-/Aggregationslogik auf Basis der database.py-Funktionen,
keine eigene I/O.
"""
import logging
from datetime import datetime, timedelta

log = logging.getLogger("smarthome.energy_analytics")

WEEKDAY_LABELS_DE = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]


def get_week_comparison():
    """
    Liefert Tagesreihen für die aktuelle Woche (Mo-So, bis inkl. heute) und
    die vorherige Woche - zum direkten Vergleich im Chart.
    Returns dict mit current_week, previous_week (je Liste von 7 dicts:
    {weekday, day, pv_kwh, house_kwh, self_consumed_kwh}), und je einer
    Summe für PV/Haus pro Woche.
    """
    today = datetime.now().date()
    monday_this_week = today - timedelta(days=today.weekday())
    monday_last_week = monday_this_week - timedelta(days=7)

    today_partial = db.get_today_partial_summary()
    daily = {d["day"]: d for d in db.get_daily_summaries(days=21)}
    daily[today_partial["day"]] = today_partial

    def build_week(monday):
        days = []
        for i in range(7):
            day_date = monday + timedelta(days=i)
            day_str = day_date.strftime("%Y-%m-%d")
            entry = daily.get(day_str)
            days.append({
                "weekday": WEEKDAY_LABELS_DE[i],
                "day": day_str,
                "pv_kwh": entry["pv_kwh"] if entry else None,
                "house_kwh": entry["house_kwh"] if entry else None,
                "self_consumed_kwh": entry["self_consumed_kwh"] if entry else None,
                "is_future": day_date > today,
            })
        return days

    current_week = build_week(monday_this_week)
    previous_week = build_week(monday_last_week)

    def week_sum(days, key):
        return round(sum(d[key] for d in days if d[key] is not None), 2)

    return {
        "current_week": current_week,
        "previous_week": previous_week,
        "current_week_pv_total": week_sum(current_week, "pv_kwh"),
        "previous_week_pv_total": week_sum(previous_week, "pv_kwh"),
        "current_week_house_total": week_sum(current_week, "house_kwh"),
        "previous_week_house_total": week_sum(previous_week, "house_kwh"),
    }


def get_month_overview(days=30):
    """
    Liefert die letzten `days` Tage als Liste (älteste zuerst) für ein
    Balkendiagramm: PV-Ertrag, Eigenverbrauch, Einspeisung pro Tag.
    Enthält den laufenden (unvollständigen) Tag am Ende.
    """
    summaries = db.get_daily_summaries(days=days)
    today_partial = db.get_today_partial_summary()

    by_day = {s["day"]: s for s in summaries}
    by_day[today_partial["day"]] = today_partial

    today = datetime.now().date()
    result = []
    for i in range(days - 1, -1, -1):
        day_date = today - timedelta(days=i)
        day_str = day_date.strftime("%Y-%m-%d")
        entry = by_day.get(day_str)
        result.append({
            "day": day_str,
            "label": f"{day_date.day}.{day_date.month}.",
            "pv_kwh": entry["pv_kwh"] if entry else 0,
            "self_consumed_kwh": entry["self_consumed_kwh"] if entry else 0,
            "exported_kwh": entry["exported_kwh"] if entry else 0,
            "imported_kwh": entry["imported_kwh"] if entry else 0,
            "house_kwh": entry["house_kwh"] if entry else 0,
            "has_data": entry is not None and entry.get("sample_count", 0) > 0,
        })
    return result


def get_daily_profile(days=14):
    """Durchschnittliches Tagesprofil (PV/Haus/Netz je Stunde) - siehe database.get_hourly_profile."""
    return db.get_hourly_profile(days=days)


def get_pv_yield_history(days=30):
    """Reine PV-Ertrags-Zeitreihe (kWh/Tag) für ein Verlaufs-Chart."""
    overview = get_month_overview(days=days)
    return [{"day": d["day"], "label": d["label"], "pv_kwh": d["pv_kwh"], "has_data": d["has_data"]} for d in overview]


def get_energy_kpis():
    """
    Kompakte Kennzahlen-Sammlung für den Kopf der Energie-Detailseite:
    aktueller Monat, bester Tag, Durchschnitt pro Tag, Wochenvergleich.
    """
    month_days = get_month_overview(days=30)
    days_with_data = [d for d in month_days if d["has_data"]]

    total_pv = round(sum(d["pv_kwh"] for d in days_with_data), 1)
    avg_per_day = round(total_pv / len(days_with_data), 2) if days_with_data else 0
    best_day = max(days_with_data, key=lambda d: d["pv_kwh"]) if days_with_data else None

    week_cmp = get_week_comparison()
    wow_change = None
    if week_cmp["previous_week_pv_total"] > 0:
        wow_change = round(
            (week_cmp["current_week_pv_total"] - week_cmp["previous_week_pv_total"])
            / week_cmp["previous_week_pv_total"] * 100, 1
        )

    return {
        "total_pv_30d": total_pv,
        "avg_pv_per_day": avg_per_day,
        "best_day": best_day,
        "week_over_week_change_pct": wow_change,
    }

'''

def _make_energy_analytics():
    import types
    ns = {"__name__": "smarthome.energy_analytics", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    ns.update({"db": _database_module})
    mod = types.ModuleType("smarthome.energy_analytics")
    mod.__dict__.update(ns)
    exec(compile(ENERGY_ANALYTICS_SOURCE, "<energy_analytics>", "exec"), mod.__dict__)
    return mod

_energy_analytics_module = _make_energy_analytics()

HEIMUEBERWACHUNG_ENGINE_SOURCE = r'''
"""
heimueberwachung.py
====================
Personenerkennung über mehrere RTSP-Heimkameras mit Zonen-Logik, Zeitfenster,
Benachrichtigung (ntfy / Telegram / E-Mail / Konsole) und einer Web-Oberfläche
mit Live-Ansicht, Ereignis-Feed UND vollständiger Kamera-/Zonen-Konfiguration
im Browser. Alles in einer Datei.

WARUM ES BEI MEHREREN KAMERAS FLÜSSIG BLEIBT
----------------------------------------------
Bilderfassung (für die Live-Ansicht) und KI-Erkennung laufen komplett
getrennt:
  - Ein Erfassungs-Thread pro Kamera holt Frames so schnell wie möglich
    und hält immer nur das jeweils neueste Bild vor -> die Live-Ansicht
    im Browser ruckelt nicht, egal wie lange die Erkennung braucht.
  - EIN gemeinsamer Erkennungs-Thread geht reihum (round-robin) alle
    Kameras durch und lässt sich dabei Zeit -> die CPU wird nicht durch
    mehrere parallele YOLO-Läufe überlastet.

WAS ERKANNT WIRD
-----------------
Standardmäßig wird nur "Person" erkannt, aber unter "Einstellungen" kann
im Browser ausgewählt werden, welche Objektklassen (Auto, Katze, Hund, ...)
zusätzlich erkannt werden sollen.

AUFNAHMEN
---------
Zusätzlich zu den bisherigen Foto-Schnappschüssen bei Alarmen kann das
System jetzt auch kurze Video-Clips aufnehmen, sobald sich ein erkanntes
Objekt in einer Zone befindet. Über "Aufnahme-Regeln" wird festgelegt,
WELCHE Objektklassen WANN (Zeitfenster) eine Aufnahme auslösen -- z.B.
Personen rund um die Uhr, aber Tiere nur nachts.

NEU IN DIESER VERSION (Web-UI)
-------------------------------
  - Komplett überarbeitetes, moderneres Design (Dashboard-Kacheln,
    Karten-Layout, sanfte Animationen, konsistente Statusfarben).
  - Live-Dashboard mit Kennzahlen: Kameras online, Zonen, Ereignisse
    heute, aktive Aufnahmen.
  - Filter & Suche im Ereignis-Feed (nach Kamera, Foto/Video, Freitext).
  - Klick auf Vorschaubild/Kamera-Bild öffnet eine große Lightbox-Ansicht.
  - Optionale Ton- und Browser-Benachrichtigungen bei neuen Ereignissen
    (direkt im Browser, unabhängig vom serverseitigen Benachrichtigungsweg).
  - Benachrichtigungen lassen sich mit einem Klick für 15 Min / 1 Std /
    8 Std stummschalten ("Schneeschieben ohne Alarm-Spam") -- inkl.
    Testbenachrichtigungs-Button in den Einstellungen.
  - Jede Zone bekommt automatisch eine eigene Farbe (Live-Ansicht und
    Zonen-Editor), damit mehrere Zonen auf einen Blick unterscheidbar sind.
  - Kleine Toast-Hinweise statt Browser-alert()-Popups.
  - Statistik-Seite mit Diagrammen (erkannte Objektklassen, Kameras,
    Verlauf der letzten 14 Tage, Aktivität nach Tagesstunde).
  - ALLES übersteht einen Neustart: Kameras/Zonen/Erkennungsklassen/
    Aufnahme-Regeln liegen in config.json, die Langzeit-Statistik in
    stats.json und jetzt auch der Ereignis-Feed (letzte Ereignisse
    inkl. Foto-/Video-Verknüpfung) in events.json. Fotos und Videos
    selbst liegen ohnehin als normale Dateien auf der Festplatte.
  - Durchgehend emoji-freies Design: schlichte, konsistente SVG-Icons
    für Navigation und Lightbox, klare Typografie, farbige Akzente
    statt Bildchen (z.B. farbiger Rahmen oben an den Dashboard-Kacheln).
  - Personenzähler ("wer ist gerade drinnen"): verbindet zwei Zonen
    einer Kamera (z.B. an der Haustür "Draussen" und "Flur") zu einem
    Zähler. Bewegt sich eine Person von der Außen- in die Innenzone,
    zählt das als "rein", umgekehrt als "raus". Der aktuelle Stand wird
    auf dem Dashboard angezeigt, lässt sich dort auch manuell +/-
    korrigieren, und übersteht dank occupancy.json einen Neustart.
    Verwaltung der Zähler unter "Einstellungen" -> "Personenzähler".
  - Externe API für ein Home-Dashboard: GET /api/dashboard liefert
    kompakte JSON-Daten (verbundene Kameras, aktuell sichtbare
    Personen/Objekte, Personenzähler-Stände, aktive Aufnahmen,
    Ereignisse heute). Optional durch einen automatisch erzeugten
    API-Schlüssel geschützt (Header "X-API-Key" oder "?api_key=");
    Schlüssel und Beispiel-URL stehen unter "Einstellungen" -> "Externe
    API".
  - Zonen-Editor: Button "Ganzes Bild als Zone" legt eine Zone über das
    komplette Kamerabild an, ohne die Ecken manuell anklicken zu müssen.
  - BUGFIX: Zonen werden jetzt auflösungsunabhängig (normiert, 0.0-1.0)
    gespeichert. Vorher konnte es passieren, dass eine Zone, die
    gezeichnet wurde bevor die Kamera verbunden war (Platzhalterbild in
    anderer Auflösung als der echte Stream), im Live-Bild an der
    falschen Stelle landete oder gar nicht mehr sichtbar/treffsicher
    war. Bereits bestehende Zonen im alten Format funktionieren
    automatisch weiter.
  - WICHTIGER BUGFIX: Der Erkennungs-Thread verarbeitet Kameras jetzt mit
    try/except pro Kamera und meldet sich nach jedem Durchlauf mit einem
    "Herzschlag". Vorher konnte EIN einziger Fehler bei EINER Kamera
    (defekter Frame, kaputte Zonen-Daten, o.ä.) den gesamten
    Erkennungs-Thread lautlos für immer beenden -- alle Kameras hätten
    dann nie wieder etwas erkannt, ohne dass man das im Web-UI bemerkt
    (Live-Bilder liefen ja unabhängig davon weiter). Fehler werden jetzt
    mit vollständigem Traceback in der Konsole ausgegeben, und das
    Dashboard zeigt eine Warnung an, falls die Erkennung doch mal
    "einschläft" (auch über /api/stats und /api/dashboard abrufbar als
    "engine_alive").
  - BUGFIX: Erkennung läuft standardmäßig auf CPU (config.json unter
    "detection.device", umschaltbar unter "Einstellungen" ->
    "Erkennungs-Engine"). Grund: ein sehr häufiger Fehler ("Could not run
    'torchvision::nms' with arguments from the 'CUDA' backend") tritt
    auf, wenn torch eine GPU findet und nutzen will, torchvision aber
    ohne passende CUDA-Unterstützung installiert wurde. CPU läuft
    garantiert überall; wer eine sauber zueinander passende
    torch/torchvision-CUDA-Installation hat, kann in den Einstellungen
    auf GPU umstellen.

INSTALLATION
------------
    pip install ultralytics opencv-python flask requests numpy

KONFIGURATION
-------------
Kameras (RTSP-URLs) und Zonen werden komplett über die Web-Oberfläche unter
"Einstellungen" verwaltet und automatisch in config.json gespeichert.
Ebenso die Erkennungsklassen und die Aufnahme-Regeln.
Nur Erkennungs-Feinabstimmung, Zeitfenster und Benachrichtigungsweg stehen
unten im Abschnitt "STANDARD-KONFIGURATION" (werden beim ersten Start
ebenfalls nach config.json geschrieben und können dort angepasst werden).

STARTEN
-------
    python heimueberwachung.py

Dann im Browser öffnen:  http://<IP-des-Rechners>:8000
Dort unter "Einstellungen" Kameras hinzufügen, Zonen per Mausklick auf das
Live-Bild einzeichnen, Erkennungsklassen wählen und Aufnahme-Regeln anlegen.
"""

import argparse
import json
import logging
import os
import shutil
import smtplib
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import cv2
import numpy as np
import requests
from flask import Flask, Response, jsonify, request, send_from_directory

# ultralytics wird erst beim tatsächlichen Lauf importiert, damit die
# Web-UI auch ohne installiertes Modell zum Testen gestartet werden kann.

# ============================================================
# EINBETTUNG INS SMART-HOME-DASHBOARD
# ============================================================
# Läuft jetzt NICHT mehr als eigener Server auf einem eigenen Port, sondern
# eingebettet im selben Prozess/Port wie das Smart-Home-Dashboard (siehe
# start_embedded() ganz unten in dieser Datei sowie app.py im Hauptprojekt).
# Alle Config-/Daten-Dateien (config.json, events.json, stats.json,
# occupancy.json, Snapshots, Aufnahmen) liegen dafür in einem eigenen,
# absoluten Ordner "instance/heimueberwachung/" innerhalb des
# Dashboard-Projekts statt relativ zum Arbeitsverzeichnis - das macht den
# Speicherort unabhängig davon, aus welchem Ordner heraus das Dashboard
# gestartet wird.

log = logging.getLogger("smarthome.heimueberwachung")

_ENGINE_BASE_DIR = _PROJECT_BASE_DIR  # Projekt-Root (alles in einer Datei)
_INSTANCE_DIR = os.path.join(_ENGINE_BASE_DIR, "instance", "heimueberwachung")
os.makedirs(_INSTANCE_DIR, exist_ok=True)


# ============================================================
# ERKENNBARE OBJEKTKLASSEN (Teilmenge der 80 COCO-Klassen, auf die
# YOLOv8 standardmäßig trainiert ist -- für die Heimüberwachung
# relevante Auswahl mit deutschen Namen für die Web-UI)
# ============================================================

COCO_CLASSES_DE = {
    0: "Person",
    1: "Fahrrad",
    2: "Auto",
    3: "Motorrad",
    5: "Bus",
    7: "LKW",
    14: "Vogel",
    15: "Katze",
    16: "Hund",
    17: "Pferd",
    18: "Schaf",
    19: "Kuh",
    24: "Rucksack",
    26: "Handtasche",
    28: "Koffer",
}


def class_name_de(class_id: int) -> str:
    return COCO_CLASSES_DE.get(class_id, f"Objekt (Klasse {class_id})")


# ============================================================
# STANDARD-KONFIGURATION (nur beim allerersten Start relevant,
# danach übernimmt config.json)
# ============================================================

CONFIG_PATH = os.path.join(_INSTANCE_DIR, "config.json")

DEFAULT_CONFIG = {
    "cameras": [],   # wird komplett über die Web-UI unter "Einstellungen" gepflegt
    "detection": {
        "model": "yolov8n.pt",
        "confidence": 0.45,
        "approach_area_growth": 1.5,
        "track_timeout_seconds": 5,
        "dwell_alert_seconds": 8,
        # Welche Objektklassen erkannt werden sollen (COCO-Klassen-IDs).
        # Wird über "Einstellungen" im Browser verwaltet. Standard: nur Person.
        "classes": [0],
        # Zeit, die sich der Erkennungs-Thread pro Kamera nimmt, bevor er zur
        # nächsten weiterzieht. Bei 5 Kameras und 0.3s ~ alle 1.5s pro Kamera
        # eine neue Erkennung. Bei GPU-Nutzung kann das deutlich gesenkt werden.
        "seconds_per_camera": 0.3,
        "inference_width": 640,
        # "cpu" oder "cuda". Standard ist "cpu", damit das Programm auch
        # läuft, wenn torch und torchvision nicht aus exakt zueinander
        # passenden CUDA-Builds installiert sind (ein sehr häufiger Fehler:
        # "Could not run 'torchvision::nms' with arguments from the 'CUDA'
        # backend" -- torch findet eine GPU und versucht sie zu nutzen,
        # aber torchvision wurde ohne CUDA-Unterstützung gebaut). Wer eine
        # passend installierte GPU-Umgebung hat, kann hier auf "cuda"
        # umstellen (deutlich schnellere Erkennung bei vielen Kameras).
        "device": "cpu",
    },
    "notifications": {
        "time_windows": [{"start": "22:00", "end": "06:00"}],  # leer = immer aktiv
        "cooldown_seconds": 60,
        "method": "console",   # "ntfy" | "telegram" | "email" | "console"
        "ntfy": {"server": "https://ntfy.sh", "topic": "mein-haus-alarm-BITTE-AENDERN"},
        "telegram": {"bot_token": "", "chat_id": ""},
        "email": {"smtp_server": "smtp.gmail.com", "smtp_port": 587, "username": "", "password": "", "to_address": ""},
    },
    "recording": {
        # Video-Aufnahme ist bewusst deaktiviert - es werden bei einer
        # Erkennung ausschließlich Fotos gespeichert (siehe save_alert_snapshot
        # in _maybe_notify / den Occupancy-Auswertungen weiter unten). Der
        # Wert wird zusätzlich beim Start erzwungen (siehe unten bei CONFIG =
        # load_config()), damit auch eine ältere config.json mit "enabled":
        # true keine Videos mehr aufnimmt.
        "enabled": False,
        "output_dir": "recordings",
        "fps": 8,
        # Sicherheitsobergrenze pro Aufnahme-Clip, damit nichts endlos aufnimmt.
        # Bleibt das Objekt länger in der Zone, beginnt danach automatisch ein
        # neuer Clip.
        "max_seconds": 30,
        # Jede Regel legt fest: WELCHE Klassen lösen WANN eine Aufnahme aus,
        # sobald sie sich in einer Zone befinden. time_windows leer = immer.
        "rules": [
            {"classes": [0], "time_windows": [], "enabled": True},
        ],
    },
    "storage": {
        "snapshot_dir": "snapshots",
        "max_events_in_feed": 100,
        # Automatisches Aufräumen der Foto-Galerie, damit die Festplatte nicht
        # unbegrenzt vollläuft. 0 = jeweils kein Limit. Wird stündlich geprüft
        # (siehe _cleanup_loop) und ist unter Heimüberwachung → Einstellungen
        # einstellbar.
        "max_photo_age_days": 30,
        "max_photos": 5000,
    },
    "web_ui": {
        "host": "0.0.0.0",
        "port": 8000,
        "stream_fps": 12,
        "stream_width": 800,
        "jpeg_quality": 70,
    },
    "api": {
        # Wird beim allerersten Start automatisch mit einem zufälligen
        # Schlüssel befüllt (siehe ensure_api_key()). Externe Systeme
        # (z.B. ein Home-Dashboard) müssen diesen Schlüssel mitschicken,
        # um /api/dashboard abzufragen. Leer lassen = keine Prüfung.
        "key": "",
    },
    "occupancy": {
        # Personenzähler: jeder Eintrag zählt "rein"/"raus" für eine Tür.
        # Zwei Modi werden unterstützt:
        #   "two_zone" (klassisch): verbindet zwei Zonen EINER Kamera zu
        #     einem "Tor" (z.B. eine Zone knapp vor und eine knapp hinter
        #     der Tür, z.B. mit zwei Kameras). Felder: outside_zone, inside_zone.
        #   "door" (Ein-Kamera-Tür-Modus): braucht nur EINE Zone genau auf
        #     der Tür. Verschwindet eine Person darin, gilt sie als "rein";
        #     taucht sie danach sichtbar außerhalb der Zone wieder auf, als
        #     "raus". Feld: door_zone.
        "counters": [],  # [{"id","name","camera","mode","outside_zone"/"inside_zone" ODER "door_zone"}]
    },
}


# ============================================================
# CONFIG-SPEICHERUNG (config.json) – thread-sicher
# ============================================================

CONFIG_LOCK = threading.Lock()


def load_config() -> dict:
    if Path(CONFIG_PATH).exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        # fehlende Schlüssel (z.B. nach Update) mit Standardwerten auffüllen
        for key, value in DEFAULT_CONFIG.items():
            cfg.setdefault(key, value)
        # verschachtelte Schlüssel ebenfalls auffüllen (wichtig für Updates
        # von älteren config.json-Dateien ohne "classes" / "recording" etc.)
        for sub_key, sub_value in DEFAULT_CONFIG["detection"].items():
            cfg["detection"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["recording"].items():
            cfg["recording"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["notifications"].items():
            cfg["notifications"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["storage"].items():
            cfg["storage"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["web_ui"].items():
            cfg["web_ui"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["api"].items():
            cfg["api"].setdefault(sub_key, sub_value)
        for sub_key, sub_value in DEFAULT_CONFIG["occupancy"].items():
            cfg["occupancy"].setdefault(sub_key, sub_value)
        return cfg
    save_config(DEFAULT_CONFIG)
    return json.loads(json.dumps(DEFAULT_CONFIG))  # tiefe Kopie


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


CONFIG = load_config()
# Video-Aufnahme ist bewusst abgeschaltet: nur Fotos werden bei einer
# Erkennung gespeichert (siehe Galerie). Wird hier zusätzlich erzwungen,
# damit auch eine bereits vorhandene config.json mit "enabled": true (von
# vor dieser Umstellung) beim nächsten Start wieder auf "nur Bilder" springt.
if CONFIG["recording"].get("enabled", False):
    CONFIG["recording"]["enabled"] = False
    save_config(CONFIG)


def ensure_api_key():
    """Erzeugt beim allerersten Start automatisch einen zufälligen API-Schlüssel
    für den externen Dashboard-Zugriff, falls noch keiner gesetzt ist."""
    if not CONFIG["api"].get("key"):
        import secrets
        with CONFIG_LOCK:
            CONFIG["api"]["key"] = secrets.token_hex(20)
            save_config(CONFIG)


ensure_api_key()

# WICHTIG (angepasst für die Einbettung ins Smart-Home-Dashboard): snapshot_dir
# / output_dir werden zu ABSOLUTEN Pfaden aufgelöst - und zwar relativ zum
# festen Ordner instance/heimueberwachung/ statt zum aktuellen
# Arbeitsverzeichnis. So landen Fotos/Videos immer am selben Ort, ganz
# unabhängig davon, aus welchem Verzeichnis heraus app.py gestartet wird
# (Flasks send_from_directory() und cv2.imwrite()/VideoWriter würden bei
# einem nur relativen Pfad sonst je nach Startordner auseinanderlaufen).
def _resolve_under_instance(path_str: str) -> str:
    p = Path(path_str)
    return str(p if p.is_absolute() else (Path(_INSTANCE_DIR) / p).resolve())


CONFIG["storage"]["snapshot_dir"] = _resolve_under_instance(CONFIG["storage"]["snapshot_dir"])
CONFIG["recording"]["output_dir"] = _resolve_under_instance(CONFIG["recording"]["output_dir"])


# ============================================================
# HILFSFUNKTIONEN
# ============================================================

def iou(box_a, box_b) -> float:
    xa1, ya1, xa2, ya2 = box_a
    xb1, yb1, xb2, yb2 = box_b
    inter_x1, inter_y1 = max(xa1, xb1), max(ya1, yb1)
    inter_x2, inter_y2 = min(xa2, xb2), min(ya2, yb2)
    inter_w, inter_h = max(0, inter_x2 - inter_x1), max(0, inter_y2 - inter_y1)
    inter_area = inter_w * inter_h
    area_a = (xa2 - xa1) * (ya2 - ya1)
    area_b = (xb2 - xb1) * (yb2 - yb1)
    union = area_a + area_b - inter_area
    return inter_area / union if union > 0 else 0.0


def bbox_area(box) -> float:
    x1, y1, x2, y2 = box
    return max(0, x2 - x1) * max(0, y2 - y1)


def bbox_center(box):
    x1, y1, x2, y2 = box
    return (int((x1 + x2) / 2), int((y1 + y2) / 2))


def point_in_polygon(point, polygon) -> bool:
    poly_np = np.array(polygon, dtype=np.int32)
    return cv2.pointPolygonTest(poly_np, point, False) >= 0


def zone_points_px(zone: dict, frame_w: int, frame_h: int) -> list:
    """Löst die Punkte einer Zone in absolute Pixel-Koordinaten für eine
    KONKRETE Ziel-Bildgröße auf.

    Zonen werden seit diesem Update NORMIERT (0.0-1.0, relativ zur
    Kamera-Auflösung) in config.json gespeichert -- dadurch passt eine Zone
    immer exakt zum Live-Bild, egal ob sie gezeichnet wurde, während die
    Kamera schon lief oder noch nicht verbunden war (Platzhalterbild in
    anderer Auflösung), und egal in welcher Auflösung/Größe gerade
    dargestellt wird (Stream, Schnappschuss, Zonen-Editor).

    Ältere config.json-Dateien (vor diesem Update) enthalten noch absolute
    Pixel-Koordinaten. Die werden hier automatisch erkannt (Werte deutlich
    über 1.5) und unverändert weiterverwendet, damit bestehende Zonen nicht
    neu gezeichnet werden müssen.
    """
    points = zone["points"]
    max_val = max((abs(v) for p in points for v in p), default=0)
    if max_val <= 1.5:
        return [[p[0] * frame_w, p[1] * frame_h] for p in points]
    return points


def is_within_time_window(time_windows: list) -> bool:
    if not time_windows:
        return True
    now = datetime.now().time()
    for window in time_windows:
        start = datetime.strptime(window["start"], "%H:%M").time()
        end = datetime.strptime(window["end"], "%H:%M").time()
        if start <= end:
            if start <= now <= end:
                return True
        else:
            if now >= start or now <= end:
                return True
    return False


def zone_color_bgr(name: str) -> tuple:
    """Erzeugt aus dem Zonen-Namen eine stabile, gut sichtbare Farbe
    (BGR, für OpenCV), damit mehrere Zonen im Bild klar unterscheidbar
    sind, ohne dass man Farben manuell zuweisen muss."""
    h = sum(ord(c) for c in name) * 2654435761 % (2 ** 32)
    hue = h % 180  # OpenCV-Hue geht von 0-179
    hsv = np.uint8([[[hue, 220, 255]]])
    bgr = cv2.cvtColor(hsv, cv2.COLOR_HSV2BGR)[0][0]
    return (int(bgr[0]), int(bgr[1]), int(bgr[2]))


# ============================================================
# EREIGNIS-FEED (für die Web-UI, thread-sicher, übersteht einen Neustart)
# ============================================================

EVENTS_PATH = os.path.join(_INSTANCE_DIR, "events.json")


class EventFeed:
    def __init__(self, max_events: int, path: str = EVENTS_PATH):
        self._events = deque(maxlen=max_events)
        self._lock = threading.Lock()
        self._path = path
        self._load()

    def _load(self):
        if not Path(self._path).exists():
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Reihenfolge bleibt erhalten: gespeicherte Liste ist bereits
            # "neuestes zuerst", genau wie all() sie zurückgibt.
            self._events = deque(data[: self._events.maxlen], maxlen=self._events.maxlen)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Ereignis-Feed konnte nicht geladen werden ({self._path}): {e}")

    def _save(self):
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(list(self._events), f, ensure_ascii=False)
        except OSError as e:
            print(f"Ereignis-Feed konnte nicht gespeichert werden ({self._path}): {e}")

    def add(self, camera: str, zone: str, description: str, snapshot_filename: str | None,
             recording_filename: str | None = None):
        with self._lock:
            self._events.appendleft({
                "time": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "camera": camera,
                "zone": zone,
                "description": description,
                "snapshot": snapshot_filename,
                "recording": recording_filename,
            })
            self._save()

    def all(self) -> list:
        with self._lock:
            return list(self._events)


EVENT_FEED = EventFeed(CONFIG["storage"]["max_events_in_feed"])


# ============================================================
# STATISTIK-SPEICHER (langfristige Zähldaten für die Statistik-Seite)
# ============================================================
# Der Ereignis-Feed oben hält nur die letzten N Ereignisse vor (für den
# Live-Feed). Für Auswertungen ("was wurde wie oft erkannt", "wann ist am
# meisten los") werden hier dauerhafte Zähler geführt und in stats.json
# gespeichert, damit sie einen Neustart überstehen.

STATS_PATH = os.path.join(_INSTANCE_DIR, "stats.json")
STATS_LOCK = threading.Lock()

DEFAULT_STATS = {
    "class_counts": {},      # {"0": 42, "16": 3, ...}  Klassen-ID -> Anzahl Zonen-Eintritte
    "camera_counts": {},     # {"Haustuere": 42, ...}   Kamera-Name -> Anzahl Zonen-Eintritte
    "daily_counts": {},      # {"2026-08-07": {"0": 5, "16": 1}, ...}
    "hourly_counts": [0] * 24,   # Anzahl Zonen-Eintritte je Tagesstunde (0-23), gesamt
    "recordings_started": 0,     # Anzahl gestarteter Video-Aufnahmen
}


def load_stats() -> dict:
    if Path(STATS_PATH).exists():
        with open(STATS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for key, value in DEFAULT_STATS.items():
            data.setdefault(key, json.loads(json.dumps(value)))
        if len(data.get("hourly_counts", [])) != 24:
            data["hourly_counts"] = [0] * 24
        return data
    save_stats(DEFAULT_STATS)
    return json.loads(json.dumps(DEFAULT_STATS))


def save_stats(stats: dict):
    with open(STATS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False)


STATS = load_stats()


def record_detection_stat(class_id: int, camera_name: str):
    """Wird bei jedem Zonen-Eintritt eines Objekts aufgerufen (einmal pro
    Track, nicht bei jedem einzelnen Frame)."""
    with STATS_LOCK:
        cid = str(class_id)
        STATS["class_counts"][cid] = STATS["class_counts"].get(cid, 0) + 1
        STATS["camera_counts"][camera_name] = STATS["camera_counts"].get(camera_name, 0) + 1
        day = datetime.now().strftime("%Y-%m-%d")
        STATS["daily_counts"].setdefault(day, {})
        STATS["daily_counts"][day][cid] = STATS["daily_counts"][day].get(cid, 0) + 1
        STATS["hourly_counts"][datetime.now().hour] += 1
        save_stats(STATS)


def record_recording_stat():
    with STATS_LOCK:
        STATS["recordings_started"] = STATS.get("recordings_started", 0) + 1
        save_stats(STATS)


def get_today_person_count():
    """Anzahl Personen-Zonen-Eintritte des heutigen Tages (Klasse 0 = Person
    in COCO) - genutzt für den täglichen Zusammenfassungsbericht. Gibt None
    zurück, wenn die Personenerkennung nicht läuft (kein sinnvoller Wert)."""
    if _camera_manager is None:
        return None
    day = datetime.now().strftime("%Y-%m-%d")
    with STATS_LOCK:
        return STATS.get("daily_counts", {}).get(day, {}).get("0", 0)


def reset_stats():
    global STATS
    with STATS_LOCK:
        STATS = json.loads(json.dumps(DEFAULT_STATS))
        save_stats(STATS)


# ============================================================
# PERSONENZÄHLER ("wer ist gerade drinnen") – übersteht Neustart
# ============================================================
# Jeder Zähler verbindet zwei Zonen einer Kamera zu einem "Tor" (z.B. an
# der Haustür eine Zone "Draussen" und eine Zone "Flur"). Wechselt eine
# erkannte Person zwischen diesen beiden Zonen, wird der aktuelle Stand
# entsprechend erhöht oder verringert. Der Stand selbst liegt in
# occupancy.json, die Zähler-DEFINITIONEN (welche Kamera/Zonen) liegen in
# config.json unter "occupancy.counters".

OCCUPANCY_PATH = os.path.join(_INSTANCE_DIR, "occupancy.json")
OCCUPANCY_LOCK = threading.Lock()


def load_occupancy() -> dict:
    if Path(OCCUPANCY_PATH).exists():
        try:
            with open(OCCUPANCY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_occupancy(data: dict):
    try:
        with open(OCCUPANCY_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError as e:
        print(f"Personenzähler konnte nicht gespeichert werden: {e}")


OCCUPANCY = load_occupancy()


def get_occupancy_count(counter_id: str) -> int:
    with OCCUPANCY_LOCK:
        return int(OCCUPANCY.get(counter_id, 0))


def adjust_occupancy(counter_id: str, delta: int) -> int:
    with OCCUPANCY_LOCK:
        new_value = max(0, int(OCCUPANCY.get(counter_id, 0)) + delta)
        OCCUPANCY[counter_id] = new_value
        save_occupancy(OCCUPANCY)
        return new_value


def set_occupancy(counter_id: str, value: int) -> int:
    with OCCUPANCY_LOCK:
        new_value = max(0, int(value))
        OCCUPANCY[counter_id] = new_value
        save_occupancy(OCCUPANCY)
        return new_value


def get_all_occupancy_counts() -> dict:
    with OCCUPANCY_LOCK:
        return dict(OCCUPANCY)


# ============================================================
# LEBENSZEICHEN DES ERKENNUNGS-THREADS ("Heartbeat")
# ============================================================
# Ohne diese Überwachung würde ein hängender/abgestürzter
# Erkennungs-Thread lautlos gar nichts mehr erkennen, ohne dass man das im
# Web-UI bemerkt (Kamera-Streams laufen ja unabhängig davon weiter). Der
# Erkennungs-Thread meldet sich hier nach jedem Durchlauf; Dashboard und
# externe API können daran erkennen, ob die Erkennung noch aktiv ist.

ENGINE_HEARTBEAT_LOCK = threading.Lock()
ENGINE_HEARTBEAT = {"last_seen": 0.0}


def touch_engine_heartbeat():
    with ENGINE_HEARTBEAT_LOCK:
        ENGINE_HEARTBEAT["last_seen"] = time.time()


def seconds_since_engine_heartbeat() -> float | None:
    with ENGINE_HEARTBEAT_LOCK:
        last_seen = ENGINE_HEARTBEAT["last_seen"]
    if last_seen == 0.0:
        return None  # Engine ist noch gar nicht gestartet / hat noch nie gemeldet
    return time.time() - last_seen


# ============================================================
# BENACHRICHTIGUNGS-STUMMSCHALTUNG ("Snooze")
# ============================================================
# Erlaubt es, serverseitige Push-Benachrichtigungen (ntfy/Telegram/E-Mail)
# vorübergehend zu unterdrücken, ohne die Erkennung oder den Ereignis-Feed
# zu beeinflussen -- Ereignisse werden weiterhin protokolliert.

NOTIFY_STATE_LOCK = threading.Lock()
NOTIFY_STATE = {"snoozed_until": 0.0}


def is_notifications_snoozed() -> bool:
    with NOTIFY_STATE_LOCK:
        return time.time() < NOTIFY_STATE["snoozed_until"]


def get_snooze_until() -> float:
    with NOTIFY_STATE_LOCK:
        return NOTIFY_STATE["snoozed_until"]


def set_snooze(minutes: float):
    with NOTIFY_STATE_LOCK:
        NOTIFY_STATE["snoozed_until"] = time.time() + minutes * 60 if minutes > 0 else 0.0


# ============================================================
# BENACHRICHTIGUNGEN
# ============================================================

class BaseNotifier:
    def send(self, title: str, message: str, image_path: str | None = None):
        raise NotImplementedError


class ConsoleNotifier(BaseNotifier):
    def send(self, title, message, image_path=None):
        print(f"\n[BENACHRICHTIGUNG] {title}\n{message}")


class NtfyNotifier(BaseNotifier):
    def __init__(self, server: str, topic: str):
        self.url = f"{server.rstrip('/')}/{topic}"

    def send(self, title, message, image_path=None):
        headers = {"Title": title.encode("utf-8"), "Priority": "high"}
        try:
            if image_path and Path(image_path).exists():
                with open(image_path, "rb") as f:
                    requests.put(
                        self.url, data=f,
                        headers={**headers, "Filename": Path(image_path).name, "Message": message.encode("utf-8")},
                        timeout=10,
                    )
            else:
                requests.post(self.url, data=message.encode("utf-8"), headers=headers, timeout=10)
        except requests.RequestException as e:
            print(f"ntfy-Benachrichtigung fehlgeschlagen: {e}")


class TelegramNotifier(BaseNotifier):
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send(self, title, message, image_path=None):
        text = f"*{title}*\n{message}"
        try:
            if image_path and Path(image_path).exists():
                url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
                with open(image_path, "rb") as f:
                    requests.post(url, data={"chat_id": self.chat_id, "caption": text, "parse_mode": "Markdown"},
                                  files={"photo": f}, timeout=15)
            else:
                url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
                requests.post(url, data={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
        except requests.RequestException as e:
            print(f"Telegram-Benachrichtigung fehlgeschlagen: {e}")


class EmailNotifier(BaseNotifier):
    def __init__(self, smtp_server, smtp_port, username, password, to_address):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.to_address = to_address

    def send(self, title, message, image_path=None):
        msg = EmailMessage()
        msg["Subject"] = title
        msg["From"] = self.username
        msg["To"] = self.to_address
        msg.set_content(message)
        if image_path and Path(image_path).exists():
            with open(image_path, "rb") as f:
                msg.add_attachment(f.read(), maintype="image", subtype="jpeg", filename=Path(image_path).name)
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.username, self.password)
                server.send_message(msg)
        except Exception as e:
            print(f"E-Mail-Benachrichtigung fehlgeschlagen: {e}")


def build_notifier(cfg: dict) -> BaseNotifier:
    method = cfg.get("method", "console")
    if method == "ntfy":
        return NtfyNotifier(**cfg["ntfy"])
    if method == "telegram":
        return TelegramNotifier(**cfg["telegram"])
    if method == "email":
        return EmailNotifier(**cfg["email"])
    return ConsoleNotifier()


# ============================================================
# TRACKING (klassen-übergreifend: Person, Auto, Katze, ...)
# ============================================================

@dataclass
class Track:
    track_id: int
    class_id: int
    bbox: tuple
    first_seen: float
    last_seen: float
    start_area: float
    zone_enter_time: dict = field(default_factory=dict)
    zone_alerted_dwell: set = field(default_factory=set)
    zone_alerted_enter: set = field(default_factory=set)
    counting_state: dict = field(default_factory=dict)  # counter_id -> "outside" | "inside"
    # Für den Ein-Kamera-Tür-Modus (siehe unten): merkt sich pro Tür-Zone,
    # ob sich dieses Objekt dort im letzten Frame gerade befunden hat --
    # wird gebraucht, um sowohl "verlässt die Tür-Zone wieder sichtbar"
    # (raus) als auch "verschwindet in der Tür-Zone" (rein) zu erkennen.
    door_zone_inside: dict = field(default_factory=dict)  # zone_name -> bool


class ObjectTracker:
    """Verfolgt erkannte Objekte über mehrere Frames hinweg (IOU-Matching).
    Ein Track behält immer seine ursprüngliche Klasse -- eine Person kann
    also nie versehentlich mit dem Track eines Autos verschmelzen."""

    def __init__(self, timeout_seconds: float):
        self.tracks: dict[int, Track] = {}
        self.next_id = 1
        self.timeout_seconds = timeout_seconds

    def update(self, detections: list) -> tuple:
        """Gibt (aktive Tracks als dict, gerade entfernte Tracks als Liste)
        zurück. Die entfernten Tracks werden für den Tür-Modus gebraucht:
        so lässt sich erkennen, dass ein Objekt "verschwunden" ist, während
        es sich zuletzt in einer Tür-Zone befand."""
        # detections: Liste aus (bbox, class_id)
        now = time.time()
        unmatched = list(range(len(detections)))

        for track in self.tracks.values():
            best_iou, best_idx = 0.0, None
            for idx in unmatched:
                bbox, class_id = detections[idx]
                if class_id != track.class_id:
                    continue  # nur innerhalb derselben Objektklasse zuordnen
                score = iou(track.bbox, bbox)
                if score > best_iou:
                    best_iou, best_idx = score, idx
            if best_iou > 0.3:
                bbox, _class_id = detections[best_idx]
                track.bbox = bbox
                track.last_seen = now
                unmatched.remove(best_idx)

        for idx in unmatched:

            bbox, class_id = detections[idx]
            self.tracks[self.next_id] = Track(
                track_id=self.next_id, class_id=class_id, bbox=bbox, first_seen=now, last_seen=now,
                start_area=bbox_area(bbox),
            )
            self.next_id += 1

        stale = [tid for tid, t in self.tracks.items() if now - t.last_seen > self.timeout_seconds]
        removed = [self.tracks[tid] for tid in stale]
        for tid in stale:
            del self.tracks[tid]

        return self.tracks, removed


# ============================================================
# VIDEO-AUFNAHME PRO ZONE
# ============================================================
# WICHTIG FÜR BROWSER-WIEDERGABE: Browser (Chrome, Firefox, Safari, ...)
# können Videos nur abspielen, wenn sie mit einem Codec wie H.264 (AVC)
# kodiert sind. Der früher genutzte OpenCV-Fourcc "mp4v" erzeugt zwar eine
# .mp4-Datei, aber mit dem alten MPEG-4-Part-2-Codec, den kein gängiger
# Browser abspielen kann -- die Datei lädt zwar, bleibt aber schwarz/leer.
# Damit Aufnahmen zuverlässig im Browser laufen, wird beim ersten Start
# automatisch ermittelt, welcher Weg auf diesem System funktioniert:
#   1. OpenCV kann selbst direkt H.264 schreiben ("avc1") -> das ist der
#      schnellste Weg, wird also bevorzugt.
#   2. Sonst: es wird zunächst verlustfrei/schnell als MJPG in eine
#      Zwischendatei aufgenommen und danach automatisch mit dem externen
#      Programm "ffmpeg" (falls installiert) in browserkompatibles H.264
#      umgewandelt.
#   3. Ist weder das eine noch das andere möglich, wird als letzter
#      Ausweg wieder "mp4v" verwendet (Datei wird gespeichert, spielt aber
#      evtl. nicht in jedem Browser) -- mit deutlicher Warnung in der
#      Konsole, dass "ffmpeg" installiert werden sollte.

_VIDEO_BACKEND = None
_VIDEO_BACKEND_LOCK = threading.Lock()


def detect_video_backend() -> str:
    """Ermittelt einmalig (und danach zwischengespeichert), auf welchem Weg
    dieses System browserkompatible H.264-Videos erzeugen kann.
    Rückgabe: "avc1" (OpenCV direkt), "ffmpeg" (Nachbearbeitung) oder
    "mp4v" (Notlösung, evtl. nicht browserkompatibel)."""
    global _VIDEO_BACKEND
    if _VIDEO_BACKEND is not None:
        return _VIDEO_BACKEND
    with _VIDEO_BACKEND_LOCK:
        if _VIDEO_BACKEND is not None:
            return _VIDEO_BACKEND

        test_path = str(Path(tempfile.gettempdir()) / f"_heimueberwachung_codec_test_{os.getpid()}.mp4")
        try:
            fourcc = cv2.VideoWriter_fourcc(*"avc1")
            writer = cv2.VideoWriter(test_path, fourcc, 10, (64, 64))
            opened = writer.isOpened()
            if opened:
                writer.write(np.zeros((64, 64, 3), dtype=np.uint8))
            writer.release()
            wrote_bytes = Path(test_path).exists() and Path(test_path).stat().st_size > 0
        except Exception:
            opened, wrote_bytes = False, False
        finally:
            Path(test_path).unlink(missing_ok=True)

        if opened and wrote_bytes:
            _VIDEO_BACKEND = "avc1"
            print("Video-Aufnahme: OpenCV kann H.264 direkt schreiben -- "
                  "Aufnahmen werden ohne Zusatzschritt browserkompatibel gespeichert.")
        elif shutil.which("ffmpeg"):
            _VIDEO_BACKEND = "ffmpeg"
            print("Video-Aufnahme: nutze das externe Programm 'ffmpeg', um Aufnahmen "
                  "nach dem Speichern automatisch in browserkompatibles H.264 umzuwandeln.")
        else:
            _VIDEO_BACKEND = "mp4v"
            print("WARNUNG: Weder eine OpenCV-Installation mit H.264-Unterstützung noch "
                  "'ffmpeg' wurden gefunden. Videos werden im Format 'mp4v' gespeichert und "
                  "lassen sich möglicherweise NICHT direkt im Browser abspielen. Für "
                  "browserkompatible Aufnahmen bitte ffmpeg installieren, z.B. unter Linux "
                  "'sudo apt install ffmpeg' oder unter Windows 'winget install ffmpeg'.")
        return _VIDEO_BACKEND


class ZoneRecorder(threading.Thread):
    """Nimmt Frames der jeweils neuesten Kamera-Bilder auf, solange ein
    passendes Objekt in der Zone ist (oder bis max_seconds erreicht ist).
    Erkennt automatisch den besten verfügbaren Weg zu einem browserkompatiblen
    H.264-Video (siehe detect_video_backend())."""

    def __init__(self, camera: "CameraStream", filepath: str, fps: float, max_seconds: float):
        super().__init__(daemon=True)
        self.camera = camera
        self.filepath = filepath
        self.fps = max(1, fps)
        self.max_seconds = max_seconds
        self.started_at = time.time()
        self._stop_flag = threading.Event()
        # Solange True: es werden noch Kamera-Frames aufgenommen. Danach kann
        # bei manchen Backends noch eine kurze Nachbearbeitung (ffmpeg) laufen,
        # während der Thread technisch noch "am Leben" ist -- das Web-UI nutzt
        # dieses Flag, um die "● REC"-Anzeige exakt beim Ende der eigentlichen
        # Aufnahme (nicht erst nach der Umwandlung) zu beenden.
        self.capturing = True

    def stop(self):
        self._stop_flag.set()

    def elapsed_seconds(self) -> float:
        return time.time() - self.started_at

    def run(self):
        backend = detect_video_backend()
        try:
            if backend == "ffmpeg":
                self._run_via_ffmpeg()
            else:
                self._run_direct(fourcc_tag="avc1" if backend == "avc1" else "mp4v")
        except Exception:
            import traceback
            print(f"Fehler bei Video-Aufnahme ({self.filepath}):")
            traceback.print_exc()
        finally:
            self.capturing = False

    def _capture_frames(self, writer_factory):
        """Gemeinsame Aufnahme-Schleife: holt Frames, bis Zeitlimit erreicht
        oder gestoppt wird. writer_factory(w, h) muss einen fertigen
        cv2.VideoWriter zurückgeben (wird beim ersten Frame einmalig
        aufgerufen, sobald die tatsächliche Auflösung bekannt ist)."""
        writer = None
        interval = 1.0 / self.fps
        wrote_any = False
        try:
            while not self._stop_flag.is_set() and self.elapsed_seconds() < self.max_seconds:
                frame = self.camera.get_latest_frame()
                if frame is not None:
                    if writer is None:
                        h, w = frame.shape[:2]
                        writer = writer_factory(w, h)
                    writer.write(frame)
                    wrote_any = True
                time.sleep(interval)
        finally:
            if writer is not None:
                writer.release()
        return wrote_any

    def _run_direct(self, fourcc_tag: str):
        """Schreibt die Aufnahme direkt im Zielformat (funktioniert nur,
        wenn OpenCV den gewählten Codec unterstützt)."""
        self._capture_frames(
            lambda w, h: cv2.VideoWriter(self.filepath, cv2.VideoWriter_fourcc(*fourcc_tag), self.fps, (w, h))
        )

    def _run_via_ffmpeg(self):
        """Nimmt zunächst zuverlässig als MJPG/AVI auf (funktioniert praktisch
        überall mit OpenCV) und wandelt die Datei danach mit ffmpeg in
        browserkompatibles H.264-MP4 um."""
        tmp_path = self.filepath + ".raw.avi"
        wrote_any = self._capture_frames(
            lambda w, h: cv2.VideoWriter(tmp_path, cv2.VideoWriter_fourcc(*"MJPG"), self.fps, (w, h))
        )
        self.capturing = False  # eigentliche Aufnahme ist fertig, ab hier nur noch Umwandlung

        if not wrote_any:
            Path(tmp_path).unlink(missing_ok=True)
            return
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-i", tmp_path,
                 "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
                 "-movflags", "+faststart", self.filepath],
                check=True, timeout=180,
            )
        except Exception as e:
            print(f"ffmpeg-Umwandlung fehlgeschlagen ({self.filepath}): {e}")
            # Rohaufnahme steht als Fallback weiterhin unter tmp_path zur
            # Verfügung, falls jemand sie manuell umwandeln möchte.
            return
        finally:
            Path(tmp_path).unlink(missing_ok=True)


# ============================================================
# KAMERA-ERFASSUNG (eigener Thread pro Kamera)
# ============================================================

class CameraStream:
    def __init__(self, name: str, rtsp_url: str, zones: list, det_cfg: dict):
        self.name = name
        self.rtsp_url = rtsp_url
        self.zones = zones
        self.det_cfg = det_cfg

        self.tracker = ObjectTracker(det_cfg["track_timeout_seconds"])
        self.last_notify_time: dict = {}

        # eine laufende Aufnahme pro Zonen-Name (zone_name -> ZoneRecorder)
        self.active_recordings: dict[str, ZoneRecorder] = {}

        self._frame_lock = threading.Lock()
        self._latest_frame = None
        self._overlay_boxes = []
        self._connected = False

        self.running = True
        self._thread = threading.Thread(target=self._capture_loop, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self.running = False

    def stop_and_wait(self, timeout: float = 3.0):
        """Stoppt die Kamera und wartet, bis der Erfassungs-Thread sauber
        beendet ist (verhindert Abstürze durch abrupt gekillte ffmpeg/OpenCV-
        Verbindungen, z.B. wenn eine Kamera per Web-UI entfernt/geändert wird).
        Beendet außerdem alle noch laufenden Video-Aufnahmen dieser Kamera."""
        self.running = False
        for recorder in list(self.active_recordings.values()):
            recorder.stop()
        self.active_recordings.clear()
        self._thread.join(timeout=timeout)

    def _capture_loop(self):
        cap = cv2.VideoCapture(self.rtsp_url)
        while self.running:
            ok, frame = cap.read()
            if not ok:
                self._connected = False
                time.sleep(2)
                if not self.running:
                    break
                cap.release()
                cap = cv2.VideoCapture(self.rtsp_url)
                continue
            self._connected = True
            with self._frame_lock:
                self._latest_frame = frame
        cap.release()

    def get_latest_frame(self):
        with self._frame_lock:
            return None if self._latest_frame is None else self._latest_frame.copy()

    def set_overlay(self, boxes: list):
        self._overlay_boxes = boxes

    def is_connected(self) -> bool:
        return self._connected

    def _placeholder_frame(self, width: int, height: int, text: str):
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        frame[:] = (30, 30, 30)
        cv2.putText(frame, text, (20, height // 2), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (150, 150, 150), 2)
        cv2.putText(frame, self.name, (10, 22), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        return frame

    def render_jpeg(self, width: int, quality: int) -> bytes | None:
        frame = self.get_latest_frame()
        height = int(width * 9 / 16)

        if frame is None:
            frame = self._placeholder_frame(width, height, "Verbinde zur Kamera ...")
        else:
            h, w = frame.shape[:2]
            scale = width / w
            resized_h = int(h * scale)
            frame = cv2.resize(frame, (width, resized_h))

            for zone in self.zones:
                pts = np.array(zone_points_px(zone, width, resized_h), dtype=np.int32)
                active_recorder = self.active_recordings.get(zone["name"])
                is_recording = active_recorder is not None and getattr(active_recorder, "capturing", True)
                zone_color = (0, 0, 255) if is_recording else zone_color_bgr(zone["name"])
                cv2.polylines(frame, [pts], isClosed=True, color=zone_color, thickness=2)
                label = zone["name"] + ("  ● REC" if is_recording else "")
                cv2.putText(frame, label, (pts[0][0], max(15, pts[0][1] - 8)),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, zone_color, 2)

            for bbox, color, label in self._overlay_boxes:
                x1, y1, x2, y2 = [int(v * scale) for v in bbox]
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                if label:
                    cv2.putText(frame, label, (x1, max(15, y1 - 8)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

            cv2.putText(frame, self.name, (10, 22), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
        return buf.tobytes() if ok else None

    def render_still_full(self, quality: int = 85) -> bytes | None:
        """Unverändertes Einzelbild in Originalauflösung, für den Zonen-Editor."""
        frame = self.get_latest_frame()
        if frame is None:
            frame = self._placeholder_frame(800, 450, "Kein Bild verfuegbar")
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
        return buf.tobytes() if ok else None

    def save_alert_snapshot(self, bbox, zone: dict, snapshot_dir: str) -> str:
        frame = self.get_latest_frame()
        if frame is None:
            return ""
        x1, y1, x2, y2 = [int(v) for v in bbox]
        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
        frame_h, frame_w = frame.shape[:2]
        poly = np.array(zone_points_px(zone, frame_w, frame_h), dtype=np.int32)
        cv2.polylines(frame, [poly], isClosed=True, color=zone_color_bgr(zone["name"]), thickness=2)

        Path(snapshot_dir).mkdir(parents=True, exist_ok=True)
        filename = f"{self.name}_{zone['name']}_{int(time.time())}.jpg".replace(" ", "_")
        path = str(Path(snapshot_dir) / filename)
        ok = cv2.imwrite(path, frame)
        return filename if ok else ""


# ============================================================
# KAMERA-VERWALTUNG (hinzufügen/entfernen/bearbeiten zur Laufzeit,
# von der Web-UI aus genutzt; von InferenceEngine gemeinsam verwendet)
# ============================================================

class CameraManager:
    def __init__(self, det_cfg: dict):
        self.det_cfg = det_cfg
        self._lock = threading.Lock()
        self._cameras: dict[str, CameraStream] = {}

    def load_initial(self, camera_configs: list):
        with self._lock:
            for cfg in camera_configs:
                cam = CameraStream(cfg["name"], cfg["rtsp_url"], cfg.get("zones", []), self.det_cfg)
                cam.start()
                self._cameras[cfg["name"]] = cam

    def list(self) -> list:
        with self._lock:
            return list(self._cameras.values())

    def get(self, name: str):
        with self._lock:
            return self._cameras.get(name)

    def exists(self, name: str) -> bool:
        with self._lock:
            return name in self._cameras

    def add(self, name: str, rtsp_url: str) -> tuple[bool, str]:
        with self._lock:
            if name in self._cameras:
                return False, "Eine Kamera mit diesem Namen existiert bereits."
            if not name.strip():
                return False, "Name darf nicht leer sein."
            if not rtsp_url.strip():
                return False, "RTSP-URL darf nicht leer sein."
            cam = CameraStream(name, rtsp_url, [], self.det_cfg)
            cam.start()
            self._cameras[name] = cam
        self._persist()
        return True, "OK"

    def update_rtsp(self, name: str, new_rtsp_url: str) -> tuple[bool, str]:
        with self._lock:
            cam = self._cameras.get(name)
            if cam is None:
                return False, "Kamera nicht gefunden."
            old_zones = cam.zones
            new_cam = CameraStream(name, new_rtsp_url, old_zones, self.det_cfg)
            new_cam.start()
            self._cameras[name] = new_cam
        # altes Verbindungsobjekt außerhalb des Locks sauber beenden,
        # damit die Web-UI währenddessen nicht blockiert
        threading.Thread(target=cam.stop_and_wait, daemon=True).start()
        self._persist()
        return True, "OK"

    def update_zones(self, name: str, zones: list) -> tuple[bool, str]:
        with self._lock:
            cam = self._cameras.get(name)
            if cam is None:
                return False, "Kamera nicht gefunden."
            cam.zones = zones
        self._persist()
        return True, "OK"

    def remove(self, name: str) -> tuple[bool, str]:
        with self._lock:
            cam = self._cameras.pop(name, None)
            if cam is None:
                return False, "Kamera nicht gefunden."
        threading.Thread(target=cam.stop_and_wait, daemon=True).start()
        self._persist()
        return True, "OK"

    def shutdown_all(self):
        """Wird beim Beenden des Programms aufgerufen, wartet auf alle Kameras."""
        with self._lock:
            cams = list(self._cameras.values())
        for cam in cams:
            cam.stop_and_wait(timeout=2.0)

    def _persist(self):
        with CONFIG_LOCK:
            CONFIG["cameras"] = [
                {"name": c.name, "rtsp_url": c.rtsp_url, "zones": c.zones} for c in self.list()
            ]
            save_config(CONFIG)


# ============================================================
# GEMEINSAMER ERKENNUNGS-THREAD (round-robin, EIN Modell geteilt)
# ============================================================

class InferenceEngine(threading.Thread):
    def __init__(self, camera_manager: CameraManager, model, det_cfg: dict, notif_cfg: dict,
                 rec_cfg: dict, storage_cfg: dict, occupancy_cfg: dict, notifier):
        super().__init__(daemon=True)
        self.camera_manager = camera_manager
        self.model = model
        self.det_cfg = det_cfg
        self.notif_cfg = notif_cfg
        self.rec_cfg = rec_cfg
        self.storage_cfg = storage_cfg
        self.occupancy_cfg = occupancy_cfg
        self.notifier = notifier
        self.running = True
        Path(storage_cfg["snapshot_dir"]).mkdir(parents=True, exist_ok=True)
        Path(rec_cfg["output_dir"]).mkdir(parents=True, exist_ok=True)

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            cameras = self.camera_manager.list()
            if not cameras:
                touch_engine_heartbeat()
                time.sleep(1)
                continue
            for camera in cameras:
                if not self.running:
                    break
                touch_engine_heartbeat()
                try:
                    self._process_camera(camera)
                except Exception as e:
                    # WICHTIG: Ohne dieses try/except würde JEDER Fehler bei
                    # der Verarbeitung EINER Kamera den gesamten
                    # Erkennungs-Thread lautlos für immer beenden -> keine
                    # Kamera würde mehr erkannt, bis das Programm neu
                    # gestartet wird. Stattdessen wird der Fehler geloggt
                    # und mit der nächsten Kamera/dem nächsten Durchlauf
                    # weitergemacht.
                    import traceback
                    print(f"\n[FEHLER] Erkennung für Kamera '{camera.name}' fehlgeschlagen: {e}")
                    traceback.print_exc()
                time.sleep(self.det_cfg["seconds_per_camera"])

    def _process_camera(self, camera: CameraStream):
        frame = camera.get_latest_frame()
        if frame is None:
            return

        classes = self.det_cfg.get("classes") or []
        if not classes:
            # keine Klasse ausgewählt -> nichts zu erkennen, aber alte
            # Overlays/Tracks nicht ewig stehen lassen
            camera.set_overlay([])
            return

        h, w = frame.shape[:2]
        infer_width = self.det_cfg["inference_width"]
        scale = infer_width / w if w > infer_width else 1.0
        small = cv2.resize(frame, (int(w * scale), int(h * scale))) if scale != 1.0 else frame

        results = self.model.predict(
            small, classes=classes, conf=self.det_cfg["confidence"],
            device=self.det_cfg.get("device", "cpu"), verbose=False,
        )
        detections = []
        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                class_id = int(box.cls[0])
                detections.append(((x1 / scale, y1 / scale, x2 / scale, y2 / scale), class_id))

        tracks, removed_tracks = camera.tracker.update(detections)
        overlay = []
        for track in tracks.values():
            overlay.append((track.bbox, (0, 200, 0), class_name_de(track.class_id)))
        self._evaluate_zones(camera, tracks, overlay, frame_w=w, frame_h=h)
        self._evaluate_door_disappearances(camera, removed_tracks)
        camera.set_overlay(overlay)

    def _evaluate_zones(self, camera: CameraStream, tracks: dict, overlay: list, frame_w: int, frame_h: int):
        now = time.time()
        # merkt sich pro Zone, ob GERADE ein Objekt drin ist, das laut
        # Aufnahme-Regeln eine Video-Aufnahme rechtfertigt
        zone_qualifying_presence = {zone["name"]: False for zone in camera.zones}

        for track in tracks.values():
            center = bbox_center(track.bbox)
            area = bbox_area(track.bbox)
            growth = area / track.start_area if track.start_area > 0 else 1.0
            approaching = growth >= self.det_cfg["approach_area_growth"]
            class_name = class_name_de(track.class_id)

            for zone in camera.zones:
                zone_name = zone["name"]
                inside = point_in_polygon(center, zone_points_px(zone, frame_w, frame_h))

                if inside:
                    if zone_name not in track.zone_enter_time:
                        track.zone_enter_time[zone_name] = now
                    dwell = now - track.zone_enter_time[zone_name]

                    for i, (bbox, color, label) in enumerate(overlay):
                        if bbox == track.bbox:
                            # Live-Timer im Bild: Klasse, Zone und Aufenthaltsdauer
                            overlay[i] = (bbox, (0, 0, 255), f"{class_name} · {zone_name} · {int(dwell)}s")

                    if zone_name not in track.zone_alerted_enter:
                        track.zone_alerted_enter.add(zone_name)
                        record_detection_stat(track.class_id, camera.name)
                        self._evaluate_occupancy_counters(camera, track, zone_name)
                        desc = f"{class_name} betritt Bereich '{zone_name}'"
                        if approaching:
                            desc += " und nähert sich der Kamera"
                        self._maybe_notify(camera, zone_name, desc, track, zone)

                    elif dwell >= self.det_cfg["dwell_alert_seconds"] and zone_name not in track.zone_alerted_dwell:
                        track.zone_alerted_dwell.add(zone_name)
                        desc = (f"{class_name} hält sich seit {int(dwell)}s im Bereich '{zone_name}' auf "
                                f"– möglicherweise auffälliges Verhalten")
                        self._maybe_notify(camera, zone_name, desc, track, zone)

                    if self._recording_rule_matches(track.class_id):
                        zone_qualifying_presence[zone_name] = True
                else:
                    # Bevor der "war zuletzt drin"-Status gelöscht wird: falls
                    # dies eine Tür-Zone im Ein-Kamera-Modus ist und das Objekt
                    # gerade eben (dieser Frame) die Zone sichtbar verlassen
                    # hat, zählt das als "raus" (siehe _evaluate_door_exit).
                    if track.door_zone_inside.get(zone_name):
                        self._evaluate_door_exit(camera, track, zone_name, dwell=now - track.zone_enter_time.get(zone_name, now))
                    track.zone_enter_time.pop(zone_name, None)
                    track.zone_alerted_enter.discard(zone_name)
                    track.zone_alerted_dwell.discard(zone_name)

                track.door_zone_inside[zone_name] = inside

        for zone in camera.zones:
            zone_name = zone["name"]
            if zone_qualifying_presence[zone_name]:
                self._start_recording_if_needed(camera, zone_name, zone)
            else:
                self._stop_recording_if_active(camera, zone_name)

    def _evaluate_occupancy_counters(self, camera: CameraStream, track: Track, zone_name: str):
        """Zwei-Zonen-Personenzähler (klassischer Modus, braucht eine Zone
        VOR und eine Zone HINTER der Tür, z.B. auf zwei Kameras oder einer
        Kamera mit Blick auf beide Bereiche): wechselt eine Person zwischen
        Außen- und Innenzone eines konfigurierten Zählers, wird "drinnen"
        +1/-1 angepasst. Nur die Klasse "Person" wird gezählt."""
        if track.class_id != 0:
            return
        for counter in self.occupancy_cfg.get("counters", []):
            if counter.get("mode", "two_zone") != "two_zone":
                continue
            if counter.get("camera") != camera.name:
                continue
            outside_zone = counter.get("outside_zone")
            inside_zone = counter.get("inside_zone")
            if zone_name not in (outside_zone, inside_zone):
                continue

            counter_id = counter["id"]
            new_state = "outside" if zone_name == outside_zone else "inside"
            old_state = track.counting_state.get(counter_id)

            if old_state is not None and old_state != new_state:
                if new_state == "inside":
                    new_count = adjust_occupancy(counter_id, 1)
                    desc = f"Person betritt das Haus (Zähler '{counter['name']}') – jetzt {new_count} drinnen"
                else:
                    new_count = adjust_occupancy(counter_id, -1)
                    desc = f"Person verlässt das Haus (Zähler '{counter['name']}') – jetzt {new_count} drinnen"

                zone_dict = next((z for z in camera.zones if z["name"] == zone_name), None)
                snapshot_filename = ""
                if zone_dict is not None:
                    snapshot_filename = camera.save_alert_snapshot(track.bbox, zone_dict, self.storage_cfg["snapshot_dir"])
                EVENT_FEED.add(camera.name, zone_name, desc, snapshot_filename)

            track.counting_state[counter_id] = new_state

    # ------------------------------------------------------
    # Tür-Modus (EINE Kamera, EINE Zone genau über der Tür):
    # -----------------------------------------------------
    # Statt zwei Zonen/Kameras zu brauchen, reicht hier eine einzige Zone
    # direkt auf der Tür/dem Eingang. Die Idee: eine Person, die man zuletzt
    # sichtbar IN der Tür-Zone gesehen hat und die danach verschwindet
    # (die Kamera zeigt ja nur nach draußen, "verschwinden in der Tür" heißt
    # also "ist reingegangen"), wird als "rein" gezählt. Taucht umgekehrt eine
    # Person, die gerade noch in der Tür-Zone war, sichtbar AUSSERHALB der
    # Zone wieder auf (sie kommt zur Kamera hin zurück ins Bild), wird das als
    # "raus" gezählt.
    #
    # WICHTIG -- Grenzen dieses Modus: Da nur eine Kamera/Zone genutzt wird,
    # ist die Erkennung eine Heuristik. Läuft z.B. jemand nur zufällig am
    # Rand der Tür-Zone vorbei (ohne wirklich die Tür zu benutzen), kann das
    # fälschlich als "raus" gezählt werden. Je enger die Zone auf die Tür
    # selbst zugeschnitten ist (nicht den ganzen Gehweg mit einschließt),
    # desto zuverlässiger ist die Zählung. Für höchste Genauigkeit bleibt der
    # klassische Zwei-Zonen-Modus (z.B. mit einer zweiten Kamera drinnen) die
    # bessere Wahl -- der Tür-Modus ist der pragmatische Weg, wenn nur eine
    # Kamera von außen zur Verfügung steht.

    MIN_DOOR_ZONE_DWELL_SECONDS = 0.6  # filtert kurzes Kamera-/Erkennungsflackern heraus

    def _door_counters_for_zone(self, camera: CameraStream, zone_name: str) -> list:
        return [
            c for c in self.occupancy_cfg.get("counters", [])
            if c.get("mode") == "door" and c.get("camera") == camera.name and c.get("door_zone") == zone_name
        ]

    def _evaluate_door_exit(self, camera: CameraStream, track: Track, zone_name: str, dwell: float):
        """Eine Person, die eben noch in der Tür-Zone war, ist jetzt (noch
        getrackt) außerhalb der Zone sichtbar -> "raus"."""
        if track.class_id != 0:
            return
        if dwell < self.MIN_DOOR_ZONE_DWELL_SECONDS:
            return  # zu kurz in der Zone gewesen, vermutlich nur vorbeigelaufen
        for counter in self._door_counters_for_zone(camera, zone_name):
            counter_id = counter["id"]
            new_count = adjust_occupancy(counter_id, -1)
            desc = f"Person verlässt das Haus (Tür-Modus '{counter['name']}') – jetzt {new_count} drinnen"
            zone_dict = next((z for z in camera.zones if z["name"] == zone_name), None)
            snapshot_filename = ""
            if zone_dict is not None:
                snapshot_filename = camera.save_alert_snapshot(track.bbox, zone_dict, self.storage_cfg["snapshot_dir"])
            EVENT_FEED.add(camera.name, zone_name, desc, snapshot_filename)

    def _evaluate_door_disappearances(self, camera: CameraStream, removed_tracks: list):
        """Wird für Tracks aufgerufen, die der Tracker gerade wegen Zeitüberschreitung
        entfernt hat (Objekt war eine Weile nicht mehr sichtbar). War die zuletzt
        bekannte Position innerhalb einer Tür-Zone, gilt das als "rein"."""
        for track in removed_tracks:
            if track.class_id != 0:
                continue
            for zone_name, was_inside in track.door_zone_inside.items():
                if not was_inside:
                    continue
                for counter in self._door_counters_for_zone(camera, zone_name):
                    counter_id = counter["id"]
                    new_count = adjust_occupancy(counter_id, 1)
                    desc = f"Person betritt das Haus (Tür-Modus '{counter['name']}') – jetzt {new_count} drinnen"
                    zone_dict = next((z for z in camera.zones if z["name"] == zone_name), None)
                    snapshot_filename = ""
                    if zone_dict is not None:
                        snapshot_filename = camera.save_alert_snapshot(track.bbox, zone_dict, self.storage_cfg["snapshot_dir"])
                    EVENT_FEED.add(camera.name, zone_name, desc, snapshot_filename)

    def _maybe_notify(self, camera: CameraStream, zone_name: str, description: str, track: Track, zone: dict):
        within_window = is_within_time_window(self.notif_cfg.get("time_windows", []))

        cooldown = self.notif_cfg.get("cooldown_seconds", 60)
        last_time = camera.last_notify_time.get(zone_name, 0)
        on_cooldown = time.time() - last_time < cooldown

        snapshot_filename = camera.save_alert_snapshot(track.bbox, zone, self.storage_cfg["snapshot_dir"])
        EVENT_FEED.add(camera.name, zone_name, description, snapshot_filename)
        print(f"[{camera.name}] {description}")

        # Der Ereignis-Feed wird immer aktualisiert; ob zusätzlich eine
        # Push-Benachrichtigung (ntfy/Telegram/E-Mail) rausgeht, hängt vom
        # Zeitfenster, dem Cooldown UND einer eventuellen Stummschaltung ab.
        if not within_window or on_cooldown or is_notifications_snoozed():
            return
        camera.last_notify_time[zone_name] = time.time()

        title = f"{class_name_de(track.class_id)} erkannt – {camera.name}"
        message = f"{description}\nKamera: {camera.name}\nZeit: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        snapshot_path = str(Path(self.storage_cfg["snapshot_dir"]) / snapshot_filename) if snapshot_filename else None
        self.notifier.send(title, message, snapshot_path)

    # ------------------------------------------------------
    # Video-Aufnahme: Regel-Prüfung + Start/Stop pro Zone
    # ------------------------------------------------------

    def _recording_rule_matches(self, class_id: int) -> bool:
        if not self.rec_cfg.get("enabled", True):
            return False
        for rule in self.rec_cfg.get("rules", []):
            if not rule.get("enabled", True):
                continue
            if class_id not in rule.get("classes", []):
                continue
            if is_within_time_window(rule.get("time_windows", [])):
                return True
        return False

    def _start_recording_if_needed(self, camera: CameraStream, zone_name: str, zone: dict):
        existing = camera.active_recordings.get(zone_name)
        if existing is not None and existing.is_alive():
            return  # läuft bereits

        Path(self.rec_cfg["output_dir"]).mkdir(parents=True, exist_ok=True)
        filename = f"{camera.name}_{zone_name}_{int(time.time())}.mp4".replace(" ", "_")
        filepath = str(Path(self.rec_cfg["output_dir"]) / filename)

        recorder = ZoneRecorder(
            camera=camera, filepath=filepath,
            fps=self.rec_cfg.get("fps", 8), max_seconds=self.rec_cfg.get("max_seconds", 30),
        )
        camera.active_recordings[zone_name] = recorder
        recorder.start()
        record_recording_stat()
        EVENT_FEED.add(camera.name, zone_name, f"Video-Aufnahme gestartet im Bereich '{zone_name}'",
                        None, recording_filename=filename)

    def _stop_recording_if_active(self, camera: CameraStream, zone_name: str):
        recorder = camera.active_recordings.pop(zone_name, None)
        if recorder is not None:
            recorder.stop()


# ============================================================
# WEB-UI (Flask)
# ============================================================

NAV_HTML = """
<header class="topbar">
  <div class="topbar-inner">
    <div class="brand"><span class="dot"></span> Heimüberwachung</div>
    <nav class="nav" id="mainnav">
      <a href="/"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"></path><circle cx="12" cy="13" r="4"></circle></svg> Live-Ansicht</a>
      <a href="/gallery"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><circle cx="8.5" cy="8.5" r="1.5"></circle><polyline points="21 15 16 10 5 21"></polyline></svg> Galerie</a>
      <a href="/statistik"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"></line><line x1="12" y1="20" x2="12" y2="4"></line><line x1="6" y1="20" x2="6" y2="14"></line></svg> Statistik</a>
      <a href="/settings"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"></path></svg> Einstellungen</a>
    </nav>
  </div>
</header>
<script>
(function(){
  var path = window.location.pathname;
  document.querySelectorAll('#mainnav a').forEach(function(a){
    var href = a.getAttribute('href');
    if (href === '/' ? path === '/' : path.indexOf(href) === 0) a.classList.add('active');
  });
})();
function toast(msg, type) {
  type = type || 'success';
  var c = document.getElementById('toast-container');
  if (!c) { c = document.createElement('div'); c.id = 'toast-container'; c.className = 'toast-container'; document.body.appendChild(c); }
  var el = document.createElement('div');
  el.className = 'toast ' + type;
  el.textContent = msg;
  c.appendChild(el);
  setTimeout(function(){ el.remove(); }, 4000);
}
// Lightbox mit Navigation: kann eine einzelne Datei ODER eine ganze Liste
// (Foto- und Video-Feed / Galerie) anzeigen. Zwischen mehreren Elementen
// kann per Pfeiltasten, Klick auf die Pfeil-Buttons oder auf Touch-Geräten
// per Wischgeste (links/rechts) gewechselt werden.
//
// Aufruf: openLightbox(items, startIndex)
//   items = [{url: "...", video: true|false}, ...]
// Rückwärtskompatibel: openLightbox(url, isVideo) mit url als String
// funktioniert weiterhin (wird intern in ein 1-Element-Array umgewandelt).
var LB = { items: [], index: 0 };

function openLightbox(items, indexOrIsVideo) {
  if (typeof items === 'string') {
    items = [{ url: items, video: !!indexOrIsVideo }];
    indexOrIsVideo = 0;
  }
  LB.items = items || [];
  LB.index = indexOrIsVideo || 0;
  if (LB.items.length === 0) return;
  renderLightbox(true);
}

function renderLightbox(create) {
  var box = document.querySelector('.lightbox');
  if (create || !box) {
    if (box) box.remove();
    box = document.createElement('div');
    box.className = 'lightbox';
    box.onclick = function(e) { if (e.target === box) closeLightbox(); };
    box.innerHTML =
      '<button class="close" onclick="closeLightbox()" aria-label="Schließen"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg></button>' +
      '<button class="lb-nav lb-prev" onclick="event.stopPropagation(); lbStep(-1)" aria-label="Vorheriges"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"></polyline></svg></button>' +
      '<button class="lb-nav lb-next" onclick="event.stopPropagation(); lbStep(1)" aria-label="Nächstes"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg></button>' +
      '<div class="lb-media"></div>' +
      '<div class="lb-counter"></div>';
    document.body.appendChild(box);
    box.addEventListener('touchstart', lbTouchStart, { passive: true });
    box.addEventListener('touchend', lbTouchEnd, { passive: true });
  }

  var item = LB.items[LB.index];
  var mediaWrap = box.querySelector('.lb-media');
  mediaWrap.innerHTML = item.video
    ? ('<video src="' + item.url + '" controls autoplay playsinline></video>')
    : ('<img src="' + item.url + '" alt="">');

  var multi = LB.items.length > 1;
  box.querySelector('.lb-prev').style.display = multi ? 'flex' : 'none';
  box.querySelector('.lb-next').style.display = multi ? 'flex' : 'none';
  box.querySelector('.lb-counter').style.display = multi ? 'block' : 'none';
  box.querySelector('.lb-counter').textContent = (LB.index + 1) + ' / ' + LB.items.length;
}

function closeLightbox() {
  var box = document.querySelector('.lightbox');
  if (box) box.remove();
}

function lbStep(dir) {
  if (!LB.items.length) return;
  LB.index = (LB.index + dir + LB.items.length) % LB.items.length;
  renderLightbox(false);
}

document.addEventListener('keydown', function(e) {
  if (!document.querySelector('.lightbox')) return;
  if (e.key === 'Escape') closeLightbox();
  else if (e.key === 'ArrowLeft') lbStep(-1);
  else if (e.key === 'ArrowRight') lbStep(1);
});

var lbTouchStartX = null, lbTouchStartY = null;
function lbTouchStart(e) {
  lbTouchStartX = e.changedTouches[0].clientX;
  lbTouchStartY = e.changedTouches[0].clientY;
}
function lbTouchEnd(e) {
  if (lbTouchStartX === null) return;
  var dx = e.changedTouches[0].clientX - lbTouchStartX;
  var dy = e.changedTouches[0].clientY - lbTouchStartY;
  // Nur als Wisch werten, wenn deutlich mehr horizontal als vertikal bewegt
  // wurde (sonst würde z.B. Scrollen versehentlich als Wisch erkannt).
  if (Math.abs(dx) > 55 && Math.abs(dx) > Math.abs(dy) * 1.5) {
    lbStep(dx > 0 ? -1 : 1);
  }
  lbTouchStartX = null;
  lbTouchStartY = null;
}
</script>
"""

BASE_STYLE = """
<style>
  :root {
    --bg: #0a0d14;
    --bg-alt: #10141d;
    --card: #161b26;
    --border: #262c3a;
    --text: #e9ebf0;
    --muted: #8891a3;
    --accent: #5eb1ff;
    --accent-soft: rgba(94,177,255,0.12);
    --violet: #8b7cff;
    --success: #3ddc97;
    --danger: #ff6b6b;
    --warning: #ffc857;
    --radius: 14px;
    --radius-sm: 8px;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    background: radial-gradient(1200px 600px at 10% -10%, #131a26 0%, var(--bg) 55%);
    color: var(--text); margin:0; padding:0 0 60px;
  }
  .wrap { max-width: 1280px; margin: 0 auto; padding: 0 20px; }
  header.topbar {
    position: sticky; top:0; z-index: 50; backdrop-filter: blur(10px);
    background: rgba(10,13,20,0.85); border-bottom:1px solid var(--border);
  }
  .topbar-inner { max-width:1280px; margin:0 auto; padding:14px 20px; display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
  .brand { display:flex; align-items:center; gap:10px; font-weight:700; font-size:1.05rem; }
  .brand .dot { width:10px; height:10px; border-radius:50%; background: var(--success); box-shadow: 0 0 8px var(--success); }
  .nav { display:flex; gap:8px; }
  .nav a { color: var(--muted); text-decoration:none; padding:8px 14px; border-radius:999px; font-size:0.9rem; font-weight:500; transition: all .15s; display:inline-flex; align-items:center; gap:7px; }
  .nav a svg { flex-shrink:0; }
  .nav a:hover, .nav a.active { background: var(--accent-soft); color: var(--accent); }
  h1 { font-size:1.4rem; margin: 24px 0 4px; letter-spacing:-0.01em; }
  h2 { font-size:1.05rem; margin: 28px 0 12px; color: var(--text); font-weight:600; }
  .subtitle { color: var(--muted); font-size:0.9rem; margin-bottom:20px; }
  a { color: var(--accent); }

  .stats-row { display:grid; grid-template-columns: repeat(auto-fit, minmax(150px,1fr)); gap:12px; margin: 16px 0 24px; }
  .stat-card { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); padding:14px 16px; }
  .stat-card .num { font-size:1.6rem; font-weight:700; }
  .stat-card .lbl { font-size:0.75rem; color: var(--muted); margin-top:2px; text-transform:uppercase; letter-spacing:.04em; }
  .stat-card.accent .num { color: var(--accent); }
  .stat-card.danger .num { color: var(--danger); }

  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap:14px; }
  .cam { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); overflow:hidden; transition: border-color .15s; }
  .cam:hover { border-color: #34405a; }
  .cam img { width:100%; display:block; background:#000; aspect-ratio:16/9; object-fit:cover; cursor:zoom-in; }
  .cam .label { padding:10px 12px; font-size:0.88rem; display:flex; justify-content:space-between; align-items:center; }
  .cam .label .name { font-weight:600; }
  .badge { display:inline-flex; align-items:center; gap:5px; font-size:0.74rem; padding:3px 9px; border-radius:999px; font-weight:600; }
  .badge.ok { background: rgba(61,220,151,0.12); color: var(--success); }
  .badge.off { background: rgba(255,107,107,0.12); color: var(--danger); }
  .badge .pulse { width:6px; height:6px; border-radius:50%; background:currentColor; }
  .badge.ok .pulse { animation: pulse 1.6s infinite; }
  @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:.35;} }

  .panel { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); padding:18px 20px; margin-bottom:16px; }
  .panel-row { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }

  .filters { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; align-items:center; }
  select, input[type=text], input[type=time], input[type=number] {
    background: var(--bg-alt); border:1px solid var(--border); color: var(--text);
    padding:9px 12px; border-radius: var(--radius-sm); font-size:0.9rem; outline:none;
  }
  select:focus, input:focus { border-color: var(--accent); }

  button { background: linear-gradient(135deg, var(--accent), var(--violet)); border:none; color:#0a0d14;
    padding:9px 16px; border-radius: var(--radius-sm); cursor:pointer; font-weight:700; font-size:0.88rem; transition: transform .1s, opacity .15s; }
  button:hover { opacity:0.92; }
  button:active { transform: scale(0.97); }
  button.danger { background: var(--danger); color:#fff; }
  button.secondary { background: var(--bg-alt); color: var(--text); border:1px solid var(--border); }
  button.ghost { background:transparent; color: var(--muted); border:1px solid var(--border); }
  button.sm { padding:5px 10px; font-size:0.78rem; }

  table.status { width:100%; border-collapse:collapse; font-size:0.88rem; }
  table.status th { text-align:left; padding:8px; color: var(--muted); font-weight:600; font-size:0.75rem; text-transform:uppercase; letter-spacing:.03em; border-bottom:1px solid var(--border); }
  table.status td { padding:10px 8px; border-bottom:1px solid var(--border); }
  table.status tr:last-child td { border-bottom:none; }

  .event { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); padding:12px; margin-bottom:10px; display:flex; gap:14px; align-items:center; }
  .event .thumb { width:110px; height:74px; border-radius:8px; object-fit:cover; background:#000; cursor:zoom-in; flex-shrink:0; }
  .event .thumb-placeholder { width:110px; height:74px; border-radius:8px; background: var(--bg-alt); flex-shrink:0; display:flex; align-items:center; justify-content:center; color: var(--muted); font-size:1.4rem; }
  .event .meta { font-size:0.8rem; color: var(--muted); display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .event .desc { font-size:0.93rem; margin-top:3px; }
  .event .cam-name { color: var(--accent); font-weight:700; }
  .type-badge { font-size:0.68rem; padding:2px 8px; border-radius:999px; background: var(--accent-soft); color: var(--accent); font-weight:700; }
  .type-badge.video { background: rgba(255,107,107,0.12); color: var(--danger); }

  .muted { color: var(--muted); font-size:0.85rem; }
  .zone-tag { display:inline-flex; align-items:center; gap:6px; padding:5px 12px; border-radius:999px; margin:4px 6px 4px 0; font-size:0.83rem; font-weight:600; }
  .zone-tag button { padding:1px 7px; font-size:0.72rem; }

  .class-grid { display:grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap:8px 12px; margin: 12px 0; }
  .class-grid label { display:flex; align-items:center; gap:7px; font-size:0.88rem; cursor:pointer; padding:6px 8px; border-radius:8px; transition: background .1s; }
  .class-grid label:hover { background: var(--bg-alt); }
  .rule-card { background: var(--bg-alt); border:1px solid var(--border); border-radius: var(--radius-sm); padding:14px; margin-bottom:10px; }

  #still-wrap { position:relative; display:inline-block; max-width:100%; border-radius: var(--radius); overflow:hidden; }
  #still { max-width:100%; display:block; }
  #overlay { position:absolute; top:0; left:0; cursor:crosshair; }

  .toast-container { position:fixed; bottom:20px; right:20px; z-index:200; display:flex; flex-direction:column; gap:8px; }
  .toast { background: var(--card); border:1px solid var(--border); border-left:4px solid var(--accent); padding:12px 16px; border-radius: var(--radius-sm); font-size:0.88rem; box-shadow:0 6px 20px rgba(0,0,0,0.4); animation: slidein .2s ease; min-width:220px; }
  .toast.error { border-left-color: var(--danger); }
  .toast.success { border-left-color: var(--success); }
  @keyframes slidein { from { transform: translateX(20px); opacity:0;} to { transform: translateX(0); opacity:1; } }

  .lightbox { position:fixed; inset:0; background:rgba(0,0,0,0.9); z-index:300; display:flex; align-items:center; justify-content:center; padding:30px; animation: fadein .12s ease; touch-action: pan-y; }
  @keyframes fadein { from { opacity:0; } to { opacity:1; } }
  .lightbox .lb-media { display:flex; align-items:center; justify-content:center; width:100%; height:100%; }
  .lightbox img, .lightbox video { max-width:100%; max-height:85vh; border-radius:10px; box-shadow:0 20px 60px rgba(0,0,0,0.55); }
  .lightbox .close { position:absolute; top:20px; right:20px; color:#fff; cursor:pointer; background:rgba(255,255,255,0.08); border:none; border-radius:50%; width:40px; height:40px; display:flex; align-items:center; justify-content:center; z-index:2; }
  .lightbox .close:hover { background:rgba(255,255,255,0.18); }
  .lightbox .lb-nav { position:absolute; top:50%; transform:translateY(-50%); color:#fff; cursor:pointer; background:rgba(255,255,255,0.08); border:none; border-radius:50%; width:46px; height:46px; align-items:center; justify-content:center; z-index:2; }
  .lightbox .lb-nav:hover { background:rgba(255,255,255,0.2); }
  .lightbox .lb-prev { left:14px; }
  .lightbox .lb-next { right:14px; }
  .lightbox .lb-counter { position:absolute; bottom:18px; left:50%; transform:translateX(-50%); color:rgba(255,255,255,0.85); font-size:0.8rem; font-weight:600; background:rgba(255,255,255,0.1); padding:5px 14px; border-radius:999px; letter-spacing:.02em; }
  @media (max-width:640px) {
    .lightbox { padding:10px; }
    .lightbox .lb-nav { width:38px; height:38px; }
    .lightbox .lb-prev { left:6px; }
    .lightbox .lb-next { right:6px; }
  }

  .toggle-row { display:flex; align-items:center; gap:10px; }
  .switch { position:relative; width:42px; height:24px; flex-shrink:0; }
  .switch input { opacity:0; width:0; height:0; }
  .slider { position:absolute; inset:0; background: var(--border); border-radius:999px; cursor:pointer; transition:.2s; }
  .slider:before { content:""; position:absolute; width:18px; height:18px; left:3px; top:3px; background:#fff; border-radius:50%; transition:.2s; }
  input:checked + .slider { background: var(--accent); }
  input:checked + .slider:before { transform: translateX(18px); }

  .snooze-banner { background: rgba(255,200,87,0.1); border:1px solid rgba(255,200,87,0.3); color: var(--warning); padding:10px 16px; border-radius: var(--radius-sm); margin:16px 0; display:flex; justify-content:space-between; align-items:center; font-size:0.88rem; flex-wrap:wrap; gap:8px; }

  .gallery-grid { display:grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap:14px; }
  .gallery-item { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); overflow:hidden; cursor:zoom-in; transition: border-color .15s, transform .1s; }
  .gallery-item:hover { border-color: #34405a; transform: translateY(-2px); }
  .gallery-item .media-wrap { position:relative; width:100%; aspect-ratio:4/3; background:#000; }
  .gallery-item img, .gallery-item video { width:100%; height:100%; object-fit:cover; display:block; }
  .gallery-item .type-badge { position:absolute; top:8px; left:8px; }
  .gallery-item .cap { padding:8px 10px; font-size:0.78rem; color: var(--muted); display:flex; justify-content:space-between; gap:6px; }
  .gallery-item .cap .cam { color: var(--accent); font-weight:700; }

  @keyframes fadein { from { opacity:0; transform: translateY(6px); } to { opacity:1; transform: translateY(0); } }
  .panel, .cam, .stat-card, .event, .gallery-item, .rule-card { animation: fadein .35s ease both; }

  .stat-card { position:relative; overflow:hidden; transition: transform .15s, border-color .15s; border-top:3px solid var(--border); }
  .stat-card:hover { transform: translateY(-2px); border-color:#34405a; }
  .stat-card.accent { border-top-color: var(--accent); }
  .stat-card.danger { border-top-color: var(--danger); }

  .last-updated { font-size:0.76rem; color: var(--muted); display:flex; align-items:center; gap:6px; }
  .last-updated .dot-live { width:6px; height:6px; border-radius:50%; background: var(--success); animation: pulse 1.6s infinite; }

  .chart-panel canvas { max-height: 300px; }
  .chart-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap:14px; }
  .empty-state { text-align:center; padding: 40px 20px; color: var(--muted); }
  .empty-state .big { font-size:2.4rem; margin-bottom:8px; }

  .occupancy-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:12px; margin-bottom:20px; }
  .occupancy-card { background: var(--card); border:1px solid var(--border); border-radius: var(--radius); padding:16px 18px; display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .occupancy-card .info .num { font-size:1.9rem; font-weight:700; line-height:1; }
  .occupancy-card .info .lbl { font-size:0.78rem; color: var(--muted); margin-top:4px; }
  .occupancy-card .adjust { display:flex; gap:6px; }
  .occupancy-card .adjust button { padding:4px 10px; font-size:0.95rem; line-height:1; }
</style>
"""

INDEX_PAGE = """
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8"><title>Heimüberwachung</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%230a0d14'/%3E%3Cpath d='M17.5 8.5h-2.2l-1-1.5h-4.6l-1 1.5H6.5A1.5 1.5 0 0 0 5 10v7A1.5 1.5 0 0 0 6.5 18.5h11A1.5 1.5 0 0 0 19 17v-7a1.5 1.5 0 0 0-1.5-1.5z' fill='none' stroke='%235eb1ff' stroke-width='1.6' stroke-linejoin='round'/%3E%3Ccircle cx='12' cy='13' r='2.8' fill='none' stroke='%235eb1ff' stroke-width='1.6'/%3E%3C/svg%3E">
__STYLE__
</head>
<body>
__NAV__
<div class="wrap">

<div id="engine-warning"></div>
<div id="snooze-banner"></div>

<h1>Live-Ansicht</h1>
<div class="panel-row" style="margin-bottom:6px;">
  <div class="subtitle" style="margin-bottom:0;">Übersicht aller Kameras und aktueller Aktivität</div>
  <div class="last-updated"><span class="dot-live"></span><span id="last-updated-text">wird geladen ...</span></div>
</div>

<div class="stats-row" id="stats-row">
  <div class="stat-card"><div class="num">–</div><div class="lbl">Lade ...</div></div>
</div>

<div class="panel-row" style="margin-bottom:16px;">
  <div class="toggle-row"><label class="switch"><input type="checkbox" id="sound-toggle"><span class="slider"></span></label><span class="muted">Ton bei neuen Ereignissen</span></div>
  <div class="toggle-row"><label class="switch"><input type="checkbox" id="notif-toggle"><span class="slider"></span></label><span class="muted">Browser-Benachrichtigungen</span></div>
</div>

<div id="occupancy-section"></div>

<div class="grid" id="cam-grid">__CAMERAS_HTML__</div>

<h2>Wer ist gerade wo?</h2>
<div class="panel">
  <table class="status">
    <thead><tr><th>Kamera</th><th>Zone</th><th>Erkannt als</th><th>Seit</th></tr></thead>
    <tbody id="status-body"><tr><td colspan="4" class="muted">Lade ...</td></tr></tbody>
  </table>
</div>

<h2>Letzte Ereignisse</h2>
<div class="filters">
  <select id="filter-camera"><option value="">Alle Kameras</option></select>
  <select id="filter-type">
    <option value="">Alle Typen</option>
    <option value="video">Nur Videos</option>
    <option value="photo">Nur Fotos</option>
  </select>
  <input type="text" id="filter-search" placeholder="Suche in Beschreibung ...">
</div>
<div id="event-list">Lade ...</div>

</div>

<script>
const soundToggle = document.getElementById('sound-toggle');
const notifToggle = document.getElementById('notif-toggle');
soundToggle.checked = localStorage.getItem('soundEnabled') === '1';
notifToggle.checked = localStorage.getItem('notifEnabled') === '1';
soundToggle.onchange = () => localStorage.setItem('soundEnabled', soundToggle.checked ? '1' : '0');
notifToggle.onchange = () => {
  if (notifToggle.checked && Notification.permission !== 'granted') {
    Notification.requestPermission().then(p => {
      if (p !== 'granted') { notifToggle.checked = false; toast('Berechtigung verweigert', 'error'); }
      localStorage.setItem('notifEnabled', notifToggle.checked ? '1' : '0');
    });
  } else {
    localStorage.setItem('notifEnabled', notifToggle.checked ? '1' : '0');
  }
};

function beep() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const o = ctx.createOscillator(); const g = ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    o.frequency.value = 880; g.gain.value = 0.08;
    o.start(); setTimeout(() => { o.stop(); ctx.close(); }, 180);
  } catch(e) {}
}

let lastEventKey = null;
let allEvents = [];

async function loadEvents(firstLoad) {
  const res = await fetch('/api/events');
  allEvents = await res.json();

  if (allEvents.length) {
    const key = allEvents[0].time + allEvents[0].camera + allEvents[0].description;
    if (!firstLoad && lastEventKey !== null && key !== lastEventKey) {
      if (soundToggle.checked) beep();
      if (notifToggle.checked && Notification.permission === 'granted') {
        new Notification(allEvents[0].camera, { body: allEvents[0].description });
      }
    }
    lastEventKey = key;
  }

  populateCameraFilter();
  renderEvents();
}

function populateCameraFilter() {
  const sel = document.getElementById('filter-camera');
  const current = sel.value;
  const cams = [...new Set(allEvents.map(e => e.camera))];
  sel.innerHTML = '<option value="">Alle Kameras</option>' + cams.map(c => `<option value="${c}">${c}</option>`).join('');
  sel.value = current;
}

function renderEvents() {
  const camFilter = document.getElementById('filter-camera').value;
  const typeFilter = document.getElementById('filter-type').value;
  const search = document.getElementById('filter-search').value.toLowerCase();

  let events = allEvents.filter(e => {
    if (camFilter && e.camera !== camFilter) return false;
    if (typeFilter === 'video' && !e.recording) return false;
    if (typeFilter === 'photo' && (e.recording || !e.snapshot)) return false;
    if (search && !e.description.toLowerCase().includes(search) && !e.camera.toLowerCase().includes(search)) return false;
    return true;
  });

  const container = document.getElementById('event-list');
  if (events.length === 0) {
    container.innerHTML = '<p class="muted">Keine Ereignisse gefunden.</p>';
    return;
  }
  // Baut parallel zur Anzeige eine Liste aller Fotos/Videos in genau der
  // gezeigten Reihenfolge, damit man in der Lightbox mit Pfeiltasten/Wischen
  // durch alle sichtbaren Ereignisse blättern kann (nicht nur das angeklickte).
  const mediaItems = events.map(e => {
    if (e.recording) return { url: `/recordings/${e.recording}`, video: true };
    if (e.snapshot) return { url: `/snapshots/${e.snapshot}`, video: false };
    return null;
  });
  window.__eventMedia = mediaItems.filter(Boolean);

  container.innerHTML = events.map((e, idx) => {
    const mediaIdx = mediaItems.slice(0, idx).filter(Boolean).length;
    let thumb = '<div class="thumb-placeholder">–</div>';
    if (e.recording) {
      thumb = `<video class="thumb" src="/recordings/${e.recording}" muted onclick="openLightbox(window.__eventMedia, ${mediaIdx})" onerror="this.outerHTML='<div class=\\'thumb-placeholder\\'>Nicht verfuegbar</div>'"></video>`;
    } else if (e.snapshot) {
      thumb = `<img class="thumb" src="/snapshots/${e.snapshot}" onclick="openLightbox(window.__eventMedia, ${mediaIdx})" onerror="this.outerHTML='<div class=\\'thumb-placeholder\\'>Nicht verfuegbar</div>'">`;
    }
    const typeBadge = e.recording ? '<span class="type-badge video">Video</span>' : (e.snapshot ? '<span class="type-badge">Foto</span>' : '');
    return `
    <div class="event">
      ${thumb}
      <div>
        <div class="meta"><span class="cam-name">${e.camera}</span> · ${e.zone} · ${e.time} ${typeBadge}</div>
        <div class="desc">${e.description}</div>
      </div>
    </div>`;
  }).join('');
}

document.getElementById('filter-camera').onchange = renderEvents;
document.getElementById('filter-type').onchange = renderEvents;
document.getElementById('filter-search').oninput = renderEvents;

async function loadStatus() {
  const res = await fetch('/api/live-status');
  const rows = await res.json();
  const body = document.getElementById('status-body');
  if (rows.length === 0) {
    body.innerHTML = '<tr><td colspan="4" class="muted">Aktuell nichts in einer Zone.</td></tr>';
    return;
  }
  body.innerHTML = rows.map(r => `
    <tr><td>${r.camera}</td><td>${r.zone}</td><td>${r.class_name}</td><td>${r.dwell_seconds}s</td></tr>
  `).join('');
}

async function loadStats() {
  const res = await fetch('/api/stats');
  const s = await res.json();
  const fmt = n => n.toLocaleString('de-DE');
  document.getElementById('stats-row').innerHTML = `
    <div class="stat-card accent"><div class="num">${fmt(s.cameras_connected)}/${fmt(s.cameras_total)}</div><div class="lbl">Kameras online</div></div>
    <div class="stat-card"><div class="num">${fmt(s.zones_total)}</div><div class="lbl">Zonen</div></div>
    <div class="stat-card"><div class="num">${fmt(s.events_today)}</div><div class="lbl">Ereignisse heute</div></div>
    <div class="stat-card ${s.active_recordings > 0 ? 'danger' : ''}"><div class="num">${fmt(s.active_recordings)}</div><div class="lbl">Aktive Aufnahmen</div></div>
  `;
  document.getElementById('last-updated-text').textContent = 'Aktualisiert um ' + new Date().toLocaleTimeString('de-DE');

  const engineWarning = document.getElementById('engine-warning');
  if (s.cameras_total > 0 && !s.engine_alive) {
    const ago = s.engine_last_seen_seconds_ago != null ? Math.round(s.engine_last_seen_seconds_ago) : '?';
    engineWarning.innerHTML = `<div class="snooze-banner" style="border-color:rgba(255,107,107,0.4); background:rgba(255,107,107,0.1); color:var(--danger);">
      <span>Die Erkennung reagiert seit ${ago}s nicht mehr (Kamera-Livebilder laufen trotzdem weiter). Bitte die Konsole auf Fehler prüfen und das Programm neu starten.</span>
    </div>`;
  } else {
    engineWarning.innerHTML = '';
  }

  const banner = document.getElementById('snooze-banner');
  if (s.notifications_snoozed) {
    const mins = Math.max(0, Math.round((s.snoozed_until - Date.now()/1000) / 60));
    banner.innerHTML = `<div class="snooze-banner"><span>Push-Benachrichtigungen sind noch ca. ${mins} Min. stummgeschaltet (Ereignis-Feed läuft normal weiter).</span>
      <button class="sm secondary" onclick="unsnooze()">Jetzt aktivieren</button></div>`;
  } else {
    banner.innerHTML = '';
  }
}

async function unsnooze() {
  await fetch('/api/notifications/snooze', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({minutes: 0})});
  toast('Benachrichtigungen wieder aktiv');
  loadStats();
}

async function loadOccupancy() {
  const res = await fetch('/api/occupancy');
  const counters = await res.json();
  const section = document.getElementById('occupancy-section');
  if (counters.length === 0) {
    section.innerHTML = '';
    return;
  }
  section.innerHTML = `
    <h2>Personen im Haus</h2>
    <div class="occupancy-grid">
      ${counters.map(c => `
        <div class="occupancy-card">
          <div class="info">
            <div class="num">${c.count}</div>
            <div class="lbl">${c.name}</div>
          </div>
          <div class="adjust">
            <button class="secondary sm" onclick="adjustOccupancy('${c.id}', ${Math.max(0, c.count - 1)})">-</button>
            <button class="secondary sm" onclick="adjustOccupancy('${c.id}', ${c.count + 1})">+</button>
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

async function adjustOccupancy(counterId, value) {
  await fetch(`/api/occupancy/${counterId}/set`, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({count: value})
  });
  loadOccupancy();
}

loadEvents(true);
loadStatus();
loadStats();
loadOccupancy();
setInterval(() => loadEvents(false), 5000);
setInterval(loadStatus, 2000);
setInterval(loadStats, 10000);
setInterval(loadOccupancy, 4000);
</script>
</body>
</html>
"""

SETTINGS_PAGE = """
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8"><title>Einstellungen – Heimüberwachung</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%230a0d14'/%3E%3Cpath d='M17.5 8.5h-2.2l-1-1.5h-4.6l-1 1.5H6.5A1.5 1.5 0 0 0 5 10v7A1.5 1.5 0 0 0 6.5 18.5h11A1.5 1.5 0 0 0 19 17v-7a1.5 1.5 0 0 0-1.5-1.5z' fill='none' stroke='%235eb1ff' stroke-width='1.6' stroke-linejoin='round'/%3E%3Ccircle cx='12' cy='13' r='2.8' fill='none' stroke='%235eb1ff' stroke-width='1.6'/%3E%3C/svg%3E">
__STYLE__
</head>
<body>
__NAV__
<div class="wrap">
<h1>Einstellungen</h1>
<div class="subtitle">Kameras, Erkennung, Aufnahmen und Benachrichtigungen verwalten</div>

<h2>Benachrichtigungen</h2>
<div class="panel">
  <div class="panel-row">
    <div><strong>Aktive Methode:</strong> <span id="notif-method" class="muted">–</span></div>
    <button class="secondary" onclick="testNotification()">Testbenachrichtigung senden</button>
  </div>
  <div style="margin-top:16px;">
    <div class="muted" style="margin-bottom:8px;">Push-Benachrichtigungen vorübergehend stummschalten (der Ereignis-Feed läuft dabei normal weiter):</div>
    <div style="display:flex; gap:8px; flex-wrap:wrap;">
      <button class="secondary sm" onclick="snooze(15)">15 Min</button>
      <button class="secondary sm" onclick="snooze(60)">1 Std</button>
      <button class="secondary sm" onclick="snooze(480)">8 Std</button>
      <button class="ghost sm" onclick="snooze(0)">Deaktivieren</button>
    </div>
    <div id="snooze-status" class="muted" style="margin-top:10px;"></div>
  </div>
</div>

<h1>Kameras verwalten</h1>

<div id="camera-list">Lade ...</div>

<h2>Neue Kamera hinzufügen</h2>
<div class="panel">
  <div class="panel-row">
    <input type="text" id="new-name" placeholder="Name (z.B. Haustuer)">
    <input type="text" id="new-rtsp" placeholder="rtsp://benutzer:passwort@192.168.1.50:554/stream1" style="flex:1; min-width:280px;">
    <button onclick="addCamera()">Hinzufügen</button>
  </div>
  <p class="muted" style="margin-bottom:0;">Die RTSP-URL findest du im Handbuch deiner Kamera bzw. in der Kamera-App unter "Netzwerk" / "RTSP".</p>
</div>

<h1>Was soll erkannt werden?</h1>
<div class="panel">
  <p class="muted">Wähle aus, welche Objektklassen die KI in allen Kameras erkennen soll.</p>
  <div id="class-grid" class="class-grid">Lade ...</div>
  <button onclick="saveClasses()">Erkennungsklassen speichern</button>
</div>

<h1>Erkennungs-Engine</h1>
<div class="panel">
  <p class="muted">
    Auf welcher Hardware die KI rechnet. Standard ist CPU – das läuft
    überall zuverlässig, auch wenn <code>torch</code> und
    <code>torchvision</code> nicht aus exakt zueinander passenden
    CUDA-Builds installiert sind (ein häufiger Fehler:
    "torchvision::nms ... CUDA backend"). Nur auf GPU/CUDA umstellen, wenn
    eine passende NVIDIA-Treiber-/CUDA-Installation vorhanden ist – bei
    vielen Kameras deutlich schneller.
  </p>
  <div class="panel-row">
    <select id="detection-device">
      <option value="cpu">CPU (empfohlen, läuft überall)</option>
      <option value="cuda">GPU / CUDA (schneller, benötigt passende Installation)</option>
    </select>
    <button onclick="saveDetectionDevice()">Speichern</button>
  </div>
</div>

<h1>Aufnahme-Regeln (Video)</h1>
<div class="panel">
  <div class="panel-row">
    <label class="toggle-row"><label class="switch"><input type="checkbox" id="recording-enabled"><span class="slider"></span></label> Video-Aufnahmen grundsätzlich aktiv</label>
  </div>
  <p class="muted">
    Jede Regel legt fest: Wenn eines der ausgewählten Objekte in einer Zone ist
    UND die aktuelle Uhrzeit im Zeitfenster liegt (leer = immer), wird ein
    Video-Clip aufgenommen. Beispiel: Personen rund um die Uhr, Katzen/Hunde
    nur nachts.
  </p>
  <div id="rule-list"></div>
  <button class="secondary" onclick="addRule()">+ Regel hinzufügen</button>
  <div style="margin-top:12px;">
    <button onclick="saveRules()">Aufnahme-Regeln speichern</button>
  </div>
</div>

<h1>Türen-Modus &amp; Personenzähler</h1>
<div class="panel">
  <p class="muted">
    Mit dem Türen-Modus legst du eine Zone über eine Tür (oder einen Eingang)
    und zählst automatisch mit, wie viele Personen gerade im Haus sind – z.B.
    an der Haustür. Es gibt zwei Varianten, je nachdem, welche Kameras du hast:
  </p>
  <ul class="muted" style="padding-left:18px; line-height:1.7;">
    <li><b>Tür-Modus (eine Kamera):</b> du zeichnest EINE Zone genau auf die Tür/den Eingang. Verschwindet eine Person darin (die Kamera zeigt ja nur nach draußen), zählt das als "rein". Taucht sie danach wieder sichtbar außerhalb der Zone auf, zählt das als "raus". Praktisch, wenn du drinnen keine Kamera hast – dafür etwas ungenauer als die Zwei-Zonen-Variante.</li>
    <li><b>Zwei-Zonen-Modus (klassisch):</b> du brauchst zwei Zonen (z.B. eine Kamera-Zone knapp vor und eine knapp hinter der Tür, etwa mit zwei Kameras). Wechselt eine Person von außen nach innen, zählt das als "rein", umgekehrt als "raus". Zuverlässiger, aber es braucht Sicht auf beide Seiten der Tür.</li>
  </ul>
  <div id="occupancy-list"></div>

  <div class="panel-row" style="margin-top:14px; align-items:flex-end;">
    <div>
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Name</div>
      <input type="text" id="occ-name" placeholder="z.B. Haustuer">
    </div>
    <div>
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Modus</div>
      <select id="occ-mode" onchange="renderOccupancyModeFields()">
        <option value="door">Tür-Modus (eine Zone, eine Kamera)</option>
        <option value="two_zone">Zwei-Zonen-Modus (klassisch)</option>
      </select>
    </div>
    <div>
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Kamera</div>
      <select id="occ-camera" onchange="renderOccupancyZoneOptions()"></select>
    </div>
    <div id="occ-field-door">
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Tür-Zone</div>
      <select id="occ-door"></select>
    </div>
    <div id="occ-field-outside" style="display:none;">
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Außenzone</div>
      <select id="occ-outside"></select>
    </div>
    <div id="occ-field-inside" style="display:none;">
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Innenzone</div>
      <select id="occ-inside"></select>
    </div>
    <button onclick="addOccupancyCounter()">Zähler anlegen</button>
  </div>
</div>

<h1>Externe API (Home-Dashboard)</h1>
<div class="panel">
  <p class="muted">
    Über diesen Endpunkt kann ein externes Home-Dashboard aktuelle Daten abrufen
    (verbundene Kameras, sichtbare Personen/Objekte, Personenzähler-Stände,
    aktive Aufnahmen). Der Schlüssel muss als Header <code>X-API-Key</code>
    oder als Query-Parameter <code>?api_key=</code> mitgeschickt werden.
  </p>
  <div class="panel-row" style="margin-top:10px;">
    <div style="flex:1; min-width:280px;">
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">Endpunkt</div>
      <input type="text" id="api-endpoint" readonly style="width:100%;">
    </div>
  </div>
  <div class="panel-row" style="margin-top:10px;">
    <div style="flex:1; min-width:280px;">
      <div class="muted" style="margin-bottom:4px; font-size:0.8rem;">API-Schlüssel</div>
      <input type="text" id="api-key-value" readonly style="width:100%;">
    </div>
    <div style="display:flex; gap:8px;">
      <button class="secondary sm" onclick="copyApiKey()">Kopieren</button>
      <button class="ghost sm" onclick="regenerateApiKey()">Neu erzeugen</button>
    </div>
  </div>
</div>

</div>

<script>
let AVAILABLE_CLASSES = [];
let rules = [];
let occupancyCounters = [];
let occupancyCameras = [];

async function loadNotifStatus() {
  const res = await fetch('/api/notifications/status');
  const s = await res.json();
  const methodNames = {ntfy: 'ntfy', telegram: 'Telegram', email: 'E-Mail', console: 'Konsole (nur Logausgabe)'};
  document.getElementById('notif-method').textContent = methodNames[s.method] || s.method;
  const statusEl = document.getElementById('snooze-status');
  if (s.snoozed) {
    const mins = Math.max(0, Math.round((s.until - Date.now()/1000) / 60));
    statusEl.innerHTML = `Aktuell stummgeschaltet für ca. ${mins} Min.`;
  } else {
    statusEl.innerHTML = 'Aktuell nicht stummgeschaltet.';
  }
}

async function snooze(minutes) {
  await fetch('/api/notifications/snooze', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({minutes})
  });
  toast(minutes > 0 ? `Für ${minutes} Minuten stummgeschaltet` : 'Stummschaltung aufgehoben');
  loadNotifStatus();
}

async function testNotification() {
  const res = await fetch('/api/notifications/test', {method: 'POST'});
  const data = await res.json();
  if (data.ok) toast('Testbenachrichtigung gesendet!'); else toast(data.message || 'Fehler', 'error');
}

async function loadClasses() {
  const res = await fetch('/api/detection-classes');
  const data = await res.json();
  AVAILABLE_CLASSES = data.available;
  const selected = new Set(data.selected);
  document.getElementById('class-grid').innerHTML = AVAILABLE_CLASSES.map(c => `
    <label><input type="checkbox" value="${c.id}" ${selected.has(c.id) ? 'checked' : ''} class="det-class-cb"> ${c.name}</label>
  `).join('');
}

async function saveClasses() {
  const ids = [...document.querySelectorAll('.det-class-cb:checked')].map(el => parseInt(el.value));
  const res = await fetch('/api/detection-classes', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({classes: ids})
  });
  const data = await res.json();
  if (data.ok) toast('Erkennungsklassen gespeichert!'); else toast(data.message || 'Fehler', 'error');
}

async function loadDetectionDevice() {
  const res = await fetch('/api/detection-device');
  const data = await res.json();
  document.getElementById('detection-device').value = data.device;
}

async function saveDetectionDevice() {
  const device = document.getElementById('detection-device').value;
  const res = await fetch('/api/detection-device', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({device})
  });
  const data = await res.json();
  if (data.ok) toast('Erkennungs-Engine gespeichert! Wirkt ab der nächsten Erkennung.');
  else toast(data.message || 'Fehler', 'error');
}

async function loadRules() {
  const res = await fetch('/api/recording-rules');
  const data = await res.json();
  AVAILABLE_CLASSES = data.available_classes;
  rules = data.rules;
  document.getElementById('recording-enabled').checked = data.enabled;
  renderRules();
}

function renderRules() {
  const container = document.getElementById('rule-list');
  if (rules.length === 0) {
    container.innerHTML = '<p class="muted">Noch keine Aufnahme-Regeln definiert.</p>';
    return;
  }
  container.innerHTML = rules.map((r, i) => `
    <div class="rule-card">
      <div class="panel-row">
        <label class="toggle-row"><label class="switch"><input type="checkbox" onchange="rules[${i}].enabled = this.checked" ${r.enabled ? 'checked' : ''}><span class="slider"></span></label> Regel aktiv</label>
        <button class="danger sm" onclick="deleteRule(${i})">Regel löschen</button>
      </div>
      <div class="class-grid">
        ${AVAILABLE_CLASSES.map(c => `
          <label><input type="checkbox" value="${c.id}"
            onchange="toggleRuleClass(${i}, ${c.id}, this.checked)"
            ${r.classes.includes(c.id) ? 'checked' : ''}> ${c.name}</label>
        `).join('')}
      </div>
      <div class="panel-row">
        <label>Von <input type="time" value="${(r.time_windows[0] && r.time_windows[0].start) || ''}"
          onchange="setRuleTime(${i}, 'start', this.value)"></label>
        <label>Bis <input type="time" value="${(r.time_windows[0] && r.time_windows[0].end) || ''}"
          onchange="setRuleTime(${i}, 'end', this.value)"></label>
        <span class="muted">(beide leer lassen = rund um die Uhr)</span>
      </div>
    </div>
  `).join('');
}

function toggleRuleClass(ruleIdx, classId, checked) {
  const set = new Set(rules[ruleIdx].classes);
  if (checked) set.add(classId); else set.delete(classId);
  rules[ruleIdx].classes = [...set];
}

function setRuleTime(ruleIdx, part, value) {
  const r = rules[ruleIdx];
  if (!r.time_windows || r.time_windows.length === 0) r.time_windows = [{start: '', end: ''}];
  r.time_windows[0][part] = value;
  const w = r.time_windows[0];
  if (!w.start && !w.end) r.time_windows = [];
}

function addRule() {
  rules.push({classes: [], time_windows: [], enabled: true});
  renderRules();
}

function deleteRule(idx) {
  rules.splice(idx, 1);
  renderRules();
}

async function saveRules() {
  const enabled = document.getElementById('recording-enabled').checked;
  const cleanRules = rules
    .filter(r => r.classes.length > 0)
    .map(r => ({
      classes: r.classes,
      enabled: r.enabled,
      time_windows: (r.time_windows && r.time_windows.length && r.time_windows[0].start && r.time_windows[0].end)
        ? [{start: r.time_windows[0].start, end: r.time_windows[0].end}]
        : [],
    }));
  const res = await fetch('/api/recording-rules', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled, rules: cleanRules})
  });
  const data = await res.json();
  if (data.ok) { toast('Aufnahme-Regeln gespeichert!'); loadRules(); } else { toast(data.message || 'Fehler', 'error'); }
}

async function loadCameras() {
  const res = await fetch('/api/cameras');
  const cams = await res.json();
  const container = document.getElementById('camera-list');
  if (cams.length === 0) {
    container.innerHTML = '<p class="muted">Noch keine Kameras hinzugefügt.</p>';
    return;
  }
  container.innerHTML = cams.map(c => `
    <div class="panel">
      <div class="panel-row">
        <div>
          <strong>${c.name}</strong>
          <span class="badge ${c.connected ? 'ok' : 'off'}" style="margin-left:8px;"><span class="pulse"></span>${c.connected ? 'Verbunden' : 'Offline'}</span>
          <div class="muted" style="margin-top:4px;">${c.zones.length} Zone(n) definiert</div>
        </div>
        <div style="display:flex; gap:8px; flex-wrap:wrap;">
          <a href="/settings/zones/${encodeURIComponent(c.name)}"><button class="secondary sm">Zonen bearbeiten</button></a>
          <button class="secondary sm" onclick="toggleEdit('${c.name}')">RTSP ändern</button>
          <button class="danger sm" onclick="deleteCamera('${c.name}')">Löschen</button>
        </div>
      </div>
      <div id="edit-${c.name}" style="display:none; margin-top:12px;">
        <div class="panel-row">
          <input type="text" id="rtsp-${c.name}" value="${c.rtsp_url}" style="flex:1; min-width:280px;">
          <button onclick="updateRtsp('${c.name}')">Speichern</button>
        </div>
      </div>
    </div>
  `).join('');
}

function toggleEdit(name) {
  const el = document.getElementById('edit-' + name);
  el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

async function addCamera() {
  const name = document.getElementById('new-name').value.trim();
  const rtsp = document.getElementById('new-rtsp').value.trim();
  if (!name || !rtsp) { toast('Bitte Name und RTSP-URL angeben.', 'error'); return; }
  const res = await fetch('/api/cameras', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name, rtsp_url: rtsp})
  });
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  document.getElementById('new-name').value = '';
  document.getElementById('new-rtsp').value = '';
  toast('Kamera hinzugefügt!');
  loadCameras();
}
document.getElementById('new-rtsp').addEventListener('keydown', e => { if (e.key === 'Enter') addCamera(); });

async function updateRtsp(name) {
  const rtsp = document.getElementById('rtsp-' + name).value.trim();
  const res = await fetch(`/api/cameras/${encodeURIComponent(name)}`, {
    method: 'PUT', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({rtsp_url: rtsp})
  });
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  toast('RTSP-URL aktualisiert!');
  loadCameras();
}

async function deleteCamera(name) {
  if (!confirm(`Kamera "${name}" wirklich löschen?`)) return;
  const res = await fetch(`/api/cameras/${encodeURIComponent(name)}`, {method: 'DELETE'});
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  toast('Kamera gelöscht.');
  loadCameras();
}

async function loadOccupancyCameras() {
  const res = await fetch('/api/cameras');
  occupancyCameras = await res.json();
  const camSel = document.getElementById('occ-camera');
  camSel.innerHTML = occupancyCameras.map(c => `<option value="${c.name}">${c.name}</option>`).join('')
    || '<option value="">Keine Kameras vorhanden</option>';
  renderOccupancyModeFields();
}

function renderOccupancyModeFields() {
  const mode = document.getElementById('occ-mode').value;
  document.getElementById('occ-field-door').style.display = mode === 'door' ? '' : 'none';
  document.getElementById('occ-field-outside').style.display = mode === 'two_zone' ? '' : 'none';
  document.getElementById('occ-field-inside').style.display = mode === 'two_zone' ? '' : 'none';
  renderOccupancyZoneOptions();
}

function renderOccupancyZoneOptions() {
  const camName = document.getElementById('occ-camera').value;
  const cam = occupancyCameras.find(c => c.name === camName);
  const zones = cam ? cam.zones : [];
  const options = zones.length
    ? zones.map(z => `<option value="${z.name}">${z.name}</option>`).join('')
    : '<option value="">Keine Zonen definiert</option>';
  document.getElementById('occ-door').innerHTML = options;
  document.getElementById('occ-outside').innerHTML = options;
  document.getElementById('occ-inside').innerHTML = options;
}

async function loadOccupancyCounters() {
  const res = await fetch('/api/occupancy');
  occupancyCounters = await res.json();
  const container = document.getElementById('occupancy-list');
  if (occupancyCounters.length === 0) {
    container.innerHTML = '<p class="muted">Noch keine Personenzähler angelegt.</p>';
    return;
  }
  container.innerHTML = occupancyCounters.map(c => {
    const modeLabel = c.mode === 'door'
      ? `Tür-Modus · Zone "${c.door_zone}"`
      : `Zwei-Zonen-Modus · ${c.outside_zone} &#8596; ${c.inside_zone}`;
    return `
    <div class="rule-card">
      <div class="panel-row">
        <div>
          <strong>${c.name}</strong>
          <div class="muted" style="margin-top:2px;">${c.camera} · ${modeLabel}</div>
        </div>
        <button class="danger sm" onclick="deleteOccupancyCounter('${c.id}')">Zähler löschen</button>
      </div>
      <div class="panel-row" style="margin-top:10px;">
        <div class="muted">Aktueller Stand: <strong style="color:var(--text);">${c.count}</strong> Person(en) drinnen</div>
        <div style="display:flex; gap:6px; align-items:center;">
          <input type="number" min="0" id="occ-manual-${c.id}" placeholder="korrigieren" style="width:110px;">
          <button class="secondary sm" onclick="setOccupancyManual('${c.id}')">Setzen</button>
        </div>
      </div>
    </div>
  `;
  }).join('');
}

async function addOccupancyCounter() {
  const name = document.getElementById('occ-name').value.trim();
  const mode = document.getElementById('occ-mode').value;
  const camera = document.getElementById('occ-camera').value;
  if (!name || !camera) { toast('Bitte Name und Kamera angeben.', 'error'); return; }

  let payload = {name, camera, mode};
  if (mode === 'door') {
    const door_zone = document.getElementById('occ-door').value;
    if (!door_zone) { toast('Bitte die Tür-Zone wählen.', 'error'); return; }
    payload.door_zone = door_zone;
  } else {
    const outside_zone = document.getElementById('occ-outside').value;
    const inside_zone = document.getElementById('occ-inside').value;
    if (!outside_zone || !inside_zone) { toast('Bitte Außen- und Innenzone wählen.', 'error'); return; }
    payload.outside_zone = outside_zone;
    payload.inside_zone = inside_zone;
  }

  const res = await fetch('/api/occupancy', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  document.getElementById('occ-name').value = '';
  toast('Personenzähler angelegt!');
  loadOccupancyCounters();
}

async function deleteOccupancyCounter(id) {
  if (!confirm('Diesen Personenzähler wirklich löschen?')) return;
  const res = await fetch(`/api/occupancy/${id}`, {method: 'DELETE'});
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  toast('Personenzähler gelöscht.');
  loadOccupancyCounters();
}

async function setOccupancyManual(id) {
  const input = document.getElementById('occ-manual-' + id);
  const value = parseInt(input.value, 10);
  if (isNaN(value) || value < 0) { toast('Bitte eine gültige Zahl eingeben.', 'error'); return; }
  const res = await fetch(`/api/occupancy/${id}/set`, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({count: value})
  });
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  toast('Stand korrigiert.');
  loadOccupancyCounters();
}

async function loadApiKey() {
  const res = await fetch('/api/api-key');
  const data = await res.json();
  document.getElementById('api-key-value').value = data.key;
  document.getElementById('api-endpoint').value = window.location.origin + '/api/dashboard?api_key=' + data.key;
}

function copyApiKey() {
  const input = document.getElementById('api-key-value');
  input.select();
  navigator.clipboard.writeText(input.value).then(() => toast('API-Schlüssel kopiert!'));
}

async function regenerateApiKey() {
  if (!confirm('Einen neuen API-Schlüssel erzeugen? Der alte Schlüssel funktioniert danach nicht mehr.')) return;
  const res = await fetch('/api/api-key/regenerate', {method: 'POST'});
  const data = await res.json();
  if (data.ok) { toast('Neuer API-Schlüssel erzeugt.'); loadApiKey(); }
}

loadCameras();
loadClasses();
loadDetectionDevice();
loadRules();
loadNotifStatus();
loadOccupancyCameras();
loadOccupancyCounters();
loadApiKey();
setInterval(loadNotifStatus, 15000);
setInterval(loadOccupancyCounters, 10000);
</script>
</body>
</html>
"""

ZONE_EDITOR_PAGE = """
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8"><title>Zonen bearbeiten – __CAM_NAME__</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%230a0d14'/%3E%3Cpath d='M17.5 8.5h-2.2l-1-1.5h-4.6l-1 1.5H6.5A1.5 1.5 0 0 0 5 10v7A1.5 1.5 0 0 0 6.5 18.5h11A1.5 1.5 0 0 0 19 17v-7a1.5 1.5 0 0 0-1.5-1.5z' fill='none' stroke='%235eb1ff' stroke-width='1.6' stroke-linejoin='round'/%3E%3Ccircle cx='12' cy='13' r='2.8' fill='none' stroke='%235eb1ff' stroke-width='1.6'/%3E%3C/svg%3E">
__STYLE__
</head>
<body>
__NAV__
<div class="wrap">
<h1>Zonen bearbeiten: __CAM_NAME__</h1>
<p class="subtitle">Klicke mind. 3 Eckpunkte auf das Bild, dann "Zone abschließen". Mehrere Zonen möglich – jede bekommt automatisch eine eigene Farbe.</p>

<div id="still-wrap">
  <img id="still" src="/api/still/__CAM_NAME_ENC__?t=0">
  <canvas id="overlay"></canvas>
</div>

<div style="margin-top:14px; display:flex; gap:8px; flex-wrap:wrap;">
  <button class="secondary" onclick="undoPoint()">Letzten Punkt entfernen</button>
  <button onclick="finishZone()">Zone abschließen</button>
  <button class="secondary" onclick="useFullImageAsZone()">Ganzes Bild als Zone</button>
  <button class="secondary" onclick="refreshStill()">Bild aktualisieren</button>
  <button onclick="saveZones()">Alle Zonen speichern</button>
</div>

<h2>Definierte Zonen</h2>
<div id="zone-list"></div>

</div>

<script>
const CAM_NAME = __CAM_NAME_JSON__;
let zones = __INITIAL_ZONES__;
let current = [];
let zonesConvertedToPixels = false;

function zoneColor(name) {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 75%, 58%)`;
}

const img = document.getElementById('still');
const canvas = document.getElementById('overlay');
const ctx = canvas.getContext('2d');

// Zonen werden in config.json normiert (0.0-1.0, relativ zur Kamera-
// Auflösung) gespeichert, damit sie unabhängig von der aktuellen Bildgröße
// immer an der richtigen Stelle sitzen. Für den Editor hier rechnen wir sie
// EINMALIG in absolute Canvas-Pixel um (ältere, bereits absolute Zonen
// werden automatisch erkannt und unverändert übernommen).
function isNormalized(points) {
  const maxVal = Math.max(0, ...points.flat().map(v => Math.abs(v)));
  return maxVal <= 1.5;
}

function convertZonesToPixelsOnce() {
  if (zonesConvertedToPixels) return;
  zones = zones.map(z => ({
    name: z.name,
    points: isNormalized(z.points)
      ? z.points.map(p => [p[0] * canvas.width, p[1] * canvas.height])
      : z.points,
  }));
  zonesConvertedToPixels = true;
}

function resizeCanvas() {
  canvas.width = img.naturalWidth || 800;
  canvas.height = img.naturalHeight || 450;
  canvas.style.width = img.clientWidth + 'px';
  canvas.style.height = img.clientHeight + 'px';
  convertZonesToPixelsOnce();
  redraw();
}
img.onload = resizeCanvas;
window.addEventListener('resize', resizeCanvas);

function drawPoly(points, color, label, closed) {
  ctx.strokeStyle = color; ctx.fillStyle = color; ctx.lineWidth = 3;
  ctx.beginPath();
  points.forEach((p, i) => {
    if (i === 0) ctx.moveTo(p[0], p[1]); else ctx.lineTo(p[0], p[1]);
  });
  if (closed && points.length > 2) ctx.closePath();
  ctx.stroke();
  points.forEach(p => ctx.fillRect(p[0]-4, p[1]-4, 8, 8));
  if (label) { ctx.font = 'bold 18px sans-serif'; ctx.fillText(label, points[0][0], Math.max(16, points[0][1]-10)); }
}

function redraw() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  zones.forEach(z => drawPoly(z.points, zoneColor(z.name), z.name, true));
  if (current.length) drawPoly(current, '#ffa500', null, false);
  renderZoneList();
}

canvas.addEventListener('click', e => {
  const rect = canvas.getBoundingClientRect();
  const scaleX = canvas.width / rect.width;
  const scaleY = canvas.height / rect.height;
  const x = Math.round((e.clientX - rect.left) * scaleX);
  const y = Math.round((e.clientY - rect.top) * scaleY);
  current.push([x, y]);
  redraw();
});

function undoPoint() {
  if (current.length) current.pop();
  redraw();
}

function finishZone() {
  if (current.length < 3) { toast('Mindestens 3 Punkte nötig.', 'error'); return; }
  const name = prompt('Name der Zone:', 'Zone' + (zones.length + 1));
  if (!name) return;
  zones.push({name: name, points: current});
  current = [];
  redraw();
  toast('Zone "' + name + '" hinzugefügt (noch nicht gespeichert)');
}

function useFullImageAsZone() {
  const name = prompt('Name der Zone:', 'Gesamtes Bild');
  if (!name) return;
  // kleiner Einzug (2px), damit die Umrandung am Bildrand sichtbar bleibt
  const inset = 2;
  const w = canvas.width, h = canvas.height;
  const points = [
    [inset, inset],
    [w - inset, inset],
    [w - inset, h - inset],
    [inset, h - inset],
  ];
  zones.push({name: name, points: points});
  current = [];
  redraw();
  toast('Zone "' + name + '" über das gesamte Bild hinzugefügt (noch nicht gespeichert)');
}

function deleteZone(idx) {
  zones.splice(idx, 1);
  redraw();
}

function renderZoneList() {
  const container = document.getElementById('zone-list');
  if (zones.length === 0) {
    container.innerHTML = '<p class="muted">Noch keine Zonen definiert.</p>';
    return;
  }
  container.innerHTML = zones.map((z, i) => `
    <span class="zone-tag" style="background:${zoneColor(z.name)}22; color:${zoneColor(z.name)}; border:1px solid ${zoneColor(z.name)}55;">${z.name} (${z.points.length} Punkte)
      <button class="danger" onclick="deleteZone(${i})">×</button>
    </span>
  `).join('');
}

function refreshStill() {
  img.src = `/api/still/${encodeURIComponent(CAM_NAME)}?t=` + Date.now();
}

async function saveZones() {
  const normalizedZones = zones.map(z => ({
    name: z.name,
    points: z.points.map(p => [p[0] / canvas.width, p[1] / canvas.height]),
  }));
  const res = await fetch(`/api/cameras/${encodeURIComponent(CAM_NAME)}/zones`, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({zones: normalizedZones})
  });
  const data = await res.json();
  if (!data.ok) { toast(data.message, 'error'); return; }
  toast('Zonen gespeichert!');
}

resizeCanvas();
</script>
</body>
</html>
"""

GALLERY_PAGE = """
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8"><title>Galerie – Heimüberwachung</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%230a0d14'/%3E%3Cpath d='M17.5 8.5h-2.2l-1-1.5h-4.6l-1 1.5H6.5A1.5 1.5 0 0 0 5 10v7A1.5 1.5 0 0 0 6.5 18.5h11A1.5 1.5 0 0 0 19 17v-7a1.5 1.5 0 0 0-1.5-1.5z' fill='none' stroke='%235eb1ff' stroke-width='1.6' stroke-linejoin='round'/%3E%3Ccircle cx='12' cy='13' r='2.8' fill='none' stroke='%235eb1ff' stroke-width='1.6'/%3E%3C/svg%3E">
__STYLE__
</head>
<body>
__NAV__
<div class="wrap">
<h1>Galerie</h1>
<div class="subtitle">Alle gespeicherten Fotos und Video-Aufnahmen an einem Ort – zum Ansehen anklicken.</div>

<div class="filters">
  <select id="filter-camera"><option value="">Alle Kameras</option></select>
  <select id="filter-type">
    <option value="">Alle Typen</option>
    <option value="photo">Nur Fotos</option>
    <option value="video">Nur Videos</option>
  </select>
  <input type="text" id="filter-search" placeholder="Suche nach Dateiname ...">
  <span class="muted" id="gallery-count"></span>
</div>

<div class="gallery-grid" id="gallery-grid">Lade ...</div>

</div>

<script>
let allItems = [];

async function loadGallery() {
  const res = await fetch('/api/gallery');
  allItems = await res.json();
  populateCameraFilter();
  renderGallery();
}

function populateCameraFilter() {
  const sel = document.getElementById('filter-camera');
  const current = sel.value;
  const cams = [...new Set(allItems.map(i => i.camera))].sort();
  sel.innerHTML = '<option value="">Alle Kameras</option>' + cams.map(c => `<option value="${c}">${c}</option>`).join('');
  sel.value = current;
}

function renderGallery() {
  const camFilter = document.getElementById('filter-camera').value;
  const typeFilter = document.getElementById('filter-type').value;
  const search = document.getElementById('filter-search').value.toLowerCase();

  const items = allItems.filter(i => {
    if (camFilter && i.camera !== camFilter) return false;
    if (typeFilter && i.type !== typeFilter) return false;
    if (search && !i.filename.toLowerCase().includes(search)) return false;
    return true;
  });

  document.getElementById('gallery-count').textContent = items.length + ' Datei(en)';

  const grid = document.getElementById('gallery-grid');
  if (items.length === 0) {
    grid.innerHTML = '<p class="muted">Keine Fotos oder Videos gefunden. Sobald es Ereignisse gibt, erscheinen sie hier.</p>';
    return;
  }
  // Für Pfeiltasten/Wisch-Navigation in der Lightbox: Liste aller gerade
  // sichtbaren (gefilterten) Galerie-Elemente in Anzeige-Reihenfolge.
  window.__galleryMedia = items.map(i => ({ url: i.url, video: i.type === 'video' }));

  grid.innerHTML = items.map((i, idx) => {
    const media = i.type === 'video'
      ? `<video src="${i.url}" muted onerror="this.parentElement.innerHTML='<div class=\\'thumb-placeholder\\' style=\\'width:100%;height:100%;\\'>Nicht verfuegbar</div>'"></video>`
      : `<img src="${i.url}" loading="lazy" onerror="this.parentElement.innerHTML='<div class=\\'thumb-placeholder\\' style=\\'width:100%;height:100%;\\'>Nicht verfuegbar</div>'">`;
    const badge = i.type === 'video' ? '<span class="type-badge video">Video</span>' : '<span class="type-badge">Foto</span>';
    return `
    <div class="gallery-item" onclick="openLightbox(window.__galleryMedia, ${idx})">
      <div class="media-wrap">${media}${badge}</div>
      <div class="cap"><span class="cam">${i.camera}</span><span>${i.time}</span></div>
    </div>`;
  }).join('');
}

document.getElementById('filter-camera').onchange = renderGallery;
document.getElementById('filter-type').onchange = renderGallery;
document.getElementById('filter-search').oninput = renderGallery;

loadGallery();
</script>
</body>
</html>
"""

STATISTICS_PAGE = """
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8"><title>Statistik – Heimüberwachung</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Crect width='24' height='24' rx='6' fill='%230a0d14'/%3E%3Cpath d='M17.5 8.5h-2.2l-1-1.5h-4.6l-1 1.5H6.5A1.5 1.5 0 0 0 5 10v7A1.5 1.5 0 0 0 6.5 18.5h11A1.5 1.5 0 0 0 19 17v-7a1.5 1.5 0 0 0-1.5-1.5z' fill='none' stroke='%235eb1ff' stroke-width='1.6' stroke-linejoin='round'/%3E%3Ccircle cx='12' cy='13' r='2.8' fill='none' stroke='%235eb1ff' stroke-width='1.6'/%3E%3C/svg%3E">
__STYLE__
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.4/chart.umd.min.js"></script>
</head>
<body>
__NAV__
<div class="wrap">
<div class="panel-row" style="margin-bottom:6px;">
  <div>
    <h1 style="margin-bottom:2px;">Statistik</h1>
    <div class="subtitle" style="margin-bottom:0;">Auswertung aller bisherigen Erkennungen – seit Beginn der Aufzeichnung.</div>
  </div>
  <button class="ghost sm" onclick="resetStats()">Statistik zurücksetzen</button>
</div>

<div class="stats-row" id="summary-row">
  <div class="stat-card"><div class="num">–</div><div class="lbl">Lade ...</div></div>
</div>

<div class="chart-grid">
  <div class="panel chart-panel">
    <h2 style="margin-top:0;">Was wurde erkannt?</h2>
    <canvas id="chart-classes"></canvas>
  </div>
  <div class="panel chart-panel">
    <h2 style="margin-top:0;">Erkennungen je Kamera</h2>
    <canvas id="chart-cameras"></canvas>
  </div>
  <div class="panel chart-panel">
    <h2 style="margin-top:0;">Verlauf der letzten 14 Tage</h2>
    <canvas id="chart-daily"></canvas>
  </div>
  <div class="panel chart-panel">
    <h2 style="margin-top:0;">Zu welcher Uhrzeit ist am meisten los?</h2>
    <canvas id="chart-hourly"></canvas>
  </div>
</div>

</div>

<script>
Chart.defaults.color = '#8891a3';
Chart.defaults.borderColor = '#262c3a';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif";

const PALETTE = ['#5eb1ff', '#8b7cff', '#3ddc97', '#ffc857', '#ff6b6b', '#4fd1c5', '#f472b6', '#a3e635', '#fb923c', '#38bdf8'];

let charts = {};

function destroyCharts() {
  Object.values(charts).forEach(c => c && c.destroy());
  charts = {};
}

async function loadStatistics() {
  const res = await fetch('/api/statistics');
  const data = await res.json();
  destroyCharts();

  const fmt = n => n.toLocaleString('de-DE');
  document.getElementById('summary-row').innerHTML = `
    <div class="stat-card accent"><div class="num">${fmt(data.total_detections)}</div><div class="lbl">Erkennungen gesamt</div></div>
    <div class="stat-card"><div class="num">${data.top_class || '–'}</div><div class="lbl">Häufigstes Objekt</div></div>
    <div class="stat-card"><div class="num">${data.top_camera || '–'}</div><div class="lbl">Aktivste Kamera</div></div>
    <div class="stat-card"><div class="num">${fmt(data.recordings_started)}</div><div class="lbl">Aufnahmen gestartet</div></div>
  `;

  if (data.total_detections === 0) {
    document.querySelector('.chart-grid').innerHTML = `
      <div class="panel empty-state" style="grid-column:1/-1;">
        <div class="big">–</div>
        <div>Noch keine Erkennungen aufgezeichnet.</div>
        <div class="muted">Sobald Objekte in einer Zone erkannt werden, füllt sich die Statistik automatisch.</div>
      </div>`;
    return;
  }

  charts.classes = new Chart(document.getElementById('chart-classes'), {
    type: 'bar',
    data: {
      labels: data.by_class.map(d => d.name),
      datasets: [{ label: 'Erkennungen', data: data.by_class.map(d => d.count), backgroundColor: PALETTE, borderRadius: 6 }]
    },
    options: {
      indexAxis: 'y', responsive: true,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { color: '#1c2230' }, ticks: { precision: 0 } }, y: { grid: { display: false } } }
    }
  });

  charts.cameras = new Chart(document.getElementById('chart-cameras'), {
    type: 'doughnut',
    data: {
      labels: data.by_camera.map(d => d.name),
      datasets: [{ data: data.by_camera.map(d => d.count), backgroundColor: PALETTE }]
    },
    options: { responsive: true, plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, padding: 12 } } } }
  });

  charts.daily = new Chart(document.getElementById('chart-daily'), {
    type: 'line',
    data: {
      labels: data.daily.map(d => d.label),
      datasets: [{
        label: 'Erkennungen pro Tag', data: data.daily.map(d => d.count),
        borderColor: '#5eb1ff', backgroundColor: 'rgba(94,177,255,0.15)',
        fill: true, tension: 0.3, pointRadius: 3, pointBackgroundColor: '#5eb1ff'
      }]
    },
    options: {
      responsive: true, plugins: { legend: { display: false } },
      scales: { x: { grid: { display: false } }, y: { grid: { color: '#1c2230' }, ticks: { precision: 0 }, beginAtZero: true } }
    }
  });

  charts.hourly = new Chart(document.getElementById('chart-hourly'), {
    type: 'bar',
    data: {
      labels: Array.from({length: 24}, (_, h) => String(h).padStart(2, '0') + ':00'),
      datasets: [{ label: 'Erkennungen', data: data.hourly_counts, backgroundColor: '#8b7cff', borderRadius: 4 }]
    },
    options: {
      responsive: true, plugins: { legend: { display: false } },
      scales: { x: { grid: { display: false } }, y: { grid: { color: '#1c2230' }, ticks: { precision: 0 }, beginAtZero: true } }
    }
  });
}

async function resetStats() {
  if (!confirm('Statistik wirklich zurücksetzen? Das kann nicht rückgängig gemacht werden.')) return;
  await fetch('/api/statistics/reset', { method: 'POST' });
  toast('Statistik zurückgesetzt.');
  loadStatistics();
}

loadStatistics();
</script>
</body>
</html>
"""


def create_app(camera_manager: CameraManager, web_cfg: dict, storage_cfg: dict, rec_cfg: dict, notifier: BaseNotifier) -> Flask:
    app = Flask(__name__)

    @app.route("/")
    def index():
        cams = camera_manager.list()
        cams_html = "".join(
            f'<div class="cam"><img src="/stream/{c.name}" onclick="openLightbox([{{url:this.src, video:false}}], 0)">'
            f'<div class="label"><span class="name">{c.name}</span>'
            f'<span class="badge {"ok" if c.is_connected() else "off"}"><span class="pulse"></span>'
            f'{"Verbunden" if c.is_connected() else "Keine Verbindung"}</span></div></div>'
            for c in cams
        ) or '<p class="muted">Noch keine Kameras. Unter <a href="/settings">Einstellungen</a> welche hinzufügen.</p>'
        html = INDEX_PAGE.replace("__STYLE__", BASE_STYLE).replace("__NAV__", NAV_HTML)
        html = html.replace("__CAMERAS_HTML__", cams_html)
        return html

    @app.route("/settings")
    def settings():
        html = SETTINGS_PAGE.replace("__STYLE__", BASE_STYLE).replace("__NAV__", NAV_HTML)
        return html

    @app.route("/gallery")
    def gallery():
        html = GALLERY_PAGE.replace("__STYLE__", BASE_STYLE).replace("__NAV__", NAV_HTML)
        return html

    @app.route("/statistik")
    def statistik():
        html = STATISTICS_PAGE.replace("__STYLE__", BASE_STYLE).replace("__NAV__", NAV_HTML)
        return html

    @app.route("/api/statistics")
    def api_statistics():
        with STATS_LOCK:
            class_counts = dict(STATS["class_counts"])
            camera_counts = dict(STATS["camera_counts"])
            daily_counts = dict(STATS["daily_counts"])
            hourly_counts = list(STATS["hourly_counts"])
            recordings_started = STATS.get("recordings_started", 0)

        by_class = sorted(
            ({"name": class_name_de(int(cid)), "count": c} for cid, c in class_counts.items()),
            key=lambda d: -d["count"],
        )
        by_camera = sorted(
            ({"name": name, "count": c} for name, c in camera_counts.items()),
            key=lambda d: -d["count"],
        )

        # Verlauf der letzten 14 Tage (auch Tage ohne Ereignisse werden mit 0 angezeigt)
        daily = []
        for i in range(13, -1, -1):
            day = datetime.now() - timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            count = sum(daily_counts.get(day_str, {}).values())
            daily.append({"label": day.strftime("%d.%m."), "count": count})

        total_detections = sum(class_counts.values())

        return jsonify({
            "total_detections": total_detections,
            "top_class": by_class[0]["name"] if by_class else None,
            "top_camera": by_camera[0]["name"] if by_camera else None,
            "recordings_started": recordings_started,
            "by_class": by_class,
            "by_camera": by_camera,
            "daily": daily,
            "hourly_counts": hourly_counts,
        })

    @app.route("/api/statistics/reset", methods=["POST"])
    def api_statistics_reset():
        reset_stats()
        return jsonify({"ok": True})

    @app.route("/api/gallery")
    def api_gallery():
        items = []
        snap_dir = Path(storage_cfg["snapshot_dir"])
        rec_dir = Path(rec_cfg["output_dir"])
        if snap_dir.exists():
            for f in snap_dir.iterdir():
                if f.is_file():
                    items.append({"filename": f.name, "type": "photo", "url": f"/snapshots/{f.name}",
                                  "mtime": f.stat().st_mtime})
        if rec_dir.exists():
            for f in rec_dir.iterdir():
                if f.is_file():
                    items.append({"filename": f.name, "type": "video", "url": f"/recordings/{f.name}",
                                  "mtime": f.stat().st_mtime})
        items.sort(key=lambda x: -x["mtime"])
        for it in items:
            it["time"] = datetime.fromtimestamp(it["mtime"]).strftime("%d.%m.%Y %H:%M:%S")
            # Kamera-Name ist der Teil des Dateinamens vor dem ersten "_"
            # (Dateien werden als "{Kamera}_{Zone}_{Zeitstempel}.ext" gespeichert)
            it["camera"] = it["filename"].split("_")[0]
            del it["mtime"]
        return jsonify(items[:500])

    @app.route("/settings/zones/<name>")
    def zone_editor(name):
        if not camera_manager.exists(name):
            return "Kamera nicht gefunden", 404
        cam = camera_manager.get(name)
        html = ZONE_EDITOR_PAGE.replace("__STYLE__", BASE_STYLE).replace("__NAV__", NAV_HTML)
        html = html.replace("__CAM_NAME__", name).replace("__CAM_NAME_ENC__", name)
        html = html.replace("__CAM_NAME_JSON__", json.dumps(name))
        html = html.replace("__INITIAL_ZONES__", json.dumps(cam.zones))
        return html

    @app.route("/stream/<name>")
    def stream(name):
        camera = camera_manager.get(name)
        if camera is None:
            return "Kamera nicht gefunden", 404

        def generate():
            interval = 1.0 / web_cfg["stream_fps"]
            while True:
                jpeg = camera.render_jpeg(web_cfg["stream_width"], web_cfg["jpeg_quality"])
                if jpeg is not None:
                    yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n")
                time.sleep(interval)

        return Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")

    @app.route("/api/still/<name>")
    def api_still(name):
        camera = camera_manager.get(name)
        if camera is None:
            return "Kamera nicht gefunden", 404
        jpeg = camera.render_still_full()
        if jpeg is None:
            return "Kein Bild verfügbar", 503
        return Response(jpeg, mimetype="image/jpeg")

    @app.route("/api/events")
    def api_events():
        return jsonify(EVENT_FEED.all())

    @app.route("/api/live-status")
    def api_live_status():
        now = time.time()
        status = []
        for cam in camera_manager.list():
            for track in cam.tracker.tracks.values():
                for zone_name, enter_ts in track.zone_enter_time.items():
                    status.append({
                        "camera": cam.name,
                        "zone": zone_name,
                        "class_name": class_name_de(track.class_id),
                        "dwell_seconds": int(now - enter_ts),
                    })
        status.sort(key=lambda s: -s["dwell_seconds"])
        return jsonify(status)

    @app.route("/api/stats")
    def api_stats():
        cams = camera_manager.list()
        today_str = datetime.now().strftime("%d.%m.%Y")
        events = EVENT_FEED.all()
        events_today = sum(1 for e in events if e["time"].startswith(today_str))
        active_recordings = sum(len(c.active_recordings) for c in cams)
        zones_total = sum(len(c.zones) for c in cams)

        heartbeat_age = seconds_since_engine_heartbeat()
        # Schwelle: mit wie vielen Kameras muss die Runde im schlechtesten
        # Fall dauern, bevor man wirklich von "hängt" statt "ist nur dran"
        # sprechen kann. Grosszügiger Puffer, um Fehlalarme zu vermeiden.
        stale_threshold = max(30.0, CONFIG["detection"]["seconds_per_camera"] * max(1, len(cams)) * 4)
        engine_alive = heartbeat_age is not None and heartbeat_age < stale_threshold

        return jsonify({
            "cameras_total": len(cams),
            "cameras_connected": sum(1 for c in cams if c.is_connected()),
            "zones_total": zones_total,
            "events_today": events_today,
            "active_recordings": active_recordings,
            "notifications_snoozed": is_notifications_snoozed(),
            "snoozed_until": get_snooze_until(),
            "engine_alive": engine_alive,
            "engine_last_seen_seconds_ago": heartbeat_age,
        })

    @app.route("/api/notifications/status")
    def api_notifications_status():
        return jsonify({
            "snoozed": is_notifications_snoozed(),
            "until": get_snooze_until(),
            "method": CONFIG["notifications"].get("method", "console"),
        })

    @app.route("/api/notifications/snooze", methods=["POST"])
    def api_notifications_snooze():
        data = request.get_json(force=True)
        try:
            minutes = float(data.get("minutes", 0))
        except (TypeError, ValueError):
            minutes = 0
        set_snooze(minutes)
        return jsonify({"ok": True, "until": get_snooze_until()})

    @app.route("/api/notifications/test", methods=["POST"])
    def api_notifications_test():
        try:
            notifier.send(
                "Testbenachrichtigung",
                "Das ist eine Testbenachrichtigung deiner Heimüberwachung. Wenn du das siehst, funktioniert der "
                f"konfigurierte Weg ({CONFIG['notifications'].get('method', 'console')}).",
                None,
            )
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "message": str(e)})

    @app.route("/api/cameras", methods=["GET"])
    def api_cameras_list():
        return jsonify([
            {"name": c.name, "rtsp_url": c.rtsp_url, "zones": c.zones, "connected": c.is_connected()}
            for c in camera_manager.list()
        ])

    @app.route("/api/cameras", methods=["POST"])
    def api_cameras_add():
        data = request.get_json(force=True)
        name = (data.get("name") or "").strip()
        rtsp_url = (data.get("rtsp_url") or "").strip()
        ok, message = camera_manager.add(name, rtsp_url)
        return jsonify({"ok": ok, "message": message})

    @app.route("/api/cameras/<name>", methods=["PUT"])
    def api_cameras_update(name):
        data = request.get_json(force=True)
        rtsp_url = (data.get("rtsp_url") or "").strip()
        if not rtsp_url:
            return jsonify({"ok": False, "message": "RTSP-URL darf nicht leer sein."})
        ok, message = camera_manager.update_rtsp(name, rtsp_url)
        return jsonify({"ok": ok, "message": message})

    @app.route("/api/cameras/<name>", methods=["DELETE"])
    def api_cameras_delete(name):
        ok, message = camera_manager.remove(name)
        return jsonify({"ok": ok, "message": message})

    @app.route("/api/cameras/<name>/zones", methods=["POST"])
    def api_cameras_zones(name):
        data = request.get_json(force=True)
        zones = data.get("zones", [])
        # einfache Validierung der Struktur
        for z in zones:
            if "name" not in z or "points" not in z or len(z["points"]) < 3:
                return jsonify({"ok": False, "message": "Ungültige Zonen-Daten."})
        ok, message = camera_manager.update_zones(name, zones)
        return jsonify({"ok": ok, "message": message})

    @app.route("/api/detection-classes", methods=["GET"])
    def api_detection_classes_get():
        return jsonify({
            "available": [{"id": cid, "name": name} for cid, name in sorted(COCO_CLASSES_DE.items(), key=lambda x: x[1])],
            "selected": CONFIG["detection"]["classes"],
        })

    @app.route("/api/detection-classes", methods=["POST"])
    def api_detection_classes_set():
        data = request.get_json(force=True)
        classes = [c for c in data.get("classes", []) if c in COCO_CLASSES_DE]
        with CONFIG_LOCK:
            CONFIG["detection"]["classes"] = classes
            save_config(CONFIG)
        return jsonify({"ok": True})

    @app.route("/api/detection-device", methods=["GET"])
    def api_detection_device_get():
        return jsonify({"device": CONFIG["detection"].get("device", "cpu")})

    @app.route("/api/detection-device", methods=["POST"])
    def api_detection_device_set():
        data = request.get_json(force=True)
        device = data.get("device", "cpu")
        if device not in ("cpu", "cuda"):
            return jsonify({"ok": False, "message": "Ungültiger Wert (nur 'cpu' oder 'cuda')."})
        with CONFIG_LOCK:
            CONFIG["detection"]["device"] = device
            save_config(CONFIG)
        return jsonify({"ok": True})

    @app.route("/api/recording-rules", methods=["GET"])
    def api_recording_rules_get():
        return jsonify({
            "available_classes": [{"id": cid, "name": name} for cid, name in sorted(COCO_CLASSES_DE.items(), key=lambda x: x[1])],
            "rules": CONFIG["recording"]["rules"],
            "enabled": CONFIG["recording"].get("enabled", True),
        })

    @app.route("/api/recording-rules", methods=["POST"])
    def api_recording_rules_set():
        data = request.get_json(force=True)
        clean_rules = []
        for r in data.get("rules", []):
            classes = [c for c in r.get("classes", []) if c in COCO_CLASSES_DE]
            if not classes:
                continue
            clean_rules.append({
                "classes": classes,
                "time_windows": r.get("time_windows", []),
                "enabled": bool(r.get("enabled", True)),
            })
        with CONFIG_LOCK:
            CONFIG["recording"]["rules"] = clean_rules
            CONFIG["recording"]["enabled"] = bool(data.get("enabled", True))
            save_config(CONFIG)
        return jsonify({"ok": True})

    # ------------------------------------------------------
    # Personenzähler (rein/raus, z.B. an der Haustür)
    # ------------------------------------------------------

    @app.route("/api/occupancy", methods=["GET"])
    def api_occupancy_list():
        counters = []
        for counter in CONFIG["occupancy"].get("counters", []):
            counters.append({
                "id": counter["id"],
                "name": counter["name"],
                "camera": counter["camera"],
                "mode": counter.get("mode", "two_zone"),
                "outside_zone": counter.get("outside_zone"),
                "inside_zone": counter.get("inside_zone"),
                "door_zone": counter.get("door_zone"),
                "count": get_occupancy_count(counter["id"]),
            })
        return jsonify(counters)

    @app.route("/api/occupancy", methods=["POST"])
    def api_occupancy_add():
        data = request.get_json(force=True)
        name = (data.get("name") or "").strip()
        camera_name = (data.get("camera") or "").strip()
        mode = data.get("mode") or "two_zone"

        if not name or not camera_name:
            return jsonify({"ok": False, "message": "Bitte Name und Kamera angeben."})
        cam = camera_manager.get(camera_name)
        if cam is None:
            return jsonify({"ok": False, "message": "Kamera nicht gefunden."})
        zone_names = {z["name"] for z in cam.zones}

        import uuid
        counter_id = uuid.uuid4().hex[:12]
        entry = {"id": counter_id, "name": name, "camera": camera_name, "mode": mode}

        if mode == "door":
            door_zone = (data.get("door_zone") or "").strip()
            if not door_zone:
                return jsonify({"ok": False, "message": "Bitte die Tür-Zone angeben."})
            if door_zone not in zone_names:
                return jsonify({"ok": False, "message": "Die Tür-Zone muss bei dieser Kamera existieren."})
            entry["door_zone"] = door_zone
        else:
            outside_zone = (data.get("outside_zone") or "").strip()
            inside_zone = (data.get("inside_zone") or "").strip()
            if not outside_zone or not inside_zone:
                return jsonify({"ok": False, "message": "Bitte Außen- und Innenzone angeben."})
            if outside_zone == inside_zone:
                return jsonify({"ok": False, "message": "Außen- und Innenzone müssen unterschiedlich sein."})
            if outside_zone not in zone_names or inside_zone not in zone_names:
                return jsonify({"ok": False, "message": "Beide Zonen müssen bei dieser Kamera existieren."})
            entry["outside_zone"] = outside_zone
            entry["inside_zone"] = inside_zone

        with CONFIG_LOCK:
            CONFIG["occupancy"]["counters"].append(entry)
            save_config(CONFIG)
        set_occupancy(counter_id, 0)
        return jsonify({"ok": True})

    @app.route("/api/occupancy/<counter_id>", methods=["DELETE"])
    def api_occupancy_delete(counter_id):
        with CONFIG_LOCK:
            before = len(CONFIG["occupancy"]["counters"])
            CONFIG["occupancy"]["counters"] = [c for c in CONFIG["occupancy"]["counters"] if c["id"] != counter_id]
            found = len(CONFIG["occupancy"]["counters"]) != before
            save_config(CONFIG)
        return jsonify({"ok": found, "message": None if found else "Zähler nicht gefunden."})

    @app.route("/api/occupancy/<counter_id>/set", methods=["POST"])
    def api_occupancy_set(counter_id):
        data = request.get_json(force=True)
        try:
            value = int(data.get("count", 0))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": "Ungültiger Wert."})
        exists = any(c["id"] == counter_id for c in CONFIG["occupancy"].get("counters", []))
        if not exists:
            return jsonify({"ok": False, "message": "Zähler nicht gefunden."})
        new_value = set_occupancy(counter_id, value)
        return jsonify({"ok": True, "count": new_value})

    @app.route("/api/api-key", methods=["GET"])
    def api_api_key_get():
        return jsonify({"key": CONFIG["api"].get("key", "")})

    @app.route("/api/api-key/regenerate", methods=["POST"])
    def api_api_key_regenerate():
        import secrets
        with CONFIG_LOCK:
            CONFIG["api"]["key"] = secrets.token_hex(20)
            save_config(CONFIG)
        return jsonify({"ok": True, "key": CONFIG["api"]["key"]})

    # ------------------------------------------------------
    # Externe API für ein Home-Dashboard
    # ------------------------------------------------------
    # Schlanker, schreibgeschützter Endpunkt für externe Systeme (z.B. ein
    # Home-Dashboard wie Homepage/Homarr, Home Assistant o.ä.). Ist in
    # config.json unter "api.key" ein Schlüssel gesetzt, muss dieser als
    # Header "X-API-Key" oder Query-Parameter "?api_key=" mitgeschickt
    # werden. Ohne gesetzten Schlüssel ist der Endpunkt offen erreichbar.

    def _check_api_key() -> bool:
        required = CONFIG["api"].get("key", "")
        if not required:
            return True
        provided = request.headers.get("X-API-Key") or request.args.get("api_key", "")
        return provided == required

    @app.route("/api/dashboard", methods=["GET"])
    def api_dashboard():
        if not _check_api_key():
            return jsonify({"ok": False, "message": "Ungültiger oder fehlender API-Schlüssel."}), 401

        cams = camera_manager.list()
        cameras_out = []
        persons_visible_total = 0
        objects_visible_total: dict = {}

        for cam in cams:
            live_tracks = list(cam.tracker.tracks.values())
            objects_here: dict = {}
            for t in live_tracks:
                cname = class_name_de(t.class_id)
                objects_here[cname] = objects_here.get(cname, 0) + 1
                objects_visible_total[cname] = objects_visible_total.get(cname, 0) + 1
            persons_here = objects_here.get("Person", 0)
            persons_visible_total += persons_here
            cameras_out.append({
                "name": cam.name,
                "connected": cam.is_connected(),
                "persons_visible": persons_here,
                "objects_visible": objects_here,
                "active_recordings": len(cam.active_recordings),
            })

        occupancy_out = []
        occupancy_total = 0
        for counter in CONFIG["occupancy"].get("counters", []):
            count = get_occupancy_count(counter["id"])
            occupancy_total += count
            occupancy_out.append({"id": counter["id"], "name": counter["name"], "count": count})

        today_str = datetime.now().strftime("%d.%m.%Y")
        events_today = sum(1 for e in EVENT_FEED.all() if e["time"].startswith(today_str))

        heartbeat_age = seconds_since_engine_heartbeat()
        stale_threshold = max(30.0, CONFIG["detection"]["seconds_per_camera"] * max(1, len(cams)) * 4)
        engine_alive = heartbeat_age is not None and heartbeat_age < stale_threshold

        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "engine_alive": engine_alive,
            "engine_last_seen_seconds_ago": heartbeat_age,
            "cameras_total": len(cams),
            "cameras_connected": sum(1 for c in cams if c.is_connected()),
            "cameras": cameras_out,
            "persons_visible_total": persons_visible_total,
            "objects_visible_total": objects_visible_total,
            "occupancy": occupancy_out,
            "occupancy_total": occupancy_total,
            "active_recordings": sum(len(c.active_recordings) for c in cams),
            "events_today": events_today,
        })

    @app.route("/snapshots/<path:filename>")
    def snapshots(filename):
        return send_from_directory(storage_cfg["snapshot_dir"], filename)

    @app.route("/recordings/<path:filename>")
    def recordings(filename):
        return send_from_directory(rec_cfg["output_dir"], filename)

    return app


# ============================================================
# EINBETTUNG: gemeinsam mit dem Smart-Home-Dashboard im selben Prozess/Port
# ============================================================
# Ersetzt den früheren eigenständigen Server-Start (run_system()/app.run()
# auf einem eigenen Port). Wird von app.py (Hauptprojekt) beim Start
# aufgerufen - ein Prozess, ein Port, kein zweiter Server nötig.

_camera_manager = None
_engine = None
_notifier = None
_WHOLE_FRAME_ZONE = {"name": "Ganzes Bild", "points": [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]}


def start_embedded():
    """
    Startet die Personenerkennung im selben Python-Prozess/derselben Datei wie
    das Smart-Home-Dashboard - EIN Befehl (`python3 app.py` bzw. die
    Einzeldatei-Version), KEIN zweiter Server, KEIN zweiter Port, KEIN
    externer Link: Live-Ansicht, Kamera-Verwaltung, Ereignisse und
    Einstellungen laufen als ganz normale Routen auf demselben Flask-Prozess
    wie das restliche Dashboard (siehe app.py, Bereich "Heimüberwachung") und
    werden im eigenen Stil direkt im Reiter "Heimüberwachung"
    dargestellt - die (sehr umfangreiche) eigene Web-Oberfläche der
    Heimüberwachung (create_app() weiter oben) wird dafür NICHT mehr
    verwendet/gestartet.

    Wirft eine Exception, falls z.B. ultralytics/opencv nicht installiert
    sind - der Aufrufer (app.py) fängt das ab und lässt das restliche
    Dashboard unbeeinflusst weiterlaufen.
    """
    global _camera_manager, _engine, _notifier
    from ultralytics import YOLO

    log.info("Lade YOLO-Modell (%s) für die eingebettete Personenerkennung ...", CONFIG["detection"]["model"])
    model = YOLO(CONFIG["detection"]["model"])
    _notifier = build_notifier(CONFIG["notifications"])

    _camera_manager = CameraManager(CONFIG["detection"])
    _camera_manager.load_initial(CONFIG["cameras"])

    _engine = InferenceEngine(
        camera_manager=_camera_manager, model=model, det_cfg=CONFIG["detection"],
        notif_cfg=CONFIG["notifications"], rec_cfg=CONFIG["recording"],
        storage_cfg=CONFIG["storage"], occupancy_cfg=CONFIG["occupancy"], notifier=_notifier,
    )
    _engine.start()

    log.info(
        "Heimüberwachung eingebettet (%d Kamera(s) konfiguriert), läuft nativ "
        "im Reiter 'Heimüberwachung' des Dashboards - kein zweiter Port.",
        len(_camera_manager.list()),
    )
    return _camera_manager


def stop_embedded():
    global _camera_manager, _engine
    if _engine is not None:
        _engine.stop()
    if _camera_manager is not None:
        _camera_manager.shutdown_all()
    _camera_manager = None
    _engine = None


def is_available():
    """True, sobald die Personenerkennung erfolgreich eingebettet/gestartet wurde."""
    return _camera_manager is not None


def get_dashboard_snapshot():
    """
    In-Prozess-Äquivalent zum /api/dashboard-Endpunkt (siehe create_app oben) -
    ein direkter Funktionsaufruf statt eines HTTP-Requests, da die
    Personenerkennung jetzt im selben Prozess läuft wie das Dashboard.
    Returns dict oder None, falls die Personenerkennung nicht läuft.
    """
    if _camera_manager is None:
        return None

    cams = _camera_manager.list()
    cameras_out = []
    persons_visible_total = 0
    objects_visible_total = {}

    for cam in cams:
        live_tracks = list(cam.tracker.tracks.values())
        objects_here = {}
        for t in live_tracks:
            cname = class_name_de(t.class_id)
            objects_here[cname] = objects_here.get(cname, 0) + 1
            objects_visible_total[cname] = objects_visible_total.get(cname, 0) + 1
        persons_here = objects_here.get("Person", 0)
        persons_visible_total += persons_here
        cameras_out.append({
            "name": cam.name,
            "connected": cam.is_connected(),
            "persons_visible": persons_here,
            "objects_visible": objects_here,
            "active_recordings": len(cam.active_recordings),
        })

    occupancy_out = []
    occupancy_total = 0
    for counter in CONFIG["occupancy"].get("counters", []):
        count = get_occupancy_count(counter["id"])
        occupancy_total += count
        occupancy_out.append({"id": counter["id"], "name": counter["name"], "count": count})

    today_str = datetime.now().strftime("%d.%m.%Y")
    all_events = EVENT_FEED.all()
    events_today = sum(1 for e in all_events if e["time"].startswith(today_str))
    latest_event = all_events[0] if all_events else None

    heartbeat_age = seconds_since_engine_heartbeat()
    stale_threshold = max(30.0, CONFIG["detection"]["seconds_per_camera"] * max(1, len(cams)) * 4)
    engine_alive = heartbeat_age is not None and heartbeat_age < stale_threshold

    return {
        "timestamp": datetime.now().isoformat(),
        "engine_alive": engine_alive,
        "engine_last_seen_seconds_ago": heartbeat_age,
        "cameras_total": len(cams),
        "cameras_connected": sum(1 for c in cams if c.is_connected()),
        "cameras": cameras_out,
        "persons_visible_total": persons_visible_total,
        "objects_visible_total": objects_visible_total,
        "occupancy": occupancy_out,
        "occupancy_total": occupancy_total,
        "active_recordings": sum(len(c.active_recordings) for c in cams),
        "events_today": events_today,
        "latest_event": latest_event,
        "gallery_count": count_gallery_photos(),
        "reachable": True,
    }


def get_still_bytes(name, quality=85):
    """Einzelbild einer Kamera - direkter Funktionsaufruf, kein HTTP-Roundtrip
    (im Gegensatz zur früheren Variante über einen zweiten, separaten Server)."""
    if _camera_manager is None:
        return None
    camera = _camera_manager.get(name)
    if camera is None:
        return None
    return camera.render_still_full(quality=quality)


def iter_camera_mjpeg(name, fps=8, width=800, quality=80):
    """Generator: liefert fortlaufend MJPEG-Frames einer Kamera - exakt dieselbe
    Technik wie der Live-Stream der eigenständigen Heimüberwachung (dort unter
    /stream/<name>), hier aber per direktem Funktionsaufruf im selben Prozess
    statt über einen zweiten Server/Port. Wird vom Dashboard genutzt, damit die
    Kamera-Kacheln echtes Live-Video statt einzelner Snapshots zeigen."""
    if _camera_manager is None:
        return
    interval = 1.0 / max(1, fps)
    while True:
        camera = _camera_manager.get(name)
        if camera is None:
            break
        jpeg = camera.render_jpeg(width, quality)
        if jpeg is not None:
            yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n")
        time.sleep(interval)


# ---- Foto-Galerie (Erkennungs-Aufnahmen) --------------------------------
# Bewusst NUR Fotos - die Video-Aufnahme ist deaktiviert (siehe CONFIG["recording"]);
# jedes erkannte Objekt einer ausgewählten Klasse legt automatisch ein
# Standbild in CONFIG["storage"]["snapshot_dir"] ab (siehe save_alert_snapshot /
# _maybe_notify weiter oben), diese Funktionen listen/verwalten genau diese Fotos.

_GALLERY_EXTENSIONS = (".jpg", ".jpeg", ".png")


def _gallery_dir() -> Path:
    return Path(CONFIG["storage"]["snapshot_dir"])


def _gallery_files():
    d = _gallery_dir()
    if not d.exists():
        return []
    return [f for f in d.iterdir() if f.is_file() and f.suffix.lower() in _GALLERY_EXTENSIONS]


def count_gallery_photos() -> int:
    return len(_gallery_files())


def list_gallery(limit=200, camera=None, date_from=None, date_to=None):
    """Gespeicherte Erkennungs-Fotos, neueste zuerst. Der Dateiname folgt dem
    Schema '<Kamera>_<Zone>_<Unix-Zeitstempel>.jpg' (siehe save_alert_snapshot),
    daraus wird die Kamera fürs Filtern/Anzeigen extrahiert.

    camera: nur Fotos dieser Kamera (exakter Name).
    date_from / date_to: 'YYYY-MM-DD' Strings, inklusive Grenzen, lokale Zeit."""
    files = sorted(_gallery_files(), key=lambda f: f.stat().st_mtime, reverse=True)

    from_ts = None
    to_ts = None
    try:
        if date_from:
            from_ts = datetime.strptime(date_from, "%Y-%m-%d").timestamp()
        if date_to:
            to_ts = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).timestamp()
    except ValueError:
        pass

    out = []
    for f in files:
        stat = f.stat()
        if from_ts is not None and stat.st_mtime < from_ts:
            continue
        if to_ts is not None and stat.st_mtime >= to_ts:
            continue
        parts = f.stem.split("_")
        cam_name = parts[0] if parts else ""
        if camera and cam_name != camera:
            continue
        out.append({
            "filename": f.name,
            "camera": cam_name,
            "time": datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m.%Y %H:%M:%S"),
            "timestamp": stat.st_mtime,
        })
        if len(out) >= max(0, int(limit)):
            break
    return out


def get_snapshot_path(filename):
    """Abgesicherter Pfad zu einem Foto - verhindert Zugriff außerhalb des
    Snapshot-Ordners (z.B. über '..' im übergebenen Dateinamen)."""
    d = _gallery_dir().resolve()
    candidate = (d / Path(filename).name).resolve()
    if d != candidate.parent:
        return None
    if not candidate.is_file():
        return None
    return str(candidate)


def delete_snapshot(filename):
    path = get_snapshot_path(filename)
    if path is None:
        return False, "Foto nicht gefunden."
    try:
        os.remove(path)
        return True, None
    except Exception as e:
        return False, str(e)


def delete_snapshots(filenames):
    """Löscht mehrere Fotos auf einmal (Mehrfachauswahl in der Galerie).
    Gibt (anzahl_geloescht, anzahl_fehlgeschlagen) zurück."""
    ok_count = 0
    fail_count = 0
    for name in filenames:
        success, _ = delete_snapshot(name)
        if success:
            ok_count += 1
        else:
            fail_count += 1
    return ok_count, fail_count


def clear_gallery():
    """Löscht alle gespeicherten Fotos, gibt die Anzahl gelöschter Dateien zurück."""
    deleted = 0
    for f in _gallery_files():
        try:
            f.unlink()
            deleted += 1
        except Exception:
            pass
    return deleted


def cleanup_gallery():
    """Automatisches Aufräumen der Galerie gemäß CONFIG["storage"]["max_photo_age_days"]
    und ["max_photos"] (0 = jeweils kein Limit) - läuft regelmäßig im
    Hintergrund (siehe _cleanup_loop im Scheduler), damit die Festplatte nicht
    unbegrenzt vollläuft. Gibt (anzahl_geloescht, grund) zurück."""
    max_age_days = CONFIG["storage"].get("max_photo_age_days", 0) or 0
    max_photos = CONFIG["storage"].get("max_photos", 0) or 0
    if max_age_days <= 0 and max_photos <= 0:
        return 0

    files = sorted(_gallery_files(), key=lambda f: f.stat().st_mtime, reverse=True)  # neueste zuerst
    deleted = 0

    if max_age_days > 0:
        cutoff = time.time() - max_age_days * 86400
        kept = []
        for f in files:
            if f.stat().st_mtime < cutoff:
                try:
                    f.unlink()
                    deleted += 1
                except Exception:
                    pass
            else:
                kept.append(f)
        files = kept

    if max_photos > 0 and len(files) > max_photos:
        for f in files[max_photos:]:  # älteste, die über dem Limit liegen
            try:
                f.unlink()
                deleted += 1
            except Exception:
                pass

    if deleted:
        log.info("Galerie-Aufräumen: %d Foto(s) automatisch gelöscht (Limits: %s Tage / %s Fotos).",
                  deleted, max_age_days or "kein", max_photos or "kein")
    return deleted


def get_gallery_settings():
    return {
        "max_photo_age_days": CONFIG["storage"].get("max_photo_age_days", 0) or 0,
        "max_photos": CONFIG["storage"].get("max_photos", 0) or 0,
        "current_count": count_gallery_photos(),
    }


def set_gallery_settings(max_photo_age_days=None, max_photos=None):
    with CONFIG_LOCK:
        if max_photo_age_days is not None:
            CONFIG["storage"]["max_photo_age_days"] = max(0, int(max_photo_age_days))
        if max_photos is not None:
            CONFIG["storage"]["max_photos"] = max(0, int(max_photos))
        save_config(CONFIG)
    return get_gallery_settings()


# ============================================================
# ZUGRIFFSFUNKTIONEN FÜR DIE NATIVE WEB-OBERFLÄCHE
# ============================================================
# Alles hier läuft rein in-process (keine eigene Web-Oberfläche, kein eigener
# Port mehr) - wird von neuen Routen in app.py aufgerufen (Bereich
# "Heimüberwachung"), die Oberfläche dazu ist Teil von index.html im
# Dashboard-Stil.

def list_cameras_full():
    """Kamera-Liste inkl. RTSP-URL fürs Bearbeiten (die normale
    get_dashboard_snapshot()-Liste zeigt die URL bewusst nicht, für die
    Live-Ansicht reicht Name/Status/Personenzahl)."""
    if _camera_manager is None:
        return []
    return [
        {"name": c.name, "rtsp_url": c.rtsp_url, "connected": c.is_connected(), "zones": len(c.zones)}
        for c in _camera_manager.list()
    ]


def get_camera_zones(name):
    """Liefert die Zonen einer Kamera (normierte Punkte 0.0-1.0) für den
    Zonen-Editor im Dashboard."""
    if _camera_manager is None:
        return None
    cam = _camera_manager.get(name)
    if cam is None:
        return None
    return cam.zones


def set_camera_zones(name, zones):
    """Speichert eine neue Zonen-Liste für eine Kamera. Erwartet eine Liste
    von {"name": str, "points": [[x,y], ...]} mit normierten Koordinaten
    (0.0-1.0, wie vom Zonen-Editor im Browser geliefert). Mindestens 3 Punkte
    pro Zone, sonst wird die betroffene Zone übersprungen. Ohne verbleibende
    gültige Zone würde die Kamera nie mehr Ereignisse melden - deshalb wird
    in dem Fall automatisch wieder die Standard-Zone "Ganzes Bild" gesetzt."""
    if _camera_manager is None:
        return False, "Personenerkennung läuft nicht."
    clean = []
    seen_names = set()
    for z in (zones or []):
        zname = str(z.get("name") or "").strip()[:40]
        points = z.get("points") or []
        if not zname or len(points) < 3:
            continue
        try:
            pts = [[max(0.0, min(1.0, float(p[0]))), max(0.0, min(1.0, float(p[1])))] for p in points]
        except (TypeError, ValueError, IndexError):
            continue
        base_name, i = zname, 2
        while zname in seen_names:
            zname = f"{base_name} ({i})"
            i += 1
        seen_names.add(zname)
        clean.append({"name": zname, "points": pts})
    if not clean:
        clean = [dict(_WHOLE_FRAME_ZONE)]
    return _camera_manager.update_zones(name, clean)


def add_camera(name, rtsp_url):
    """Fügt eine Kamera hinzu und legt automatisch eine Zone über das ganze
    Bild an, damit Erkennungen sofort im Ereignis-Feed/bei Benachrichtigungen
    ankommen, ohne dass ein eigener Zonen-Editor bedient werden muss."""
    if _camera_manager is None:
        return False, "Personenerkennung läuft nicht."
    ok, msg = _camera_manager.add(name.strip(), rtsp_url.strip())
    if ok:
        _camera_manager.update_zones(name.strip(), [dict(_WHOLE_FRAME_ZONE)])
    return ok, msg


def update_camera(name, rtsp_url):
    if _camera_manager is None:
        return False, "Personenerkennung läuft nicht."
    ok, msg = _camera_manager.update_rtsp(name, rtsp_url.strip())
    if ok and not _camera_manager.get(name).zones:
        _camera_manager.update_zones(name, [dict(_WHOLE_FRAME_ZONE)])
    return ok, msg


def remove_camera(name):
    if _camera_manager is None:
        return False, "Personenerkennung läuft nicht."
    return _camera_manager.remove(name)


def list_events(limit=50):
    """Letzte Ereignisse (neueste zuerst) für den Ereignis-Feed."""
    events = EVENT_FEED.all()
    return events[:limit]


def get_available_classes():
    """Alle wählbaren Erkennungsklassen (id -> deutscher Name)."""
    return [{"id": cid, "name": name} for cid, name in sorted(COCO_CLASSES_DE.items(), key=lambda kv: kv[1])]


def get_selected_classes():
    return list(CONFIG["detection"].get("classes") or [])


def set_selected_classes(class_ids):
    with CONFIG_LOCK:
        CONFIG["detection"]["classes"] = [int(c) for c in class_ids]
        save_config(CONFIG)
    if _engine is not None:
        _engine.det_cfg = CONFIG["detection"]


def get_recording_enabled():
    return bool(CONFIG["recording"].get("enabled", True))


def set_recording_enabled(enabled):
    with CONFIG_LOCK:
        CONFIG["recording"]["enabled"] = bool(enabled)
        save_config(CONFIG)


def get_notification_status():
    return {"snoozed": is_notifications_snoozed(), "snoozed_until": get_snooze_until()}


def snooze_notifications(minutes):
    set_snooze(float(minutes))
    return get_notification_status()


def get_notification_settings():
    """Aktuelle Benachrichtigungs-Einstellungen - Passwörter/Tokens werden nie
    zurückgegeben (nur ob eines gesetzt ist), damit sie nicht im Browser
    landen."""
    cfg = CONFIG["notifications"]
    email_cfg = cfg.get("email", {})
    ntfy_cfg = cfg.get("ntfy", {})
    tg_cfg = cfg.get("telegram", {})
    return {
        "method": cfg.get("method", "console"),
        "ntfy": {"server": ntfy_cfg.get("server", ""), "topic": ntfy_cfg.get("topic", "")},
        "telegram": {"bot_token_set": bool(tg_cfg.get("bot_token")), "chat_id": tg_cfg.get("chat_id", "")},
        "email": {
            "smtp_server": email_cfg.get("smtp_server", ""),
            "smtp_port": email_cfg.get("smtp_port", 587),
            "username": email_cfg.get("username", ""),
            "to_address": email_cfg.get("to_address", ""),
            "password_set": bool(email_cfg.get("password")),
        },
    }


def set_notification_settings(data):
    """Aktualisiert die Benachrichtigungs-Einstellungen und baut den aktiven
    Notifier neu auf, damit die Änderung sofort greift (kein Neustart nötig).
    Passwort/Token werden nur überschrieben, wenn tatsächlich ein neuer Wert
    mitgeschickt wurde (leeres Feld = vorhandenes behalten)."""
    global _notifier
    with CONFIG_LOCK:
        ncfg = CONFIG["notifications"]
        if data.get("method") in ("console", "ntfy", "telegram", "email"):
            ncfg["method"] = data["method"]

        ntfy_in = data.get("ntfy") or {}
        if "server" in ntfy_in:
            ncfg["ntfy"]["server"] = ntfy_in["server"]
        if "topic" in ntfy_in:
            ncfg["ntfy"]["topic"] = ntfy_in["topic"]

        tg_in = data.get("telegram") or {}
        if "chat_id" in tg_in:
            ncfg["telegram"]["chat_id"] = tg_in["chat_id"]
        if tg_in.get("bot_token"):
            ncfg["telegram"]["bot_token"] = tg_in["bot_token"]

        email_in = data.get("email") or {}
        for key in ("smtp_server", "username", "to_address"):
            if key in email_in:
                ncfg["email"][key] = email_in[key]
        if email_in.get("smtp_port"):
            try:
                ncfg["email"]["smtp_port"] = int(email_in["smtp_port"])
            except (TypeError, ValueError):
                pass
        if email_in.get("password"):
            ncfg["email"]["password"] = email_in["password"]

        save_config(CONFIG)

    if _engine is not None:
        try:
            _notifier = build_notifier(CONFIG["notifications"])
            _engine.notifier = _notifier
        except Exception as e:
            log.warning("Notifier konnte nach Einstellungsänderung nicht neu aufgebaut werden: %s", e)

    return get_notification_settings()


def send_test_notification():
    if _notifier is None:
        return False, "Keine Benachrichtigungsmethode konfiguriert (siehe instance/heimueberwachung/config.json -> notifications)."
    try:
        _notifier.send("Test-Benachrichtigung", "Dies ist eine Testbenachrichtigung von SmartHome Dashboard / Heimüberwachung.")
        return True, "Testbenachrichtigung gesendet."
    except Exception as e:
        return False, f"Fehler beim Senden: {e}"


'''

def _make_heimueberwachung_engine():
    import types
    ns = {"__name__": "smarthome.heimueberwachung_engine", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    mod = types.ModuleType("smarthome.heimueberwachung_engine")
    mod.__dict__.update(ns)
    exec(compile(HEIMUEBERWACHUNG_ENGINE_SOURCE, "<heimueberwachung_engine>", "exec"), mod.__dict__)
    return mod

_heimueberwachung_engine_module = _make_heimueberwachung_engine()

SURVEILLANCE_SOURCE = r'''
"""
surveillance.py
Dünner Zugriffs-Layer für die Personenerkennung/Kameras.

WICHTIG: Die Heimüberwachung läuft NICHT mehr als eigener Server auf einem
eigenen Port. Sie ist komplett in denselben Prozess/Port wie dieses
Dashboard eingebettet (siehe backend/heimueberwachung_engine.py::start_embedded(),
aufgerufen von app.py beim Start) - nur EIN Server, keine zweite IP-Adresse,
kein API-Key, keine Netzwerk-Konfiguration nötig.

Dieses Modul reicht Aufrufe einfach an die eingebettete Engine weiter
(direkte Funktionsaufrufe im selben Prozess, kein HTTP-Roundtrip) und fängt
sauber ab, falls die Engine (noch) nicht läuft - z.B. weil die optionalen
Abhängigkeiten (ultralytics, opencv) nicht installiert sind, oder weil die
Personenerkennung in den Einstellungen deaktiviert ist.
"""
import logging

log = logging.getLogger("smarthome.surveillance")

try:
    _engine = _heimueberwachung_engine_module  # (im selben Prozess bereits geladen, siehe unten)
except Exception as e:  # z.B. fehlende Pakete (cv2/ultralytics) - Dashboard soll trotzdem starten
    _engine = None
    log.warning("Heimüberwachungs-Modul konnte nicht geladen werden (%s). "
                "Personenerkennung ist deaktiviert, bis die Abhängigkeiten "
                "(pip install ultralytics opencv-python-headless) installiert sind.", e)


def is_embedded_available():
    """True, sobald die Personenerkennung erfolgreich im selben Prozess läuft."""
    return _engine is not None and _engine.is_available()


def get_dashboard_data():
    """
    Liefert dieselben Daten wie früher der externe /api/dashboard-Endpunkt
    (Personen live, Kameras, Personenzähler, Ereignisse) - jetzt aber per
    direktem Funktionsaufruf statt HTTP-Request an einen zweiten Server.
    Returns dict oder None, falls die Personenerkennung nicht läuft.
    """
    if not is_embedded_available():
        return None
    try:
        return _engine.get_dashboard_snapshot()
    except Exception as e:
        log.warning("Fehler beim Abfragen der Heimüberwachung: %s", e)
        return None


def get_still_bytes(name):
    """Einzelbild einer Kamera (per Name) - direkter Funktionsaufruf."""
    if not is_embedded_available():
        return None
    try:
        return _engine.get_still_bytes(name)
    except Exception as e:
        log.debug("Fehler beim Abrufen des Kamerabilds '%s': %s", name, e)
        return None


def stream_mjpeg(name, fps=8, width=800, quality=80):
    """Generator für den echten Live-MJPEG-Stream einer Kamera (dieselbe
    Technik wie /stream/<name> in der eigenständigen Heimüberwachung) -
    direkter Funktionsaufruf, kein zweiter Server/Port."""
    if not is_embedded_available():
        return
    try:
        yield from _engine.iter_camera_mjpeg(name, fps=fps, width=width, quality=quality)
    except Exception as e:
        log.debug("Fehler beim Streamen von Kamera '%s': %s", name, e)


def list_gallery(limit=200, camera=None, date_from=None, date_to=None):
    """Gespeicherte Erkennungs-Fotos (neueste zuerst) - nur Bilder, da die
    Video-Aufnahme bewusst deaktiviert ist. Optional nach Kamera und/oder
    Zeitraum (YYYY-MM-DD) gefiltert."""
    if not is_embedded_available():
        return []
    try:
        return _engine.list_gallery(limit=limit, camera=camera, date_from=date_from, date_to=date_to)
    except Exception as e:
        log.warning("Fehler beim Laden der Galerie: %s", e)
        return []


def get_snapshot_bytes(filename):
    if not is_embedded_available():
        return None
    try:
        path = _engine.get_snapshot_path(filename)
        if path is None:
            return None
        with open(path, "rb") as f:
            return f.read()
    except Exception as e:
        log.debug("Fehler beim Lesen des Fotos '%s': %s", filename, e)
        return None


def delete_snapshot(filename):
    if not is_embedded_available():
        return False, "Personenerkennung läuft nicht."
    try:
        return _engine.delete_snapshot(filename)
    except Exception as e:
        return False, str(e)


def delete_snapshots(filenames):
    if not is_embedded_available():
        return 0, len(filenames)
    try:
        return _engine.delete_snapshots(filenames)
    except Exception as e:
        log.warning("Fehler beim Löschen mehrerer Fotos: %s", e)
        return 0, len(filenames)


def clear_gallery():
    if not is_embedded_available():
        return 0
    try:
        return _engine.clear_gallery()
    except Exception as e:
        log.warning("Fehler beim Leeren der Galerie: %s", e)
        return 0


def get_gallery_settings():
    if not is_embedded_available():
        return {"max_photo_age_days": 0, "max_photos": 0, "current_count": 0}
    try:
        return _engine.get_gallery_settings()
    except Exception as e:
        log.warning("Fehler beim Laden der Galerie-Einstellungen: %s", e)
        return {"max_photo_age_days": 0, "max_photos": 0, "current_count": 0}


def set_gallery_settings(max_photo_age_days=None, max_photos=None):
    if not is_embedded_available():
        return None
    try:
        return _engine.set_gallery_settings(max_photo_age_days=max_photo_age_days, max_photos=max_photos)
    except Exception as e:
        log.warning("Fehler beim Speichern der Galerie-Einstellungen: %s", e)
        return None


def cleanup_gallery():
    if not is_embedded_available():
        return 0
    try:
        return _engine.cleanup_gallery()
    except Exception as e:
        log.warning("Fehler beim automatischen Galerie-Aufräumen: %s", e)
        return 0


# ---- Kamera-Verwaltung, Ereignisse, Einstellungen (alles nativ, kein Port) ----

def list_cameras_full():
    if not is_embedded_available():
        return []
    return _engine.list_cameras_full()


def add_camera(name, rtsp_url):
    if not is_embedded_available():
        return False, "Personenerkennung ist nicht aktiv (Einstellungen → Überwachung)."
    return _engine.add_camera(name, rtsp_url)


def update_camera(name, rtsp_url):
    if not is_embedded_available():
        return False, "Personenerkennung ist nicht aktiv."
    return _engine.update_camera(name, rtsp_url)


def remove_camera(name):
    if not is_embedded_available():
        return False, "Personenerkennung ist nicht aktiv."
    return _engine.remove_camera(name)


def get_camera_zones(name):
    if not is_embedded_available():
        return None
    return _engine.get_camera_zones(name)


def set_camera_zones(name, zones):
    if not is_embedded_available():
        return False, "Personenerkennung ist nicht aktiv (Einstellungen → Überwachung)."
    return _engine.set_camera_zones(name, zones)


def get_today_person_count():
    if _engine is None:
        return None
    try:
        return _engine.get_today_person_count()
    except Exception as e:
        log.debug("Fehler beim Ermitteln der heutigen Personenzahl: %s", e)
        return None


def list_events(limit=50):
    if not is_embedded_available():
        return []
    return _engine.list_events(limit)


def get_available_classes():
    if _engine is None:
        return []
    return _engine.get_available_classes()


def get_selected_classes():
    if _engine is None:
        return []
    return _engine.get_selected_classes()


def set_selected_classes(class_ids):
    if not is_embedded_available():
        return
    _engine.set_selected_classes(class_ids)


def get_recording_enabled():
    if _engine is None:
        return True
    return _engine.get_recording_enabled()


def set_recording_enabled(enabled):
    if not is_embedded_available():
        return
    _engine.set_recording_enabled(enabled)


def get_notification_status():
    if _engine is None:
        return {"snoozed": False, "snoozed_until": 0}
    return _engine.get_notification_status()


def snooze_notifications(minutes):
    if not is_embedded_available():
        return {"snoozed": False, "snoozed_until": 0}
    return _engine.snooze_notifications(minutes)


def get_notification_settings():
    """Konfiguration ist unabhängig davon lesbar, ob die Personenerkennung
    gerade aktiv Kameras verarbeitet - nur die Python-Abhängigkeiten müssen
    geladen sein (CONFIG existiert immer, sobald das Modul importiert ist)."""
    if _engine is None:
        return {"method": "console", "ntfy": {"server": "", "topic": ""},
                "telegram": {"bot_token_set": False, "chat_id": ""},
                "email": {"smtp_server": "", "smtp_port": 587, "username": "", "to_address": "", "password_set": False}}
    return _engine.get_notification_settings()


def set_notification_settings(data):
    if _engine is None:
        return None
    return _engine.set_notification_settings(data)


def send_test_notification():
    if not is_embedded_available():
        return False, "Personenerkennung ist nicht aktiv."
    return _engine.send_test_notification()

'''

def _make_surveillance():
    import types
    ns = {"__name__": "smarthome.surveillance", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    ns.update({"_heimueberwachung_engine_module": _heimueberwachung_engine_module})
    mod = types.ModuleType("smarthome.surveillance")
    mod.__dict__.update(ns)
    exec(compile(SURVEILLANCE_SOURCE, "<surveillance>", "exec"), mod.__dict__)
    return mod

_surveillance_module = _make_surveillance()

SCHEDULER_SOURCE = r'''
"""
scheduler.py
Hintergrund-Worker-Threads, die periodisch:
  - Fronius Solar-Daten pollen (alle 10s)
  - Miner-Stats pollen + Automatisierung anwenden (alle 15s)
  - Strompreise pollen (alle 15 Minuten)
  - Bitcoin-Preis pollen (alle 60s)
  - Wetter pollen (alle 10 Minuten)
  - Heimüberwachung (Kameras/Personenerkennung) pollen (alle 10s)
  - Benachrichtigungsregeln pruefen (bei jedem Solar-Update)

Alle Ergebnisse werden in die DB geschrieben (fuer History/Charts) UND per
SocketIO live an alle verbundenen Browser gesendet.
"""
import logging
import threading
from datetime import datetime, timedelta


log = logging.getLogger("smarthome.scheduler")

_socketio = None
_stop_event = threading.Event()
_last_price_cache = {}

# ── Zentraler In-Memory-Cache ─────────────────────────────────────────────
# Der Scheduler ist die einzige Quelle, die tatsächlich extern pollt (Fronius,
# aWATTar, CoinGecko, Open-Meteo, Miner-TCP-Calls, Kamera-Checks). Damit der
# /api/dashboard-data-Endpoint beim Laden der Seite nicht erneut all diese
# (langsamen, teils externen) Calls synchron ausführen muss, hält der
# Scheduler hier immer den letzten bekannten Stand vor - der Endpoint liest
# nur noch aus diesem Speicher, das macht den initialen Page-Load deutlich
# schneller und unabhängig von der Erreichbarkeit externer APIs.
LATEST = {
    "solar": None,
    "energy_prices": None,
    "weather": None,
    "btc_price": None,
    "miners": [],
    "surveillance": None,
}
_latest_lock = threading.Lock()

# Vom Hauptprozess (siehe app.py-Abschnitt) gesetzter Callback zum Versenden
# von Browser-Push-Benachrichtigungen (Web Push). None = Browser-Push nicht
# verfügbar (z.B. pywebpush nicht installiert) - Loop läuft dann einfach ohne
# Push weiter, alle anderen Benachrichtigungswege bleiben unberührt.
push_callback = None
_last_pushed_event_key = None


def get_latest(key, default=None):
    with _latest_lock:
        return LATEST.get(key, default)


def _set_latest(key, value):
    with _latest_lock:
        LATEST[key] = value


def init_scheduler(socketio_instance):
    global _socketio
    _socketio = socketio_instance


def _emit(event, data):
    if _socketio:
        try:
            _socketio.emit(event, data)
        except Exception as e:
            log.debug("SocketIO emit Fehler (%s): %s", event, e)


def _notify(title, message, type_="info"):
    db.add_notification(title, message, type_)
    _emit("notification", {"title": title, "message": message, "type": type_,
                            "timestamp": datetime.now().isoformat(), "read": False})


# ── Solar Loop ────────────────────────────────────────────────────
def _solar_loop():
    while not _stop_event.is_set():
        try:
            ip = db.get_setting("fronius_ip", "192.168.178.100")
            data = fronius.get_full_solar_data(ip)
            if data:
                db.add_energy_history(
                    data["pv_power"], data["house_load"], data["grid_import"], data["battery_soc"]
                )
                _check_energy_notifications(data)
                _set_latest("solar", data)
                _emit("solar_update", data)
            else:
                offline_data = {
                    "pv_power": 0, "house_load": 0, "grid_import": 0,
                    "battery_soc": 0, "autonomy": 0, "self_consumption": 0,
                    "pv_day": 0, "offline": True,
                }
                _set_latest("solar", offline_data)
                _emit("solar_update", offline_data)
        except Exception as e:
            log.exception("Fehler im Solar-Loop: %s", e)
        _stop_event.wait(10)


def _check_energy_notifications(data):
    bat_low = float(db.get_setting("notification_battery_low", 20))
    bat_full = float(db.get_setting("notification_battery_full", 85))
    high_import = float(db.get_setting("notification_high_import", 2000))

    soc = data.get("battery_soc", 0)
    grid = data.get("grid_import", 0)

    if soc <= bat_low and not db.has_recent_notification("Batterie niedrig", 60):
        _notify("Batterie niedrig", f"Batterieladestand bei {soc:.0f}%", "warning")
    if soc >= bat_full and not db.has_recent_notification("Batterie voll", 60):
        _notify("Batterie voll", f"Batterie ist zu {soc:.0f}% geladen", "info")
    if grid >= high_import and not db.has_recent_notification("Hoher Netzbezug", 30):
        _notify("Hoher Netzbezug", f"Aktueller Netzbezug: {grid:.0f} W", "warning")


# ── Tagesbericht ──────────────────────────────────────────────────
# Fasst einmal täglich, zu einer einstellbaren Uhrzeit, die wichtigsten
# Kennzahlen des Tages als EINE Benachrichtigung zusammen (Personen erkannt,
# PV-Erzeugung, Eigenverbrauch/Einspeisung, Ersparnis, Wetter). Läuft im
# selben Benachrichtigungssystem mit wie alle anderen Meldungen - taucht also
# ganz normal im Glocken-Menü auf und kann optional per E-Mail/ntfy/Telegram/
# Browser-Push zugestellt werden (siehe Einstellungen -> Benachrichtigungen).
#
# Anti-Spam: es wird ausschließlich EIN Bericht pro Kalendertag verschickt -
# das gewählte Datum wird nach dem Versand in "daily_report_last_date"
# gemerkt, ein erneuter Lauf am selben Tag (z.B. nach einem Neustart oder
# wenn die Uhrzeit mehrfach im Prüfintervall liegt) wird übersprungen.
def _build_daily_report_message():
    parts = []

    try:
        today = db.get_today_partial_summary()
        pv_kwh = today.get("pv_kwh", 0) or 0
        self_kwh = today.get("self_consumed_kwh", 0) or 0
        export_kwh = today.get("exported_kwh", 0) or 0
        parts.append(f"☼ PV-Erzeugung: {pv_kwh:.1f} kWh")
        parts.append(f"Eigenverbrauch {self_kwh:.1f} kWh · Einspeisung {export_kwh:.1f} kWh")
    except Exception as e:
        log.warning("Tagesbericht: Energie-Zusammenfassung fehlgeschlagen: %s", e)

    try:
        prices = get_latest("energy_prices")
        grid_price = prices["current_price"] if prices and prices.get("current_price") else 0.25
        savings = savings_mod.compute_savings(grid_price=grid_price)
        parts.append(f"Ersparnis heute: {savings['daily_savings']:.2f} €")
    except Exception as e:
        log.warning("Tagesbericht: Ersparnis-Berechnung fehlgeschlagen: %s", e)

    try:
        persons_today = surveillance.get_today_person_count()
        if persons_today is not None:
            parts.append(f"Personen erkannt: {persons_today}")
    except Exception as e:
        log.warning("Tagesbericht: Personenzähler fehlgeschlagen: %s", e)

    try:
        weather = get_latest("weather")
        if weather:
            parts.append(f"Wetter: {weather.get('conditions', '–')}, {round(weather.get('temperature', 0))}°")
    except Exception as e:
        log.warning("Tagesbericht: Wetterdaten fehlgeschlagen: %s", e)

    return " · ".join(parts) if parts else "Keine Daten für den heutigen Tag verfügbar."


# ── Morgennachricht ───────────────────────────────────────────────
# Vorschau auf den kommenden Tag (Wetter morgens/mittags/abends + geschätzte
# PV-Erzeugung), wählbar als Übersicht-Widget (siehe WIDGET_DEFS im Frontend).
# Die Nachricht "kippt" auf den nächsten Kalendertag, sobald die eingestellte
# Uhrzeit erreicht ist (morning_message_time, Standard 18:00) - ab dann sieht
# man schon abends die Vorschau auf morgen, und diese bleibt bis zur selben
# Uhrzeit am Folgetag bestehen (beschreibt dann den mittlerweile aktuellen Tag).
def _morning_message_efficiency(optimism_pct):
    """Wandelt den Optimismusgrad (0-100, Einstellungen) in einen
    Wirkungsgrad-Faktor für external_apis.compute_solar_forecast() um:
    0 = vorsichtig geschätzt (50%), 100 = optimistisch / theoretisches
    Maximum (100%). Der Standard-Systemwirkungsgrad aus external_apis
    (SOLAR_SYSTEM_EFFICIENCY, 78%) entspricht ungefähr Optimismusgrad 56."""
    try:
        pct = max(0.0, min(100.0, float(optimism_pct)))
    except (TypeError, ValueError):
        pct = 60.0
    return round(0.50 + (pct / 100.0) * 0.50, 3)


def _morning_message_target_date():
    now = datetime.now()
    cutoff = str(db.get_setting("morning_message_time", "18:00") or "18:00")
    if now.strftime("%H:%M") >= cutoff:
        return now.date() + timedelta(days=1)
    return now.date()


def _build_morning_message():
    """Baut die Morgennachricht-Vorschau für den jeweils relevanten Tag
    (siehe _morning_message_target_date): Wetter morgens/mittags/abends als
    strukturierte Liste (für die Mini-Kurve im Widget) + geschätzte
    PV-Erzeugung. Gibt IMMER ein dict zurück (kein Exception-Durchreichen),
    damit das Frontend-Widget nie leer bleibt. "text" ist eine reine
    Fließtext-Zusammenfassung als Fallback (z.B. für Nicht-JS-Kontexte)."""
    if str(db.get_setting("morning_message_enabled", "1")) != "1":
        return {"date": None, "label": "", "available": False,
                "text": "Morgennachricht ist deaktiviert (Einstellungen → Solar).",
                "slots": [], "temp_min": None, "temp_max": None,
                "rain_probability": None, "pv_kwh": None,
                "installed_kwp": None, "optimism": None}

    target = _morning_message_target_date()
    target_str = target.strftime("%Y-%m-%d")
    weekday = external_apis.WEEKDAY_LABELS_DE[target.weekday()]
    label = f"{weekday}, {target.day}.{target.month}."

    empty = {"slots": [], "temp_min": None, "temp_max": None,
             "rain_probability": None, "pv_kwh": None,
             "installed_kwp": None, "optimism": None}

    weather = get_latest("weather")
    if not weather or not weather.get("hourly"):
        return {"date": target_str, "label": label, "available": False,
                "text": "Noch keine Wettervorhersage verfügbar.", **empty}

    slots = []
    temp_min = temp_max = rain_probability = None
    try:
        hourly_target = [h for h in weather["hourly"] if h["time"][:10] == target_str]

        def _closest_hour(hour):
            if not hourly_target:
                return None
            return min(hourly_target, key=lambda h: abs(int(h["hour"][:2]) - hour))

        for slot_name, hour in (("Morgens", 8), ("Mittags", 13), ("Abends", 19)):
            h = _closest_hour(hour)
            if h and h.get("temperature") is not None:
                slots.append({
                    "name": slot_name,
                    "temp": h["temperature"],
                    "conditions": h.get("conditions", ""),
                    "category": h.get("category", "cloudy"),
                })

        daily = next((d for d in weather.get("daily", []) if d.get("day") == target_str), None)
        if daily and daily.get("temp_min") is not None and daily.get("temp_max") is not None:
            temp_min = daily["temp_min"]
            temp_max = daily["temp_max"]
            rain_probability = daily.get("precipitation_probability", 0)
    except Exception as e:
        log.warning("Morgennachricht: Wettervorhersage fehlgeschlagen: %s", e)

    pv_kwh = installed_kwp = optimism = None
    try:
        installed_kwp = float(db.get_setting("pv_installed_kwp", 0) or 0)
        if installed_kwp > 0:
            optimism_raw = db.get_setting("morning_message_optimism", "60")
            optimism = int(float(optimism_raw or 60))
            efficiency = _morning_message_efficiency(optimism_raw)
            forecast = external_apis.compute_solar_forecast(weather["hourly"], installed_kwp, efficiency=efficiency)
            key = "today_kwh" if target_str == datetime.now().strftime("%Y-%m-%d") else "tomorrow_kwh"
            pv_kwh = round(forecast.get(key, 0), 1)
        else:
            installed_kwp = None
    except Exception as e:
        log.warning("Morgennachricht: PV-Prognose fehlgeschlagen: %s", e)
        installed_kwp = None

    # Fließtext-Fallback (z.B. falls die Nachricht mal per Push verschickt werden soll)
    text_parts = []
    if slots:
        text_parts.append(" · ".join(f"{s['name']} {s['temp']}° {s['conditions']}".strip() for s in slots))
    if temp_min is not None and temp_max is not None:
        text_parts.append(f"{temp_min}° bis {temp_max}° · Regenrisiko {rain_probability}%")
    if pv_kwh is not None:
        text_parts.append(f"☼ geschätzte PV-Erzeugung: {pv_kwh:.1f} kWh (bei {installed_kwp:g} kWp, Optimismus {optimism}%)")

    return {
        "date": target_str,
        "label": label,
        "available": True,
        "text": " · ".join(text_parts) if text_parts else "Keine Vorhersage-Daten verfügbar.",
        "slots": slots,
        "temp_min": temp_min,
        "temp_max": temp_max,
        "rain_probability": rain_probability,
        "pv_kwh": pv_kwh,
        "installed_kwp": installed_kwp,
        "optimism": optimism,
    }


def _daily_report_loop():
    while not _stop_event.is_set():
        try:
            if str(db.get_setting("daily_report_enabled", "0")) == "1":
                target_time = str(db.get_setting("daily_report_time", "20:00") or "20:00")
                now = datetime.now()
                today_str = now.strftime("%Y-%m-%d")
                last_sent = str(db.get_setting("daily_report_last_date", ""))
                if now.strftime("%H:%M") >= target_time and last_sent != today_str:
                    message = _build_daily_report_message()
                    _notify("Tagesbericht", message, "info")
                    db.set_settings({"daily_report_last_date": today_str})
        except Exception as e:
            log.exception("Fehler im Tagesbericht-Loop: %s", e)
        # 30s-Prüfintervall reicht locker - der Bericht wird ohnehin nur
        # einmal pro Tag verschickt (siehe last_sent-Vergleich oben).
        _stop_event.wait(30)


# ── Miner Loop ────────────────────────────────────────────────────
def _miner_loop():
    while not _stop_event.is_set():
        try:
            miner_rows = db.get_miners()
            updated = []
            total_hr = 0
            total_pw = 0
            active = 0

            for m in miner_rows:
                stats = miners_mod.get_miner_stats(m)
                status = stats["status"]
                if m.get("last_status") != status:
                    db.update_miner(m["id"], {
                        "last_status": status,
                        "last_state_change": datetime.now().isoformat(),
                    })
                    _log_miner_status_change(m, status)
                    m["last_status"] = status
                    m["last_state_change"] = datetime.now().isoformat()

                merged = {**m, "hashrate": stats["hashrate"], "temperature": stats["temperature"], "status": status}
                updated.append(merged)
                db.add_miner_stats_history(m["id"], stats["hashrate"], stats["temperature"], m.get("power_watts", 0), status)

                if status == "running":
                    active += 1
                    total_hr += stats["hashrate"]
                    total_pw += m.get("power_watts", 0)

            settings = db.get_all_settings()
            solar_rows = db.get_energy_history(hours=0.02)
            latest_solar = solar_rows[-1] if solar_rows else None
            current_price = _last_price_cache.get("current_price")

            solar_for_automation = None
            if latest_solar:
                solar_for_automation = {
                    "battery_soc": latest_solar.get("battery_soc", 0),
                    "grid_import": latest_solar.get("grid_import", 0),
                    "pv_power": latest_solar.get("pv_power", 0),
                }

            actions = automation.decide_miner_actions(updated, solar_for_automation, current_price, settings)
            for miner_id, action in actions.items():
                miner = next((x for x in updated if x["id"] == miner_id), None)
                if not miner:
                    continue
                ok = miners_mod.set_miner_power(miner, turn_on=(action == "resume"))
                if ok:
                    new_status = "running" if action == "resume" else "paused"
                    db.update_miner(miner_id, {"last_status": new_status, "last_state_change": datetime.now().isoformat()})
                    for u in updated:
                        if u["id"] == miner_id:
                            u["status"] = new_status
                    event_type = "auto_started" if action == "resume" else "auto_stopped"
                    event_msg = f"Automatisch {'gestartet' if action == 'resume' else 'gestoppt'} (PV-Automatik)"
                    db.add_miner_event(miner_id, event_type, event_msg)
                    _notify(
                        f"Miner {'gestartet' if action == 'resume' else 'gestoppt'}",
                        f"{miner['name']} wurde automatisch {'gestartet' if action == 'resume' else 'gestoppt'}",
                        "info",
                    )

            surplus = max(0, -(latest_solar.get("grid_import", 0))) if latest_solar else 0
            db.add_miner_history(total_hr, total_pw, active, surplus)
            _set_latest("miners", updated)
            _emit("miners_update", {"miners": updated})
        except Exception as e:
            log.exception("Fehler im Miner-Loop: %s", e)
        _stop_event.wait(15)


def _log_miner_status_change(miner, new_status):
    """Loggt einen vom Polling erkannten Statuswechsel (z.B. Miner ging offline/online,
    oder jemand hat ihn direkt am Gerät pausiert) als Event - aber nur für
    'echte' Übergänge, nicht beim allerersten Polling nach Neustart."""
    prev_status = miner.get("last_status")
    if prev_status is None:
        return
    if prev_status == new_status:
        return
    label_map = {"running": "läuft", "paused": "pausiert", "offline": "offline"}
    db.add_miner_event(
        miner["id"], "status_changed",
        f"Status geändert: {label_map.get(prev_status, prev_status)} → {label_map.get(new_status, new_status)}",
    )




# ── Price Loop ────────────────────────────────────────────────────
def _price_loop():
    while not _stop_event.is_set():
        try:
            data = external_apis.get_electricity_prices()
            if data:
                _last_price_cache.update(data)
                _set_latest("energy_prices", data)
                _emit("price_update", data)
        except Exception as e:
            log.exception("Fehler im Price-Loop: %s", e)
        _stop_event.wait(900)


# ── BTC Loop ──────────────────────────────────────────────────────
def _btc_loop():
    while not _stop_event.is_set():
        try:
            data = external_apis.get_bitcoin_price()
            if data:
                db.add_btc_history(data["price_eur"])
                _set_latest("btc_price", data)
                _emit("btc_update", data)
        except Exception as e:
            log.exception("Fehler im BTC-Loop: %s", e)
        _stop_event.wait(60)


# ── Weather Loop ──────────────────────────────────────────────────
def _weather_loop():
    while not _stop_event.is_set():
        try:
            data = external_apis.get_weather()
            if data:
                _set_latest("weather", data)
                _emit("weather_update", data)
        except Exception as e:
            log.exception("Fehler im Weather-Loop: %s", e)
        _stop_event.wait(600)


# ── Heimüberwachung / Personenerkennung Loop ───────────────────────
def _surveillance_loop():
    """Pollt periodisch die eingebettete Heimüberwachung (siehe
    backend/heimueberwachung_engine.py, gestartet in app.py) - läuft im
    selben Prozess, daher direkter Funktionsaufruf statt HTTP-Request."""
    global _last_pushed_event_key
    while not _stop_event.is_set():
        try:
            data = surveillance.get_dashboard_data()
            if data is None and str(db.get_setting("surveillance_enabled", "0")) == "1":
                # Aktiviert, aber (noch) nicht verfügbar - z.B. Abhängigkeiten fehlen
                data = {"reachable": False}
            _set_latest("surveillance", data)
            if data is not None:
                _emit("surveillance_update", data)

            # Browser-Push bei neuer Erkennung (zusätzlich zu E-Mail/ntfy/
            # Telegram, die bereits direkt in der Engine ausgelöst werden).
            if push_callback is not None:
                events = surveillance.list_events(limit=1)
                if events:
                    newest = events[0]
                    key = f"{newest.get('time')}|{newest.get('camera')}|{newest.get('description')}"
                    if _last_pushed_event_key is not None and key != _last_pushed_event_key:
                        title = f"{newest.get('camera', 'Kamera')}: {newest.get('description', 'Erkennung')}"
                        try:
                            push_callback(title, newest.get("zone", ""), "/#cameras")
                        except Exception as e:
                            log.warning("Browser-Push fehlgeschlagen: %s", e)
                    _last_pushed_event_key = key
        except Exception as e:
            log.exception("Fehler im Surveillance-Loop: %s", e)
        _stop_event.wait(10)


# ── Cleanup Loop ──────────────────────────────────────────────────
def _cleanup_loop():
    # Einmal sofort beim Start ausführen, damit nach einem Neustart bereits
    # vorhandene Altdaten gleich in daily_energy_summary aufgenommen werden.
    try:
        db.cleanup_old_history()
    except Exception as e:
        log.exception("Fehler im initialen Cleanup-Lauf: %s", e)
    try:
        surveillance.cleanup_gallery()
    except Exception as e:
        log.exception("Fehler beim initialen Galerie-Aufräumen: %s", e)
    while not _stop_event.is_set():
        _stop_event.wait(3600)
        try:
            db.cleanup_old_history()
        except Exception as e:
            log.exception("Fehler im Cleanup-Loop: %s", e)
        try:
            surveillance.cleanup_gallery()
        except Exception as e:
            log.exception("Fehler beim Galerie-Aufräumen: %s", e)


_threads = []


def start_background_threads(socketio_instance):
    init_scheduler(socketio_instance)
    targets = [_solar_loop, _miner_loop, _price_loop, _btc_loop, _weather_loop, _surveillance_loop, _cleanup_loop, _daily_report_loop]
    for target in targets:
        t = threading.Thread(target=target, daemon=True, name=target.__name__)
        t.start()
        _threads.append(t)
    log.info("Alle %d Hintergrund-Threads gestartet.", len(_threads))


def stop_background_threads():
    _stop_event.set()

'''

def _make_scheduler():
    import types
    ns = {"__name__": "smarthome.scheduler", "__file__": __file__, "_PROJECT_BASE_DIR": _PROJECT_BASE_DIR}
    ns.update({"db": _database_module, "fronius": _fronius_module, "miners_mod": _miners_module, "external_apis": _external_apis_module, "automation": _automation_module, "surveillance": _surveillance_module, "savings_mod": _savings_module})
    mod = types.ModuleType("smarthome.scheduler")
    mod.__dict__.update(ns)
    exec(compile(SCHEDULER_SOURCE, "<scheduler>", "exec"), mod.__dict__)
    return mod

_scheduler_module = _make_scheduler()

# ---------- Eingebettete Web-Oberfläche (HTML/CSS/JS) ----------
INDEX_HTML = r'''
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SmartHome Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.2/socket.io.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="/static/dashboard.css">
</head>
<body>

<div id="bg-layer" class="bg-mode-weather bg-cat-clear"></div>

<!-- Loading -->
<div id="loading-overlay">
  <div class="sun-loader"><div class="sun-core"></div><div class="sun-ray r1"></div><div class="sun-ray r2"></div><div class="sun-ray r3"></div><div class="sun-ray r4"></div></div>
  <div class="loading-text">SmartHome Dashboard lädt …</div>
</div>

<div class="toast-container" id="toast-container"></div>

<!-- Befehlspalette -->
<div class="command-palette-overlay" id="command-palette">
  <div class="command-palette">
    <div class="command-input-row">
      <svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M15.5 14h-.79l-.28-.27a6.5 6.5 0 1 0-.7.7l.27.28v.79l5 4.99L20.49 19zm-6 0A4.5 4.5 0 1 1 14 9.5 4.5 4.5 0 0 1 9.5 14"/></svg>
      <input type="text" id="command-input" placeholder="Wohin möchtest du? (z.B. Miner, Kamera, Einstellungen …)" autocomplete="off">
      <kbd>Esc</kbd>
    </div>
    <div class="command-list" id="command-list"></div>
  </div>
</div>

<!-- Vollbild Kamera -->
<div id="fullscreen-overlay">
  <div id="fullscreen-header">
    <span id="fullscreen-video-label">Kamera</span>
    <button id="fullscreen-close" onclick="closeFullscreen()" aria-label="Schließen">✕</button>
  </div>
  <img id="fullscreen-img" src="" alt="Kamera Vollbild">
  <div id="fullscreen-controls">
    <div class="fs-cam-select" id="fs-cam-select"></div>
    <div class="fs-meta">
      <span><span class="camera-live-dot" style="display:inline-block;vertical-align:middle;margin-right:5px"></span>Live <span id="fs-last-update">–</span></span>
      <button class="fs-cam-btn" onclick="downloadCurrentStill()">Bild speichern</button>
    </div>
  </div>
</div>

<!-- Galerie Lightbox (Erkennungs-Fotos) -->
<div id="gallery-lightbox">
  <div class="gallery-lightbox-header">
    <span id="gallery-lightbox-meta">Foto</span>
    <button class="gallery-lightbox-close" onclick="heimCloseLightbox()" aria-label="Schließen">✕</button>
  </div>
  <img id="gallery-lightbox-img" src="" alt="Erkennungs-Foto">
  <button class="gallery-lightbox-nav prev" onclick="heimLightboxStep(-1)" aria-label="Vorheriges Foto">‹</button>
  <button class="gallery-lightbox-nav next" onclick="heimLightboxStep(1)" aria-label="Nächstes Foto">›</button>
  <div class="gallery-lightbox-controls">
    <span></span>
    <button class="fs-cam-btn" onclick="heimDeleteCurrentPhoto()">Foto löschen</button>
  </div>
</div>

<div class="app-shell">

  <div class="sidebar-backdrop" id="sidebar-backdrop" onclick="closeSidebarMobile()"></div>

  <!-- ════ SIDEBAR ════ -->
  <aside class="sidebar" id="sidebar">
    <div class="sidebar-brand">
      <span class="brand-mark">⌁</span>
      <span class="brand-name">SmartHome Dashboard</span>
      <button class="sidebar-collapse-btn" id="sidebar-collapse-btn" onclick="toggleSidebar()" aria-label="Seitenleiste einklappen">
        <svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"/></svg>
      </button>
    </div>

    <nav class="sidebar-nav">
      <button class="nav-item active" data-page="overview" onclick="navigateTo('overview')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M3 13h8V3H3zm0 8h8v-6H3zm10 0h8V11h-8zm0-18v6h8V3z"/></svg>
        <span class="nav-label">Übersicht</span>
      </button>
      <button class="nav-item" data-page="energy" onclick="navigateTo('energy')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M11 21h-1l1-7H7.5c-.58 0-.57-.32-.38-.66.19-.34.05-.08.07-.12C8.48 10.94 10.42 7.54 13 3h1l-1 7h3.5c.49 0 .56.33.47.51l-.07.15C12.96 17.55 11 21 11 21z"/></svg>
        <span class="nav-label">Energie</span>
      </button>
      <button class="nav-item" data-page="miner" onclick="navigateTo('miner')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M11.8 10.9c-2.27-.59-3-1.2-3-2.15 0-1.09 1.01-1.85 2.7-1.85 1.78 0 2.44.85 2.5 2.1h2.21c-.07-1.72-1.12-3.3-3.21-3.81V3h-3v2.16c-1.94.42-3.5 1.68-3.5 3.61 0 2.31 1.91 3.46 4.7 4.13 2.5.6 3 1.48 3 2.41 0 .69-.49 1.79-2.7 1.79-2.06 0-2.87-.92-2.98-2.1h-2.2c.12 2.19 1.76 3.42 3.68 3.83V21h3v-2.15c1.95-.37 3.5-1.5 3.5-3.55 0-2.84-2.43-3.81-4.7-4.4z"/></svg>
        <span class="nav-label">Miner</span>
      </button>
      <button class="nav-item" data-page="cameras" onclick="navigateTo('cameras')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M9.4 10.5l4.77-8.26C13.47 2.09 12.75 2 12 2c-2.4 0-4.6.85-6.32 2.25l3.66 6.35.06-.1zM21.54 9c-.92-2.92-3.15-5.26-6-6.34L11.88 9h9.66zm.26 1h-7.49l.29.5 4.76 8.25C20.9 16.57 22 14.41 22 12c0-.69-.1-1.37-.2-2zM8.54 12l-3.9-6.75C3.39 7.16 2 9.4 2 12c0 .69.1 1.37.2 2h7.49l-1.15-2zm-6.08 3c.92 2.92 3.15 5.26 6 6.34L12.12 15H2.46zm11.27 0l-3.9 6.76c.7.15 1.42.24 2.17.24 2.4 0 4.6-.85 6.32-2.25l-3.66-6.35-.93 1.6z"/></svg>
        <span class="nav-label">Heimüberwachung</span>
      </button>
      <button class="nav-item" data-page="family" onclick="navigateTo('family')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"/></svg>
        <span class="nav-label">Familie</span>
      </button>
    </nav>

    <div class="sidebar-footer">
      <button class="nav-item" onclick="openModal('notif-modal')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M12 22c1.1 0 2-.9 2-2h-4c0 1.1.89 2 2 2zm6-6v-5c0-3.07-1.64-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5s-1.5.67-1.5 1.5v.68C7.63 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z"/></svg>
        <span class="nav-label">Benachrichtigungen</span>
        <span class="notif-badge" id="notif-badge" hidden>0</span>
      </button>
      <button class="nav-item" onclick="openModal('settings-modal')">
        <svg class="nav-icon" viewBox="0 0 24 24" width="19" height="19"><path fill="currentColor" d="M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.65c-.11.2-.06.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6"/></svg>
        <span class="nav-label">Einstellungen</span>
      </button>
      <div class="sidebar-status">
        <span class="status-dot" id="status-dot"></span>
        <span id="status-text" class="nav-label">Verbinde …</span>
      </div>
    </div>
  </aside>

  <!-- ════ MAIN AREA ════ -->
  <div class="main-area">

    <header>
      <button class="mobile-menu-btn" onclick="toggleSidebar()" aria-label="Menü">
        <svg viewBox="0 0 24 24" width="22" height="22"><path fill="currentColor" d="M3 18h18v-2H3zm0-5h18v-2H3zm0-7v2h18V6z"/></svg>
      </button>
      <div class="header-titles">
        <h1 class="page-title" id="page-title">Übersicht</h1>
        <span class="header-greeting" id="header-greeting"></span>
      </div>
      <div class="header-right">
        <button class="tv-mode-btn" id="tv-mode-btn" onclick="toggleTvMode()" title="Wandmontage-Modus (Taste 't')">
          <svg viewBox="0 0 24 24" width="16" height="16"><rect x="2.5" y="4.5" width="19" height="13" rx="1.6" fill="none" stroke="currentColor" stroke-width="1.8"/><path stroke="currentColor" stroke-width="1.8" stroke-linecap="round" d="M8.5 20.5h7"/></svg>
        </button>
        <button class="cmdk-btn" onclick="openCommandPalette()" title="Befehlspalette (Strg+K)">
          <svg viewBox="0 0 24 24" width="15" height="15"><path fill="currentColor" d="M15.5 14h-.79l-.28-.27a6.5 6.5 0 1 0-.7.7l.27.28v.79l5 4.99L20.49 19zm-6 0A4.5 4.5 0 1 1 14 9.5 4.5 4.5 0 0 1 9.5 14"/></svg>
          <span>Suchen</span>
          <kbd>Strg K</kbd>
        </button>
        <span class="header-time" id="header-time">--:--:--</span>
      </div>
    </header>

    <main>

      <!-- ═══════════════════════ SEITE: ÜBERSICHT ═══════════════════════ -->
      <section class="page active" id="page-overview">

        <!-- Schnellübersicht — Inhalt der vier Kacheln ist personalisierbar,
             siehe Einstellungen → Persönlich → "Schnellübersicht-Widgets".
             Wird komplett per JS aus WIDGET_DEFS + personalization.widgets
             gerendert (renderQuickstatSlots in dashboard.js-Block unten). -->
        <div class="quickstats-row" id="quickstats-row"></div>

        <!-- Energie kompakt -->
        <div class="block">
          <div class="block-head">
            <h2>Energie <span class="block-head-faded">live</span></h2>
            <button class="link-btn-nav" onclick="navigateTo('energy')">Detailanalyse ›</button>
          </div>
          <div class="energy-grid">
            <div class="metric-card sun" id="card-pv">
              <div class="metric-icon"><svg viewBox="0 0 24 24" width="1em" height="1em"><circle cx="12" cy="12" r="4.6" fill="currentColor"/><g stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M12 2.5v2.6M12 18.9v2.6M21.5 12h-2.6M5.1 12H2.5M18.4 5.6l-1.8 1.8M7.4 16.6l-1.8 1.8M18.4 18.4l-1.8-1.8M7.4 7.4L5.6 5.6"/></g></svg></div>
              <div class="metric-label">PV-Erzeugung</div>
              <div class="metric-value" id="pv-power">0 <span class="unit">W</span></div>
              <div class="metric-sub" id="pv-day">Heute: 0 kWh</div>
              <div class="meter"><div class="meter-fill sun" id="pv-bar"></div></div>
            </div>
            <div class="metric-card teal" id="card-load">
              <div class="metric-icon"><svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M12 3.2 3 10.5V21h6v-6.5h6V21h6V10.5z"/></svg></div>
              <div class="metric-label">Hausverbrauch</div>
              <div class="metric-value" id="house-load">0 <span class="unit">W</span></div>
              <div class="metric-sub">Alle Verbraucher</div>
            </div>
            <div class="metric-card" id="grid-card">
              <div class="metric-icon"><svg viewBox="0 0 24 24" width="1em" height="1em"><g fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 8h13l-3-3M20 16H7l3 3"/></g></svg></div>
              <div class="metric-label">Netz</div>
              <div class="metric-value" id="grid-power">0 <span class="unit">W</span></div>
              <div class="metric-sub" id="grid-sub">–</div>
            </div>
            <div class="metric-card green" id="card-battery">
              <div class="metric-icon"><svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M13 2L4 14h6l-1 8 9-12h-6z"/></svg></div>
              <div class="metric-label">Batterie</div>
              <div class="metric-value" id="battery-soc">0<span class="unit">%</span></div>
              <div class="metric-sub">Ladestand</div>
              <div class="meter"><div class="meter-fill green" id="battery-bar"></div></div>
            </div>
          </div>
          <div class="chart-box" style="margin-top:16px"><canvas id="pv-chart-overview"></canvas></div>
        </div>

        <!-- Strompreise & Bitcoin -->
        <div class="grid-2">
          <section class="card">
            <div class="block-head"><h2>Strompreis <span class="block-head-faded">aWATTar</span></h2></div>
            <div class="price-top">
              <div>
                <div class="price-now" id="current-price">0.0000 €/kWh</div>
                <div class="price-meta" id="price-updated">–</div>
              </div>
              <div class="price-side">
                <div class="price-side-row">
                  <span class="price-side-label">Günstigste Stunde</span>
                  <span class="price-side-val good" id="cheapest-price">–</span>
                  <span class="price-side-sub" id="cheapest-hour">–</span>
                </div>
                <div class="price-side-row">
                  <span class="price-side-label">24h Δ</span>
                  <span class="price-side-val" id="price-delta">–</span>
                </div>
              </div>
            </div>
            <div class="chart-box sm"><canvas id="price-chart"></canvas></div>
          </section>
          <section class="card">
            <div class="block-head"><h2>Bitcoin <span class="block-head-faded">CoinGecko</span></h2></div>
            <div class="btc-top">
              <div>
                <div class="metric-label">EUR</div>
                <div class="btc-eur" id="btc-price-eur">– €</div>
              </div>
              <div>
                <div class="metric-label">USD</div>
                <div class="btc-usd" id="btc-price-usd">– $</div>
              </div>
              <div>
                <div class="metric-label">24h Δ</div>
                <div class="btc-change" id="btc-change">0.0%</div>
              </div>
            </div>
            <div class="chart-box sm"><canvas id="btc-chart"></canvas></div>
          </section>
        </div>

        <!-- Wetter -->
        <div class="block">
          <div class="block-head"><h2>Wetter <span class="block-head-faded" id="weather-location">Ruprechtshofen</span></h2></div>
          <section class="card">
            <div class="weather-main">
              <div class="weather-icon" id="weather-icon"><svg viewBox="0 0 24 24" width="1em" height="1em"><circle cx="7.2" cy="7.5" r="3.1" fill="currentColor" opacity="0.9"/><path fill="currentColor" d="M9.5 20a4.6 4.6 0 0 1-.5-9.17 5.6 5.6 0 0 1 3-2.53 5.4 5.4 0 0 1 1.3-.2 5.6 5.6 0 0 1 5.5 4.6A4.4 4.4 0 0 1 18.4 20z"/></svg></div>
              <div>
                <div class="weather-temp" id="weather-temp">–°</div>
                <div class="weather-cond" id="weather-cond">Lädt …</div>
              </div>
            </div>
            <div class="weather-grid">
              <div class="weather-stat"><span>Feuchte</span><strong id="weather-humidity">–%</strong></div>
              <div class="weather-stat"><span>Wind</span><strong id="weather-wind">– km/h</strong></div>
              <div class="weather-stat"><span>Wolken</span><strong id="weather-clouds">–%</strong></div>
            </div>

            <div class="weather-divider"></div>

            <div class="weather-hourly-scroll" id="weather-hourly-scroll">
              <div class="empty-state">Lädt Vorhersage …</div>
            </div>

            <div class="weather-divider"></div>

            <div class="weather-daily-row" id="weather-daily-row">
              <div class="empty-state">Lädt Vorhersage …</div>
            </div>
          </section>
        </div>

        <!-- System -->
        <div class="block">
          <section class="card">
            <div class="block-head"><h2>System</h2></div>
            <div class="status-list">
              <div class="status-row"><span class="status-dot-ind" id="si-pv"></span> PV-Anlage (Fronius) <span class="status-row-meta" id="si-pv-text">–</span></div>
              <div class="status-row"><span class="status-dot-ind" id="si-miner"></span> Miner-Dienst <span class="status-row-meta" id="si-miner-text">–</span></div>
              <div class="status-row"><span class="status-dot-ind" id="si-cam"></span> Heimüberwachung <span class="status-row-meta" id="si-cam-text">–</span></div>
              <div class="status-row"><span class="status-dot-ind" id="si-btc"></span> Bitcoin-Kurs <span class="status-row-meta" id="si-btc-text">–</span></div>
            </div>
            <div class="status-foot">Letztes Update: <span id="last-update-time">--:--:--</span></div>
          </section>
        </div>

      </section>

      <!-- ═══════════════════════ SEITE: ENERGIE ═══════════════════════ -->
      <section class="page" id="page-energy">

        <div class="block-head" style="margin-bottom:4px">
          <h2 style="font-size:15px">Kennzahlen</h2>
        </div>
        <div class="energy-grid secondary" style="margin-bottom:20px">
          <div class="metric-card-sm">
            <div class="metric-label">PV-Ertrag (30 Tage)</div>
            <div class="metric-value-sm" id="kpi-total-30d">0 kWh</div>
            <div class="metric-foot">Summe letzte 30 Tage</div>
          </div>
          <div class="metric-card-sm">
            <div class="metric-label">Ø Ertrag pro Tag</div>
            <div class="metric-value-sm" id="kpi-avg-day">0 kWh</div>
            <div class="metric-foot">Letzte 30 Tage</div>
          </div>
          <div class="metric-card-sm">
            <div class="metric-label">Bester Tag</div>
            <div class="metric-value-sm" id="kpi-best-day">– kWh</div>
            <div class="metric-foot" id="kpi-best-day-date">–</div>
          </div>
          <div class="metric-card-sm">
            <div class="metric-label">Woche vs. Vorwoche</div>
            <div class="metric-value-sm" id="kpi-wow">–</div>
            <div class="metric-foot">PV-Ertrag</div>
          </div>
        </div>

        <div class="block">
          <div class="block-head">
            <h2>PV &amp; Hauslast <span class="block-head-faded">24 Stunden</span></h2>
          </div>
          <div class="card">
            <div class="chart-box"><canvas id="pv-chart"></canvas></div>
          </div>
        </div>

        <div class="block" id="solar-forecast-block" hidden>
          <div class="block-head">
            <h2>Solarprognose <span class="block-head-faded">Schätzung anhand der Wettervorhersage</span></h2>
          </div>
          <div class="card">
            <div class="energy-grid secondary" style="margin-bottom:14px">
              <div class="metric-card-sm yellow">
                <div class="metric-label">Heute voraussichtlich</div>
                <div class="metric-value-sm" id="solar-forecast-today">– kWh</div>
                <div class="metric-foot">bei <span id="solar-forecast-kwp">–</span> kWp installiert</div>
              </div>
              <div class="metric-card-sm teal">
                <div class="metric-label">Morgen voraussichtlich</div>
                <div class="metric-value-sm" id="solar-forecast-tomorrow">– kWh</div>
                <div class="metric-foot">Globalstrahlung-basiert</div>
              </div>
            </div>
            <div class="chart-box" style="height:180px"><canvas id="solar-forecast-chart"></canvas></div>
            <p class="form-hint" style="margin-top:10px">Grobe Schätzung aus der Open-Meteo-Wettervorhersage (Globalstrahlung × installierte Leistung × 78% Systemwirkungsgrad) — Ausrichtung, Neigung und Verschattung werden nicht berücksichtigt. Installierte Leistung unter Einstellungen → PV-Anlage.</p>
          </div>
        </div>

        <div class="block">
          <div class="block-head">
            <h2>Wochenvergleich <span class="block-head-faded">PV-Ertrag pro Tag</span></h2>
          </div>
          <div class="card">
            <div class="chart-box"><canvas id="week-compare-chart"></canvas></div>
          </div>
        </div>

        <div class="grid-2">
          <section class="card">
            <div class="block-head"><h2>Tagesprofil <span class="block-head-faded">Ø je Uhrzeit</span></h2></div>
            <div class="chart-box"><canvas id="daily-profile-chart"></canvas></div>
          </section>
          <section class="card">
            <div class="block-head"><h2>PV-Ertrags-Historie <span class="block-head-faded">30 Tage</span></h2></div>
            <div class="chart-box"><canvas id="pv-history-chart"></canvas></div>
          </section>
        </div>

        <div class="block">
          <div class="block-head"><h2>Monatsübersicht <span class="block-head-faded">Erzeugung · Eigenverbrauch · Einspeisung</span></h2></div>
          <div class="card">
            <div class="chart-box"><canvas id="month-overview-chart"></canvas></div>
          </div>
        </div>

        <div class="grid-2">
          <section class="card">
            <div class="block-head"><h2>Einsparungen</h2></div>
            <div class="savings-grid">
              <div class="saving-item"><div class="saving-value" id="savings-today">0 €</div><div class="saving-label">Heute</div></div>
              <div class="saving-item"><div class="saving-value" id="savings-month">0 €</div><div class="saving-label">Monat</div></div>
              <div class="saving-item"><div class="saving-value" id="savings-year">0 €</div><div class="saving-label">Jahr</div></div>
              <div class="saving-item"><div class="saving-value alt" id="savings-roi">– J</div><div class="saving-label">Amortisation</div></div>
            </div>
            <div class="savings-foot">
              Eigenverbrauch <strong id="self-kwh">0</strong> kWh &nbsp;·&nbsp; Einspeisung <strong id="export-kwh">0</strong> kWh
            </div>
          </section>
          <section class="card">
            <div class="block-head">
              <h2>Autarkie &amp; Eigenverbrauch</h2>
            </div>
            <div class="energy-grid secondary" style="grid-template-columns:1fr 1fr;gap:14px">
              <div class="metric-card-sm">
                <div class="metric-label">Autarkie</div>
                <div class="metric-value-sm" id="autonomy">0%</div>
                <div class="meter thin"><div class="meter-fill green" id="autonomy-bar"></div></div>
              </div>
              <div class="metric-card-sm">
                <div class="metric-label">Eigenverbrauch</div>
                <div class="metric-value-sm" id="self-consumption">0%</div>
                <div class="meter thin"><div class="meter-fill sun" id="self-bar"></div></div>
              </div>
            </div>
            <div class="metric-foot" style="margin-top:14px">PV-Überschuss aktuell: <strong id="pv-surplus">0 W</strong> · <span id="surplus-sub">Verfügbar</span></div>
          </section>
        </div>

      </section>

      <!-- ═══════════════════════ SEITE: MINER ═══════════════════════ -->
      <section class="page" id="page-miner">
        <div class="block-head">
          <h2 style="font-size:15px">Steuerung</h2>
          <div class="block-actions">
            <span class="badge badge-offline" id="automation-badge">Automatisierung inaktiv</span>
            <button class="btn btn-ghost btn-sm" onclick="openModal('miner-settings-modal')">Automatik</button>
            <button class="btn btn-green btn-sm" onclick="controlAllMiners('resume')">Alle starten</button>
            <button class="btn btn-red btn-sm" onclick="controlAllMiners('pause')">Alle stoppen</button>
            <button class="btn btn-primary btn-sm" onclick="openModal('add-miner-modal')">+ Miner</button>
          </div>
        </div>
        <div class="energy-grid" style="margin-bottom:18px">
          <div class="metric-card-sm purple">
            <div class="metric-label">Hashrate gesamt</div>
            <div class="metric-value-sm" id="total-hashrate">0.00 TH/s</div>
            <div class="metric-foot" id="active-miners-count">0 aktive Miner</div>
          </div>
          <div class="metric-card-sm red">
            <div class="metric-label">Verbrauch</div>
            <div class="metric-value-sm" id="total-miner-power">0 W</div>
            <div class="metric-foot">Gesamtleistung</div>
          </div>
          <div class="metric-card-sm yellow">
            <div class="metric-label">Effizienz</div>
            <div class="metric-value-sm" id="miner-efficiency">– J/TH</div>
            <div class="metric-foot">Ø über alle Miner</div>
          </div>
          <div class="metric-card-sm teal">
            <div class="metric-label">Auto / Manuell</div>
            <div class="metric-value-sm" id="miner-mode-display">–</div>
            <div class="metric-foot" id="miner-mode-sub">Miner-Modi</div>
          </div>
        </div>
        <div class="card miner-table-card">
          <div class="miner-table-wrap">
            <table id="miner-table">
              <thead>
                <tr><th>Miner</th><th>IP : Port</th><th>Verbrauch</th><th>Schwellwerte</th><th>Status</th><th>Hashrate</th><th>Temp.</th><th>Modus</th><th></th></tr>
              </thead>
              <tbody id="miner-tbody">
                <tr><td colspan="9" class="empty-state-cell">Noch keine Miner eingerichtet</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <div class="card" style="margin-top:16px">
          <div class="block-head">
            <h2>Miner-Historie <span class="block-head-faded">24 Stunden</span></h2>
            <button class="btn btn-ghost btn-sm" onclick="loadMinerHistory()">Aktualisieren</button>
          </div>
          <div class="chart-box"><canvas id="miner-chart"></canvas></div>
        </div>
      </section>

      <!-- ═══════════════════════ SEITE: HEIMÜBERWACHUNG ═══════════════════════ -->
      <section class="page" id="page-cameras">
        <div class="block-head">
          <h2 style="font-size:15px">Heimüberwachung <span class="block-head-faded">Personenerkennung</span></h2>
          <span class="status-row" style="font-size:12px;gap:6px">
            <span class="status-dot-ind" id="si-surv"></span><span id="si-surv-text">Deaktiviert</span>
          </span>
        </div>

        <div id="surveillance-empty" class="empty-state">
          Personenerkennung ist deaktiviert. <button class="link-btn" onclick="openSurveillanceSettings()">Jetzt aktivieren</button>
        </div>

        <div id="surveillance-body" hidden>
          <div class="energy-grid" id="surveillance-summary" style="margin-bottom:18px">
            <div class="metric-card-sm purple">
              <div class="metric-label">Personen erkannt</div>
              <div class="metric-value-sm" id="surv-persons-total">0</div>
              <div class="metric-foot">Aktuell, alle Kameras</div>
            </div>
            <div class="metric-card-sm teal">
              <div class="metric-label">Kameras verbunden</div>
              <div class="metric-value-sm" id="surv-cams-connected">0/0</div>
              <div class="metric-foot">Heimüberwachung</div>
            </div>
            <div class="metric-card-sm yellow">
              <div class="metric-label">Ereignisse heute</div>
              <div class="metric-value-sm" id="surv-events-today">0</div>
              <div class="metric-foot">Erkennungen</div>
            </div>
            <div class="metric-card-sm red">
              <div class="metric-label">Fotos gespeichert</div>
              <div class="metric-value-sm" id="surv-photos-total">0</div>
              <div class="metric-foot">Erkennungs-Fotos</div>
            </div>
          </div>

          <div class="tabs">
            <button class="tab-btn active" onclick="switchTab(event,'heim-tab-live','page-cameras')">Live-Ansicht</button>
            <button class="tab-btn" onclick="switchTab(event,'heim-tab-cameras','page-cameras')">Kameras verwalten</button>
            <button class="tab-btn" onclick="switchTab(event,'heim-tab-gallery','page-cameras')">Galerie</button>
            <button class="tab-btn" onclick="switchTab(event,'heim-tab-events','page-cameras')">Ereignisse</button>
            <button class="tab-btn" onclick="switchTab(event,'heim-tab-settings','page-cameras')">Einstellungen</button>
          </div>

          <div id="heim-tab-live" class="tab-content active">
            <div class="cameras-grid" id="cameras-grid-page"></div>
            <div id="surveillance-occupancy-list" class="surv-list" hidden></div>
          </div>

          <div id="heim-tab-cameras" class="tab-content">
            <div id="heim-camera-list"></div>
            <div class="divider"></div>
            <h4 class="form-section-title">Neue Kamera hinzufügen</h4>
            <div class="form-row">
              <div class="form-group"><label class="form-label">Name</label><input class="form-input" id="heim-newcam-name" placeholder="z. B. Eingang"></div>
              <div class="form-group"><label class="form-label">RTSP-URL</label><input class="form-input" id="heim-newcam-url" placeholder="rtsp://benutzer:passwort@192.168.178.36:554/stream1"></div>
            </div>
            <p class="form-hint">Die Erkennung läuft automatisch über das gesamte Kamerabild — welche Objektarten erkannt werden, stellst du unter "Einstellungen" ein.</p>
            <div class="form-actions"><button class="btn btn-primary" onclick="heimAddCamera()">Kamera hinzufügen</button></div>
          </div>

          <div id="heim-tab-gallery" class="tab-content">
            <div class="block-head" style="margin-bottom:12px">
              <h2 style="font-size:13px">Aufnahmen <span class="block-head-faded">Fotos bei erkannten Objekten — es werden bewusst nur Bilder gespeichert, keine Videos</span></h2>
              <div class="form-row" style="gap:8px">
                <button class="btn btn-ghost btn-sm" id="heim-gallery-select-btn" onclick="heimToggleSelectMode()">Auswählen</button>
                <button class="btn btn-ghost btn-sm" onclick="heimClearGallery()">Alle löschen</button>
              </div>
            </div>
            <div class="form-row" id="heim-gallery-filters" style="margin-bottom:12px;align-items:flex-end">
              <div class="form-group"><label class="form-label">Kamera</label>
                <select class="form-input" id="heim-gallery-filter-camera" onchange="heimLoadGallery()"><option value="">Alle Kameras</option></select>
              </div>
              <div class="form-group"><label class="form-label">Von</label><input class="form-input" type="date" id="heim-gallery-filter-from" onchange="heimLoadGallery()"></div>
              <div class="form-group"><label class="form-label">Bis</label><input class="form-input" type="date" id="heim-gallery-filter-to" onchange="heimLoadGallery()"></div>
              <button class="btn btn-ghost btn-sm" onclick="heimResetGalleryFilters()">Filter zurücksetzen</button>
            </div>
            <div id="heim-gallery-select-bar" class="heim-gallery-select-bar" hidden>
              <span id="heim-gallery-select-count">0 ausgewählt</span>
              <div class="form-row" style="gap:8px">
                <button class="btn btn-ghost btn-sm" onclick="heimSelectAllGallery()">Alle auswählen</button>
                <button class="btn btn-red btn-sm" onclick="heimDeleteSelectedGallery()">Löschen</button>
              </div>
            </div>
            <div id="heim-gallery-empty" class="empty-state" hidden>Keine Aufnahmen für diese Auswahl.</div>
            <div class="gallery-grid" id="heim-gallery-grid"></div>
          </div>

          <div id="heim-tab-events" class="tab-content">
            <div class="form-row" style="align-items:flex-end;gap:10px;flex-wrap:wrap;margin-bottom:14px">
              <div class="form-group" style="max-width:170px;margin-bottom:0">
                <label class="form-label">Tag</label>
                <input class="form-input" type="date" id="heim-events-date" onchange="loadHeimEvents()">
              </div>
              <div class="segmented" id="heim-events-view-toggle">
                <button type="button" class="seg-btn active" data-view="list" onclick="setHeimEventsView('list')">Liste</button>
                <button type="button" class="seg-btn" data-view="timeline" onclick="setHeimEventsView('timeline')">Zeitleiste</button>
                <button type="button" class="seg-btn" data-view="heatmap" onclick="setHeimEventsView('heatmap')">Heatmap</button>
              </div>
              <button class="btn btn-ghost btn-sm" onclick="heimEventsToday()">Heute</button>
            </div>

            <div id="heim-events-timeline-wrap" hidden>
              <div class="event-timeline-hours" id="heim-events-timeline-hours"></div>
              <div class="event-timeline" id="heim-events-timeline"></div>
              <div id="heim-events-timeline-detail" class="event-timeline-detail"></div>
            </div>

            <div id="heim-events-heatmap-wrap" hidden>
              <div id="heim-events-heatmap" class="event-heatmap"></div>
              <p class="form-hint">Zeigt, zu welcher Tagesstunde welche Kamera am häufigsten ausgelöst hat — dunklere Zellen bedeuten mehr Ereignisse.</p>
            </div>

            <div id="heim-events-list"></div>
          </div>

          <div id="heim-tab-settings" class="tab-content">
            <h4 class="form-section-title">Erkennungsklassen</h4>
            <p class="form-hint">Welche Objektarten sollen erkannt werden? Bei jeder Erkennung wird automatisch ein Foto in der Galerie gespeichert.</p>
            <div id="heim-classes-list" class="heim-classes-grid"></div>
            <div class="form-actions"><button class="btn btn-primary btn-sm" onclick="heimSaveClasses()">Speichern</button></div>

            <div class="divider"></div>

            <h4 class="form-section-title">Galerie aufräumen</h4>
            <p class="form-hint" id="heim-gallery-current-count">–</p>
            <div class="form-row">
              <div class="form-group"><label class="form-label">Fotos automatisch löschen nach (Tagen)</label><input class="form-input" id="heim-gallery-max-age" type="number" min="0" value="30"></div>
              <div class="form-group"><label class="form-label">Max. Anzahl Fotos (ältestes zuerst löschen)</label><input class="form-input" id="heim-gallery-max-count" type="number" min="0" value="5000"></div>
            </div>
            <p class="form-hint">0 = jeweils kein Limit. Wird stündlich im Hintergrund geprüft.</p>
            <div class="form-actions"><button class="btn btn-primary btn-sm" onclick="heimSaveGallerySettings()">Speichern</button></div>

            <div class="divider"></div>

            <h4 class="form-section-title">Benachrichtigungen</h4>
            <p class="form-hint" id="heim-notif-status">–</p>
            <div class="form-group"><label class="form-label">Versandweg</label>
              <select class="form-input" id="heim-notif-method" onchange="heimNotifMethodChanged()">
                <option value="console">Nur Logausgabe (keine echte Benachrichtigung)</option>
                <option value="email">E-Mail</option>
                <option value="ntfy">ntfy</option>
                <option value="telegram">Telegram</option>
              </select>
            </div>

            <div id="heim-notif-email-fields" class="heim-notif-method-fields" hidden>
              <div class="form-row">
                <div class="form-group"><label class="form-label">SMTP-Server</label><input class="form-input" id="heim-notif-email-server" placeholder="smtp.gmail.com"></div>
                <div class="form-group"><label class="form-label">Port</label><input class="form-input" id="heim-notif-email-port" type="number" placeholder="587"></div>
              </div>
              <div class="form-row">
                <div class="form-group"><label class="form-label">Absender / Benutzername</label><input class="form-input" id="heim-notif-email-user" placeholder="dein.name@gmail.com"></div>
                <div class="form-group"><label class="form-label">Passwort <span id="heim-notif-email-pass-hint" class="form-hint" style="display:inline"></span></label><input class="form-input" id="heim-notif-email-pass" type="password" placeholder="App-Passwort"></div>
              </div>
              <div class="form-group"><label class="form-label">Empfänger-Adresse</label><input class="form-input" id="heim-notif-email-to" placeholder="empfaenger@example.com"></div>
              <p class="form-hint">Bei Gmail wird ein "App-Passwort" benötigt (nicht das normale Konto-Passwort) — siehe Google-Kontoeinstellungen → Sicherheit → App-Passwörter.</p>
            </div>

            <div id="heim-notif-ntfy-fields" class="heim-notif-method-fields" hidden>
              <div class="form-row">
                <div class="form-group"><label class="form-label">ntfy-Server</label><input class="form-input" id="heim-notif-ntfy-server" placeholder="https://ntfy.sh"></div>
                <div class="form-group"><label class="form-label">Topic</label><input class="form-input" id="heim-notif-ntfy-topic" placeholder="mein-geheimes-topic"></div>
              </div>
            </div>

            <div id="heim-notif-telegram-fields" class="heim-notif-method-fields" hidden>
              <div class="form-row">
                <div class="form-group"><label class="form-label">Bot-Token <span id="heim-notif-telegram-token-hint" class="form-hint" style="display:inline"></span></label><input class="form-input" id="heim-notif-telegram-token" type="password" placeholder="123456:ABC-..."></div>
                <div class="form-group"><label class="form-label">Chat-ID</label><input class="form-input" id="heim-notif-telegram-chatid" placeholder="123456789"></div>
              </div>
            </div>

            <div class="form-actions">
              <button class="btn btn-primary btn-sm" onclick="heimSaveNotificationSettings()">Speichern</button>
              <button class="btn btn-ghost btn-sm" onclick="heimTestNotification()">Test senden</button>
              <button class="btn btn-ghost btn-sm" onclick="heimSnooze(60)">1h stummschalten</button>
              <button class="btn btn-ghost btn-sm" onclick="heimSnooze(0)">Stummschaltung aufheben</button>
            </div>

            <div class="divider"></div>

            <h4 class="form-section-title">Browser-Benachrichtigungen</h4>
            <p class="form-hint" id="heim-push-status">Push-Benachrichtigungen direkt in diesem Browser, unabhängig von E-Mail/ntfy/Telegram.</p>
            <div class="form-row">
              <button class="btn btn-primary btn-sm" id="heim-push-enable-btn" onclick="heimEnableBrowserPush()">Für diesen Browser aktivieren</button>
              <button class="btn btn-ghost btn-sm" onclick="heimTestBrowserPush()">Test senden</button>
              <button class="btn btn-ghost btn-sm" id="heim-push-disable-btn" onclick="heimDisableBrowserPush()" hidden>Für diesen Browser deaktivieren</button>
            </div>
          </div>
        </div>
      </section>

      <!-- ═══════════════════════ SEITE: FAMILIE ═══════════════════════ -->
      <section class="page" id="page-family">
        <div class="grid-3">
          <section class="card">
            <div class="block-head">
              <h2>Einkaufsliste <span class="count-pill" id="shopping-count">0</span></h2>
              <button class="btn-icon-add" onclick="openAddShopping()" aria-label="Artikel hinzufügen">+</button>
            </div>
            <div id="shopping-list-container"><div class="empty-state">Liste ist leer</div></div>
            <div id="add-shopping-form" class="inline-form" hidden>
              <input class="form-input" id="new-shopping-item" placeholder="Artikel eingeben …" onkeydown="if(event.key==='Enter')addShoppingItem()">
              <button class="btn btn-primary btn-sm" onclick="addShoppingItem()">Hinzufügen</button>
              <button class="btn btn-ghost btn-sm" onclick="document.getElementById('add-shopping-form').hidden=true">Abbrechen</button>
            </div>
          </section>
          <section class="card">
            <div class="block-head">
              <h2>Termine</h2>
              <button class="btn-icon-add" onclick="openModal('calendar-modal')" aria-label="Termin hinzufügen">+</button>
            </div>
            <div id="calendar-container"><div class="empty-state">Keine Termine geplant</div></div>
          </section>
          <section class="card">
            <div class="block-head">
              <h2>Notizen</h2>
              <button class="btn btn-ghost btn-sm" onclick="saveNotes()">Speichern</button>
            </div>
            <textarea class="notes-area" id="family-notes" placeholder="Platz für gemeinsame Notizen …"></textarea>
          </section>
        </div>
      </section>

    </main>
  </div>
</div>

<!-- ════ MODALS ════ -->

<!-- Add Miner -->
<div class="modal-overlay" id="add-miner-modal">
  <div class="modal">
    <div class="modal-header"><h3>Miner hinzufügen</h3><button class="modal-close" onclick="closeModal('add-miner-modal')">✕</button></div>
    <div class="form-group">
      <label class="form-label">Firmware</label>
      <div class="segmented" id="add-firmware-toggle">
        <button type="button" class="seg-btn active" data-val="braiins" onclick="setFirmwareToggle('add',this)">Braiins OS</button>
        <button type="button" class="seg-btn" data-val="bitmain" onclick="setFirmwareToggle('add',this)">Bitmain Stock</button>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Name *</label><input class="form-input" id="add-name" placeholder="z. B. Antminer S19"></div>
      <div class="form-group"><label class="form-label">IP-Adresse *</label><input class="form-input" id="add-ip" placeholder="192.168.178.x"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">API-Port (CGMiner, Stats)</label><input class="form-input" id="add-port" type="number" value="4028"></div>
      <div class="form-group"><label class="form-label">Verbrauch (Watt)</label><input class="form-input" id="add-watts" type="number" placeholder="3250"></div>
    </div>
    <div class="form-row" id="add-bitmain-webport-row" hidden>
      <div class="form-group"><label class="form-label">Web-UI Port</label><input class="form-input" id="add-webport" type="number" value="80"></div>
      <div class="form-group"></div>
    </div>
    <div class="form-row">
      <div class="form-group" id="add-user-group"><label class="form-label" id="add-user-label">Benutzer</label><input class="form-input" id="add-user" value="admin"></div>
      <div class="form-group" id="add-pass-group"><label class="form-label" id="add-pass-label">Passwort</label><input class="form-input" id="add-pass" type="password"></div>
    </div>
    <div class="form-group">
      <label class="form-label">Automatik-Auslöser</label>
      <select class="form-input" id="add-trigger-source" onchange="updateTriggerLabels('add', true)">
        <option value="pv_surplus">PV-Überschuss (Einspeisung)</option>
        <option value="grid_import">Netzbezug</option>
        <option value="pv_production">PV-Rohleistung (Erzeugung)</option>
        <option value="battery_soc">Batterie-Ladezustand</option>
      </select>
      <p class="form-hint" id="add-trigger-hint" style="margin-bottom:0">Miner schaltet je nach PV-Überschuss ein/aus, teilt sich die Leistung mit anderen so eingestellten Minern nach Priorität.</p>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label" id="add-threshold-on-label">Einschalten ab (W Überschuss)</label><input class="form-input" id="add-threshold-on" type="number" value="500"></div>
      <div class="form-group"><label class="form-label" id="add-threshold-off-label">Ausschalten bei (W Bezug)</label><input class="form-input" id="add-threshold-off" type="number" value="400"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Priorität (1 = zuerst an)</label><input class="form-input" id="add-priority" type="number" value="1" min="1" max="10"></div>
      <div class="form-group"><label class="form-label">Notiz</label><input class="form-input" id="add-note" placeholder="Optional"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Min. Laufzeit (Sek.)</label><input class="form-input" id="add-min-runtime" type="number" value="300"></div>
      <div class="form-group"><label class="form-label">Min. Aus-Zeit (Sek.)</label><input class="form-input" id="add-min-offtime" type="number" value="300"></div>
    </div>
    <div class="form-actions"><button class="btn btn-ghost" onclick="closeModal('add-miner-modal')">Abbrechen</button><button class="btn btn-primary" onclick="addMiner()">Miner hinzufügen</button></div>
  </div>
</div>

<!-- Edit Miner -->
<div class="modal-overlay" id="edit-miner-modal">
  <div class="modal">
    <div class="modal-header"><h3>Miner bearbeiten</h3><button class="modal-close" onclick="closeModal('edit-miner-modal')">✕</button></div>
    <input type="hidden" id="edit-miner-id">
    <div class="form-group">
      <label class="form-label">Firmware</label>
      <div class="segmented" id="edit-firmware-toggle">
        <button type="button" class="seg-btn" data-val="braiins" onclick="setFirmwareToggle('edit',this)">Braiins OS</button>
        <button type="button" class="seg-btn" data-val="bitmain" onclick="setFirmwareToggle('edit',this)">Bitmain Stock</button>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Name</label><input class="form-input" id="edit-name"></div>
      <div class="form-group"><label class="form-label">IP-Adresse</label><input class="form-input" id="edit-ip"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">API-Port</label><input class="form-input" id="edit-port" type="number"></div>
      <div class="form-group"><label class="form-label">Verbrauch (W)</label><input class="form-input" id="edit-watts" type="number"></div>
    </div>
    <div class="form-row" id="edit-bitmain-webport-row" hidden>
      <div class="form-group"><label class="form-label">Web-UI Port</label><input class="form-input" id="edit-webport" type="number"></div>
      <div class="form-group"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label" id="edit-user-label">Benutzer</label><input class="form-input" id="edit-user"></div>
      <div class="form-group"><label class="form-label" id="edit-pass-label">Passwort</label><input class="form-input" id="edit-pass" type="password" placeholder="Unverändert lassen, wenn leer"></div>
    </div>
    <div class="form-group">
      <label class="form-label">Automatik-Auslöser</label>
      <select class="form-input" id="edit-trigger-source" onchange="updateTriggerLabels('edit', true)">
        <option value="pv_surplus">PV-Überschuss (Einspeisung)</option>
        <option value="grid_import">Netzbezug</option>
        <option value="pv_production">PV-Rohleistung (Erzeugung)</option>
        <option value="battery_soc">Batterie-Ladezustand</option>
      </select>
      <p class="form-hint" id="edit-trigger-hint" style="margin-bottom:0"></p>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label" id="edit-threshold-on-label">Einschalten ab (W)</label><input class="form-input" id="edit-threshold-on" type="number"></div>
      <div class="form-group"><label class="form-label" id="edit-threshold-off-label">Ausschalten bei (W)</label><input class="form-input" id="edit-threshold-off" type="number"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Priorität</label><input class="form-input" id="edit-priority" type="number" min="1" max="10"></div>
      <div class="form-group"><label class="form-label">Notiz</label><input class="form-input" id="edit-note"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Min. Laufzeit (Sek.)</label><input class="form-input" id="edit-min-runtime" type="number"></div>
      <div class="form-group"><label class="form-label">Min. Aus-Zeit (Sek.)</label><input class="form-input" id="edit-min-offtime" type="number"></div>
    </div>
    <div class="form-group">
      <div class="toggle-switch"><label class="toggle"><input type="checkbox" id="edit-automation"><span class="toggle-slider"></span></label><span>Automatische Steuerung für diesen Miner</span></div>
    </div>
    <div class="form-actions"><button class="btn btn-ghost" onclick="closeModal('edit-miner-modal')">Abbrechen</button><button class="btn btn-primary" onclick="saveMinerEdit()">Speichern</button></div>
  </div>
</div>

<!-- Miner Automation Settings -->
<div class="modal-overlay" id="miner-settings-modal">
  <div class="modal">
    <div class="modal-header"><h3>Miner-Automatisierung</h3><button class="modal-close" onclick="closeModal('miner-settings-modal')">✕</button></div>
    <div class="form-group"><div class="toggle-switch"><label class="toggle"><input type="checkbox" id="ms-enabled" checked><span class="toggle-slider"></span></label><span>Automatische Steuerung aktivieren</span></div></div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Einschalten bei PV-Überschuss (W)</label><input class="form-input" id="ms-surplus" type="number" value="2000"></div>
      <div class="form-group"><label class="form-label">Ausschalten bei Netzbezug (W)</label><input class="form-input" id="ms-draw" type="number" value="500"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Mindest-Batterie (%)</label><input class="form-input" id="ms-battery" type="number" value="20"></div>
      <div class="form-group"><label class="form-label">Max. Strompreis (€/kWh)</label><input class="form-input" id="ms-price" type="number" step="0.01" value="0.30"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Startzeit</label><input class="form-input" id="ms-start" type="time" value="08:00"></div>
      <div class="form-group"><label class="form-label">Endzeit</label><input class="form-input" id="ms-end" type="time" value="18:00"></div>
    </div>
    <p class="form-hint">Außerhalb des Zeitfensters oder bei zu niedriger Batterie / zu teurem Strom werden automatisierte Miner immer gestoppt.</p>
    <div class="form-actions"><button class="btn btn-ghost" onclick="closeModal('miner-settings-modal')">Schließen</button><button class="btn btn-primary" onclick="saveMinerSettings()">Speichern</button></div>
  </div>
</div>

<!-- General Settings -->
<div class="modal-overlay" id="settings-modal">
  <div class="modal">
    <div class="modal-header"><h3>Einstellungen</h3><button class="modal-close" onclick="closeModal('settings-modal')">✕</button></div>
    <div class="tabs">
      <button class="tab-btn active" onclick="switchTab(event,'tab-notif','settings-modal')">Benachrichtigungen</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-pv','settings-modal')">PV-Anlage</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-surveillance','settings-modal')">Überwachung</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-finance','settings-modal')">Finanzen</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-design','settings-modal')">Design</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-personal','settings-modal')">Persönlich</button>
      <button class="tab-btn" onclick="switchTab(event,'tab-data','settings-modal')">Daten</button>
    </div>
    <div id="tab-notif" class="tab-content active">
      <div class="form-row">
        <div class="form-group"><label class="form-label">Batterie niedrig (%)</label><input class="form-input" id="set-bat-low" type="number" value="20"></div>
        <div class="form-group"><label class="form-label">Batterie voll (%)</label><input class="form-input" id="set-bat-full" type="number" value="85"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Hoher Netzbezug (W)</label><input class="form-input" id="set-high-import" type="number" value="2000"></div>
        <div class="form-group"><label class="form-label">Günstiger Preis (€/kWh)</label><input class="form-input" id="set-cheap-price" type="number" step="0.01" value="0.20"></div>
      </div>
      <div class="divider"></div>
      <div class="form-group">
        <div class="toggle-switch"><label class="toggle"><input type="checkbox" id="set-daily-report-enabled" onchange="document.getElementById('daily-report-time-row').hidden = !this.checked"><span class="toggle-slider"></span></label><span>Täglichen Bericht senden</span></div>
      </div>
      <div class="form-row" id="daily-report-time-row" hidden>
        <div class="form-group">
          <label class="form-label">Uhrzeit</label>
          <input class="form-input" id="set-daily-report-time" type="time" value="20:00">
        </div>
        <div class="form-group"></div>
      </div>
      <p class="form-hint">Fasst einmal täglich zur gewählten Uhrzeit die wichtigsten Kennzahlen des Tages in EINER Benachrichtigung zusammen: Personen erkannt, PV-Erzeugung, Eigenverbrauch/Einspeisung, Ersparnis und Wetter. Es wird garantiert nur ein Bericht pro Tag verschickt.</p>
    </div>
    <div id="tab-pv" class="tab-content">
      <div class="form-group"><label class="form-label">Fronius Wechselrichter IP-Adresse</label><input class="form-input" id="set-fronius-ip" value="192.168.178.100"></div>
      <p class="form-hint">Die Fronius Solar API muss am Wechselrichter aktiviert sein (Menü → Netzwerk → Solar API → Aktiviert).</p>
      <div class="divider"></div>
      <div class="form-group"><label class="form-label">Installierte PV-Leistung (kWp)</label><input class="form-input" id="set-pv-kwp" type="number" step="0.1" value="5"></div>
      <p class="form-hint">Wird für die Solarprognose genutzt (voraussichtliche Erzeugung anhand der Wettervorhersage) — steht auf der Übersichtsseite sowie in der Morgennachricht unten.</p>
      <div class="divider"></div>
      <div class="form-group">
        <div class="toggle-switch"><label class="toggle"><input type="checkbox" id="set-morning-msg-enabled" onchange="document.getElementById('morning-msg-options').hidden = !this.checked"><span class="toggle-slider"></span></label><span>Morgennachricht anzeigen</span></div>
      </div>
      <div id="morning-msg-options" hidden>
        <div class="form-row">
          <div class="form-group">
            <label class="form-label">Ab Uhrzeit (am Vortag) anzeigen</label>
            <input class="form-input" id="set-morning-msg-time" type="time" value="18:00">
          </div>
          <div class="form-group">
            <label class="form-label">Optimismusgrad: <span id="morning-msg-optimism-val">60</span>%</label>
            <input type="range" id="set-morning-msg-optimism" min="0" max="100" step="5" value="60"
                   oninput="document.getElementById('morning-msg-optimism-val').textContent = this.value">
          </div>
        </div>
        <p class="form-hint">Zeigt ab der gewählten Uhrzeit schon abends eine Vorschau auf den nächsten Tag: Wetter morgens/mittags/abends sowie die geschätzte PV-Erzeugung (berechnet aus Wettervorhersage, installierter kWp-Leistung oben und dem Optimismusgrad — niedrig = vorsichtig geschätzt, hoch = optimistisch/theoretisches Maximum). Als Widget wählbar unter Einstellungen → Persönlich → Schnellübersicht-Widgets.</p>
      </div>
    </div>
    <div id="tab-surveillance" class="tab-content">
      <div class="form-group">
        <div class="toggle-switch"><label class="toggle"><input type="checkbox" id="set-surv-enabled"><span class="toggle-slider"></span></label><span>Personenerkennung aktivieren</span></div>
      </div>
      <p class="form-hint">Läuft im selben Server wie dieses Dashboard — kein zweiter Server, kein zweiter Port, kein externer Link. Nach dem Aktivieren muss das Dashboard einmal neu gestartet werden (lädt dabei das Erkennungsmodell).</p>
      <p class="form-hint">Kameras, Erkennungsklassen, Aufnahmen und Benachrichtigungen werden direkt im Reiter "Heimüberwachung" in der Seitenleiste verwaltet.</p>
    </div>
    <div id="tab-finance" class="tab-content">
      <div class="form-row">
        <div class="form-group"><label class="form-label">Einspeisevergütung (€/kWh)</label><input class="form-input" id="set-buyback" type="number" step="0.01" value="0.07"></div>
        <div class="form-group"><label class="form-label">Anlagekosten gesamt (€)</label><input class="form-input" id="set-install-cost" type="number" value="12000"></div>
      </div>
    </div>
    <div id="tab-design" class="tab-content">
      <p class="form-hint" style="margin-top:0">Der Hintergrund kann sich automatisch ans Wetter oder die Tageszeit anpassen, oder eine feste Farbe haben.</p>
      <button class="btn btn-primary btn-sm" onclick="closeModal('settings-modal');openModal('background-modal')">Hintergrund personalisieren</button>
      <div class="divider"></div>
      <p class="form-hint" style="margin-top:0">Was in den vier Kacheln ganz oben auf der Übersichtsseite steht, wählst du direkt unter <b>Einstellungen → Persönlich → Schnellübersicht-Widgets</b>.</p>
    </div>
    <div id="tab-personal" class="tab-content">
      <div class="form-group">
        <label class="form-label">Dein Name</label>
        <input class="form-input" id="set-user-name" placeholder="z.B. Max" maxlength="40">
        <p class="form-hint">Erscheint als persönliche Begrüßung oben im Kopfbereich, abhängig von der Tageszeit.</p>
      </div>
      <div class="divider"></div>
      <div class="form-group">
        <label class="form-label">Akzentfarbe der Oberfläche</label>
        <div class="accent-swatch-row" id="accent-swatch-row">
          <button type="button" class="accent-swatch" data-accent="amber" style="--swatch:#e8a34c" onclick="setPersonalizationAccent('amber')" title="Bernstein"></button>
          <button type="button" class="accent-swatch" data-accent="teal" style="--swatch:#4cc7c2" onclick="setPersonalizationAccent('teal')" title="Türkis"></button>
          <button type="button" class="accent-swatch" data-accent="violet" style="--swatch:#a78bd8" onclick="setPersonalizationAccent('violet')" title="Violett"></button>
          <button type="button" class="accent-swatch" data-accent="moss" style="--swatch:#7bbf6e" onclick="setPersonalizationAccent('moss')" title="Moos"></button>
          <button type="button" class="accent-swatch" data-accent="rose" style="--swatch:#e2685f" onclick="setPersonalizationAccent('rose')" title="Rose"></button>
        </div>
        <p class="form-hint">Wirkt auf Navigation, Buttons und Hervorhebungen — sofort sichtbar, unabhängig vom Hintergrund.</p>
      </div>
      <div class="divider"></div>
      <div class="form-row">
        <div class="form-group">
          <label class="form-label">Anzeigedichte</label>
          <div class="segmented" id="density-toggle">
            <button type="button" class="seg-btn" data-val="comfortable" onclick="setPersonalizationDensity('comfortable')">Komfortabel</button>
            <button type="button" class="seg-btn" data-val="compact" onclick="setPersonalizationDensity('compact')">Kompakt</button>
          </div>
        </div>
        <div class="form-group">
          <label class="form-label">Uhrzeitformat</label>
          <div class="segmented" id="timeformat-toggle">
            <button type="button" class="seg-btn" data-val="24h" onclick="setPersonalizationTimeFormat('24h')">24h</button>
            <button type="button" class="seg-btn" data-val="12h" onclick="setPersonalizationTimeFormat('12h')">12h</button>
          </div>
        </div>
      </div>
      <div class="divider"></div>
      <div class="form-group">
        <label class="form-label">Schnellübersicht-Widgets</label>
        <div class="widget-slot-row" id="widget-slot-row">
          <div>
            <label class="form-label-sm">Widget 1</label>
            <select class="form-input" id="set-widget-0" onchange="setPersonalizationWidget(0, this.value)"></select>
          </div>
          <div>
            <label class="form-label-sm">Widget 2</label>
            <select class="form-input" id="set-widget-1" onchange="setPersonalizationWidget(1, this.value)"></select>
          </div>
          <div>
            <label class="form-label-sm">Widget 3</label>
            <select class="form-input" id="set-widget-2" onchange="setPersonalizationWidget(2, this.value)"></select>
          </div>
          <div>
            <label class="form-label-sm">Widget 4</label>
            <select class="form-input" id="set-widget-3" onchange="setPersonalizationWidget(3, this.value)"></select>
          </div>
        </div>
        <p class="form-hint">Bestimmt, welche Kennzahl in welcher der vier Kacheln ganz oben auf der Übersichtsseite steht — frei wählbar, z.B. den Tagesbericht auf Widget 4. Wirkt sofort, kein Neuladen nötig.</p>
      </div>
      <p class="form-hint">Tipp: Die vier Kacheln der Schnellübersicht lassen sich auch direkt auf der Übersichtsseite per Ziehen neu anordnen. Mit <kbd>Strg</kbd>+<kbd>K</kbd> oder <kbd>/</kbd> öffnest du jederzeit die Befehlspalette; <kbd>g</kbd> gefolgt von <kbd>o</kbd>/<kbd>e</kbd>/<kbd>m</kbd>/<kbd>k</kbd>/<kbd>f</kbd> springt direkt zu einer Seite.</p>
    </div>
    <div id="tab-data" class="tab-content">
      <p class="form-hint" style="margin-top:0">Lade Verlaufsdaten als CSV herunter — z.B. für eine eigene Auswertung in Excel/LibreOffice/Pandas oder als Backup.</p>
      <div class="form-group">
        <label class="form-label">Energie-Verlauf</label>
        <div class="form-row">
          <a class="btn btn-secondary btn-sm" href="/api/export/energy.csv?hours=24" download>Letzte 24h</a>
          <a class="btn btn-secondary btn-sm" href="/api/export/energy.csv?hours=168" download>Letzte 7 Tage</a>
        </div>
      </div>
      <div class="divider"></div>
      <div class="form-group">
        <label class="form-label">Tages-Zusammenfassung</label>
        <a class="btn btn-secondary btn-sm" href="/api/export/daily-summary.csv?days=90" download>Letzte 90 Tage</a>
        <p class="form-hint">Enthält PV-Ertrag, Eigenverbrauch, Einspeisung, Netzbezug und Batterie-Ø je Tag.</p>
      </div>
      <div class="divider"></div>
      <p class="form-hint">Miner-Statistiken lassen sich pro Gerät direkt aus dessen Detailansicht exportieren (<code>/api/export/miner-stats/&lt;id&gt;.csv</code>).</p>
    </div>
    <div class="form-actions"><button class="btn btn-ghost" onclick="closeModal('settings-modal')">Schließen</button><button class="btn btn-primary" onclick="saveSettings();savePersonalTab()">Speichern</button></div>
  </div>
</div>

<!-- Calendar -->
<div class="modal-overlay" id="calendar-modal">
  <div class="modal">
    <div class="modal-header"><h3>Neuer Termin</h3><button class="modal-close" onclick="closeModal('calendar-modal')">✕</button></div>
    <div class="form-group"><label class="form-label">Titel *</label><input class="form-input" id="cal-title" placeholder="Termintitel"></div>
    <div class="form-group"><label class="form-label">Beschreibung</label><input class="form-input" id="cal-desc" placeholder="Optional"></div>
    <div class="form-group"><label class="form-label">Startzeit *</label><input class="form-input" id="cal-start" type="datetime-local"></div>
    <div class="form-actions"><button class="btn btn-ghost" onclick="closeModal('calendar-modal')">Abbrechen</button><button class="btn btn-primary" onclick="addCalendarEvent()">Speichern</button></div>
  </div>
</div>

<!-- Notifications -->
<div class="modal-overlay" id="notif-modal">
  <div class="modal">
    <div class="modal-header"><h3>Benachrichtigungen</h3><button class="modal-close" onclick="closeModal('notif-modal')">✕</button></div>
    <div class="modal-toolbar"><button class="btn btn-ghost btn-sm" onclick="markAllNotifRead()">Alle als gelesen markieren</button></div>
    <div id="notif-list"><div class="empty-state">Keine Benachrichtigungen</div></div>
  </div>
</div>

<!-- Miner Detail -->
<div class="modal-overlay" id="miner-detail-modal">
  <div class="modal modal-wide">
    <div class="modal-header">
      <div>
        <h3 id="md-name">Miner</h3>
        <div class="modal-subtitle" id="md-ip">–</div>
      </div>
      <button class="modal-close" onclick="closeModal('miner-detail-modal')">✕</button>
    </div>

    <div class="md-hero">
      <div class="md-hero-stat">
        <span class="badge badge-offline" id="md-status-badge">Offline</span>
      </div>
      <div class="md-hero-stat">
        <div class="md-hero-value" id="md-hashrate">– TH/s</div>
        <div class="md-hero-label">Hashrate</div>
      </div>
      <div class="md-hero-stat">
        <div class="md-hero-value" id="md-temp">–°C</div>
        <div class="md-hero-label">Temperatur</div>
      </div>
      <div class="md-hero-stat">
        <div class="md-hero-value" id="md-power">– W</div>
        <div class="md-hero-label">Verbrauch</div>
      </div>
      <div class="md-hero-stat">
        <div class="md-hero-value" id="md-efficiency">– J/TH</div>
        <div class="md-hero-label">Effizienz</div>
      </div>
    </div>

    <div class="form-actions" style="margin-top:0;margin-bottom:18px;justify-content:flex-start">
      <button class="btn btn-green btn-sm" id="md-start-btn" onclick="minerDetailToggle('resume')">▶ Starten</button>
      <button class="btn btn-red btn-sm" id="md-stop-btn" onclick="minerDetailToggle('pause')">⏸ Stoppen</button>
      <button class="btn btn-ghost btn-sm" onclick="refreshMinerDetail()">↻ Aktualisieren</button>
    </div>

    <div class="tabs">
      <button class="tab-btn active" onclick="switchTab(event,'md-tab-history','miner-detail-modal')">Verlauf</button>
      <button class="tab-btn" onclick="switchTab(event,'md-tab-settings','miner-detail-modal')">Einstellungen</button>
      <button class="tab-btn" onclick="switchTab(event,'md-tab-log','miner-detail-modal')">Log</button>
    </div>

    <div id="md-tab-history" class="tab-content active">
      <div class="md-chart-row">
        <div>
          <div class="md-chart-title">Hashrate</div>
          <div class="chart-box sm"><canvas id="md-hashrate-chart"></canvas></div>
        </div>
        <div>
          <div class="md-chart-title">Temperatur</div>
          <div class="chart-box sm"><canvas id="md-temp-chart"></canvas></div>
        </div>
      </div>
    </div>

    <div id="md-tab-settings" class="tab-content">
      <input type="hidden" id="md-miner-id">
      <div class="form-group">
        <label class="form-label">Firmware</label>
        <div class="segmented" id="md-firmware-toggle">
          <button type="button" class="seg-btn" data-val="braiins" onclick="setFirmwareToggle('md',this)">Braiins OS</button>
          <button type="button" class="seg-btn" data-val="bitmain" onclick="setFirmwareToggle('md',this)">Bitmain Stock</button>
        </div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Name</label><input class="form-input" id="md-edit-name"></div>
        <div class="form-group"><label class="form-label">IP-Adresse</label><input class="form-input" id="md-edit-ip"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">API-Port</label><input class="form-input" id="md-edit-port" type="number"></div>
        <div class="form-group"><label class="form-label">Verbrauch (W)</label><input class="form-input" id="md-edit-watts" type="number"></div>
      </div>
      <div class="form-row" id="md-bitmain-webport-row" hidden>
        <div class="form-group"><label class="form-label">Web-UI Port</label><input class="form-input" id="md-edit-webport" type="number"></div>
        <div class="form-group"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label" id="md-user-label">Benutzer</label><input class="form-input" id="md-edit-user"></div>
        <div class="form-group"><label class="form-label" id="md-pass-label">Passwort</label><input class="form-input" id="md-edit-pass" type="password" placeholder="Unverändert lassen, wenn leer"></div>
      </div>
      <div class="form-group">
        <label class="form-label">Automatik-Auslöser</label>
        <select class="form-input" id="md-edit-trigger-source" onchange="updateTriggerLabels('md-edit', true)">
          <option value="pv_surplus">PV-Überschuss (Einspeisung)</option>
          <option value="grid_import">Netzbezug</option>
          <option value="pv_production">PV-Rohleistung (Erzeugung)</option>
          <option value="battery_soc">Batterie-Ladezustand</option>
        </select>
        <p class="form-hint" id="md-edit-trigger-hint" style="margin-bottom:0"></p>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label" id="md-edit-threshold-on-label">Einschalten ab (W)</label><input class="form-input" id="md-edit-threshold-on" type="number"></div>
        <div class="form-group"><label class="form-label" id="md-edit-threshold-off-label">Ausschalten bei (W)</label><input class="form-input" id="md-edit-threshold-off" type="number"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Priorität</label><input class="form-input" id="md-edit-priority" type="number" min="1" max="10"></div>
        <div class="form-group"><label class="form-label">Notiz</label><input class="form-input" id="md-edit-note"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Min. Laufzeit (Sek.)</label><input class="form-input" id="md-edit-min-runtime" type="number"></div>
        <div class="form-group"><label class="form-label">Min. Aus-Zeit (Sek.)</label><input class="form-input" id="md-edit-min-offtime" type="number"></div>
      </div>
      <div class="form-group">
        <div class="toggle-switch"><label class="toggle"><input type="checkbox" id="md-edit-automation"><span class="toggle-slider"></span></label><span>Automatische Steuerung für diesen Miner</span></div>
      </div>
      <div class="form-actions">
        <button class="btn btn-red" onclick="deleteMinerFromDetail()">Miner löschen</button>
        <button class="btn btn-primary" onclick="saveMinerDetailEdit()">Speichern</button>
      </div>
    </div>

    <div id="md-tab-log" class="tab-content">
      <div id="md-event-list"><div class="empty-state">Lädt Verlauf …</div></div>
    </div>
  </div>
</div>

<!-- Hintergrund personalisieren -->
<div class="modal-overlay" id="background-modal">
  <div class="modal">
    <div class="modal-header"><h3>Hintergrund personalisieren</h3><button class="modal-close" onclick="closeModal('background-modal')">✕</button></div>
    <div class="form-group">
      <label class="form-label">Modus</label>
      <div class="bg-mode-grid" id="bg-mode-grid">
        <button type="button" class="bg-mode-card" data-mode="weather" onclick="selectBackgroundMode('weather')">
          <div class="bg-mode-preview bg-preview-weather"></div>
          <div class="bg-mode-name">Wetter</div>
          <div class="bg-mode-desc">Sanfter Verlauf je nach aktuellem Wetter (Regen, Schnee, Sonne, Wolken)</div>
        </button>
        <button type="button" class="bg-mode-card" data-mode="time" onclick="selectBackgroundMode('time')">
          <div class="bg-mode-preview bg-preview-time"></div>
          <div class="bg-mode-name">Tageszeit</div>
          <div class="bg-mode-desc">Verlauf wandert mit Morgen, Mittag, Abend und Nacht</div>
        </button>
        <button type="button" class="bg-mode-card" data-mode="static" onclick="selectBackgroundMode('static')">
          <div class="bg-mode-preview bg-preview-static"></div>
          <div class="bg-mode-name">Farbakzent</div>
          <div class="bg-mode-desc">Feste Akzentfarbe deiner Wahl</div>
        </button>
        <button type="button" class="bg-mode-card" data-mode="off" onclick="selectBackgroundMode('off')">
          <div class="bg-mode-preview bg-preview-off"></div>
          <div class="bg-mode-name">Aus</div>
          <div class="bg-mode-desc">Einfarbiger Hintergrund ohne Verlauf</div>
        </button>
      </div>
    </div>
    <div class="form-group" id="bg-accent-group" hidden>
      <label class="form-label">Akzentfarbe</label>
      <div class="bg-accent-row" id="bg-accent-row">
        <button type="button" class="bg-accent-swatch" data-accent="amber" style="--swatch:#e8a34c" onclick="selectBackgroundAccent('amber')"></button>
        <button type="button" class="bg-accent-swatch" data-accent="teal" style="--swatch:#4cc7c2" onclick="selectBackgroundAccent('teal')"></button>
        <button type="button" class="bg-accent-swatch" data-accent="violet" style="--swatch:#a78bd8" onclick="selectBackgroundAccent('violet')"></button>
        <button type="button" class="bg-accent-swatch" data-accent="moss" style="--swatch:#7bbf6e" onclick="selectBackgroundAccent('moss')"></button>
        <button type="button" class="bg-accent-swatch" data-accent="rose" style="--swatch:#e2685f" onclick="selectBackgroundAccent('rose')"></button>
      </div>
    </div>
    <div class="form-actions">
      <button class="btn btn-ghost" onclick="closeModal('background-modal')">Schließen</button>
      <button class="btn btn-primary" onclick="saveBackgroundSettings()">Speichern</button>
    </div>
  </div>
</div>

<!-- Zonen-Editor -->
<div class="modal-overlay" id="zones-modal">
  <div class="modal modal-wide">
    <div class="modal-header"><h3 id="zones-modal-title">Zonen</h3><button class="modal-close" onclick="closeModal('zones-modal')">✕</button></div>
    <p class="form-hint" style="margin-top:0">Klicke nacheinander auf mindestens 3 Punkte im Bild, um eine Zone einzuzeichnen, vergib einen Namen und schließe sie ab. Nur innerhalb einer Zone erkannte Objekte lösen Ereignisse und Benachrichtigungen aus.</p>
    <div class="zone-editor-layout">
      <div class="zone-editor-canvas-wrap">
        <canvas id="zones-canvas" onclick="zoneEditorCanvasClick(event)"></canvas>
      </div>
      <div class="zone-editor-side">
        <div class="form-group">
          <label class="form-label">Name der neuen Zone</label>
          <input class="form-input" id="zones-new-name" value="Zone 1">
        </div>
        <div class="form-actions" style="margin:0 0 14px;justify-content:flex-start;flex-wrap:wrap">
          <button class="btn btn-ghost btn-sm" onclick="zoneEditorUndoPoint()">Letzten Punkt entfernen</button>
          <button class="btn btn-ghost btn-sm" onclick="zoneEditorCancelCurrent()">Zone verwerfen</button>
          <button class="btn btn-primary btn-sm" onclick="zoneEditorFinishZone()">Zone abschließen</button>
        </div>
        <div class="divider" style="margin:10px 0"></div>
        <label class="form-label">Gespeicherte Zonen</label>
        <div id="zones-list" class="zone-list"></div>
        <button class="btn btn-ghost btn-sm" style="margin-top:10px" onclick="zoneEditorWholeFrame()">Ganzes Bild als Zone hinzufügen</button>
      </div>
    </div>
    <div class="form-actions">
      <button class="btn btn-ghost" onclick="closeModal('zones-modal')">Abbrechen</button>
      <button class="btn btn-primary" onclick="saveZonesEditor()">Zonen speichern</button>
    </div>
  </div>
</div>


<script src="/static/dashboard.js"></script>
</body>
</html>

'''

DASHBOARD_CSS = r'''
/* ════════════════════════════════════════════════════════════════
   SMARTHOME DASHBOARD — Design Tokens
   Palette: gedecktes Anthrazit-Grün als Basis (nicht reines Schwarz/Blau),
   Bernstein als PV-/Energie-Akzent, Türkis als Daten-/Netzakzent.
   Display-Font (Space Grotesk) trägt die großen Messwerte, Inter für Text,
   JetBrains Mono für IPs/Zahlenreihen.
   ════════════════════════════════════════════════════════════════ */
:root {
  --bg-base:      #0f1410;
  --bg-raised:    #161d17;
  --bg-card:      #1a221c;
  --bg-card-hi:   #202a21;
  --border:       #2a352b;
  --border-soft:  #232d24;

  --text-primary:   #eef2ea;
  --text-secondary: #9aab9a;
  --text-muted:     #6b7a6a;

  --amber:        #e8a34c;
  --amber-soft:   #e8a34c22;
  --amber-deep:   #c97f2c;
  --teal:         #4cc7c2;
  --teal-soft:    #4cc7c222;
  --moss:         #7bbf6e;
  --moss-soft:    #7bbf6e22;
  --rose:         #e2685f;
  --rose-soft:    #e2685f22;
  --violet:       #a78bd8;
  --violet-soft:  #a78bd822;
  --gold:         #d9b54a;

  --grad-amber: linear-gradient(135deg, #e8a34c, #c97f2c);
  --grad-moss:  linear-gradient(135deg, #7bbf6e, #4a9b50);
  --grad-teal:  linear-gradient(135deg, #4cc7c2, #2d8e8a);
  --grad-rose:  linear-gradient(135deg, #e2685f, #b5423a);

  --radius-lg: 16px;
  --radius-md: 12px;
  --radius-sm: 8px;
  --shadow-card: 0 8px 28px -8px rgba(0,0,0,0.45);

  --font-display: 'Space Grotesk', system-ui, sans-serif;
  --font-body: 'Inter', system-ui, sans-serif;
  --font-mono: 'JetBrains Mono', monospace;

  /* Persönlicher Akzent — per Einstellungen > Persönlich wählbar,
     überschreibt NICHT die funktionalen Semantik-Farben (PV=amber,
     Netz=teal, Batterie=moss, ...), sondern nur UI-Hervorhebungen wie
     aktive Navigation, primäre Buttons und Fokus-Ringe. */
  --user-accent: var(--amber);
  --user-accent-soft: var(--amber-soft);
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  background: var(--bg-base);
  color: var(--text-primary);
  font-family: var(--font-body);
  font-size: 14px;
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}

/* ── Personalisierbarer Hintergrund ──────────────────────────────
   #bg-layer sitzt fix hinter allem Inhalt. Welcher Gradient zum Tragen
   kommt, steuern zwei Klassen am Element: bg-mode-* (weather/time/static/off)
   und bei weather zusätzlich bg-cat-* (clear/cloudy/rain/snow/fog/thunder).
   Alles rein CSS, sehr performant, keine Partikel-Animation - bewusst
   subtil gehalten (sanfte Farbverläufe statt Effekt-Spielerei). */
#bg-layer {
  position: fixed; inset: 0; z-index: 0; pointer-events: none;
  transition: background 1.2s ease;
}

/* Aus: einfarbig, kein Verlauf */
#bg-layer.bg-mode-off { background: var(--bg-base); }

/* Statisch: fester Akzent, subtiler Radial-Verlauf oben */
#bg-layer.bg-mode-static {
  background:
    radial-gradient(circle at 18% 0%, var(--bg-accent-color, var(--amber)) 0%, transparent 45%),
    radial-gradient(circle at 85% 10%, var(--bg-accent-color, var(--amber)) 0%, transparent 35%),
    var(--bg-base);
}

/* Tageszeit: vier ruhige Verlaufsstufen, gesteuert per data-time Attribut */
#bg-layer.bg-mode-time[data-time="morning"] {
  background: linear-gradient(160deg, #3a3320 0%, #1a2418 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-time[data-time="day"] {
  background: linear-gradient(160deg, #2a3422 0%, #182018 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-time[data-time="evening"] {
  background: linear-gradient(160deg, #3d2a22 0%, #1f1a1c 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-time[data-time="night"] {
  background: linear-gradient(160deg, #141a26 0%, #10140f 45%, var(--bg-base) 100%);
}

/* Wetter: Verlauf je Kategorie, leicht moduliert per Tag/Nacht über data-time */
#bg-layer.bg-mode-weather.bg-cat-clear {
  background: linear-gradient(160deg, #3a3018 0%, #1d2417 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-clear[data-time="night"] {
  background: linear-gradient(160deg, #161d2c 0%, #11150f 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-cloudy {
  background: linear-gradient(160deg, #2b3038 0%, #1a201d 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-rain {
  background: linear-gradient(160deg, #1f2c38 0%, #16201d 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-snow {
  background: linear-gradient(160deg, #2a3340 0%, #1c2420 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-fog {
  background: linear-gradient(160deg, #262c2e 0%, #1a201d 45%, var(--bg-base) 100%);
}
#bg-layer.bg-mode-weather.bg-cat-thunder {
  background: linear-gradient(160deg, #241f30 0%, #181a1d 45%, var(--bg-base) 100%);
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; }
}

:focus-visible { outline: 2px solid var(--user-accent, var(--teal)); outline-offset: 2px; border-radius: 4px; }

/* ── Loading ──────────────────────────────────────────────────── */
#loading-overlay {
  position: fixed; inset: 0; background: var(--bg-base); z-index: 9999;
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  gap: 22px; transition: opacity .5s;
}
#loading-overlay.hidden { opacity: 0; pointer-events: none; }
.sun-loader { position: relative; width: 64px; height: 64px; }
.sun-core { position: absolute; inset: 18px; background: var(--grad-amber); border-radius: 50%; box-shadow: 0 0 30px #e8a34c55; }
.sun-ray { position: absolute; inset: 0; border: 2px solid var(--amber-soft); border-radius: 50%; animation: pulse-ray 1.8s ease-out infinite; }
.sun-ray.r2 { animation-delay: .4s; } .sun-ray.r3 { animation-delay: .8s; } .sun-ray.r4 { animation-delay: 1.2s; }
@keyframes pulse-ray { 0% { transform: scale(0.4); opacity: .9; } 100% { transform: scale(1.4); opacity: 0; } }
.loading-text { color: var(--text-secondary); font-family: var(--font-display); font-size: 15px; letter-spacing: .02em; }

/* ── App Shell (Sidebar + Main) ───────────────────────────────── */
.app-shell { display: flex; min-height: 100vh; position: relative; z-index: 1; }

.sidebar {
  width: 232px; flex-shrink: 0; background: var(--bg-raised); border-right: 1px solid var(--border-soft);
  display: flex; flex-direction: column; position: sticky; top: 0; height: 100vh; z-index: 200;
  transition: width .2s ease;
}
.sidebar-brand { display: flex; align-items: center; gap: 10px; padding: 20px 18px; position: relative; }
.sidebar-brand .brand-mark { font-size: 22px; color: var(--user-accent, var(--amber)); flex-shrink: 0; transition: color .2s; }
.sidebar-brand .brand-name { font-family: var(--font-display); font-weight: 600; font-size: 17px; letter-spacing: -0.01em; white-space: nowrap; overflow: hidden; }
.sidebar-collapse-btn { margin-left: auto; background: none; border: none; color: var(--text-muted); cursor: pointer; width: 24px; height: 24px; border-radius: 6px; display: flex; align-items: center; justify-content: center; transition: all .15s; flex-shrink: 0; }
.sidebar-collapse-btn:hover { color: var(--text-primary); background: var(--bg-card-hi); }
.sidebar-collapse-btn svg { transition: transform .2s; }

.sidebar-nav { display: flex; flex-direction: column; gap: 2px; padding: 8px 12px; flex: 1; overflow-y: auto; }
.sidebar-footer { padding: 12px; border-top: 1px solid var(--border-soft); display: flex; flex-direction: column; gap: 2px; }

.nav-item {
  display: flex; align-items: center; gap: 12px; width: 100%; background: none; border: none;
  color: var(--text-secondary); padding: 10px 12px; border-radius: var(--radius-sm); cursor: pointer;
  font-size: 13px; font-weight: 500; font-family: var(--font-body); text-align: left; transition: all .15s;
  position: relative; white-space: nowrap;
}
.nav-item:hover { background: var(--bg-card-hi); color: var(--text-primary); }
.nav-item.active { background: var(--user-accent-soft, var(--amber-soft)); color: var(--user-accent, var(--amber)); }
.nav-icon { flex-shrink: 0; opacity: .85; }
.nav-label { overflow: hidden; }
.nav-item .notif-badge { position: static; margin-left: auto; }

.sidebar-status { display: flex; align-items: center; gap: 10px; padding: 10px 12px; font-size: 12px; color: var(--text-muted); }

/* Collapsed state (desktop) */
.sidebar.collapsed { width: 72px; }
.sidebar.collapsed .brand-name,
.sidebar.collapsed .nav-label,
.sidebar.collapsed .sidebar-status .nav-label { display: none; }
.sidebar.collapsed .sidebar-collapse-btn svg { transform: rotate(180deg); }
.sidebar.collapsed .nav-item { justify-content: center; }
.sidebar.collapsed .sidebar-brand { justify-content: center; padding: 20px 8px; }
.sidebar.collapsed .sidebar-collapse-btn { position: absolute; right: -12px; top: 22px; background: var(--bg-card); border: 1px solid var(--border); width: 22px; height: 22px; }

.main-area { flex: 1; min-width: 0; display: flex; flex-direction: column; }

/* ── Header (within main area) ───────────────────────────────── */
header {
  background: var(--bg-raised); border-bottom: 1px solid var(--border-soft);
  padding: 16px 28px; display: flex; align-items: center; gap: 14px;
  position: sticky; top: 0; z-index: 100; backdrop-filter: blur(10px);
}
.header-titles { flex: 1; display: flex; flex-direction: column; gap: 1px; min-width: 0; }
.page-title { font-family: var(--font-display); font-size: 19px; font-weight: 600; letter-spacing: -0.01em; }
.header-greeting { font-size: 11.5px; color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.header-greeting:empty { display: none; }
.mobile-menu-btn { display: none; background: none; border: 1px solid var(--border); color: var(--text-secondary); width: 36px; height: 36px; border-radius: var(--radius-sm); cursor: pointer; align-items: center; justify-content: center; flex-shrink: 0; }
.header-right { display: flex; align-items: center; gap: 10px; }
.header-time { font-family: var(--font-mono); font-size: 13px; color: var(--text-secondary); }
.cmdk-btn {
  display: flex; align-items: center; gap: 7px; background: var(--bg-card); border: 1px solid var(--border);
  color: var(--text-secondary); padding: 7px 10px; border-radius: var(--radius-sm); cursor: pointer;
  font-size: 12.5px; font-family: var(--font-body); transition: border-color .15s, color .15s, transform .1s;
}
.cmdk-btn:hover { color: var(--user-accent, var(--amber)); border-color: var(--user-accent, var(--amber)); transform: translateY(-1px); }
.cmdk-btn kbd { background: var(--bg-base); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px; font-family: var(--font-mono); font-size: 10px; color: var(--text-muted); }
@media (max-width: 700px) { .cmdk-btn span, .cmdk-btn kbd { display: none; } .header-greeting { display: none; } }
.icon-btn { background: none; border: 1px solid var(--border); color: var(--text-secondary); width: 36px; height: 36px; border-radius: var(--radius-sm); cursor: pointer; display: flex; align-items: center; justify-content: center; transition: all .15s; position: relative; }
.icon-btn:hover { background: var(--bg-card-hi); color: var(--text-primary); border-color: var(--amber); }
.notif-badge { background: var(--rose); color: #1a0a08; min-width: 16px; height: 16px; padding: 0 4px; border-radius: 8px; font-size: 10px; display: flex; align-items: center; justify-content: center; font-weight: 700; }

/* ── Pages ────────────────────────────────────────────────────── */
.page { display: none; flex-direction: column; gap: 22px; }
.page.active { display: flex; }

/* ── Quickstats (Übersichts-Kopfzeile) ───────────────────────────*/
.quickstats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.quickstat { background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-lg); padding: 16px 18px; display: flex; align-items: center; gap: 14px; box-shadow: var(--shadow-card); }
.quickstat-icon { width: 42px; height: 42px; border-radius: 11px; display: flex; align-items: center; justify-content: center; font-size: 19px; flex-shrink: 0; }
.quickstat-icon.sun { background: var(--amber-soft); }
.quickstat-icon.teal { background: var(--teal-soft); }
.quickstat-icon.violet { background: var(--violet-soft); }
.quickstat-icon.gold { background: #d9b54a22; }
.quickstat-icon.rose { background: var(--rose-soft); }
.quickstat-icon.moss { background: var(--moss-soft); }
.quickstat-value.quickstat-value-sm { font-size: 13px; font-weight: 500; line-height: 1.35; }
.quickstat-value { font-family: var(--font-display); font-size: 21px; font-weight: 600; font-variant-numeric: tabular-nums; line-height: 1.2; }
.quickstat-label { font-size: 11px; color: var(--text-muted); margin-top: 1px; }

/* ── Morgennachricht-Widget (eigenes Layout: Kopf + Mini-Kurve + PV-Zeile) ── */
.quickstat-mm { flex-direction: column; align-items: stretch; gap: 8px; }
.mm-head { display: flex; align-items: center; gap: 12px; }
.mm-head-text { min-width: 0; }
.mm-sub { font-size: 12px; color: var(--text-secondary); margin-top: 1px; }
.mm-chart { width: 100%; }
.mm-chart-svg { width: 100%; height: 70px; display: block; overflow: visible; }
.mm-chart-area { fill: var(--amber-soft); }
.mm-chart-line { fill: none; stroke: var(--amber); stroke-width: 1.6; stroke-linecap: round; stroke-linejoin: round; }
.mm-chart-dot { fill: var(--bg-card); stroke: var(--amber); stroke-width: 1.8; }
.mm-chart-temp { font-size: 10.5px; font-weight: 700; fill: var(--text-primary); font-family: var(--font-display); }
.mm-chart-name { font-size: 8.5px; fill: var(--text-muted); text-transform: uppercase; letter-spacing: .04em; }
.mm-pv { display: flex; align-items: center; gap: 14px; font-size: 12.5px; color: var(--text-secondary); border-top: 1px solid var(--border-soft); padding-top: 8px; }
.mm-pv-value, .mm-pv-rain { display: inline-flex; align-items: center; gap: 5px; font-weight: 600; color: var(--text-primary); }
.mm-pv-value svg, .mm-pv-rain svg { width: 14px; height: 14px; color: var(--amber); flex-shrink: 0; }
.mm-pv-rain svg { color: var(--teal); }

.link-btn-nav { background: none; border: none; color: var(--teal); cursor: pointer; font-size: 12px; font-weight: 600; padding: 0; }
.link-btn-nav:hover { color: var(--amber); }


main { padding: 24px 28px 60px; max-width: 1500px; width: 100%; margin: 0 auto; display: flex; flex-direction: column; gap: 22px; }
.block { display: flex; flex-direction: column; gap: 14px; }
.block-head { display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
.block-head h2 { font-family: var(--font-display); font-size: 16px; font-weight: 600; letter-spacing: -0.005em; display: flex; align-items: baseline; gap: 10px; }
.block-head-faded { font-family: var(--font-body); font-size: 11px; font-weight: 400; color: var(--text-muted); text-transform: none; }
.block-tag { font-size: 11px; color: var(--text-muted); background: var(--bg-card); padding: 3px 10px; border-radius: 20px; border: 1px solid var(--border); }
.block-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }

.card { background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-lg); padding: 20px; box-shadow: var(--shadow-card); }

.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px; }
.grid-3-1 { display: grid; grid-template-columns: 2fr 1fr; gap: 18px; }

/* ── Metric Cards (Energie-Übersicht) ────────────────────────── */
.energy-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
.energy-grid.secondary { grid-template-columns: repeat(3, 1fr); }

.metric-card {
  background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-md);
  padding: 18px; display: flex; flex-direction: column; gap: 6px; position: relative; overflow: hidden;
  transition: border-color .2s, transform .2s;
}
.metric-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: var(--border); }
.metric-card.sun::before { background: var(--grad-amber); }
.metric-card.teal::before { background: var(--grad-teal); }
.metric-card.green::before { background: var(--grad-moss); }
.metric-card:hover { border-color: var(--border); transform: translateY(-1px); }
.metric-icon { font-size: 17px; opacity: .55; position: absolute; top: 16px; right: 16px; }
.metric-label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .06em; }
.metric-value { font-family: var(--font-display); font-size: 30px; font-weight: 600; font-variant-numeric: tabular-nums; line-height: 1.1; }
.metric-value .unit { font-size: 15px; font-weight: 500; color: var(--text-secondary); margin-left: 2px; }
.metric-sub { font-size: 12px; color: var(--text-secondary); }
.metric-card.sun .metric-value { color: var(--amber); }
.metric-card.teal .metric-value { color: var(--teal); }
.metric-card.green .metric-value { color: var(--moss); }
.metric-card.red .metric-value { color: var(--rose); }

.metric-card-sm { background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-md); padding: 14px 16px; display: flex; flex-direction: column; gap: 4px; border-left: 3px solid var(--border); }
.metric-card-sm.purple { border-left-color: var(--violet); }
.metric-card-sm.red { border-left-color: var(--rose); }
.metric-card-sm.yellow { border-left-color: var(--gold); }
.metric-card-sm.teal { border-left-color: var(--teal); }
.metric-value-sm { font-family: var(--font-display); font-size: 21px; font-weight: 600; font-variant-numeric: tabular-nums; }
.metric-foot { font-size: 11px; color: var(--text-muted); }

.meter { height: 5px; background: var(--bg-base); border-radius: 3px; overflow: hidden; margin-top: 4px; }
.meter.thin { height: 4px; }
.meter-fill { height: 100%; border-radius: 3px; width: 0%; transition: width .6s cubic-bezier(.4,0,.2,1); }
.meter-fill.sun { background: var(--grad-amber); }
.meter-fill.green { background: var(--grad-moss); }
.meter-fill.teal { background: var(--grad-teal); }

/* ── Charts ───────────────────────────────────────────────────── */
.chart-box { position: relative; height: 230px; }
.chart-box.sm { height: 165px; }

/* ── Strompreis / BTC ─────────────────────────────────────────── */
.price-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; gap: 14px; flex-wrap: wrap; }
.price-now { font-family: var(--font-display); font-size: 30px; font-weight: 600; color: var(--amber); font-variant-numeric: tabular-nums; }
.price-meta { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
.price-side { display: flex; flex-direction: column; gap: 8px; align-items: flex-end; text-align: right; }
.price-side-row { display: flex; flex-direction: column; gap: 1px; }
.price-side-label { font-size: 11px; color: var(--text-muted); }
.price-side-val { font-family: var(--font-display); font-weight: 600; font-size: 16px; }
.price-side-val.good { color: var(--moss); }
.price-side-sub { font-size: 11px; color: var(--text-muted); }

.btc-top { display: flex; gap: 28px; margin-bottom: 16px; flex-wrap: wrap; }
.btc-eur { font-family: var(--font-display); font-size: 26px; font-weight: 600; color: var(--gold); font-variant-numeric: tabular-nums; }
.btc-usd { font-family: var(--font-display); font-size: 17px; font-weight: 500; color: var(--text-secondary); font-variant-numeric: tabular-nums; }
.btc-change { font-family: var(--font-display); font-size: 17px; font-weight: 600; font-variant-numeric: tabular-nums; }

/* ── Cameras ──────────────────────────────────────────────────── */
.surv-list { display: flex; flex-direction: column; gap: 6px; }
.surv-row { display: flex; align-items: center; gap: 10px; background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-md); padding: 9px 14px; font-size: 13px; }
.surv-row-name { flex: 1; color: var(--text-secondary); }
.surv-row-value { font-weight: 600; color: var(--text-primary); }

.heim-classes-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 8px; margin: 12px 0 16px; }
.heim-class-chip { display: flex; align-items: center; gap: 8px; background: var(--bg-card); border: 1px solid var(--border-soft); border-radius: var(--radius-md); padding: 8px 12px; font-size: 13px; color: var(--text-secondary); cursor: pointer; transition: border-color .15s; }
.heim-class-chip:has(input:checked) { border-color: var(--amber); color: var(--text-primary); }
.heim-class-chip input { accent-color: var(--amber); }

.cameras-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; }
.camera-tile { display: flex; flex-direction: column; gap: 6px; }
.camera-tile-top { display: flex; align-items: center; justify-content: space-between; font-size: 12px; color: var(--text-secondary); }
.camera-feed { width: 100%; aspect-ratio: 16/9; background: #06090a; border-radius: var(--radius-sm); overflow: hidden; position: relative; border: 1px solid var(--border-soft); }
.camera-feed img { width: 100%; height: 100%; object-fit: cover; display: block; }
.camera-overlay { position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; background: #06090acc; font-size: 12px; color: var(--text-muted); gap: 6px; text-align: center; padding: 10px; }
.camera-label { position: absolute; top: 8px; left: 8px; background: #06090acc; padding: 2px 8px; border-radius: 5px; font-size: 11px; font-weight: 600; }
.camera-persons-badge { position: absolute; bottom: 8px; left: 8px; background: var(--rose-soft); color: var(--text-primary); padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 600; }
.camera-actions { position: absolute; top: 8px; right: 8px; display: flex; gap: 5px; }
.cam-btn { width: 28px; height: 28px; border-radius: 6px; border: none; cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 13px; background: #06090acc; color: #fff; transition: background .15s; }
.cam-btn:hover { background: #000; }
.camera-update { font-size: 10px; color: var(--text-muted); display: flex; align-items: center; gap: 5px; }
.camera-live-dot { width: 6px; height: 6px; border-radius: 50%; background: #e5484d; box-shadow: 0 0 6px #e5484d; animation: camlive-pulse 1.4s infinite; flex-shrink: 0; }
.camera-live-dot.offline { background: var(--text-muted); box-shadow: none; animation: none; }
@keyframes camlive-pulse { 0%, 100% { opacity: 1; } 50% { opacity: .25; } }

#fullscreen-overlay { display: none; position: fixed; inset: 0; background: #000; z-index: 9000; flex-direction: column; }
#fullscreen-overlay.active { display: flex; }
#fullscreen-header { padding: 12px 18px; background: linear-gradient(#000a, transparent); display: flex; align-items: center; justify-content: space-between; position: absolute; top: 0; left: 0; right: 0; z-index: 2; }
#fullscreen-video-label { font-size: 14px; font-weight: 600; color: #fff; font-family: var(--font-display); }
#fullscreen-close { background: var(--rose-soft); border: 1px solid var(--rose); color: #fff; width: 34px; height: 34px; border-radius: 8px; cursor: pointer; font-size: 16px; }
#fullscreen-img { width: 100%; height: 100%; object-fit: contain; display: block; }
#fullscreen-controls { position: absolute; bottom: 0; left: 0; right: 0; padding: 14px 18px; background: linear-gradient(transparent, #000a); display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
.fs-cam-select { display: flex; gap: 8px; flex-wrap: wrap; }
.fs-meta { display: flex; gap: 10px; align-items: center; font-size: 12px; color: var(--text-muted); }
.fs-cam-btn { padding: 7px 14px; border-radius: var(--radius-sm); border: 1px solid #ffffff33; background: #0006; color: #fff; cursor: pointer; font-size: 13px; transition: all .15s; }
.fs-cam-btn.active { background: var(--amber); border-color: var(--amber); color: #1a0f02; }

/* ── Galerie (Erkennungs-Fotos) ─────────────────────────────────────── */
.gallery-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 10px; }
.gallery-tile { position: relative; border-radius: var(--radius-sm); overflow: hidden; cursor: pointer; background: var(--panel-2); border: 1px solid var(--border); aspect-ratio: 4/3; }
.gallery-tile img { width: 100%; height: 100%; object-fit: cover; display: block; transition: transform .2s; }
.gallery-tile:hover img { transform: scale(1.04); }
.gallery-tile-meta { position: absolute; left: 0; right: 0; bottom: 0; padding: 6px 8px; background: linear-gradient(transparent, #000c); display: flex; justify-content: space-between; font-size: 10px; color: #fff; gap: 6px; }
.gallery-tile-meta span:first-child { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.gallery-tile-meta span:last-child { flex-shrink: 0; opacity: .85; }
.gallery-tile.selectable { cursor: default; }
.gallery-tile-check { position: absolute; top: 8px; left: 8px; width: 22px; height: 22px; border-radius: 6px; background: #0008; border: 2px solid #fff9; z-index: 2; display: flex; align-items: center; justify-content: center; color: #fff; font-size: 13px; }
.gallery-tile.selected { outline: 3px solid var(--amber); outline-offset: -3px; }
.gallery-tile.selected .gallery-tile-check { background: var(--amber); border-color: var(--amber); color: #1a0f02; }
.heim-gallery-select-bar { display: flex; align-items: center; justify-content: space-between; background: var(--panel-2); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 10px 14px; margin-bottom: 12px; font-size: 12px; color: var(--text-secondary); }
#gallery-lightbox { display: none; position: fixed; inset: 0; background: #000; z-index: 9100; flex-direction: column; }
#gallery-lightbox.active { display: flex; }
#gallery-lightbox #gallery-lightbox-img { width: 100%; height: 100%; object-fit: contain; display: block; }
.gallery-lightbox-header { padding: 12px 18px; background: linear-gradient(#000a, transparent); display: flex; align-items: center; justify-content: space-between; position: absolute; top: 0; left: 0; right: 0; z-index: 2; color: #fff; font-size: 13px; }
.gallery-lightbox-close { background: var(--rose-soft); border: 1px solid var(--rose); color: #fff; width: 34px; height: 34px; border-radius: 8px; cursor: pointer; font-size: 16px; }
.gallery-lightbox-controls { position: absolute; bottom: 0; left: 0; right: 0; padding: 14px 18px; background: linear-gradient(transparent, #000a); display: flex; align-items: center; justify-content: space-between; }
.gallery-lightbox-nav { position: absolute; top: 50%; transform: translateY(-50%); background: #0006; border: 1px solid #ffffff33; color: #fff; width: 42px; height: 42px; border-radius: 50%; font-size: 22px; cursor: pointer; z-index: 2; }
.gallery-lightbox-nav.prev { left: 16px; }
.gallery-lightbox-nav.next { right: 16px; }

/* ── Weather ──────────────────────────────────────────────────── */
.weather-main { display: flex; align-items: center; gap: 16px; }
.weather-icon { font-size: 38px; }
.weather-temp { font-family: var(--font-display); font-size: 38px; font-weight: 300; line-height: 1; }
.weather-cond { font-size: 13px; color: var(--text-secondary); margin-top: 4px; }
.weather-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 14px; }
.weather-stat { display: flex; flex-direction: column; gap: 2px; font-size: 11px; color: var(--text-muted); }
.weather-stat strong { font-size: 14px; color: var(--text-primary); font-weight: 600; }

.weather-divider { height: 1px; background: var(--border-soft); margin: 16px 0; }

.weather-hourly-scroll { display: flex; gap: 4px; overflow-x: auto; padding-bottom: 4px; scrollbar-width: thin; }
.weather-hour-item { flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; gap: 6px; padding: 10px 12px; border-radius: var(--radius-sm); min-width: 58px; transition: background .15s; }
.weather-hour-item:hover { background: var(--bg-card-hi); }
.weather-hour-item.now { background: var(--amber-soft); }
.weather-hour-time { font-size: 11px; color: var(--text-muted); font-family: var(--font-mono); }
.weather-hour-icon { font-size: 19px; }
.weather-hour-temp { font-size: 13px; font-weight: 600; font-family: var(--font-display); }
.weather-hour-precip { font-size: 10px; color: var(--teal); display: flex; align-items: center; gap: 2px; }
.weather-hour-precip.zero { color: var(--text-muted); opacity: .5; }

.weather-daily-row { display: grid; grid-template-columns: repeat(7, 1fr); gap: 6px; }
.weather-day-item { display: flex; flex-direction: column; align-items: center; gap: 5px; padding: 10px 6px; border-radius: var(--radius-sm); transition: background .15s; cursor: default; }
.weather-day-item:hover { background: var(--bg-card-hi); }
.weather-day-name { font-size: 11px; color: var(--text-muted); font-weight: 600; }
.weather-day-icon { font-size: 18px; }
.weather-day-temps { font-size: 11px; display: flex; gap: 4px; align-items: center; }
.weather-day-temps .max { color: var(--text-primary); font-weight: 600; }
.weather-day-temps .min { color: var(--text-muted); }
.weather-day-precip { font-size: 10px; color: var(--teal); }

/* ── Shopping / Calendar / Notes ──────────────────────────────── */
.count-pill { font-size: 11px; color: var(--text-muted); background: var(--bg-base); padding: 1px 8px; border-radius: 12px; font-weight: 500; }
.btn-icon-add { width: 28px; height: 28px; border-radius: 8px; border: 1px solid var(--border); background: var(--bg-base); color: var(--text-secondary); cursor: pointer; font-size: 16px; line-height: 1; display: flex; align-items: center; justify-content: center; transition: all .15s; }
.btn-icon-add:hover { color: var(--amber); border-color: var(--amber); }

.shopping-item { display: flex; align-items: center; gap: 10px; padding: 9px 0; border-bottom: 1px solid var(--border-soft); }
.shopping-item:last-child { border-bottom: none; }
.shopping-check { width: 18px; height: 18px; border-radius: 50%; border: 2px solid var(--border); cursor: pointer; flex-shrink: 0; transition: all .15s; display: flex; align-items: center; justify-content: center; }
.shopping-check:hover { border-color: var(--moss); }
.shopping-check.done { background: var(--moss); border-color: var(--moss); }
.shopping-check.done::after { content: '✓'; color: #06120a; font-size: 11px; font-weight: 700; }
.shopping-text { flex: 1; font-size: 13px; }
.shopping-item.completed .shopping-text { color: var(--text-muted); text-decoration: line-through; }
.shopping-added-by { font-size: 11px; color: var(--text-muted); }
.shopping-delete { background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 13px; padding: 4px; opacity: 0; transition: opacity .15s; }
.shopping-item:hover .shopping-delete { opacity: 1; }
.shopping-delete:hover { color: var(--rose); }

.inline-form { display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; }
.inline-form .form-input { flex: 1; min-width: 140px; }

.calendar-event { padding: 10px 14px; background: var(--bg-base); border-radius: var(--radius-sm); border-left: 3px solid var(--teal); margin-bottom: 8px; position: relative; }
.calendar-event:last-child { margin-bottom: 0; }
.calendar-event-title { font-weight: 600; font-size: 13px; }
.calendar-event-time { font-size: 11px; color: var(--text-muted); margin-top: 2px; font-family: var(--font-mono); }
.calendar-event-desc { font-size: 11px; color: var(--text-secondary); margin-top: 3px; }
.calendar-delete { position: absolute; top: 8px; right: 10px; background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 12px; opacity: 0; transition: opacity .15s; }
.calendar-event:hover .calendar-delete { opacity: 1; }
.calendar-delete:hover { color: var(--rose); }

.notes-area { width: 100%; background: var(--bg-base); border: 1px solid var(--border); color: var(--text-primary); border-radius: var(--radius-sm); padding: 12px; font-size: 13px; resize: vertical; min-height: 130px; font-family: var(--font-body); outline: none; transition: border-color .15s; }
.notes-area:focus { border-color: var(--teal); }

/* ── Savings / Status ─────────────────────────────────────────── */
.savings-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 14px; }
.saving-item { text-align: center; }
.saving-value { font-family: var(--font-display); font-size: 21px; font-weight: 600; color: var(--moss); }
.saving-value.alt { color: var(--teal); }
.saving-label { font-size: 11px; color: var(--text-muted); margin-top: 2px; }
.savings-foot { font-size: 12px; color: var(--text-muted); border-top: 1px solid var(--border-soft); padding-top: 12px; }

.status-list { display: flex; flex-direction: column; }
.status-row { display: flex; align-items: center; gap: 9px; padding: 9px 0; border-bottom: 1px solid var(--border-soft); font-size: 13px; color: var(--text-secondary); }
.status-row:last-child { border-bottom: none; }
.status-row-meta { margin-left: auto; font-size: 12px; color: var(--text-muted); }
.status-dot-ind { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; background: var(--text-muted); }
.status-dot-ind.si-ok { background: var(--moss); box-shadow: 0 0 6px var(--moss); }
.status-dot-ind.si-warn { background: var(--gold); }
.status-dot-ind.si-err { background: var(--rose); }
.status-foot { font-size: 12px; color: var(--text-muted); margin-top: 10px; border-top: 1px solid var(--border-soft); padding-top: 10px; }

/* ── Buttons ──────────────────────────────────────────────────── */
.btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: var(--radius-sm); border: none; cursor: pointer; font-size: 13px; font-weight: 600; font-family: var(--font-body); transition: all .15s; white-space: nowrap; }
.btn-primary { background: var(--user-accent, var(--amber)); background-image: linear-gradient(135deg, var(--user-accent, var(--amber)), var(--amber-deep)); color: #1a0f02; }
.btn-primary:hover { filter: brightness(1.08); transform: translateY(-1px); }
.btn-green { background: var(--grad-moss); color: #06180a; }
.btn-green:hover { filter: brightness(1.08); }
.btn-red { background: var(--grad-rose); color: #1c0805; }
.btn-red:hover { filter: brightness(1.08); }
.btn-ghost { background: none; border: 1px solid var(--border); color: var(--text-secondary); }
.btn-ghost:hover { border-color: var(--amber); color: var(--text-primary); }
.btn-sm { padding: 6px 12px; font-size: 12px; border-radius: 7px; }
.link-btn { background: none; border: none; color: var(--teal); text-decoration: underline; cursor: pointer; font-size: inherit; padding: 0; }

/* ── Badges ───────────────────────────────────────────────────── */
.badge { display: inline-flex; align-items: center; gap: 5px; padding: 4px 11px; border-radius: 20px; font-size: 11px; font-weight: 600; white-space: nowrap; }
.badge::before { content: ''; width: 6px; height: 6px; border-radius: 50%; }
.badge-running { background: var(--moss-soft); color: var(--moss); border: 1px solid #7bbf6e44; }
.badge-running::before { background: var(--moss); box-shadow: 0 0 6px var(--moss); }
.badge-paused { background: var(--rose-soft); color: var(--rose); border: 1px solid #e2685f44; }
.badge-paused::before { background: var(--rose); }
.badge-offline { background: var(--bg-base); color: var(--text-muted); border: 1px solid var(--border); }
.badge-offline::before { background: var(--text-muted); }
.badge-manual { background: var(--violet-soft); color: var(--violet); border: 1px solid #a78bd844; }
.badge-auto { background: var(--teal-soft); color: var(--teal); border: 1px solid #4cc7c244; }

/* ── Miner table ──────────────────────────────────────────────── */
.miner-table-card { padding: 0; overflow: hidden; }
.miner-table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
thead tr { border-bottom: 1px solid var(--border-soft); }
thead th { padding: 13px 14px; text-align: left; font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .05em; white-space: nowrap; font-weight: 600; }
tbody tr { border-bottom: 1px solid var(--border-soft); transition: background .12s; }
tbody tr:last-child { border-bottom: none; }
tbody tr:hover { background: var(--bg-card-hi); }
tbody td { padding: 12px 14px; font-size: 13px; vertical-align: middle; }
.miner-name { font-weight: 600; color: var(--text-primary); }
.miner-note { font-size: 11px; color: var(--text-muted); margin-top: 2px; }
.miner-ip { font-family: var(--font-mono); font-size: 12px; color: var(--text-secondary); background: var(--bg-base); padding: 3px 7px; border-radius: 5px; }
.miner-fw { font-size: 10px; color: var(--text-muted); margin-top: 3px; text-transform: uppercase; letter-spacing: .03em; }
.empty-state-cell { text-align: center; color: var(--text-muted); padding: 36px; font-size: 13px; }
.empty-state { color: var(--text-muted); text-align: center; padding: 26px 10px; font-size: 13px; }

.row-actions { display: flex; gap: 5px; flex-wrap: wrap; }
.row-btn { width: 30px; height: 30px; border-radius: 7px; border: 1px solid var(--border); background: var(--bg-base); color: var(--text-secondary); cursor: pointer; display: flex; align-items: center; justify-content: center; font-size: 13px; transition: all .15s; }
.row-btn:hover { border-color: var(--amber); color: var(--text-primary); }
.row-btn.danger:hover { border-color: var(--rose); color: var(--rose); }

/* ── Modals ───────────────────────────────────────────────────── */
.modal-overlay { position: fixed; inset: 0; background: #060a07cc; z-index: 1000; display: none; align-items: center; justify-content: center; backdrop-filter: blur(4px); padding: 20px; }
.modal-overlay.active { display: flex; }
.modal { background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 28px; width: 100%; max-width: 540px; max-height: 90vh; overflow-y: auto; box-shadow: 0 20px 60px -10px #000a; animation: modalIn .18s ease; }
.modal-wide { max-width: 740px; }
@keyframes modalIn { from { transform: scale(.96) translateY(6px); opacity: 0; } to { transform: scale(1) translateY(0); opacity: 1; } }
.modal-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 22px; }
.modal-header h3 { font-family: var(--font-display); font-size: 18px; font-weight: 600; }
.modal-close { background: none; border: none; color: var(--text-muted); font-size: 18px; cursor: pointer; padding: 4px; border-radius: 6px; transition: color .15s; }
.modal-close:hover { color: var(--text-primary); }
.modal-toolbar { display: flex; justify-content: flex-end; margin-bottom: 12px; }
.divider { height: 1px; background: var(--border-soft); margin: 20px 0; }
.form-section-title { font-size: 13px; font-weight: 600; margin-bottom: 14px; color: var(--text-secondary); }

/* ── Forms ────────────────────────────────────────────────────── */
.form-group { margin-bottom: 16px; }
.form-label { display: block; font-size: 11px; color: var(--text-muted); margin-bottom: 6px; text-transform: uppercase; letter-spacing: .05em; font-weight: 600; }
.form-input { width: 100%; background: var(--bg-base); border: 1px solid var(--border); color: var(--text-primary); border-radius: var(--radius-sm); padding: 10px 13px; font-size: 13px; outline: none; transition: border-color .15s; font-family: var(--font-body); }
.form-input:focus { border-color: var(--teal); }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.form-actions { display: flex; gap: 10px; justify-content: flex-end; margin-top: 22px; }
.form-hint { font-size: 12px; color: var(--text-muted); margin: -6px 0 16px; line-height: 1.5; }

.toggle-switch { display: flex; align-items: center; gap: 10px; font-size: 13px; }
.toggle { position: relative; width: 42px; height: 23px; flex-shrink: 0; }
.toggle input { opacity: 0; width: 0; height: 0; }
.toggle-slider { position: absolute; inset: 0; background: var(--border); border-radius: 24px; transition: background .25s; cursor: pointer; }
.toggle-slider::before { content: ''; position: absolute; width: 17px; height: 17px; left: 3px; top: 3px; background: #fff; border-radius: 50%; transition: transform .25s; }
.toggle input:checked + .toggle-slider { background: var(--moss); }
.toggle input:checked + .toggle-slider::before { transform: translateX(19px); }

.segmented { display: flex; background: var(--bg-base); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 3px; gap: 3px; }
.seg-btn { flex: 1; background: none; border: none; color: var(--text-secondary); padding: 8px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all .15s; }
.seg-btn.active { background: var(--bg-card-hi); color: var(--amber); }

/* ── Tabs ─────────────────────────────────────────────────────── */
.tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border); margin-bottom: 18px; flex-wrap: wrap; }
.tab-btn { padding: 9px 16px; background: none; border: none; color: var(--text-muted); font-size: 13px; font-weight: 500; cursor: pointer; border-bottom: 2px solid transparent; transition: all .15s; margin-bottom: -1px; }
.tab-btn.active { color: var(--amber); border-bottom-color: var(--amber); }
.tab-content { display: none; }
.tab-content.active { display: block; }

/* ── Miner Detail Modal ───────────────────────────────────────── */
.miner-row-clickable { cursor: pointer; }
.modal-subtitle { font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); margin-top: 2px; }
.md-hero { display: grid; grid-template-columns: auto 1fr 1fr 1fr 1fr; gap: 18px; align-items: center; background: var(--bg-base); border: 1px solid var(--border-soft); border-radius: var(--radius-md); padding: 16px 18px; margin-bottom: 18px; }
.md-hero-stat { text-align: center; }
.md-hero-value { font-family: var(--font-display); font-size: 19px; font-weight: 600; font-variant-numeric: tabular-nums; }
.md-hero-label { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .04em; margin-top: 2px; }
.md-chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.md-chart-title { font-size: 12px; color: var(--text-secondary); font-weight: 600; margin-bottom: 8px; }
#md-event-list .notif-item { border-left-color: var(--violet); }

/* ── Hintergrund Personalisierung ─────────────────────────────── */
.bg-mode-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
.bg-mode-card { background: var(--bg-base); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 14px; cursor: pointer; text-align: left; transition: all .15s; display: flex; flex-direction: column; gap: 8px; }
.bg-mode-card:hover { border-color: var(--amber); }
.bg-mode-card.selected { border-color: var(--amber); background: var(--amber-soft); }
.bg-mode-preview { height: 44px; border-radius: 7px; }
.bg-preview-weather { background: linear-gradient(135deg, #2d4a5e, #1a2e1f); }
.bg-preview-time { background: linear-gradient(135deg, #e8a34c44, #1a2230); }
.bg-preview-static { background: linear-gradient(135deg, #e8a34c33, #0f1410); }
.bg-preview-off { background: var(--bg-card); }
.bg-mode-name { font-size: 13px; font-weight: 600; }
.bg-mode-desc { font-size: 11px; color: var(--text-muted); line-height: 1.4; }
.bg-accent-row { display: flex; gap: 10px; }
.bg-accent-swatch { width: 32px; height: 32px; border-radius: 50%; border: 2px solid transparent; background: var(--swatch); cursor: pointer; transition: all .15s; }
.bg-accent-swatch.selected { border-color: var(--text-primary); transform: scale(1.12); }

/* ── Toast ────────────────────────────────────────────────────── */
.toast-container { position: fixed; bottom: 22px; right: 22px; z-index: 9998; display: flex; flex-direction: column; gap: 8px; }
.toast { background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 13px 16px; min-width: 270px; display: flex; align-items: center; gap: 10px; animation: toastIn .25s ease; box-shadow: var(--shadow-card); font-size: 13px; }
.toast.success { border-left: 3px solid var(--moss); }
.toast.error { border-left: 3px solid var(--rose); }
.toast.info { border-left: 3px solid var(--teal); }
@keyframes toastIn { from { transform: translateX(110%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

/* ── Notifications list ───────────────────────────────────────── */
.notif-item { padding: 13px; background: var(--bg-base); border-radius: var(--radius-sm); border-left: 3px solid var(--teal); margin-bottom: 8px; }
.notif-item:last-child { margin-bottom: 0; }
.notif-item.warning { border-left-color: var(--gold); }
.notif-item.error { border-left-color: var(--rose); }
.notif-title { font-weight: 600; font-size: 13px; }
.notif-msg { font-size: 12px; color: var(--text-secondary); margin-top: 3px; }
.notif-time { font-size: 11px; color: var(--text-muted); margin-top: 5px; font-family: var(--font-mono); }
.notif-item.unread { box-shadow: inset 3px 0 0 0 transparent; background: var(--bg-card-hi); }

/* ── Responsive ───────────────────────────────────────────────── */
@media (max-width: 1100px) {
  .energy-grid { grid-template-columns: repeat(2, 1fr); }
  .energy-grid.secondary { grid-template-columns: repeat(3, 1fr); }
  .grid-3 { grid-template-columns: repeat(2, 1fr); }
  .savings-grid { grid-template-columns: repeat(2, 1fr); }
  .grid-3-1 { grid-template-columns: 1fr; }
  .quickstats-row { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 900px) {
  .sidebar { position: fixed; left: 0; top: 0; transform: translateX(-100%); transition: transform .25s ease; box-shadow: 0 0 40px #000a; }
  .sidebar.mobile-open { transform: translateX(0); }
  .sidebar.collapsed { width: 232px; }
  .sidebar.collapsed .brand-name, .sidebar.collapsed .nav-label, .sidebar.collapsed .sidebar-status .nav-label { display: inline; }
  .sidebar-collapse-btn { display: none; }
  .mobile-menu-btn { display: flex; }
  .sidebar-backdrop { display: none; position: fixed; inset: 0; background: #000a; z-index: 190; }
  .sidebar-backdrop.active { display: block; }
}
@media (max-width: 700px) {
  main { padding: 14px 14px 50px; gap: 18px; }
  .grid-2, .grid-3, .energy-grid, .energy-grid.secondary, .savings-grid, .quickstats-row { grid-template-columns: 1fr; }
  header { padding: 12px 16px; }
  .page-title { font-size: 16px; }
  .block-head { align-items: flex-start; }
  .form-row { grid-template-columns: 1fr; }
}

::-webkit-scrollbar { width: 7px; height: 7px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #3a4a3b; }

/* ════════════════════════════════════════════════════════════════
   PERSONALISIERUNG & INTERAKTION — Icon-System, Befehlspalette,
   Akzentfarben-Auswahl, Dichte-Modus, Drag & Drop, Seiten-Übergänge.
   ════════════════════════════════════════════════════════════════ */

.icon-inline { display: inline-flex; vertical-align: -2px; margin-right: 3px; }
.icon-inline svg { display: block; }

kbd {
  font-family: var(--font-mono); font-size: 10.5px; background: var(--bg-base);
  border: 1px solid var(--border); border-bottom-width: 2px; border-radius: 4px;
  padding: 1px 5px; color: var(--text-secondary);
}

/* Sanfter Seitenwechsel statt hartem Umschalten */
.page { animation: page-in .28s ease; }
@keyframes page-in { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: none; } }

/* Karten heben sich beim Hover dezent an - fühlt sich "griffiger" an */
.card, .quickstat, .metric-card, .metric-card-sm { transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease; }
.card:hover, .quickstat:hover, .metric-card:hover, .metric-card-sm:hover {
  transform: translateY(-2px);
  box-shadow: 0 14px 32px -12px rgba(0,0,0,0.55);
}

/* Live-Wert-Update: kurzer Puls, wenn sich eine Kennzahl ändert */
@keyframes value-bump { 0% { transform: scale(1); } 35% { transform: scale(1.06); color: var(--user-accent, var(--amber)); } 100% { transform: scale(1); } }
.value-bump { animation: value-bump .4s ease; }

/* Kachel-Umsortierung per Drag & Drop */
.draggable-card { cursor: grab; }
.draggable-card:active { cursor: grabbing; }
.draggable-card.dragging { opacity: .45; border-style: dashed; }

/* Anzeigedichte: kompakter Modus verringert Innenabstände und Zeilenhöhen */
body.density-compact .card { padding: 14px 16px; }
body.density-compact .quickstat { padding: 12px 14px; gap: 10px; }
body.density-compact .metric-card { padding: 12px 14px; }
body.density-compact .block { margin-bottom: 14px; }
body.density-compact main { gap: 16px; }
body.density-compact .quickstat-value { font-size: 18px; }

/* Akzentfarben-Auswahl (Persönlich-Tab) */
.accent-swatch-row { display: flex; gap: 12px; }
.widget-slot-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
.form-label-sm { font-size: 11px; color: var(--text-muted); margin-bottom: 4px; display: block; }
@media (max-width: 640px) { .widget-slot-row { grid-template-columns: repeat(2, 1fr); } }
.accent-swatch {
  width: 34px; height: 34px; border-radius: 50%; border: 2px solid transparent;
  background: var(--swatch); cursor: pointer; transition: transform .15s, border-color .15s;
  position: relative;
}
.accent-swatch:hover { transform: scale(1.08); }
.accent-swatch.active { border-color: var(--text-primary); transform: scale(1.14); }
.accent-swatch.active::after {
  content: '✓'; position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
  color: #1a0f02; font-size: 14px; font-weight: 700;
}

/* ── Befehlspalette ─────────────────────────────────────────────── */
.command-palette-overlay {
  display: none; position: fixed; inset: 0; z-index: 500;
  background: rgba(6,9,7,0.6); backdrop-filter: blur(3px);
  align-items: flex-start; justify-content: center; padding-top: 12vh;
}
.command-palette-overlay.open { display: flex; }
.command-palette {
  width: min(560px, 92vw); background: var(--bg-raised); border: 1px solid var(--border);
  border-radius: var(--radius-lg); box-shadow: 0 24px 60px -16px rgba(0,0,0,0.65);
  overflow: hidden; animation: cmdk-in .16s ease;
}
@keyframes cmdk-in { from { opacity: 0; transform: translateY(-8px) scale(.98); } to { opacity: 1; transform: none; } }
.command-input-row {
  display: flex; align-items: center; gap: 10px; padding: 14px 16px;
  border-bottom: 1px solid var(--border-soft); color: var(--text-muted);
}
.command-input-row input {
  flex: 1; background: none; border: none; outline: none; color: var(--text-primary);
  font-size: 14.5px; font-family: var(--font-body);
}
.command-list { max-height: 50vh; overflow-y: auto; padding: 6px; }
.command-item {
  display: flex; align-items: center; justify-content: space-between; gap: 10px;
  padding: 10px 12px; border-radius: var(--radius-sm); cursor: pointer; font-size: 13.5px;
  color: var(--text-secondary);
}
.command-item.active, .command-item:hover { background: var(--user-accent-soft, var(--amber-soft)); color: var(--text-primary); }
.command-shortcut { font-family: var(--font-mono); font-size: 10.5px; color: var(--text-muted); }

/* ── Zonen-Editor ───────────────────────────────────────────────── */
.zone-editor-layout { display: grid; grid-template-columns: 1fr 260px; gap: 18px; align-items: start; }
.zone-editor-canvas-wrap { background: var(--bg-base); border: 1px solid var(--border); border-radius: var(--radius-md); overflow: hidden; line-height: 0; }
#zones-canvas { width: 100%; display: block; cursor: crosshair; }
.zone-editor-side { min-width: 0; }
.zone-list { display: flex; flex-direction: column; gap: 6px; max-height: 220px; overflow-y: auto; }
.zone-list-item {
  display: flex; align-items: center; gap: 8px; background: var(--bg-card);
  border: 1px solid var(--border-soft); border-radius: var(--radius-sm); padding: 8px 10px; font-size: 12.5px;
}
.zone-swatch { width: 10px; height: 10px; border-radius: 3px; flex-shrink: 0; }
.zone-list-name { font-weight: 600; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.zone-list-points { color: var(--text-muted); font-size: 11px; flex-shrink: 0; }
@media (max-width: 700px) {
  .zone-editor-layout { grid-template-columns: 1fr; }
}

/* ── Ereignis-Zeitleiste (Zeitleiste-Ansicht in "Heimüberwachung → Ereignisse") ──
   Horizontale 24h-Leiste mit einem Punkt pro Ereignis, Position = Uhrzeit
   des Tages in Prozent (0-100%). Klick auf einen Punkt zeigt Details unten. */
.event-timeline-hours { position: relative; height: 16px; margin: 0 7px 2px; font-size: 10px; color: var(--text-muted); }
.event-timeline-hours span { position: absolute; transform: translateX(-50%); }
.event-timeline {
  position: relative; height: 44px; background: var(--bg-base); border: 1px solid var(--border);
  border-radius: var(--radius-sm); margin: 0 0 12px;
}
.event-timeline-marker {
  position: absolute; top: 50%; width: 12px; height: 12px; border-radius: 50%;
  transform: translate(-50%, -50%); border: 2px solid var(--bg-card); cursor: pointer;
  padding: 0; transition: transform .12s ease; box-shadow: 0 1px 3px rgba(0,0,0,.35);
}
.event-timeline-marker:hover, .event-timeline-marker.active { transform: translate(-50%, -50%) scale(1.35); z-index: 2; }
.event-timeline-detail {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap; background: var(--bg-card);
  border: 1px solid var(--border-soft); border-radius: var(--radius-sm); padding: 10px 12px; font-size: 13px; min-height: 20px;
}
.event-timeline-detail-time { color: var(--text-muted); font-variant-numeric: tabular-nums; font-size: 12px; }
.event-timeline-detail img { height: 60px; border-radius: 6px; display: block; }

/* ── Bewegungs-Heatmap (Kamera × Tagesstunde) ──────────────────────── */
.event-heatmap { display: flex; flex-direction: column; gap: 3px; overflow-x: auto; padding-bottom: 4px; }
.event-heatmap-row { display: grid; grid-template-columns: 110px repeat(24, 1fr); gap: 3px; align-items: center; min-width: 640px; }
.event-heatmap-row.head { font-size: 9px; color: var(--text-muted); text-align: center; }
.event-heatmap-cam-name { font-size: 12px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding-right: 6px; }
.event-heatmap-cell {
  height: 22px; border-radius: 4px; background: var(--bg-base); border: 1px solid var(--border-soft);
  cursor: default; position: relative;
}
.event-heatmap-cell.has-events { border-color: transparent; }

/* ── TV-/Wandmontage-Modus ──────────────────────────────────────────
   Für ein an der Wand montiertes Tablet gedacht: Sidebar weg, größere
   Schrift, ruhigeres Header. Umschaltbar per Knopf oder Taste "t"
   (siehe tvMode-Funktionen weiter unten). Zustand wird in localStorage
   gemerkt, damit ein Wand-Tablet nach einem Reload TV-Modus behält. */
body.tv-mode .sidebar { display: none; }
body.tv-mode .mobile-menu-btn { display: none; }
body.tv-mode .cmdk-btn span, body.tv-mode .cmdk-btn kbd { display: none; }
body.tv-mode main { max-width: 100%; padding: 34px 44px 60px; gap: 30px; }
body.tv-mode .page-title { font-size: 30px; }
body.tv-mode .header-greeting { font-size: 15px; }
body.tv-mode .header-time { font-size: 20px; }
body.tv-mode .quickstats-row { grid-template-columns: repeat(4, 1fr); gap: 20px; }
body.tv-mode .quickstat { padding: 22px 24px; gap: 18px; }
body.tv-mode .quickstat-icon { width: 56px; height: 56px; font-size: 26px; border-radius: 14px; }
body.tv-mode .quickstat-value { font-size: 30px; }
body.tv-mode .quickstat-value.quickstat-value-sm { font-size: 16px; }
body.tv-mode .quickstat-label { font-size: 13px; }
body.tv-mode .mm-sub { font-size: 15px; }
body.tv-mode .mm-chart-svg { height: 90px; }
body.tv-mode .mm-chart-temp { font-size: 13px; }
body.tv-mode .mm-chart-name { font-size: 10px; }
body.tv-mode .mm-pv { font-size: 15px; }
body.tv-mode .block-head h2 { font-size: 19px; }
body.tv-mode .metric-card, body.tv-mode .metric-card-sm { padding: 20px; }
body.tv-mode .metric-value { font-size: 26px; }
body.tv-mode .draggable-card { cursor: default; }
.tv-mode-btn { background: var(--bg-card); border: 1px solid var(--border); color: var(--text-secondary); width: 34px; height: 34px; border-radius: var(--radius-sm); display: flex; align-items: center; justify-content: center; cursor: pointer; transition: all .15s; flex-shrink: 0; }
.tv-mode-btn:hover { color: var(--user-accent, var(--amber)); border-color: var(--user-accent, var(--amber)); transform: translateY(-1px); }
.tv-mode-btn.active { color: var(--user-accent, var(--amber)); border-color: var(--user-accent, var(--amber)); background: var(--user-accent-soft, var(--amber-soft)); }
@media (max-width: 700px) {
  body.tv-mode .quickstats-row { grid-template-columns: repeat(2, 1fr); }
}

'''

DASHBOARD_JS = r'''
/* ════════════════════════════════════════════════════════════════
   SMARTHOME DASHBOARD — Dashboard Logic
   ════════════════════════════════════════════════════════════════ */
const state = {
  miners: [], solar: null, prices: null, btc: null, weather: null,
  cameras: [], notifications: [], calendar: [], shopping: [], notes: {},
  surveillance: null,
};
let pvChart, pvChartOverview, minerChart, priceChart, btcChart;
let weekCompareChart, dailyProfileChart, pvHistoryChart, monthOverviewChart, solarForecastChart;
let fullscreenCamId = null;
let fullscreenInterval = null;
let currentPage = 'overview';
const ENERGY_PAGE_LOADED = { value: false };

const CHART_COLORS = {
  amber: '#e8a34c', teal: '#4cc7c2', rose: '#e2685f', moss: '#7bbf6e',
  violet: '#a78bd8', gold: '#d9b54a', muted: '#6b7a6a', grid: '#2a352b55',
};

/* ── Charts ─────────────────────────────────────────────────────── */
function initCharts() {
  Chart.defaults.color = CHART_COLORS.muted;
  Chart.defaults.font.family = "'Inter', system-ui, sans-serif";
  Chart.defaults.borderColor = CHART_COLORS.grid;

  pvChart = new Chart(document.getElementById('pv-chart'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'PV (W)', data: [], borderColor: CHART_COLORS.amber, backgroundColor: CHART_COLORS.amber + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 },
      { label: 'Haus (W)', data: [], borderColor: CHART_COLORS.teal, backgroundColor: CHART_COLORS.teal + '10', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 },
      { label: 'Netz (W)', data: [], borderColor: CHART_COLORS.rose, fill: false, tension: .4, pointRadius: 0, borderWidth: 1.5, borderDash: [4, 3] },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, padding: 14, font: { size: 11 } } } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  pvChartOverview = new Chart(document.getElementById('pv-chart-overview'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'PV (W)', data: [], borderColor: CHART_COLORS.amber, backgroundColor: CHART_COLORS.amber + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 },
      { label: 'Haus (W)', data: [], borderColor: CHART_COLORS.teal, backgroundColor: CHART_COLORS.teal + '10', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, padding: 14, font: { size: 11 } } } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  minerChart = new Chart(document.getElementById('miner-chart'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'Hashrate (TH/s)', data: [], borderColor: CHART_COLORS.violet, backgroundColor: CHART_COLORS.violet + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2, yAxisID: 'y' },
      { label: 'PV-Überschuss (W)', data: [], borderColor: CHART_COLORS.moss, backgroundColor: 'transparent', fill: false, tension: .4, pointRadius: 0, borderWidth: 1.5, yAxisID: 'y1' },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: { y: { grid: { color: CHART_COLORS.grid }, position: 'left', ticks: { font: { size: 10 } } }, y1: { grid: { display: false }, position: 'right', ticks: { font: { size: 10 } } }, x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  priceChart = new Chart(document.getElementById('price-chart'), {
    type: 'bar',
    data: { labels: [], datasets: [{ label: '€/kWh', data: [], backgroundColor: CHART_COLORS.teal + 'aa', borderRadius: 3 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { display: false }, ticks: { font: { size: 9 } } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  btcChart = new Chart(document.getElementById('btc-chart'), {
    type: 'line',
    data: { labels: [], datasets: [{ label: 'BTC €', data: [], borderColor: CHART_COLORS.gold, backgroundColor: CHART_COLORS.gold + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });
}

/* ── Energie-Detailseite: zusätzliche Charts (lazy init) ─────────── */
function initEnergyPageCharts() {
  if (weekCompareChart) return; // schon initialisiert

  weekCompareChart = new Chart(document.getElementById('week-compare-chart'), {
    type: 'bar',
    data: { labels: [], datasets: [
      { label: 'Letzte Woche', data: [], backgroundColor: CHART_COLORS.muted + '99', borderRadius: 4 },
      { label: 'Diese Woche', data: [], backgroundColor: CHART_COLORS.amber + 'cc', borderRadius: 4 },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: { x: { grid: { display: false }, ticks: { font: { size: 11 } } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } }, title: { display: true, text: 'kWh', font: { size: 10 }, color: CHART_COLORS.muted } } } },
  });

  dailyProfileChart = new Chart(document.getElementById('daily-profile-chart'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'Ø PV (W)', data: [], borderColor: CHART_COLORS.amber, backgroundColor: CHART_COLORS.amber + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 },
      { label: 'Ø Verbrauch (W)', data: [], borderColor: CHART_COLORS.teal, backgroundColor: 'transparent', fill: false, tension: .4, pointRadius: 0, borderWidth: 2 },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 9 }, maxRotation: 0 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  pvHistoryChart = new Chart(document.getElementById('pv-history-chart'), {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'PV-Ertrag (kWh)', data: [], backgroundColor: CHART_COLORS.amber + 'aa', borderRadius: 3 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { display: false }, ticks: { font: { size: 9 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });

  monthOverviewChart = new Chart(document.getElementById('month-overview-chart'), {
    type: 'bar',
    data: { labels: [], datasets: [
      { label: 'Eigenverbrauch', data: [], backgroundColor: CHART_COLORS.moss + 'cc', stack: 's', borderRadius: 2 },
      { label: 'Einspeisung', data: [], backgroundColor: CHART_COLORS.teal + 'cc', stack: 's', borderRadius: 2 },
      { label: 'Netzbezug', data: [], backgroundColor: CHART_COLORS.rose + 'cc', stack: 's2', borderRadius: 2 },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', align: 'end', labels: { boxWidth: 10, font: { size: 11 } } } },
      scales: { x: { grid: { display: false }, ticks: { font: { size: 9 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } }, stacked: true } } },
  });

  solarForecastChart = new Chart(document.getElementById('solar-forecast-chart'), {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'Voraussichtliche Leistung (kW)', data: [], backgroundColor: CHART_COLORS.amber + 'aa', borderRadius: 3 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { display: false }, ticks: { font: { size: 9 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });
}

async function loadEnergyPageData() {
  try {
    const [weekR, monthR, profileR, pvHistR, kpiR] = await Promise.all([
      fetch('/api/energy/week-comparison'),
      fetch('/api/energy/month-overview?days=30'),
      fetch('/api/energy/daily-profile'),
      fetch('/api/energy/pv-yield-history?days=30'),
      fetch('/api/energy/kpis'),
    ]);
    const week = await weekR.json();
    const month = await monthR.json();
    const profile = await profileR.json();
    const pvHist = await pvHistR.json();
    const kpis = await kpiR.json();

    renderWeekComparison(week);
    renderMonthOverview(month);
    renderDailyProfile(profile);
    renderPvHistory(pvHist);
    renderEnergyKpis(kpis);
  } catch (e) {
    console.error('Energie-Detaildaten konnten nicht geladen werden', e);
  }
}

function renderWeekComparison(data) {
  if (!weekCompareChart) return;
  const labels = data.current_week.map(d => d.weekday);
  weekCompareChart.data.labels = labels;
  weekCompareChart.data.datasets[0].data = data.previous_week.map(d => d.pv_kwh);
  weekCompareChart.data.datasets[1].data = data.current_week.map(d => d.is_future ? null : d.pv_kwh);
  weekCompareChart.update('none');
}

function renderMonthOverview(data) {
  if (!monthOverviewChart) return;
  monthOverviewChart.data.labels = data.map(d => d.label);
  monthOverviewChart.data.datasets[0].data = data.map(d => d.self_consumed_kwh);
  monthOverviewChart.data.datasets[1].data = data.map(d => d.exported_kwh);
  monthOverviewChart.data.datasets[2].data = data.map(d => -d.imported_kwh);
  monthOverviewChart.update('none');
}

function renderDailyProfile(data) {
  if (!dailyProfileChart) return;
  dailyProfileChart.data.labels = data.map(d => d.hour + ':00');
  dailyProfileChart.data.datasets[0].data = data.map(d => d.avg_pv);
  dailyProfileChart.data.datasets[1].data = data.map(d => d.avg_house);
  dailyProfileChart.update('none');
}

function renderPvHistory(data) {
  if (!pvHistoryChart) return;
  pvHistoryChart.data.labels = data.map(d => d.label);
  pvHistoryChart.data.datasets[0].data = data.map(d => d.has_data ? d.pv_kwh : null);
  pvHistoryChart.update('none');
}

function renderEnergyKpis(kpis) {
  document.getElementById('kpi-total-30d').textContent = (kpis.total_pv_30d || 0).toFixed(1) + ' kWh';
  document.getElementById('kpi-avg-day').textContent = (kpis.avg_pv_per_day || 0).toFixed(1) + ' kWh';
  if (kpis.best_day) {
    document.getElementById('kpi-best-day').textContent = kpis.best_day.pv_kwh.toFixed(1) + ' kWh';
    document.getElementById('kpi-best-day-date').textContent = kpis.best_day.label;
  } else {
    document.getElementById('kpi-best-day').textContent = '– kWh';
    document.getElementById('kpi-best-day-date').textContent = '–';
  }
  const wowEl = document.getElementById('kpi-wow');
  if (kpis.week_over_week_change_pct === null || kpis.week_over_week_change_pct === undefined) {
    wowEl.textContent = '–';
    wowEl.style.color = '';
  } else {
    const v = kpis.week_over_week_change_pct;
    wowEl.textContent = (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
    wowEl.style.color = v >= 0 ? 'var(--moss)' : 'var(--rose)';
  }
}


const socket = io();
socket.on('connect', () => setConnStatus(true));
socket.on('disconnect', () => setConnStatus(false));
socket.on('solar_update', d => updateSolar(d));
socket.on('miners_update', d => { if (d.miners) updateMiners(d.miners); });
socket.on('btc_update', d => updateBTC(d));
socket.on('price_update', d => updatePrices(d));
socket.on('weather_update', d => updateWeather(d));
socket.on('notification', n => { state.notifications.unshift(n); updateNotifBadge(); });
socket.on('surveillance_update', d => updateSurveillance(d));

function setConnStatus(c) {
  document.getElementById('status-dot').className = 'status-dot' + (c ? ' connected' : '');
  document.getElementById('status-text').textContent = c ? 'Verbunden' : 'Getrennt';
}

/* ── Dashboard Load ─────────────────────────────────────────────── */
async function loadDashboard() {
  try {
    const r = await fetch('/api/dashboard-data');
    const d = await r.json();
    if (d.settings) {
      backgroundSettings.mode = d.settings.background_mode || 'weather';
      backgroundSettings.accent = d.settings.background_accent || 'amber';
      applyBackgroundMode();
    }
    if (d.solar) updateSolar(d.solar);
    if (d.energy_prices) updatePrices(d.energy_prices);
    if (d.weather) updateWeather(d.weather);
    if (d.btc_price) updateBTC(d.btc_price);
    if (d.miners) updateMiners(d.miners);
    updateSurveillance(d.surveillance || null);
    if (d.daily_report_preview) {
      metrics.dailyReport.value = d.daily_report_preview;
      metrics.dailyReport.label = 'Tagesbericht';
      paintQuickstatWidget('dailyReport');
    }
    if (d.morning_message) {
      // metrics.morningMessage hält hier das VOLLE strukturierte Objekt vom
      // Server (slots/temp_min/pv_kwh/...), nicht {value,label} wie bei den
      // übrigen Widgets - siehe paintMorningMessageWidget für die Darstellung.
      metrics.morningMessage = d.morning_message;
      paintQuickstatWidget('morningMessage');
    }
    if (d.notifications) { state.notifications = d.notifications; updateNotifBadge(); }
    if (d.shopping_list) updateShopping(d.shopping_list);
    if (d.calendar) updateCalendar(d.calendar);
    if (d.notes) { state.notes = d.notes; document.getElementById('family-notes').value = d.notes.general || ''; }
    if (d.cost_savings) updateSavings(d.cost_savings);
    if (d.chart_data) updateCharts(d.chart_data);
    updateSolarForecast(d.solar_forecast || null);
    updateSystemStatus(d);
    document.getElementById('last-update-time').textContent = new Date().toLocaleTimeString('de-AT');
    document.getElementById('loading-overlay').classList.add('hidden');
  } catch (e) {
    console.error(e);
    document.getElementById('loading-overlay').classList.add('hidden');
    showToast('Dashboard konnte nicht geladen werden', 'error');
  }
}

function formatRelativeEventTime(timeStr) {
  // Erwartet Format "dd.mm.yyyy HH:MM:SS" (siehe EventFeed.add)
  if (!timeStr) return '';
  const m = timeStr.match(/^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return timeStr;
  const [, dd, mm, yyyy, HH, MI, SS] = m;
  const then = new Date(+yyyy, +mm - 1, +dd, +HH, +MI, +SS);
  const diffSec = Math.max(0, Math.round((Date.now() - then.getTime()) / 1000));
  if (diffSec < 60) return 'jetzt gerade';
  if (diffSec < 3600) return `vor ${Math.round(diffSec / 60)} Min.`;
  if (diffSec < 86400) return `vor ${Math.round(diffSec / 3600)} Std.`;
  return `vor ${Math.round(diffSec / 86400)} Tg.`;
}

// Aktualisiert das "Aktuelles Ereignis"-Widget aus dem zuletzt bekannten
// Ereignis (state.surveillance.latest_event). Läuft per Intervall, damit
// die "vor X Min."-Angabe auch ohne neue Daten weiterzählt.
function renderEventsWidget() {
  const ev = state.surveillance && state.surveillance.latest_event;
  if (!ev) {
    metrics.events.value = '–';
    metrics.events.label = 'Keine Ereignisse';
  } else {
    metrics.events.value = ev.description || `${ev.camera || 'Kamera'}`;
    metrics.events.label = `${ev.camera ? ev.camera + ' · ' : ''}${formatRelativeEventTime(ev.time)}`;
  }
  paintQuickstatWidget('events');
}

/* ── Solar ──────────────────────────────────────────────────────── */
function setValueBump(id, html) {
  const el = document.getElementById(id);
  if (!el) return;
  const prev = el.dataset.prevVal;
  el.innerHTML = html;
  if (prev !== undefined && prev !== html) {
    el.classList.remove('value-bump');
    void el.offsetWidth; // Reflow erzwingen, damit die Animation neu startet
    el.classList.add('value-bump');
  }
  el.dataset.prevVal = html;
}
function updateSolar(d) {
  state.solar = d;
  const fmt = v => Math.round(v) + ' W';
  setValueBump('pv-power', fmt(d.pv_power || 0).replace(' W', ' <span class="unit">W</span>'));
  document.getElementById('pv-day').textContent = 'Heute: ' + ((d.pv_day || 0) / 1000).toFixed(1) + ' kWh';
  document.getElementById('pv-bar').style.width = Math.min(100, (d.pv_power || 0) / 100) + '%';
  setValueBump('house-load', fmt(Math.abs(d.house_load || 0)).replace(' W', ' <span class="unit">W</span>'));

  // Schnellübersicht (Übersichtsseite) — Verbrauch, PV-Erzeugung und
  // Batterie werden immer aktualisiert, unabhängig davon, ob sie gerade in
  // einer sichtbaren Kachel stehen (siehe WIDGET_DEFS/paintQuickstatWidget).
  metrics.load.value = Math.round(Math.abs(d.house_load || 0)) + ' W';
  paintQuickstatWidget('load');
  metrics.pv.value = Math.round(d.pv_power || 0) + ' W';
  paintQuickstatWidget('pv');
  metrics.battery.value = Math.round(d.battery_soc || 0) + '%';
  paintQuickstatWidget('battery');

  const g = d.grid_import || 0;
  setValueBump('grid-power', fmt(Math.abs(g)).replace(' W', ' <span class="unit">W</span>'));
  document.getElementById('grid-sub').textContent = g > 50 ? 'Netzbezug' : g < -50 ? 'Einspeisung' : 'Ausgeglichen';
  const gridCard = document.getElementById('grid-card');
  gridCard.className = 'metric-card' + (g > 50 ? ' red' : g < -50 ? ' teal' : '');

  setValueBump('battery-soc', Math.round(d.battery_soc || 0) + '<span class="unit">%</span>');
  document.getElementById('battery-bar').style.width = (d.battery_soc || 0) + '%';
  document.getElementById('autonomy').textContent = Math.round(d.autonomy || 0) + '%';
  document.getElementById('autonomy-bar').style.width = (d.autonomy || 0) + '%';
  document.getElementById('self-consumption').textContent = Math.round(d.self_consumption || 0) + '%';
  document.getElementById('self-bar').style.width = (d.self_consumption || 0) + '%';

  const surplus = g < 0 ? Math.abs(g) : 0;
  document.getElementById('pv-surplus').textContent = Math.round(surplus) + ' W';
  document.getElementById('surplus-sub').textContent = surplus > 500 ? 'Miner können einschalten' : 'Zu wenig für Miner';

  if (d.offline) {
    document.getElementById('si-pv').className = 'status-dot-ind si-err';
    document.getElementById('si-pv-text').textContent = 'Nicht erreichbar';
  } else {
    document.getElementById('si-pv').className = 'status-dot-ind si-ok';
    document.getElementById('si-pv-text').textContent = 'Online';
  }
}

/* ── Solarprognose ──────────────────────────────────────────────── */
function updateSolarForecast(fc) {
  const block = document.getElementById('solar-forecast-block');
  if (!block) return;
  state.solarForecast = fc;
  if (!fc || !fc.hours || !fc.hours.length) { block.hidden = true; return; }
  block.hidden = false;

  document.getElementById('solar-forecast-today').textContent = fc.today_kwh + ' kWh';
  document.getElementById('solar-forecast-tomorrow').textContent = fc.tomorrow_kwh + ' kWh';
  document.getElementById('solar-forecast-kwp').textContent = fc.installed_kwp;

  if (solarForecastChart) {
    solarForecastChart.data.labels = fc.hours.map(h => h.hour);
    solarForecastChart.data.datasets[0].data = fc.hours.map(h => h.estimated_kw);
    solarForecastChart.update('none');
  }
}

/* ── Miners ─────────────────────────────────────────────────────── */
function updateMiners(miners) {
  state.miners = miners;
  const tbody = document.getElementById('miner-tbody');
  if (!miners || !miners.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty-state-cell">Noch keine Miner eingerichtet</td></tr>';
    document.getElementById('total-hashrate').textContent = '0.00 TH/s';
    metrics.hashrate.value = '0.00 TH/s';
    paintQuickstatWidget('hashrate');
    document.getElementById('total-miner-power').textContent = '0 W';
    document.getElementById('active-miners-count').textContent = '0 aktive Miner';
    document.getElementById('si-miner').className = 'status-dot-ind si-err';
    document.getElementById('si-miner-text').textContent = 'Keine Miner';
    document.getElementById('miner-efficiency').textContent = '– J/TH';
    document.getElementById('miner-mode-display').textContent = '–';
    return;
  }
  let totalHR = 0, totalPW = 0, active = 0, autoC = 0;
  tbody.innerHTML = miners.map(m => {
    // Ein Miner gilt als aktiv, sobald er tatsächlich Hashrate liefert - das
    // ist das zuverlässigste Signal. Der reine "status"-Wert kann kurzzeitig
    // hinterherhinken (z.B. meldet Braiins OS nach einem Pause-Befehl noch
    // ein paar Sekunden lang die zuletzt gemessene Hashrate, bevor sie auf 0
    // fällt) - ohne diese Korrektur würde die Kachel "Pausiert" anzeigen,
    // obwohl daneben eine echte Hashrate > 0 zu sehen ist.
    const hasHashrate = (m.hashrate || 0) > 0;
    const st = hasHashrate ? 'running' : (m.status || 'offline');
    const isRun = st === 'running';
    if (isRun) { active++; totalHR += m.hashrate || 0; totalPW += m.power_watts || 0; }
    if (m.automation_enabled) autoC++;
    const stBadge = st === 'running' ? '<span class="badge badge-running">Aktiv</span>' :
                    st === 'paused' ? '<span class="badge badge-paused">Pausiert</span>' :
                    '<span class="badge badge-offline">Offline</span>';
    const modeBadge = m.automation_enabled ? '<span class="badge badge-auto">Auto</span>' : '<span class="badge badge-manual">Manuell</span>';
    const hr = m.hashrate ? (m.hashrate.toFixed(2) + ' TH/s') : '– TH/s';
    const temp = m.temperature ? (m.temperature.toFixed(0) + '°C') : '–°C';
    const tc = m.temperature > 75 ? 'var(--rose)' : m.temperature > 65 ? 'var(--gold)' : 'var(--moss)';
    const fwLabel = m.firmware === 'bitmain' ? 'Bitmain Stock' : 'Braiins OS';
    return `<tr class="miner-row-clickable" onclick="openMinerDetail(${m.id})">
      <td><div class="miner-name">${escapeHtml(m.name || 'Miner')}</div>${m.note ? `<div class="miner-note">${escapeHtml(m.note)}</div>` : ''}<div class="miner-fw">${fwLabel}</div></td>
      <td><span class="miner-ip">${escapeHtml(m.ip)}:${m.api_port || 4028}</span></td>
      <td>${m.power_watts || 0} W<br><span style="font-size:11px;color:var(--text-muted)">Prio ${m.priority || 1}</span></td>
      <td><span style="font-size:11px;color:var(--moss)">▲${m.threshold_on || 500}W</span><br><span style="font-size:11px;color:var(--rose)">▼${m.threshold_off || 400}W</span></td>
      <td>${stBadge}</td>
      <td style="font-family:var(--font-mono);color:var(--violet)">${hr}</td>
      <td style="color:${tc};font-weight:600">${temp}</td>
      <td>${modeBadge}</td>
      <td><div class="row-actions" onclick="event.stopPropagation()">
        <button class="row-btn" onclick="toggleMiner(${m.id},'${isRun ? 'pause' : 'resume'}')" title="${isRun ? 'Stoppen' : 'Starten'}">${isRun ? '⏸' : '▶'}</button>
        <button class="row-btn" onclick="openMinerDetail(${m.id})" title="Details">↻</button>
        <button class="row-btn" onclick="openEditMiner(${m.id})" title="Bearbeiten">✎</button>
        <button class="row-btn danger" onclick="deleteMiner(${m.id})" title="Löschen">🗑</button>
      </div></td>
    </tr>`;
  }).join('');
  document.getElementById('total-hashrate').textContent = totalHR.toFixed(2) + ' TH/s';
  metrics.hashrate.value = totalHR.toFixed(2) + ' TH/s';
  paintQuickstatWidget('hashrate');
  document.getElementById('total-miner-power').textContent = totalPW + ' W';
  document.getElementById('active-miners-count').textContent = active + ' aktive Miner';
  if (totalPW > 0 && totalHR > 0) document.getElementById('miner-efficiency').textContent = (totalPW / totalHR / 1000).toFixed(0) + ' J/TH';
  document.getElementById('miner-mode-display').textContent = autoC + '/' + miners.length;
  document.getElementById('miner-mode-sub').textContent = autoC + ' Auto, ' + (miners.length - autoC) + ' Manuell';
  document.getElementById('si-miner').className = 'status-dot-ind ' + (active > 0 ? 'si-ok' : 'si-warn');
  document.getElementById('si-miner-text').textContent = active + ' aktiv';
  const isAuto = miners.some(m => m.automation_enabled);
  const b = document.getElementById('automation-badge');
  b.className = 'badge ' + (isAuto ? 'badge-running' : 'badge-offline');
  b.textContent = isAuto ? 'Automatisierung aktiv' : 'Automatisierung inaktiv';
}

/* ── BTC ────────────────────────────────────────────────────────── */
function updateBTC(d) {
  state.btc = d;
  document.getElementById('btc-price-eur').textContent = Number(d.price_eur || 0).toLocaleString('de-AT') + ' €';
  document.getElementById('btc-price-usd').textContent = Number(d.price_usd || 0).toLocaleString('en-US') + ' $';
  const chg = d.change_24h || 0;
  const el = document.getElementById('btc-change');
  el.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';
  el.style.color = chg >= 0 ? 'var(--moss)' : 'var(--rose)';
  document.getElementById('si-btc').className = 'status-dot-ind si-ok';
  document.getElementById('si-btc-text').textContent = 'CoinGecko';
}

/* ── Prices ─────────────────────────────────────────────────────── */
function updatePrices(d) {
  state.prices = d;
  document.getElementById('current-price').textContent = (d.current_price || 0).toFixed(4) + ' €/kWh';
  if (d.updated) document.getElementById('price-updated').textContent = 'Stand: ' + new Date(d.updated).toLocaleTimeString('de-AT');
  if (d.cheapest_today) {
    document.getElementById('cheapest-price').textContent = d.cheapest_today.price.toFixed(4) + ' €/kWh';
    document.getElementById('cheapest-hour').textContent = d.cheapest_today.hour + ':00 Uhr';
  }
  if (d.prices_today && priceChart) {
    const cheap = d.cheapest_today?.price || 0;
    priceChart.data.labels = d.prices_today.map(p => p.hour + ':00');
    priceChart.data.datasets[0].data = d.prices_today.map(p => p.price);
    priceChart.data.datasets[0].backgroundColor = d.prices_today.map(p =>
      p.is_current ? CHART_COLORS.amber + 'dd' : (p.price <= cheap * 1.1 ? CHART_COLORS.moss + '99' : CHART_COLORS.teal + '55'));
    priceChart.update('none');
    if (d.prices_today.length > 1) {
      const first = d.prices_today[0].price;
      const last = d.prices_today[d.prices_today.length - 1].price;
      const delta = first !== 0 ? ((last - first) / first * 100) : 0;
      const el = document.getElementById('price-delta');
      el.textContent = (delta >= 0 ? '+' : '') + delta.toFixed(1) + '%';
      el.style.color = delta >= 0 ? 'var(--rose)' : 'var(--moss)';
    }
  }
}

/* ── Icon-System (reine SVG-Linien-Icons, keine Emoji-Glyphen) ────── */
const ICONS = {
  sun: '<svg viewBox="0 0 24 24" width="1em" height="1em"><circle cx="12" cy="12" r="4.6" fill="currentColor"/><g stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M12 2.5v2.6M12 18.9v2.6M21.5 12h-2.6M5.1 12H2.5M18.4 5.6l-1.8 1.8M7.4 16.6l-1.8 1.8M18.4 18.4l-1.8-1.8M7.4 7.4L5.6 5.6"/></g></svg>',
  moon: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M20.4 14.7A8.9 8.9 0 1 1 9.3 3.6a7.3 7.3 0 1 0 11.1 11.1z"/></svg>',
  cloud: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M7.5 18.5a5 5 0 0 1-.6-9.97 5.8 5.8 0 0 1 11.2-1.9A4.6 4.6 0 0 1 18 18.5z"/></svg>',
  cloudSun: '<svg viewBox="0 0 24 24" width="1em" height="1em"><circle cx="7.2" cy="7.5" r="3.1" fill="currentColor" opacity="0.9"/><path fill="currentColor" d="M9.5 20a4.6 4.6 0 0 1-.5-9.17 5.6 5.6 0 0 1 3-2.53 5.4 5.4 0 0 1 1.3-.2 5.6 5.6 0 0 1 5.5 4.6A4.4 4.4 0 0 1 18.4 20z"/></svg>',
  cloudRain: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M7.5 15.2a5 5 0 0 1-.6-9.97 5.8 5.8 0 0 1 11.2-1.9 4.6 4.6 0 0 1-.1 9.17z"/><g stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><path d="M8.5 18.2l-1.2 2.4M12.5 18.2l-1.2 2.4M16.5 18.2l-1.2 2.4"/></g></svg>',
  cloudSnow: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M7.5 14.7a5 5 0 0 1-.6-9.97 5.8 5.8 0 0 1 11.2-1.9 4.6 4.6 0 0 1-.1 9.17z"/><g stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M8.5 18v3.2M7 19.2l3 1.6M11 19.2l-3 1.6M15.5 18v3.2M14 19.2l3 1.6M18 19.2l-3 1.6"/></g></svg>',
  storm: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M7.5 13.7a5 5 0 0 1-.6-9.97 5.8 5.8 0 0 1 11.2-1.9 4.6 4.6 0 0 1-.1 9.17z" opacity="0.95"/><path fill="currentColor" d="M13 13l-3.6 5.4h2.6L10.4 22l5.2-6.2h-2.7L14.8 13z"/></svg>',
  fog: '<svg viewBox="0 0 24 24" width="1em" height="1em"><g stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M4 8.5h11M4 12h16M4 15.5h13M4 19h9"/></g></svg>',
  drop: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M12 2.5S6 10.2 6 14.3a6 6 0 0 0 12 0C18 10.2 12 2.5 12 2.5z"/></svg>',
  person: '<svg viewBox="0 0 24 24" width="1em" height="1em"><circle cx="12" cy="7.2" r="3.4" fill="currentColor"/><path fill="currentColor" d="M5 20.2c.5-3.8 3.3-6 7-6s6.5 2.2 7 6a1 1 0 0 1-1 1.1H6a1 1 0 0 1-1-1.1z"/></svg>',
  chip: '<svg viewBox="0 0 24 24" width="1em" height="1em"><rect x="6.5" y="6.5" width="11" height="11" rx="1.6" fill="none" stroke="currentColor" stroke-width="1.8"/><rect x="9.7" y="9.7" width="4.6" height="4.6" rx="0.8" fill="currentColor"/><g stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M9 3.2v2.3M12 3.2v2.3M15 3.2v2.3M9 18.5v2.3M12 18.5v2.3M15 18.5v2.3M3.2 9h2.3M3.2 12h2.3M3.2 15h2.3M18.5 9h2.3M18.5 12h2.3M18.5 15h2.3"/></g></svg>',
  swap: '<svg viewBox="0 0 24 24" width="1em" height="1em"><g fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 8h13l-3-3M20 16H7l3 3"/></g></svg>',
  bolt: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M13 2L4 14h6l-1 8 9-12h-6z"/></svg>',
  house: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M12 3.2 3 10.5V21h6v-6.5h6V21h6V10.5z"/></svg>',
  eye: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M12 5c-5 0-9.3 3.1-11 7 1.7 3.9 6 7 11 7s9.3-3.1 11-7c-1.7-3.9-6-7-11-7zm0 11.5A4.5 4.5 0 1 1 12 7.5a4.5 4.5 0 0 1 0 9z"/><circle cx="12" cy="12" r="2" fill="var(--bg-card,#1a221c)"/></svg>',
  battery: '<svg viewBox="0 0 24 24" width="1em" height="1em"><rect x="3" y="7" width="16" height="10" rx="2" fill="none" stroke="currentColor" stroke-width="1.8"/><rect x="20" y="10" width="2" height="4" rx="0.8" fill="currentColor"/><rect x="5.2" y="9.2" width="11.6" height="5.6" rx="1" fill="currentColor" opacity="0.85"/></svg>',
  report: '<svg viewBox="0 0 24 24" width="1em" height="1em"><rect x="5" y="3" width="14" height="18" rx="2" fill="none" stroke="currentColor" stroke-width="1.8"/><g stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M8.2 8h7.6M8.2 12h7.6M8.2 16h4.8"/></g></svg>',
  sunrise: '<svg viewBox="0 0 24 24" width="1em" height="1em"><path fill="currentColor" d="M12 6.5a5.3 5.3 0 0 1 5.3 5.3h1.7a1 1 0 0 1 0 2H5a1 1 0 0 1 0-2h1.7A5.3 5.3 0 0 1 12 6.5z"/><g stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><path d="M12 2.3v2.1M4.6 6.9l1.5 1.5M19.4 6.9l-1.5 1.5M3 17.5h18"/></g></svg>',
};
function icon(name) { return ICONS[name] || ''; }

/* ══════════════════════════════════════════════════════════════════
   SCHNELLÜBERSICHT-WIDGETS — personalisierbar (Einstellungen →
   Persönlich → "Schnellübersicht-Widgets"). Jede der vier Kacheln
   zeigt einen frei wählbaren Datentyp (z.B. Personen erkannt,
   Verbrauch, Tagesbericht, ...). Der aktuelle Wert je Datentyp liegt
   in `metrics`, aktualisiert von den jeweiligen update*()-Funktionen
   weiter unten (updateWeather, updateSolar, updateMiners,
   updateSurveillance, loadDashboard). Welcher Datentyp in welcher der
   vier Kacheln (Slot 0-3) angezeigt wird, steht in
   personalization.widgets (siehe Personalisierungs-Block, persistiert
   in localStorage).
   ══════════════════════════════════════════════════════════════════ */
const WIDGET_DEFS = {
  persons:  { label: 'Personen erkannt',    icon: 'person',   color: 'rose' },
  load:     { label: 'Verbrauch',           icon: 'bolt',     color: 'teal' },
  hashrate: { label: 'Hashrate',            icon: 'chip',     color: 'violet' },
  temp:     { label: 'Außentemperatur',     icon: 'cloudSun', color: 'gold' },
  pv:       { label: 'PV-Erzeugung',        icon: 'sun',      color: 'sun' },
  battery:  { label: 'Batterie',            icon: 'battery',  color: 'teal' },
  events:   { label: 'Aktuelles Ereignis',  icon: 'eye',      color: 'rose' },
  dailyReport: { label: 'Tagesbericht',     icon: 'report',   color: 'moss' },
  morningMessage: { label: 'Morgennachricht', icon: 'sunrise', color: 'gold' },
};
const DEFAULT_WIDGETS = ['persons', 'load', 'hashrate', 'temp'];

// Zuletzt bekannter Wert je Widget-Typ, unabhängig davon, ob der Typ
// gerade in einer sichtbaren Kachel steht - so steht sofort ein Wert
// bereit, sobald man in den Einstellungen auf diesen Typ umschaltet.
const metrics = {
  persons:  { value: '–', label: 'Personen erkannt' },
  load:     { value: '0 W', label: 'Verbrauch' },
  hashrate: { value: '0.00 TH/s', label: 'Hashrate' },
  temp:     { value: '–°', label: 'Außentemperatur' },
  pv:       { value: '0 W', label: 'PV-Erzeugung' },
  battery:  { value: '–%', label: 'Batterie' },
  events:   { value: '–', label: 'Keine Ereignisse' },
  dailyReport: { value: '–', label: 'Tagesbericht' },
  morningMessage: { available: false, label: '', text: 'Noch nicht verfügbar', slots: [] },
};

function currentWidgetOrder() {
  const w = personalization.widgets;
  return (Array.isArray(w) && w.length === 4) ? w : DEFAULT_WIDGETS;
}

/** Baut alle vier Kacheln komplett neu auf (bei Personalisierungs- oder
 * Reihenfolge-Änderung) und befüllt sie sofort mit den zwischengespeicherten
 * Werten aus `metrics`. */
function quickstatSlotHTML(key, i) {
  const def = WIDGET_DEFS[key] || WIDGET_DEFS.persons;
  if (key === 'morningMessage') {
    return `
          <div class="quickstat quickstat-mm" data-qs-key="${key}" data-slot="${i}">
            <div class="mm-head">
              <div class="quickstat-icon ${def.color}" id="qs-slot-${i}-icon">${icon(def.icon)}</div>
              <div class="mm-head-text">
                <div class="quickstat-label" id="qs-slot-${i}-label">${escapeHtml(def.label)}</div>
                <div class="mm-sub" id="qs-slot-${i}-sub">–</div>
              </div>
            </div>
            <div class="mm-chart" id="qs-slot-${i}-chart"></div>
            <div class="mm-pv" id="qs-slot-${i}-pv"></div>
          </div>`;
  }
  return `
          <div class="quickstat" data-qs-key="${key}" data-slot="${i}">
            <div class="quickstat-icon ${def.color}" id="qs-slot-${i}-icon">${icon(def.icon)}</div>
            <div>
              <div class="quickstat-value" id="qs-slot-${i}-value">–</div>
              <div class="quickstat-label" id="qs-slot-${i}-label">${escapeHtml(def.label)}</div>
            </div>
          </div>`;
}

function renderQuickstatSlots() {
  const row = document.getElementById('quickstats-row');
  if (!row) return;
  const order = currentWidgetOrder();
  row.innerHTML = order.map((key, i) => quickstatSlotHTML(key, i)).join('');
  initQuickstatReorder();
  order.forEach(key => paintQuickstatWidget(key));
}

/** Trägt den aktuellen Wert für einen Widget-Typ in jede Kachel ein, die
 * gerade diesen Typ anzeigt (i.d.R. genau eine). Wird von den einzelnen
 * update*()-Funktionen nach jeder Datenaktualisierung aufgerufen -
 * deutlich günstiger als bei jedem Poll alle vier Kacheln neu zu bauen. */
function paintQuickstatWidget(key) {
  const m = metrics[key];
  if (!m) return;
  currentWidgetOrder().forEach((k, i) => {
    if (k !== key) return;
    if (key === 'morningMessage') { paintMorningMessageWidget(i, m); return; }
    const valEl = document.getElementById(`qs-slot-${i}-value`);
    const lblEl = document.getElementById(`qs-slot-${i}-label`);
    if (valEl) {
      valEl.textContent = m.value;
      const isLongText = key === 'dailyReport';
      valEl.classList.toggle('quickstat-value-sm', isLongText);
      valEl.title = isLongText ? m.value : '';
    }
    if (lblEl) lblEl.textContent = m.label;
  });
}

/** Eigene, kompaktere Darstellung für das "Morgennachricht"-Widget: Kopfzeile
 * (Icon + Titel + Tagesdatum), eine kleine Temperaturkurve mit beschrifteten
 * Punkten für Morgens/Mittags/Abends, und darunter die PV-Schätzung samt
 * Regenrisiko. `m` ist hier direkt das strukturierte morning_message-Objekt
 * vom Server (siehe _build_morning_message im Backend), nicht {value,label}
 * wie bei den übrigen Widgets. */
function paintMorningMessageWidget(i, m) {
  const sub = document.getElementById(`qs-slot-${i}-sub`);
  const chart = document.getElementById(`qs-slot-${i}-chart`);
  const pv = document.getElementById(`qs-slot-${i}-pv`);
  if (!sub || !chart || !pv) return;

  if (!m || !m.available) {
    sub.textContent = (m && m.text) ? m.text : 'Noch nicht verfügbar';
    chart.innerHTML = '';
    pv.innerHTML = '';
    return;
  }

  sub.textContent = m.label || '';

  const slots = m.slots || [];
  if (slots.length >= 2) {
    const w = 220, h = 70, padX = 20, padTop = 22, padBottom = 18;
    const temps = slots.map(s => s.temp);
    const lo = Math.min(...temps), hi = Math.max(...temps);
    const range = Math.max(1, hi - lo);
    const stepX = (w - padX * 2) / (slots.length - 1);
    const usableH = h - padTop - padBottom;
    const pts = slots.map((s, idx) => ({
      x: padX + idx * stepX,
      y: padTop + usableH - ((s.temp - lo) / range) * usableH,
      s,
    }));
    const line = pts.map((p, idx) => `${idx === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
    const area = `${line} L${pts[pts.length - 1].x.toFixed(1)},${h} L${pts[0].x.toFixed(1)},${h} Z`;
    const marks = pts.map(p => `
        <circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="3" class="mm-chart-dot"></circle>
        <text x="${p.x.toFixed(1)}" y="${(p.y - 8).toFixed(1)}" text-anchor="middle" class="mm-chart-temp">${Math.round(p.s.temp)}°</text>
        <text x="${p.x.toFixed(1)}" y="${h - 3}" text-anchor="middle" class="mm-chart-name">${escapeHtml(p.s.name)}</text>`).join('');
    chart.innerHTML = `
      <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" class="mm-chart-svg">
        <path d="${area}" class="mm-chart-area"></path>
        <path d="${line}" class="mm-chart-line"></path>
        ${marks}
      </svg>`;
  } else {
    chart.innerHTML = '';
  }

  const bits = [];
  if (m.pv_kwh !== null && m.pv_kwh !== undefined) {
    bits.push(`<span class="mm-pv-value">${icon('sun')} ${m.pv_kwh.toFixed(1)} kWh</span>`);
  }
  if (m.rain_probability !== null && m.rain_probability !== undefined) {
    bits.push(`<span class="mm-pv-rain">${icon('cloudRain')} ${m.rain_probability}%</span>`);
  }
  pv.innerHTML = bits.join('');
}

/* ── Weather ────────────────────────────────────────────────────── */
const WEATHER_ICONS = {
  'Sonnig': 'sun', 'Teilweise bewoelkt': 'cloudSun', 'Bewoelkt': 'cloud', 'Nebel': 'fog',
  'Leichter Regen': 'cloudRain', 'Regen': 'cloudRain', 'Starker Regen': 'cloudRain', 'Schnee': 'cloudSnow',
  'Starker Schnee': 'cloudSnow', 'Regenschauer': 'cloudRain', 'Starker Schauer': 'storm',
  'Gewitter': 'storm', 'Gewitter mit Hagel': 'storm', 'Unbekannt': 'cloudSun',
};

function weatherIcon(conditions, isDay = true) {
  if (conditions === 'Sonnig' && !isDay) return icon('moon');
  return icon(WEATHER_ICONS[conditions] || 'cloudSun');
}

function updateWeather(d) {
  state.weather = d;
  document.getElementById('weather-temp').textContent = d.temperature + '°';
  metrics.temp.value = d.temperature + '°';
  metrics.temp.label = d.conditions || 'Außentemperatur';
  paintQuickstatWidget('temp');
  document.getElementById('weather-cond').textContent = d.conditions || '–';
  document.getElementById('weather-humidity').textContent = (d.humidity ?? '–') + '%';
  document.getElementById('weather-wind').textContent = (d.windspeed ?? '–') + ' km/h';
  document.getElementById('weather-clouds').textContent = (d.clouds ?? '–') + '%';
  document.getElementById('weather-icon').innerHTML = weatherIcon(d.conditions, d.is_day !== false);

  if (Array.isArray(d.hourly)) renderHourlyForecast(d.hourly);
  if (Array.isArray(d.daily)) renderDailyForecast(d.daily);

  updateBackgroundFromWeather(d);
}

function renderHourlyForecast(hourly) {
  const el = document.getElementById('weather-hourly-scroll');
  if (!hourly.length) { el.innerHTML = '<div class="empty-state">Keine Vorhersage verfügbar</div>'; return; }
  el.innerHTML = hourly.map((h, i) => `
    <div class="weather-hour-item ${i === 0 ? 'now' : ''}">
      <div class="weather-hour-time">${i === 0 ? 'Jetzt' : h.hour}</div>
      <div class="weather-hour-icon">${weatherIcon(h.conditions)}</div>
      <div class="weather-hour-temp">${h.temperature}°</div>
      <div class="weather-hour-precip ${h.precipitation_probability ? '' : 'zero'}"><span class="icon-inline">${icon('drop')}</span>${h.precipitation_probability}%</div>
    </div>`).join('');
}

function renderDailyForecast(daily) {
  const el = document.getElementById('weather-daily-row');
  if (!daily.length) { el.innerHTML = '<div class="empty-state">Keine Vorhersage verfügbar</div>'; return; }
  el.innerHTML = daily.map((d, i) => `
    <div class="weather-day-item" title="${escapeHtml(d.conditions)}">
      <div class="weather-day-name">${i === 0 ? 'Heute' : d.weekday}</div>
      <div class="weather-day-icon">${weatherIcon(d.conditions)}</div>
      <div class="weather-day-temps"><span class="max">${d.temp_max}°</span><span class="min">${d.temp_min}°</span></div>
      <div class="weather-day-precip">${d.precipitation_probability ? '<span class="icon-inline">' + icon('drop') + '</span>' + d.precipitation_probability + '%' : ''}</div>
    </div>`).join('');
}

/* ── Personenerkennung / Live-Ansicht (Heimüberwachung) ───────────── */
function updateSurveillance(d) {
  state.surveillance = d;
  renderEventsWidget();
  const emptyEl = document.getElementById('surveillance-empty');
  const bodyEl = document.getElementById('surveillance-body');
  const occListEl = document.getElementById('surveillance-occupancy-list');
  const dotEl = document.getElementById('si-surv');
  const textEl = document.getElementById('si-surv-text');
  const sideDot = document.getElementById('si-cam');
  const sideText = document.getElementById('si-cam-text');

  const setEmpty = (msg, dotClass, text) => {
    metrics.persons.value = '–';
    paintQuickstatWidget('persons');
    if (emptyEl) { emptyEl.hidden = false; emptyEl.innerHTML = msg; }
    if (bodyEl) bodyEl.hidden = true;
    if (dotEl) dotEl.className = 'status-dot-ind' + (dotClass ? ' ' + dotClass : '');
    if (textEl) textEl.textContent = text;
    if (sideDot) sideDot.className = 'status-dot-ind' + (dotClass ? ' ' + dotClass : '');
    if (sideText) sideText.textContent = text;
  };

  if (!d) {
    setEmpty('Personenerkennung ist deaktiviert. <button class="link-btn" onclick="openSurveillanceSettings()">Jetzt aktivieren</button>', '', 'Deaktiviert');
    return;
  }
  if (d.reachable === false) {
    setEmpty('Personenerkennung ist aktiviert, aber gerade nicht verfügbar (fehlende Abhängigkeiten oder Dashboard wurde noch nicht neu gestartet). Details siehe Server-Log.', 'si-warn', 'Nicht verfügbar');
    return;
  }

  metrics.persons.value = d.persons_visible_total ?? 0;
  paintQuickstatWidget('persons');
  if (emptyEl) emptyEl.hidden = true;
  if (bodyEl) bodyEl.hidden = false;

  const persEl = document.getElementById('surv-persons-total');
  if (persEl) persEl.textContent = d.persons_visible_total ?? 0;
  const camsEl = document.getElementById('surv-cams-connected');
  if (camsEl) camsEl.textContent = (d.cameras_connected || 0) + '/' + (d.cameras_total || 0);
  const evEl = document.getElementById('surv-events-today');
  if (evEl) evEl.textContent = d.events_today ?? 0;
  const photosEl = document.getElementById('surv-photos-total');
  if (photosEl && d.gallery_count !== undefined) photosEl.textContent = d.gallery_count;

  if (dotEl) dotEl.className = 'status-dot-ind ' + (d.engine_alive ? 'si-ok' : 'si-warn');
  if (textEl) textEl.textContent = d.engine_alive ? 'Aktiv' : 'Engine reagiert nicht';
  if (sideDot) sideDot.className = 'status-dot-ind ' + ((d.cameras_connected || 0) > 0 ? 'si-ok' : 'si-warn');
  if (sideText) sideText.textContent = (d.cameras_connected || 0) + '/' + (d.cameras_total || 0) + ' verbunden';

  renderCameraGrid(d.cameras || []);

  if (occListEl) {
    const occ = d.occupancy || [];
    occListEl.hidden = occ.length === 0;
    occListEl.innerHTML = occ.length
      ? '<div class="form-section-title" style="margin-top:16px">Personenzähler</div>' +
        occ.map(o => `
        <div class="surv-row">
          <span class="surv-row-name">${escapeHtml(o.name)}</span>
          <span class="surv-row-value">${o.count}</span>
        </div>`).join('')
      : '';
  }
}

/* ── Kameras verwalten (nativ, im selben Prozess/Port) ─────────────── */
async function loadHeimCameras() {
  const list = document.getElementById('heim-camera-list');
  if (!list) return;
  try {
    const r = await fetch('/api/heim/cameras');
    const cams = await r.json();
    if (!cams.length) {
      list.innerHTML = '<div class="empty-state">Noch keine Kamera eingerichtet</div>';
      return;
    }
    list.innerHTML = cams.map(c => `
      <div class="card" data-cam="${escapeAttr(c.name)}" style="margin-bottom:12px;padding:16px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
          <strong>${escapeHtml(c.name)}</strong>
          <div style="display:flex;gap:8px;align-items:center">
            <span class="badge ${c.connected ? 'badge-running' : 'badge-offline'}">${c.connected ? 'Verbunden' : 'Getrennt'}</span>
            <button class="btn btn-ghost btn-sm heim-cam-zones">Zonen (${c.zones})</button>
            <button class="row-btn danger heim-cam-delete" title="Löschen">🗑</button>
          </div>
        </div>
        <div class="form-group" style="margin-bottom:0">
          <label class="form-label">RTSP-URL</label>
          <div style="display:flex;gap:8px">
            <input class="form-input heim-cam-url" value="${escapeAttr(c.rtsp_url)}">
            <button class="btn btn-ghost btn-sm heim-cam-save">Speichern</button>
          </div>
        </div>
      </div>`).join('');

    list.querySelectorAll('.card[data-cam]').forEach(card => {
      const name = card.dataset.cam;
      card.querySelector('.heim-cam-delete').addEventListener('click', () => heimDeleteCamera(name));
      card.querySelector('.heim-cam-zones').addEventListener('click', () => openZonesEditor(name));
      card.querySelector('.heim-cam-save').addEventListener('click', () => {
        const url = card.querySelector('.heim-cam-url').value.trim();
        heimUpdateCamera(name, url);
      });
    });
  } catch (e) {
    list.innerHTML = '<div class="empty-state">Fehler beim Laden der Kameras</div>';
  }
}

async function heimAddCamera() {
  const name = document.getElementById('heim-newcam-name').value.trim();
  const rtsp_url = document.getElementById('heim-newcam-url').value.trim();
  if (!name || !rtsp_url) { showToast('Name und RTSP-URL sind Pflichtfelder', 'error'); return; }
  const r = await fetch('/api/heim/cameras', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, rtsp_url }) });
  const d = await r.json();
  if (d.success) {
    document.getElementById('heim-newcam-name').value = '';
    document.getElementById('heim-newcam-url').value = '';
    showToast('Kamera hinzugefügt', 'success');
    loadHeimCameras(); loadDashboard();
  } else showToast('Fehler: ' + (d.error || 'unbekannt'), 'error');
}

async function heimUpdateCamera(name, rtsp_url) {
  if (!rtsp_url) { showToast('RTSP-URL darf nicht leer sein', 'error'); return; }
  const r = await fetch(`/api/heim/cameras/${encodeURIComponent(name)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rtsp_url }) });
  const d = await r.json();
  if (d.success) { showToast('Kamera aktualisiert', 'success'); loadDashboard(); }
  else showToast('Fehler: ' + (d.error || 'unbekannt'), 'error');
}

async function heimDeleteCamera(name) {
  if (!confirm(`Kamera "${name}" wirklich entfernen?`)) return;
  const r = await fetch(`/api/heim/cameras/${encodeURIComponent(name)}`, { method: 'DELETE' });
  const d = await r.json();
  if (d.success) { showToast('Kamera entfernt', 'success'); loadHeimCameras(); loadDashboard(); }
  else showToast('Fehler: ' + (d.error || 'unbekannt'), 'error');
}

/* ══════════════════════════════════════════════════════════════════
   ZONEN-EDITOR — pro Kamera beliebig viele Erkennungszonen als Polygon
   direkt auf dem Kamerabild einzeichnen. Punkte werden normiert (0.0-1.0)
   gespeichert, funktionieren also unabhängig von der tatsächlichen
   Kamera-Auflösung. Nur fertiggestellte Zonen (≥3 Punkte) werden gezählt;
   ohne jede Zone fällt automatisch wieder "Ganzes Bild" zurück, damit die
   Kamera nie "blind" für Ereignisse wird.
   ══════════════════════════════════════════════════════════════════ */
const ZONE_COLORS = ['#e8a34c', '#4cc7c2', '#a78bd8', '#7bbf6e', '#e2685f', '#d9b54a', '#5eb1ff'];
const zoneEditor = {
  camName: null,
  img: null,
  zones: [],        // fertige Zonen: [{name, points:[[x,y],...]}]  (normiert)
  current: [],      // Punkte der gerade gezeichneten Zone (normiert)
  canvas: null,
  ctx: null,
};

function zoneColor(index) { return ZONE_COLORS[index % ZONE_COLORS.length]; }

async function openZonesEditor(camName) {
  zoneEditor.camName = camName;
  zoneEditor.current = [];
  document.getElementById('zones-modal-title').textContent = `Zonen — ${camName}`;
  document.getElementById('zones-new-name').value = 'Zone 1';
  openModal('zones-modal');

  const canvas = document.getElementById('zones-canvas');
  zoneEditor.canvas = canvas;
  zoneEditor.ctx = canvas.getContext('2d');

  const img = new Image();
  img.crossOrigin = 'anonymous';
  img.onload = () => {
    zoneEditor.img = img;
    const maxW = canvas.parentElement.clientWidth || 640;
    const ratio = img.height / img.width;
    canvas.width = maxW;
    canvas.height = Math.round(maxW * ratio);
    drawZoneEditor();
  };
  img.onerror = () => showToast('Kamerabild konnte nicht geladen werden', 'error');
  img.src = `/api/surveillance/still/${encodeURIComponent(camName)}?t=${Date.now()}`;

  try {
    const r = await fetch(`/api/heim/cameras/${encodeURIComponent(camName)}/zones`);
    const d = await r.json();
    zoneEditor.zones = (d.success && Array.isArray(d.zones)) ? d.zones : [];
  } catch (e) {
    zoneEditor.zones = [];
  }
  renderZoneList();
  drawZoneEditor();
}

function drawZoneEditor() {
  const { ctx, canvas, img } = zoneEditor;
  if (!ctx || !canvas) return;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (img) ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

  const drawPoly = (points, color, label, dashed) => {
    if (!points.length) return;
    ctx.save();
    ctx.strokeStyle = color;
    ctx.fillStyle = color + '33';
    ctx.lineWidth = 2;
    if (dashed) ctx.setLineDash([6, 5]);
    ctx.beginPath();
    points.forEach(([x, y], i) => {
      const px = x * canvas.width, py = y * canvas.height;
      i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
    });
    if (!dashed) ctx.closePath();
    ctx.fill();
    ctx.stroke();
    points.forEach(([x, y]) => {
      ctx.beginPath();
      ctx.arc(x * canvas.width, y * canvas.height, 4, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    });
    if (label) {
      const [lx, ly] = points[0];
      ctx.font = '600 12px Inter, sans-serif';
      ctx.fillStyle = color;
      ctx.fillText(label, lx * canvas.width + 6, Math.max(14, ly * canvas.height - 8));
    }
    ctx.restore();
  };

  zoneEditor.zones.forEach((z, i) => drawPoly(z.points, zoneColor(i), z.name, false));
  if (zoneEditor.current.length) {
    drawPoly(zoneEditor.current, '#eef2ea', null, true);
  }
}

function zoneEditorCanvasClick(e) {
  const canvas = zoneEditor.canvas;
  const rect = canvas.getBoundingClientRect();
  const x = (e.clientX - rect.left) / rect.width;
  const y = (e.clientY - rect.top) / rect.height;
  zoneEditor.current.push([x, y]);
  drawZoneEditor();
}

function zoneEditorUndoPoint() {
  zoneEditor.current.pop();
  drawZoneEditor();
}

function zoneEditorFinishZone() {
  if (zoneEditor.current.length < 3) { showToast('Mindestens 3 Punkte für eine Zone nötig', 'error'); return; }
  const nameInput = document.getElementById('zones-new-name');
  const name = (nameInput.value || '').trim() || `Zone ${zoneEditor.zones.length + 1}`;
  zoneEditor.zones.push({ name, points: zoneEditor.current });
  zoneEditor.current = [];
  nameInput.value = `Zone ${zoneEditor.zones.length + 1}`;
  renderZoneList();
  drawZoneEditor();
}

function zoneEditorCancelCurrent() {
  zoneEditor.current = [];
  drawZoneEditor();
}

function zoneEditorDeleteZone(index) {
  zoneEditor.zones.splice(index, 1);
  renderZoneList();
  drawZoneEditor();
}

function zoneEditorWholeFrame() {
  // Fügt eine Zone über das gesamte Kamerabild hinzu, OHNE bereits
  // bestehende Zonen zu löschen (z.B. um zusätzlich zu ein paar präzisen
  // Zonen noch einen "Alles"-Fallback zu haben). Ein kleiner Einzug (0.5%)
  // sorgt dafür, dass die Umrandung am Bildrand sichtbar bleibt.
  zoneEditor.current = [];
  const inset = 0.005;
  let name = 'Ganzes Bild';
  let n = 2;
  while (zoneEditor.zones.some(z => z.name === name)) { name = `Ganzes Bild ${n++}`; }
  zoneEditor.zones.push({
    name,
    points: [[inset, inset], [1 - inset, inset], [1 - inset, 1 - inset], [inset, 1 - inset]],
  });
  renderZoneList();
  drawZoneEditor();
  showToast(`Zone "${name}" über das gesamte Bild hinzugefügt (noch nicht gespeichert)`, 'info');
}

function renderZoneList() {
  const el = document.getElementById('zones-list');
  if (!zoneEditor.zones.length) {
    el.innerHTML = '<div class="empty-state">Noch keine Zone gezeichnet — Punkte im Bild anklicken</div>';
    return;
  }
  el.innerHTML = zoneEditor.zones.map((z, i) => `
    <div class="zone-list-item">
      <span class="zone-swatch" style="background:${zoneColor(i)}"></span>
      <span class="zone-list-name">${escapeHtml(z.name)}</span>
      <span class="zone-list-points">${z.points.length} Punkte</span>
      <button class="row-btn danger" onclick="zoneEditorDeleteZone(${i})" title="Zone löschen">🗑</button>
    </div>`).join('');
}

async function saveZonesEditor() {
  if (!zoneEditor.camName) return;
  if (zoneEditor.current.length >= 3) {
    zoneEditorFinishZone();
  }
  const r = await fetch(`/api/heim/cameras/${encodeURIComponent(zoneEditor.camName)}/zones`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ zones: zoneEditor.zones }),
  });
  const d = await r.json();
  if (d.success) {
    showToast('Zonen gespeichert', 'success');
    closeModal('zones-modal');
    loadHeimCameras();
  } else showToast('Fehler: ' + (d.error || 'unbekannt'), 'error');
}

/* ── Ereignisse: Liste / Zeitleiste / Heatmap ──────────────────────
   Alle drei Ansichten teilen sich dieselben Tagesdaten (heimEventsData),
   geladen per Datum über /api/heim/events?date=YYYY-MM-DD. Der Ereignis-Feed
   ist ohnehin auf eine begrenzte Zahl an Einträgen begrenzt (siehe Backend),
   ein voller Tag lässt sich also immer in einem Rutsch laden. */
let heimEventsView = 'list';
let heimEventsData = [];

function heimTodayStr() {
  const d = new Date();
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
}

function heimEventsToday() {
  const input = document.getElementById('heim-events-date');
  if (input) input.value = heimTodayStr();
  loadHeimEvents();
}

function setHeimEventsView(view) {
  heimEventsView = view;
  document.querySelectorAll('#heim-events-view-toggle .seg-btn').forEach(b => b.classList.toggle('active', b.dataset.view === view));
  document.getElementById('heim-events-timeline-wrap').hidden = view !== 'timeline';
  document.getElementById('heim-events-heatmap-wrap').hidden = view !== 'heatmap';
  document.getElementById('heim-events-list').hidden = view !== 'list';
  renderHeimEventsCurrentView();
}

function renderHeimEventsCurrentView() {
  if (heimEventsView === 'timeline') renderEventsTimeline(heimEventsData);
  else if (heimEventsView === 'heatmap') renderEventsHeatmap(heimEventsData);
  else renderHeimEventsList(heimEventsData);
}

function renderHeimEventsList(events) {
  const list = document.getElementById('heim-events-list');
  if (!list) return;
  if (!events.length) {
    list.innerHTML = '<div class="empty-state">Keine Ereignisse für diesen Tag</div>';
    return;
  }
  list.innerHTML = events.map(ev => `
    <div class="surv-row" style="align-items:flex-start">
      <span class="surv-row-name" style="flex:none;width:135px;color:var(--text-muted);font-size:12px">${escapeHtml(ev.time)}</span>
      <span class="surv-row-name"><strong>${escapeHtml(ev.camera)}</strong> — ${escapeHtml(ev.description)}</span>
    </div>`).join('');
}

async function loadHeimEvents() {
  const dateInput = document.getElementById('heim-events-date');
  if (dateInput && !dateInput.value) dateInput.value = heimTodayStr();
  const list = document.getElementById('heim-events-list');
  try {
    const day = dateInput ? dateInput.value : heimTodayStr();
    const r = await fetch(`/api/heim/events?date=${encodeURIComponent(day)}`);
    heimEventsData = await r.json();
    renderHeimEventsCurrentView();
  } catch (e) {
    if (list) list.innerHTML = '<div class="empty-state">Fehler beim Laden der Ereignisse</div>';
  }
}

/* ── Zeitleiste ─────────────────────────────────────────────────── */
const TIMELINE_CAMERA_COLORS = ['#e8a34c', '#4cc7c2', '#a78bd8', '#7bbf6e', '#e2685f', '#d9b54a', '#6ea8e0', '#c77dd1'];
function timelineCameraColor(cameras, name) {
  const idx = Math.max(0, cameras.indexOf(name));
  return TIMELINE_CAMERA_COLORS[idx % TIMELINE_CAMERA_COLORS.length];
}
// Erwartet "dd.mm.yyyy HH:MM:SS" (siehe EventFeed.add) - liefert Minute des Tages (0-1439.99).
function eventTimeToDayMinutes(timeStr) {
  const m = (timeStr || '').match(/(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  return (+m[1]) * 60 + (+m[2]) + (+m[3]) / 60;
}

function renderEventsTimeline(events) {
  const track = document.getElementById('heim-events-timeline');
  const hoursEl = document.getElementById('heim-events-timeline-hours');
  const detailEl = document.getElementById('heim-events-timeline-detail');
  if (!track || !hoursEl || !detailEl) return;

  if (!hoursEl.childElementCount) {
    hoursEl.innerHTML = [0, 3, 6, 9, 12, 15, 18, 21, 24]
      .map(h => `<span style="left:${Math.min(h / 24 * 100, 100)}%">${String(h % 24).padStart(2, '0')}:00</span>`).join('');
  }

  if (!events.length) {
    track.innerHTML = '';
    detailEl.innerHTML = '<span class="muted">Keine Ereignisse für diesen Tag</span>';
    return;
  }

  const cameras = [...new Set(events.map(e => e.camera))];
  track.innerHTML = events.map((ev, i) => {
    const mins = eventTimeToDayMinutes(ev.time);
    if (mins === null) return '';
    const pct = (mins / 1440 * 100).toFixed(2);
    return `<button type="button" class="event-timeline-marker" style="left:${pct}%;background:${timelineCameraColor(cameras, ev.camera)}"
              title="${escapeAttr(ev.time)} · ${escapeAttr(ev.camera)} · ${escapeAttr(ev.description || '')}"
              onclick="showTimelineEventDetail(${i})"></button>`;
  }).join('');

  showTimelineEventDetail(0);
}

function showTimelineEventDetail(index) {
  const ev = heimEventsData[index];
  const detailEl = document.getElementById('heim-events-timeline-detail');
  if (!ev || !detailEl) return;
  document.querySelectorAll('#heim-events-timeline .event-timeline-marker').forEach((m, i) => m.classList.toggle('active', i === index));
  detailEl.innerHTML = `
    <span class="event-timeline-detail-time">${escapeHtml(ev.time)}</span>
    <strong>${escapeHtml(ev.camera)}</strong>
    <span>${escapeHtml(ev.description || '')}</span>
    ${ev.snapshot ? `<img src="/api/heim/gallery/${encodeURIComponent(ev.snapshot)}" alt="" loading="lazy">` : ''}
  `;
}

/* ── Bewegungs-Heatmap (Kamera × Tagesstunde) ──────────────────────── */
function renderEventsHeatmap(events) {
  const wrap = document.getElementById('heim-events-heatmap');
  if (!wrap) return;
  if (!events.length) {
    wrap.innerHTML = '<div class="empty-state">Keine Ereignisse für diesen Tag</div>';
    return;
  }

  const cameras = [...new Set(events.map(e => e.camera))].sort();
  // counts[camera][hour] = Anzahl Ereignisse
  const counts = {};
  cameras.forEach(c => { counts[c] = new Array(24).fill(0); });
  let maxCount = 1;
  events.forEach(ev => {
    const mins = eventTimeToDayMinutes(ev.time);
    if (mins === null || !counts[ev.camera]) return;
    const hour = Math.floor(mins / 60);
    counts[ev.camera][hour]++;
    maxCount = Math.max(maxCount, counts[ev.camera][hour]);
  });

  const headRow = `<div class="event-heatmap-row head"><div></div>${
    Array.from({length: 24}, (_, h) => `<div>${h}</div>`).join('')
  }</div>`;

  const bodyRows = cameras.map(cam => `
    <div class="event-heatmap-row">
      <div class="event-heatmap-cam-name" title="${escapeAttr(cam)}">${escapeHtml(cam)}</div>
      ${counts[cam].map((c, h) => {
        const alpha = c === 0 ? 0 : 0.18 + 0.72 * (c / maxCount);
        const style = c === 0 ? '' : `style="background:rgba(232,163,76,${alpha.toFixed(2)})"`;
        return `<div class="event-heatmap-cell ${c ? 'has-events' : ''}" ${style} title="${escapeAttr(cam)} · ${String(h).padStart(2,'0')}:00 Uhr · ${c} Ereignis${c === 1 ? '' : 'se'}"></div>`;
      }).join('')}
    </div>`).join('');

  wrap.innerHTML = headRow + bodyRows;
}

/* ── Einstellungen: Erkennungsklassen, Aufnahme, Benachrichtigungen ── */
async function loadHeimSettings() {
  try {
    const r = await fetch('/api/heim/detection-classes');
    const d = await r.json();
    const box = document.getElementById('heim-classes-list');
    if (box) {
      box.innerHTML = d.available.map(c => `
        <label class="heim-class-chip">
          <input type="checkbox" value="${c.id}" ${d.selected.includes(c.id) ? 'checked' : ''}>
          <span>${escapeHtml(c.name)}</span>
        </label>`).join('');
    }
  } catch (e) { /* Personenerkennung evtl. noch nicht geladen */ }

  loadHeimNotifStatus();
  heimLoadNotifSettings();
  heimLoadGallerySettings();
  heimLoadPushStatus();
}

async function heimSaveClasses() {
  const ids = Array.from(document.querySelectorAll('#heim-classes-list input:checked')).map(el => parseInt(el.value));
  const r = await fetch('/api/heim/detection-classes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ classes: ids }) });
  const d = await r.json();
  if (d.success) showToast('Erkennungsklassen gespeichert', 'success');
}

/* ── Benachrichtigungs-Einstellungen (E-Mail / ntfy / Telegram) ───────── */
function heimNotifMethodChanged() {
  const method = document.getElementById('heim-notif-method').value;
  document.getElementById('heim-notif-email-fields').hidden = method !== 'email';
  document.getElementById('heim-notif-ntfy-fields').hidden = method !== 'ntfy';
  document.getElementById('heim-notif-telegram-fields').hidden = method !== 'telegram';
}

async function heimLoadNotifSettings() {
  try {
    const r = await fetch('/api/heim/notifications/settings');
    const d = await r.json();
    document.getElementById('heim-notif-method').value = d.method || 'console';
    document.getElementById('heim-notif-ntfy-server').value = d.ntfy?.server || '';
    document.getElementById('heim-notif-ntfy-topic').value = d.ntfy?.topic || '';
    document.getElementById('heim-notif-telegram-chatid').value = d.telegram?.chat_id || '';
    document.getElementById('heim-notif-telegram-token-hint').textContent = d.telegram?.bot_token_set ? '(bereits gesetzt — leer lassen zum Beibehalten)' : '';
    document.getElementById('heim-notif-email-server').value = d.email?.smtp_server || '';
    document.getElementById('heim-notif-email-port').value = d.email?.smtp_port || 587;
    document.getElementById('heim-notif-email-user').value = d.email?.username || '';
    document.getElementById('heim-notif-email-to').value = d.email?.to_address || '';
    document.getElementById('heim-notif-email-pass-hint').textContent = d.email?.password_set ? '(bereits gesetzt — leer lassen zum Beibehalten)' : '';
    heimNotifMethodChanged();
  } catch (e) { /* ignore */ }
}

async function heimSaveNotificationSettings() {
  const body = {
    method: document.getElementById('heim-notif-method').value,
    ntfy: {
      server: document.getElementById('heim-notif-ntfy-server').value,
      topic: document.getElementById('heim-notif-ntfy-topic').value,
    },
    telegram: {
      chat_id: document.getElementById('heim-notif-telegram-chatid').value,
      bot_token: document.getElementById('heim-notif-telegram-token').value,
    },
    email: {
      smtp_server: document.getElementById('heim-notif-email-server').value,
      smtp_port: parseInt(document.getElementById('heim-notif-email-port').value) || 587,
      username: document.getElementById('heim-notif-email-user').value,
      to_address: document.getElementById('heim-notif-email-to').value,
      password: document.getElementById('heim-notif-email-pass').value,
    },
  };
  const r = await fetch('/api/heim/notifications/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) {
    showToast('Benachrichtigungs-Einstellungen gespeichert', 'success');
    document.getElementById('heim-notif-email-pass').value = '';
    document.getElementById('heim-notif-telegram-token').value = '';
    heimLoadNotifSettings();
  } else {
    showToast(d.error || 'Fehler beim Speichern', 'error');
  }
}

/* ── Galerie (nur Fotos - Video-Aufnahme ist bewusst deaktiviert) ─────── */
let heimGalleryItems = [];
let heimGalleryLightboxIdx = -1;
let heimGallerySelectMode = false;
let heimGallerySelected = new Set();

async function heimLoadGallery() {
  try {
    const camera = document.getElementById('heim-gallery-filter-camera')?.value || '';
    const from = document.getElementById('heim-gallery-filter-from')?.value || '';
    const to = document.getElementById('heim-gallery-filter-to')?.value || '';
    const params = new URLSearchParams({ limit: 300 });
    if (camera) params.set('camera', camera);
    if (from) params.set('from', from);
    if (to) params.set('to', to);
    const r = await fetch('/api/heim/gallery?' + params.toString());
    const items = await r.json();
    heimGalleryItems = Array.isArray(items) ? items : [];
    heimPopulateGalleryCameraFilter();
    renderHeimGallery();
  } catch (e) { /* Personenerkennung evtl. noch nicht geladen */ }
}

function heimPopulateGalleryCameraFilter() {
  const sel = document.getElementById('heim-gallery-filter-camera');
  if (!sel) return;
  const current = sel.value;
  const cams = Array.from(new Set(state.cameras.map(c => c.name))).sort();
  sel.innerHTML = '<option value="">Alle Kameras</option>' + cams.map(c => `<option value="${escapeAttr(c)}">${escapeHtml(c)}</option>`).join('');
  if (cams.includes(current)) sel.value = current;
}

function heimResetGalleryFilters() {
  document.getElementById('heim-gallery-filter-camera').value = '';
  document.getElementById('heim-gallery-filter-from').value = '';
  document.getElementById('heim-gallery-filter-to').value = '';
  heimLoadGallery();
}

function renderHeimGallery() {
  const grid = document.getElementById('heim-gallery-grid');
  const empty = document.getElementById('heim-gallery-empty');
  if (!grid) return;
  if (!heimGalleryItems.length) {
    grid.innerHTML = '';
    if (empty) empty.hidden = false;
    return;
  }
  if (empty) empty.hidden = true;
  grid.innerHTML = heimGalleryItems.map((it, idx) => `
    <div class="gallery-tile ${heimGallerySelectMode ? 'selectable' : ''} ${heimGallerySelected.has(it.filename) ? 'selected' : ''}" data-fn="${escapeAttr(it.filename)}" onclick="heimGalleryTileClick(${idx})">
      ${heimGallerySelectMode ? `<div class="gallery-tile-check">${heimGallerySelected.has(it.filename) ? '✓' : ''}</div>` : ''}
      <img src="/api/heim/gallery/${encodeURIComponent(it.filename)}" loading="lazy" alt="${escapeAttr(it.camera)}">
      <div class="gallery-tile-meta">
        <span>${escapeHtml(it.camera || 'Kamera')}</span>
        <span>${escapeHtml(it.time || '')}</span>
      </div>
    </div>`).join('');
}

function heimGalleryTileClick(idx) {
  const it = heimGalleryItems[idx];
  if (!it) return;
  if (heimGallerySelectMode) {
    if (heimGallerySelected.has(it.filename)) heimGallerySelected.delete(it.filename);
    else heimGallerySelected.add(it.filename);
    renderHeimGallery();
    heimUpdateSelectBar();
  } else {
    heimOpenLightbox(idx);
  }
}

function heimToggleSelectMode() {
  heimGallerySelectMode = !heimGallerySelectMode;
  heimGallerySelected.clear();
  document.getElementById('heim-gallery-select-btn').textContent = heimGallerySelectMode ? 'Fertig' : 'Auswählen';
  document.getElementById('heim-gallery-select-bar').hidden = !heimGallerySelectMode;
  renderHeimGallery();
  heimUpdateSelectBar();
}

function heimSelectAllGallery() {
  heimGalleryItems.forEach(it => heimGallerySelected.add(it.filename));
  renderHeimGallery();
  heimUpdateSelectBar();
}

function heimUpdateSelectBar() {
  const el = document.getElementById('heim-gallery-select-count');
  if (el) el.textContent = `${heimGallerySelected.size} ausgewählt`;
}

async function heimDeleteSelectedGallery() {
  if (!heimGallerySelected.size) return;
  if (!confirm(`${heimGallerySelected.size} Foto(s) wirklich löschen?`)) return;
  const r = await fetch('/api/heim/gallery', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ filenames: Array.from(heimGallerySelected) }) });
  const d = await r.json();
  showToast(`${d.deleted || 0} Foto(s) gelöscht`, 'success');
  heimGallerySelected.clear();
  heimUpdateSelectBar();
  heimLoadGallery();
}

function heimOpenLightbox(idx) {
  heimGalleryLightboxIdx = idx;
  const it = heimGalleryItems[idx];
  if (!it) return;
  document.getElementById('gallery-lightbox-img').src = `/api/heim/gallery/${encodeURIComponent(it.filename)}`;
  document.getElementById('gallery-lightbox-meta').textContent = `${it.camera || 'Kamera'} · ${it.time || ''}`;
  document.getElementById('gallery-lightbox').classList.add('active');
}

function heimCloseLightbox() {
  document.getElementById('gallery-lightbox').classList.remove('active');
  document.getElementById('gallery-lightbox-img').src = '';
  heimGalleryLightboxIdx = -1;
}

function heimLightboxStep(dir) {
  if (heimGalleryLightboxIdx < 0 || !heimGalleryItems.length) return;
  const next = (heimGalleryLightboxIdx + dir + heimGalleryItems.length) % heimGalleryItems.length;
  heimOpenLightbox(next);
}

async function heimDeleteCurrentPhoto() {
  const it = heimGalleryItems[heimGalleryLightboxIdx];
  if (!it) return;
  if (!confirm('Dieses Foto wirklich löschen?')) return;
  const r = await fetch('/api/heim/gallery', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ filename: it.filename }) });
  const d = await r.json();
  if (d.success) {
    showToast('Foto gelöscht', 'success');
    heimCloseLightbox();
    heimLoadGallery();
  } else {
    showToast(d.error || 'Fehler beim Löschen', 'error');
  }
}

async function heimClearGallery() {
  if (!heimGalleryItems.length) return;
  if (!confirm(`Wirklich alle ${heimGalleryItems.length} Fotos löschen? Das kann nicht rückgängig gemacht werden.`)) return;
  const r = await fetch('/api/heim/gallery', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'clear' }) });
  const d = await r.json();
  if (d.success) {
    showToast(`${d.deleted} Fotos gelöscht`, 'success');
    heimLoadGallery();
  }
}

async function heimLoadGallerySettings() {
  try {
    const r = await fetch('/api/heim/gallery/settings');
    const d = await r.json();
    const ageEl = document.getElementById('heim-gallery-max-age');
    const countEl = document.getElementById('heim-gallery-max-count');
    if (ageEl) ageEl.value = d.max_photo_age_days ?? 30;
    if (countEl) countEl.value = d.max_photos ?? 5000;
    const infoEl = document.getElementById('heim-gallery-current-count');
    if (infoEl) infoEl.textContent = `Aktuell ${d.current_count ?? 0} Fotos gespeichert.`;
  } catch (e) { /* ignore */ }
}

async function heimSaveGallerySettings() {
  const body = {
    max_photo_age_days: parseInt(document.getElementById('heim-gallery-max-age').value) || 0,
    max_photos: parseInt(document.getElementById('heim-gallery-max-count').value) || 0,
  };
  const r = await fetch('/api/heim/gallery/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) {
    showToast('Aufräum-Einstellungen gespeichert', 'success');
    heimLoadGallerySettings();
  }
}

async function loadHeimNotifStatus() {
  const el = document.getElementById('heim-notif-status');
  if (!el) return;
  try {
    const r = await fetch('/api/heim/notifications/status');
    const d = await r.json();
    el.textContent = d.snoozed ? 'Benachrichtigungen sind gerade stummgeschaltet.' : 'Benachrichtigungen sind aktiv.';
  } catch (e) { el.textContent = '–'; }
}

async function heimTestNotification() {
  const r = await fetch('/api/heim/notifications/test', { method: 'POST' });
  const d = await r.json();
  showToast(d.message || (d.success ? 'Gesendet' : 'Fehler'), d.success ? 'success' : 'error');
}

async function heimSnooze(minutes) {
  await fetch('/api/heim/notifications/snooze', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ minutes }) });
  showToast(minutes > 0 ? `Benachrichtigungen für ${minutes} Minuten stummgeschaltet` : 'Stummschaltung aufgehoben', 'success');
  loadHeimNotifStatus();
}

/* ── Kamera-Kacheln (echtes Live-Video per MJPEG-Stream von der Heimüberwachung,
   dieselbe Technik wie in der eigenständigen Heimüberwachungs-App) ──────── */
function renderCameraGrid(cameras) {
  state.cameras = cameras;
  const grid = document.getElementById('cameras-grid-page');
  if (!grid) return;

  if (!cameras.length) {
    grid.hidden = true;
    grid.innerHTML = '';
    grid.dataset.camKeys = '';
    return;
  }
  grid.hidden = false;

  // Nur neu aufbauen, wenn sich Kamera-Liste oder Verbindungsstatus geändert
  // haben - sonst würde jedes Dashboard-Update (alle 30s) die laufenden
  // Live-Streams (<img>-Elemente) neu verbinden und kurz ruckeln lassen.
  const camKeys = cameras.map(c => c.name + ':' + c.connected).join('|');
  if (grid.dataset.camKeys === camKeys) {
    cameras.forEach(c => {
      const persEl = document.getElementById('cam-persons-' + escapeAttr(c.name));
      if (persEl) {
        persEl.hidden = !(c.persons_visible > 0);
        persEl.innerHTML = `<span class="icon-inline">${icon('person')}</span>${c.persons_visible}`;
      }
    });
    return;
  }
  grid.dataset.camKeys = camKeys;

  grid.innerHTML = cameras.map(c => `
    <div class="camera-tile" data-cam="${escapeAttr(c.name)}">
      <div class="camera-tile-top">
        <span>${escapeHtml(c.name)}</span>
        <span class="badge ${c.connected ? 'badge-running' : 'badge-offline'}">${c.connected ? 'Verbunden' : 'Getrennt'}</span>
      </div>
      <div class="camera-feed" id="cam-feed-${escapeAttr(c.name)}">
        ${c.connected
          ? `<img src="/api/surveillance/stream/${encodeURIComponent(c.name)}" alt="${escapeAttr(c.name)}" onerror="this.remove(); this.parentElement.insertAdjacentHTML('afterbegin','<div class=camera-overlay>Kein Bild verfügbar</div>')">
             <div class="camera-label">${escapeHtml(c.name)}</div>
             <span class="camera-persons-badge" id="cam-persons-${escapeAttr(c.name)}" ${c.persons_visible > 0 ? '' : 'hidden'}><span class="icon-inline">${icon('person')}</span>${c.persons_visible}</span>
             <div class="camera-actions">
               <button class="cam-btn cam-fullscreen-btn" title="Vollbild">⛶</button>
             </div>`
          : `<div class="camera-overlay">Nicht verbunden<br><span style="font-size:11px">${escapeHtml(c.name)}</span></div>`}
      </div>
      <div class="camera-update"><span class="camera-live-dot ${c.connected ? '' : 'offline'}"></span>${c.connected ? 'Live' : 'Kein Signal'}</div>
    </div>`).join('');

  grid.querySelectorAll('.camera-tile').forEach(tile => {
    const btn = tile.querySelector('.cam-fullscreen-btn');
    if (btn) btn.addEventListener('click', () => openFullscreen(tile.dataset.cam));
  });
}

/* ── Vollbild Kamera (echtes Live-Video, kein Standbild-Polling mehr) ──── */
function openFullscreen(camName) {
  fullscreenCamId = camName;
  const overlay = document.getElementById('fullscreen-overlay');
  const img = document.getElementById('fullscreen-img');
  const select = document.getElementById('fs-cam-select');
  select.innerHTML = state.cameras.map(c =>
    `<button class="fs-cam-btn ${c.name === camName ? 'active' : ''}" id="fs-btn-${escapeAttr(c.name)}" data-cam="${escapeAttr(c.name)}">${escapeHtml(c.name)}</button>`).join('');
  select.querySelectorAll('.fs-cam-btn').forEach(b => b.addEventListener('click', () => switchFullscreenCam(b.dataset.cam)));
  const cam = state.cameras.find(c => c.name === camName);
  document.getElementById('fullscreen-video-label').textContent = cam ? cam.name : 'Kamera';
  img.src = `/api/surveillance/stream/${encodeURIComponent(camName)}`;
  overlay.classList.add('active');
  if (fullscreenInterval) clearInterval(fullscreenInterval);
  // Läuft nur noch als Uhr für "Live seit" - der Stream selbst aktualisiert
  // sich fortlaufend von selbst, das <img> muss dafür nicht neu geladen werden.
  document.getElementById('fs-last-update').textContent = new Date().toLocaleTimeString('de-AT');
  fullscreenInterval = setInterval(() => {
    document.getElementById('fs-last-update').textContent = new Date().toLocaleTimeString('de-AT');
  }, 1000);
}

function switchFullscreenCam(camName) {
  fullscreenCamId = camName;
  const cam = state.cameras.find(c => c.name === camName);
  document.getElementById('fullscreen-video-label').textContent = cam ? cam.name : 'Kamera';
  document.querySelectorAll('.fs-cam-btn').forEach(b => b.classList.toggle('active', b.id === 'fs-btn-' + escapeAttr(camName)));
  document.getElementById('fullscreen-img').src = `/api/surveillance/stream/${encodeURIComponent(camName)}`;
}

function closeFullscreen() {
  document.getElementById('fullscreen-overlay').classList.remove('active');
  // Stream-Verbindung sauber schließen, damit im Hintergrund kein offener
  // MJPEG-Request weiterläuft, nachdem das Vollbild verlassen wurde.
  document.getElementById('fullscreen-img').src = '';
  if (fullscreenInterval) { clearInterval(fullscreenInterval); fullscreenInterval = null; }
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeFullscreen(); });

function downloadCurrentStill() {
  if (!fullscreenCamId) return;
  const a = document.createElement('a');
  a.href = `/api/surveillance/still/${encodeURIComponent(fullscreenCamId)}?t=${Date.now()}`;
  a.download = `snapshot_${fullscreenCamId.replace(/\s+/g, '_')}_${new Date().toISOString().replace(/[:.]/g, '-')}.jpg`;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

/* ── Shopping ───────────────────────────────────────────────────── */
function updateShopping(items) {
  state.shopping = items;
  document.getElementById('shopping-count').textContent = items.length;
  const el = document.getElementById('shopping-list-container');
  if (!items.length) { el.innerHTML = '<div class="empty-state">Liste ist leer</div>'; return; }
  el.innerHTML = items.map(i => `
    <div class="shopping-item ${i.completed ? 'completed' : ''}">
      <div class="shopping-check${i.completed ? ' done' : ''}" onclick="completeShoppingItem(${i.id})"></div>
      <div class="shopping-text">${escapeHtml(i.item)}</div>
      <div class="shopping-added-by">${escapeHtml(i.added_by || '')}</div>
      <button class="shopping-delete" onclick="deleteShoppingItem(${i.id})" title="Entfernen">✕</button>
    </div>`).join('');
}
function openAddShopping() {
  document.getElementById('add-shopping-form').hidden = false;
  document.getElementById('new-shopping-item').focus();
}
async function addShoppingItem() {
  const item = document.getElementById('new-shopping-item').value.trim();
  if (!item) return;
  await fetch('/api/family/shopping-list', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ item, added_by: 'Dashboard' }) });
  document.getElementById('new-shopping-item').value = '';
  document.getElementById('add-shopping-form').hidden = true;
  loadDashboard(); showToast('Artikel hinzugefügt', 'success');
}
async function completeShoppingItem(id) {
  await fetch('/api/family/shopping-list', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'complete', id }) });
  loadDashboard();
}
async function deleteShoppingItem(id) {
  await fetch('/api/family/shopping-list', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
  loadDashboard();
}

/* ── Calendar ───────────────────────────────────────────────────── */
function updateCalendar(events) {
  state.calendar = events;
  const el = document.getElementById('calendar-container');
  const now = new Date();
  const up = events.filter(e => new Date(e.start_time) >= now).sort((a, b) => new Date(a.start_time) - new Date(b.start_time)).slice(0, 6);
  if (!up.length) { el.innerHTML = '<div class="empty-state">Keine Termine geplant</div>'; return; }
  el.innerHTML = up.map(e => `
    <div class="calendar-event">
      <button class="calendar-delete" onclick="deleteCalendarEvent(${e.id})" title="Löschen">✕</button>
      <div class="calendar-event-title">${escapeHtml(e.title)}</div>
      <div class="calendar-event-time">${new Date(e.start_time).toLocaleString('de-AT')}</div>
      ${e.description ? `<div class="calendar-event-desc">${escapeHtml(e.description)}</div>` : ''}
    </div>`).join('');
}
async function addCalendarEvent() {
  const title = document.getElementById('cal-title').value.trim();
  const start = document.getElementById('cal-start').value;
  if (!title || !start) { showToast('Titel und Startzeit sind Pflicht', 'error'); return; }
  await fetch('/api/family/calendar', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title, description: document.getElementById('cal-desc').value, start_time: start }) });
  document.getElementById('cal-title').value = ''; document.getElementById('cal-desc').value = ''; document.getElementById('cal-start').value = '';
  closeModal('calendar-modal'); loadDashboard(); showToast('Termin gespeichert', 'success');
}
async function deleteCalendarEvent(id) {
  await fetch('/api/family/calendar', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
  loadDashboard();
}

/* ── Notes ──────────────────────────────────────────────────────── */
async function saveNotes() {
  const c = document.getElementById('family-notes').value;
  await fetch('/api/family/notes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ category: 'general', content: c }) });
  showToast('Notizen gespeichert', 'success');
}

/* ── Savings ────────────────────────────────────────────────────── */
function updateSavings(s) {
  document.getElementById('savings-today').textContent = (s.daily_savings || 0).toFixed(2) + ' €';
  document.getElementById('savings-month').textContent = (s.monthly_savings || 0).toFixed(2) + ' €';
  document.getElementById('savings-year').textContent = (s.yearly_savings || 0).toFixed(2) + ' €';
  document.getElementById('savings-roi').textContent = s.roi_years ? s.roi_years + ' J' : '– J';
  document.getElementById('self-kwh').textContent = (s.self_consumed_kwh || 0).toFixed(1);
  document.getElementById('export-kwh').textContent = (s.exported_kwh || 0).toFixed(1);
}

/* ── Charts Update ──────────────────────────────────────────────── */
function updateCharts(cd) {
  if (cd.pv_power && pvChart) {
    pvChart.data.labels = cd.pv_power.map(d => d.x);
    pvChart.data.datasets[0].data = cd.pv_power.map(d => d.y);
    if (cd.house_load) pvChart.data.datasets[1].data = cd.house_load.map(d => d.y);
    if (cd.grid_import) pvChart.data.datasets[2].data = cd.grid_import.map(d => d.y);
    pvChart.update('none');
  }
  if (cd.pv_power && pvChartOverview) {
    pvChartOverview.data.labels = cd.pv_power.map(d => d.x);
    pvChartOverview.data.datasets[0].data = cd.pv_power.map(d => d.y);
    if (cd.house_load) pvChartOverview.data.datasets[1].data = cd.house_load.map(d => d.y);
    pvChartOverview.update('none');
  }
  if (cd.bitcoin && btcChart) {
    btcChart.data.labels = cd.bitcoin.map(d => d.x);
    btcChart.data.datasets[0].data = cd.bitcoin.map(d => d.y);
    btcChart.update('none');
  }
}

/* ── Miner History ──────────────────────────────────────────────── */
async function loadMinerHistory() {
  try {
    const r = await fetch('/api/miners/history');
    const data = await r.json();
    if (minerChart && data.length) {
      minerChart.data.labels = data.map(d => d.timestamp.substr(11, 5));
      minerChart.data.datasets[0].data = data.map(d => d.total_hashrate || 0);
      minerChart.data.datasets[1].data = data.map(d => d.surplus || 0);
      minerChart.update('none');
    }
  } catch (e) { console.error(e); }
}

/* ── System Status ─────────────────────────────────────────────── */
function updateSystemStatus(d) {
  if (!d.solar) { document.getElementById('si-pv').className = 'status-dot-ind si-err'; document.getElementById('si-pv-text').textContent = 'Offline'; }
  if (!d.btc_price) { document.getElementById('si-btc').className = 'status-dot-ind si-err'; document.getElementById('si-btc-text').textContent = 'Keine Daten'; }
}

/* ── Miner Actions ──────────────────────────────────────────────── */
async function toggleMiner(id, cmd) {
  const r = await fetch(`/api/miners/${id}/toggle`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ command: cmd }) });
  const d = await r.json();
  if (d.success) { showToast(`Miner ${cmd === 'resume' ? 'gestartet' : 'gestoppt'}`, cmd === 'resume' ? 'success' : 'info'); setTimeout(() => loadDashboard(), 1200); }
  else showToast('Befehl konnte nicht gesendet werden', 'error');
}
async function controlAllMiners(cmd) {
  const r = await fetch('/api/miners/control-all', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ command: cmd }) });
  const d = await r.json();
  if (d.success) { showToast(`Alle Miner ${cmd === 'resume' ? 'gestartet' : 'gestoppt'}`, cmd === 'resume' ? 'success' : 'info'); setTimeout(() => loadDashboard(), 1500); }
  else showToast('Befehl konnte nicht gesendet werden', 'error');
}
async function liveStatus(id) {
  const r = await fetch(`/api/miners/${id}/status`);
  const d = await r.json();
  showToast(`${d.hashrate ?? 0} TH/s · ${d.temperature ?? '–'}°C · ${d.status}`, 'info');
}
async function deleteMiner(id) {
  if (!confirm('Diesen Miner wirklich löschen?')) return;
  await fetch(`/api/miners/${id}`, { method: 'DELETE' });
  showToast('Miner gelöscht', 'success'); loadDashboard();
}

/* ── Miner Detailansicht ────────────────────────────────────────── */
let mdCurrentMinerId = null;
let mdHashrateChart = null;
let mdTempChart = null;

async function openMinerDetail(id) {
  mdCurrentMinerId = id;
  openModal('miner-detail-modal');
  await refreshMinerDetail();
}

async function refreshMinerDetail() {
  if (mdCurrentMinerId === null) return;
  const id = mdCurrentMinerId;
  try {
    const [minerR, historyR, eventsR] = await Promise.all([
      fetch(`/api/miners/${id}`),
      fetch(`/api/miners/${id}/history?hours=24`),
      fetch(`/api/miners/${id}/events?limit=30`),
    ]);
    if (!minerR.ok) { showToast('Miner nicht gefunden', 'error'); closeModal('miner-detail-modal'); return; }
    const miner = await minerR.json();
    const history = await historyR.json();
    const events = await eventsR.json();

    renderMinerDetailHero(miner);
    renderMinerDetailSettings(miner);
    renderMinerDetailCharts(history);
    renderMinerDetailEvents(events);
  } catch (e) {
    console.error('Miner-Detail konnte nicht geladen werden', e);
    showToast('Miner-Details konnten nicht geladen werden', 'error');
  }
}

function renderMinerDetailHero(m) {
  // Gleiche Logik wie in der Übersichtstabelle: Hashrate > 0 zählt als aktiv,
  // auch wenn der gemeldete "status" kurzzeitig noch "paused" ist.
  const hasHashrate = (m.hashrate || 0) > 0;
  const st = hasHashrate ? 'running' : (m.status || 'offline');
  const isRun = st === 'running';
  document.getElementById('md-name').textContent = m.name || 'Miner';
  document.getElementById('md-ip').textContent = `${m.ip}:${m.api_port || 4028} · ${m.firmware === 'bitmain' ? 'Bitmain Stock' : 'Braiins OS'}`;

  const badge = document.getElementById('md-status-badge');
  badge.className = 'badge ' + (st === 'running' ? 'badge-running' : st === 'paused' ? 'badge-paused' : 'badge-offline');
  badge.textContent = st === 'running' ? 'Aktiv' : st === 'paused' ? 'Pausiert' : 'Offline';

  document.getElementById('md-hashrate').textContent = m.hashrate ? m.hashrate.toFixed(2) + ' TH/s' : '– TH/s';
  document.getElementById('md-temp').textContent = m.temperature ? m.temperature.toFixed(0) + '°C' : '–°C';
  document.getElementById('md-power').textContent = (m.power_watts || 0) + ' W';
  document.getElementById('md-efficiency').textContent = (m.power_watts && m.hashrate) ? (m.power_watts / m.hashrate / 1000).toFixed(0) + ' J/TH' : '– J/TH';

  document.getElementById('md-start-btn').style.display = isRun ? 'none' : '';
  document.getElementById('md-stop-btn').style.display = isRun ? '' : 'none';
}

function renderMinerDetailSettings(m) {
  document.getElementById('md-miner-id').value = m.id;
  document.getElementById('md-edit-name').value = m.name || '';
  document.getElementById('md-edit-ip').value = m.ip || '';
  document.getElementById('md-edit-port').value = m.api_port || 4028;
  document.getElementById('md-edit-webport').value = m.web_port || 80;
  document.getElementById('md-edit-watts').value = m.power_watts || 3250;
  document.getElementById('md-edit-user').value = m.braiins_user || '';
  document.getElementById('md-edit-pass').value = '';
  document.getElementById('md-edit-trigger-source').value = m.trigger_source || 'pv_surplus';
  document.getElementById('md-edit-threshold-on').value = m.threshold_on || 500;
  document.getElementById('md-edit-threshold-off').value = m.threshold_off || 400;
  document.getElementById('md-edit-priority').value = m.priority || 1;
  document.getElementById('md-edit-note').value = m.note || '';
  document.getElementById('md-edit-min-runtime').value = m.min_runtime || 300;
  document.getElementById('md-edit-min-offtime').value = m.min_offtime || 300;
  document.getElementById('md-edit-automation').checked = !!m.automation_enabled;
  updateTriggerLabels('md-edit');
  const targetBtn = document.querySelector(`#md-firmware-toggle .seg-btn[data-val="${m.firmware || 'braiins'}"]`);
  if (targetBtn) setFirmwareToggle('md', targetBtn);
}

function ensureMinerDetailCharts() {
  if (mdHashrateChart) return;
  mdHashrateChart = new Chart(document.getElementById('md-hashrate-chart'), {
    type: 'line',
    data: { labels: [], datasets: [{ label: 'TH/s', data: [], borderColor: CHART_COLORS.violet, backgroundColor: CHART_COLORS.violet + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 9 }, maxTicksLimit: 6 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });
  mdTempChart = new Chart(document.getElementById('md-temp-chart'), {
    type: 'line',
    data: { labels: [], datasets: [{ label: '°C', data: [], borderColor: CHART_COLORS.rose, backgroundColor: CHART_COLORS.rose + '18', fill: true, tension: .4, pointRadius: 0, borderWidth: 2 }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 9 }, maxTicksLimit: 6 } }, y: { grid: { color: CHART_COLORS.grid }, ticks: { font: { size: 10 } } } } },
  });
}

function renderMinerDetailCharts(history) {
  ensureMinerDetailCharts();
  const labels = history.map(h => h.timestamp.substr(11, 5));
  mdHashrateChart.data.labels = labels;
  mdHashrateChart.data.datasets[0].data = history.map(h => h.hashrate || 0);
  mdHashrateChart.update('none');
  mdTempChart.data.labels = labels;
  mdTempChart.data.datasets[0].data = history.map(h => h.temperature);
  mdTempChart.update('none');
}

function renderMinerDetailEvents(events) {
  const el = document.getElementById('md-event-list');
  if (!events.length) { el.innerHTML = '<div class="empty-state">Noch keine Ereignisse aufgezeichnet</div>'; return; }
  const typeClass = { error: 'error', auto_stopped: 'warning', stopped: 'warning' };
  el.innerHTML = events.map(e => `
    <div class="notif-item ${typeClass[e.event_type] || ''}">
      <div class="notif-title">${escapeHtml(minerEventLabel(e.event_type))}</div>
      <div class="notif-msg">${escapeHtml(e.message)}</div>
      <div class="notif-time">${new Date(e.timestamp).toLocaleString('de-AT')}</div>
    </div>`).join('');
}

function minerEventLabel(type) {
  const labels = {
    created: 'Angelegt', updated: 'Einstellungen geändert', started: 'Manuell gestartet',
    stopped: 'Manuell gestoppt', auto_started: 'Automatisch gestartet', auto_stopped: 'Automatisch gestoppt',
    status_changed: 'Statusänderung erkannt', error: 'Fehler',
  };
  return labels[type] || type;
}

async function minerDetailToggle(command) {
  if (mdCurrentMinerId === null) return;
  const r = await fetch(`/api/miners/${mdCurrentMinerId}/toggle`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ command }) });
  const d = await r.json();
  if (d.success) {
    showToast(`Miner ${command === 'resume' ? 'gestartet' : 'gestoppt'}`, command === 'resume' ? 'success' : 'info');
    setTimeout(() => { refreshMinerDetail(); loadDashboard(); }, 1000);
  } else {
    showToast('Befehl konnte nicht gesendet werden', 'error');
  }
}

async function saveMinerDetailEdit() {
  const id = parseInt(document.getElementById('md-miner-id').value);
  const firmwareBtn = document.querySelector('#md-firmware-toggle .seg-btn.active');
  const body = {
    name: document.getElementById('md-edit-name').value,
    ip: document.getElementById('md-edit-ip').value,
    firmware: firmwareBtn ? firmwareBtn.dataset.val : 'braiins',
    api_port: parseInt(document.getElementById('md-edit-port').value),
    web_port: parseInt(document.getElementById('md-edit-webport').value) || 80,
    power_watts: parseInt(document.getElementById('md-edit-watts').value),
    braiins_user: document.getElementById('md-edit-user').value,
    trigger_source: document.getElementById('md-edit-trigger-source').value || 'pv_surplus',
    threshold_on: parseFloat(document.getElementById('md-edit-threshold-on').value),
    threshold_off: parseFloat(document.getElementById('md-edit-threshold-off').value),
    priority: parseInt(document.getElementById('md-edit-priority').value),
    note: document.getElementById('md-edit-note').value,
    min_runtime: parseInt(document.getElementById('md-edit-min-runtime').value),
    min_offtime: parseInt(document.getElementById('md-edit-min-offtime').value),
    automation_enabled: document.getElementById('md-edit-automation').checked ? 1 : 0,
  };
  const pass = document.getElementById('md-edit-pass').value;
  if (pass) body.braiins_pass = pass;
  const r = await fetch(`/api/miners/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) { showToast('Einstellungen gespeichert', 'success'); refreshMinerDetail(); loadDashboard(); }
  else showToast('Fehler beim Speichern', 'error');
}

async function deleteMinerFromDetail() {
  if (mdCurrentMinerId === null) return;
  if (!confirm('Diesen Miner wirklich löschen? Verlauf und Log gehen dabei verloren.')) return;
  await fetch(`/api/miners/${mdCurrentMinerId}`, { method: 'DELETE' });
  closeModal('miner-detail-modal');
  showToast('Miner gelöscht', 'success');
  loadDashboard();
}

/* ── Add / Edit Miner ───────────────────────────────────────────── */
let addFirmware = 'braiins';

const TRIGGER_SOURCE_CONFIG = {
  pv_surplus: {
    onLabel: 'Einschalten ab (W Überschuss)', offLabel: 'Ausschalten bei (W Bezug)',
    hint: 'Miner schaltet je nach PV-Überschuss ein/aus und teilt sich die verfügbare Leistung mit anderen so eingestellten Minern nach Priorität.',
    defaultOn: 500, defaultOff: 400,
  },
  grid_import: {
    onLabel: 'Einschalten, wenn Bezug unter (W)', offLabel: 'Ausschalten, wenn Bezug über (W)',
    hint: 'Miner läuft, solange der Netzbezug niedrig bleibt - unabhängig von anderen Minern.',
    defaultOn: 300, defaultOff: 800,
  },
  pv_production: {
    onLabel: 'Einschalten ab (W Erzeugung)', offLabel: 'Ausschalten unter (W Erzeugung)',
    hint: 'Miner reagiert direkt auf die PV-Rohleistung, unabhängig vom Hausverbrauch oder Netzbezug.',
    defaultOn: 2000, defaultOff: 1500,
  },
  battery_soc: {
    onLabel: 'Einschalten ab (% Ladezustand)', offLabel: 'Ausschalten unter (% Ladezustand)',
    hint: 'Miner läuft nur, wenn die Batterie ausreichend geladen ist - z. B. um erst ab 80% zu starten und unter 60% zu stoppen.',
    defaultOn: 80, defaultOff: 60,
  },
};

function updateTriggerLabels(prefix, applyDefaults = false) {
  const select = document.getElementById(`${prefix}-trigger-source`);
  if (!select) return;
  const cfg = TRIGGER_SOURCE_CONFIG[select.value] || TRIGGER_SOURCE_CONFIG.pv_surplus;
  const onLabel = document.getElementById(`${prefix}-threshold-on-label`);
  const offLabel = document.getElementById(`${prefix}-threshold-off-label`);
  const hint = document.getElementById(`${prefix}-trigger-hint`);
  if (onLabel) onLabel.textContent = cfg.onLabel;
  if (offLabel) offLabel.textContent = cfg.offLabel;
  if (hint) hint.textContent = cfg.hint;
  if (applyDefaults) {
    const onInput = document.getElementById(`${prefix}-threshold-on`);
    const offInput = document.getElementById(`${prefix}-threshold-off`);
    if (onInput) onInput.value = cfg.defaultOn;
    if (offInput) offInput.value = cfg.defaultOff;
  }
}

function setFirmwareToggle(prefix, btn) {
  const val = btn.dataset.val;
  if (prefix === 'add') addFirmware = val;
  document.querySelectorAll(`#${prefix}-firmware-toggle .seg-btn`).forEach(b => b.classList.toggle('active', b === btn));
  const isBitmain = val === 'bitmain';
  const webRow = document.getElementById(`${prefix}-bitmain-webport-row`);
  if (webRow) webRow.hidden = !isBitmain;
  const userLabel = document.getElementById(`${prefix}-user-label`);
  const passLabel = document.getElementById(`${prefix}-pass-label`);
  if (userLabel) userLabel.textContent = isBitmain ? 'Web-UI Benutzer' : 'Braiins Benutzer';
  if (passLabel) passLabel.textContent = isBitmain ? 'Web-UI Passwort' : 'Braiins Passwort';
}

async function addMiner() {
  const name = document.getElementById('add-name').value.trim();
  const ip = document.getElementById('add-ip').value.trim();
  if (!name || !ip) { showToast('Name und IP sind Pflicht', 'error'); return; }
  const body = {
    name, ip, firmware: addFirmware,
    api_port: parseInt(document.getElementById('add-port').value) || 4028,
    web_port: parseInt(document.getElementById('add-webport')?.value) || 80,
    power_watts: parseInt(document.getElementById('add-watts').value) || 3250,
    braiins_user: document.getElementById('add-user').value || 'admin',
    braiins_pass: document.getElementById('add-pass').value || '',
    trigger_source: document.getElementById('add-trigger-source').value || 'pv_surplus',
    threshold_on: parseFloat(document.getElementById('add-threshold-on').value) || 500,
    threshold_off: parseFloat(document.getElementById('add-threshold-off').value) || 400,
    priority: parseInt(document.getElementById('add-priority').value) || 1,
    min_runtime: parseInt(document.getElementById('add-min-runtime').value) || 300,
    min_offtime: parseInt(document.getElementById('add-min-offtime').value) || 300,
    note: document.getElementById('add-note').value,
  };
  const r = await fetch('/api/miners', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) { closeModal('add-miner-modal'); resetAddMinerForm(); loadDashboard(); showToast('Miner hinzugefügt', 'success'); }
  else showToast('Fehler: ' + (d.error || 'unbekannt'), 'error');
}

function resetAddMinerForm() {
  ['add-name', 'add-ip', 'add-watts', 'add-note'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('add-port').value = 4028;
  document.getElementById('add-trigger-source').value = 'pv_surplus';
  document.getElementById('add-threshold-on').value = 500;
  document.getElementById('add-threshold-off').value = 400;
  document.getElementById('add-priority').value = 1;
  document.getElementById('add-min-runtime').value = 300;
  document.getElementById('add-min-offtime').value = 300;
  updateTriggerLabels('add');
}

function openEditMiner(id) {
  const m = state.miners.find(x => x.id === id);
  if (!m) return;
  document.getElementById('edit-miner-id').value = id;
  document.getElementById('edit-name').value = m.name || '';
  document.getElementById('edit-ip').value = m.ip || '';
  document.getElementById('edit-port').value = m.api_port || 4028;
  document.getElementById('edit-webport').value = m.web_port || 80;
  document.getElementById('edit-watts').value = m.power_watts || 3250;
  document.getElementById('edit-user').value = m.braiins_user || '';
  document.getElementById('edit-pass').value = '';
  document.getElementById('edit-trigger-source').value = m.trigger_source || 'pv_surplus';
  document.getElementById('edit-threshold-on').value = m.threshold_on || 500;
  document.getElementById('edit-threshold-off').value = m.threshold_off || 400;
  document.getElementById('edit-priority').value = m.priority || 1;
  document.getElementById('edit-note').value = m.note || '';
  document.getElementById('edit-min-runtime').value = m.min_runtime || 300;
  document.getElementById('edit-min-offtime').value = m.min_offtime || 300;
  document.getElementById('edit-automation').checked = !!m.automation_enabled;
  updateTriggerLabels('edit');
  const targetBtn = document.querySelector(`#edit-firmware-toggle .seg-btn[data-val="${m.firmware || 'braiins'}"]`);
  if (targetBtn) setFirmwareToggle('edit', targetBtn);
  openModal('edit-miner-modal');
}

async function saveMinerEdit() {
  const id = parseInt(document.getElementById('edit-miner-id').value);
  const firmwareBtn = document.querySelector('#edit-firmware-toggle .seg-btn.active');
  const body = {
    name: document.getElementById('edit-name').value,
    ip: document.getElementById('edit-ip').value,
    firmware: firmwareBtn ? firmwareBtn.dataset.val : 'braiins',
    api_port: parseInt(document.getElementById('edit-port').value),
    web_port: parseInt(document.getElementById('edit-webport').value) || 80,
    power_watts: parseInt(document.getElementById('edit-watts').value),
    braiins_user: document.getElementById('edit-user').value,
    trigger_source: document.getElementById('edit-trigger-source').value || 'pv_surplus',
    threshold_on: parseFloat(document.getElementById('edit-threshold-on').value),
    threshold_off: parseFloat(document.getElementById('edit-threshold-off').value),
    priority: parseInt(document.getElementById('edit-priority').value),
    note: document.getElementById('edit-note').value,
    min_runtime: parseInt(document.getElementById('edit-min-runtime').value),
    min_offtime: parseInt(document.getElementById('edit-min-offtime').value),
    automation_enabled: document.getElementById('edit-automation').checked ? 1 : 0,
  };
  const pass = document.getElementById('edit-pass').value;
  if (pass) body.braiins_pass = pass;
  const r = await fetch(`/api/miners/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) { closeModal('edit-miner-modal'); loadDashboard(); showToast('Miner aktualisiert', 'success'); }
  else showToast('Fehler beim Speichern', 'error');
}

/* ── Miner Automation Settings ──────────────────────────────────── */
async function loadMinerSettings() {
  try {
    const r = await fetch('/api/miners/settings');
    const d = await r.json();
    document.getElementById('ms-enabled').checked = d.automation_enabled !== false;
    document.getElementById('ms-surplus').value = d.surplus_threshold || 2000;
    document.getElementById('ms-draw').value = d.draw_threshold || 500;
    document.getElementById('ms-battery').value = d.battery_threshold || 20;
    document.getElementById('ms-price').value = d.price_threshold || 0.30;
    document.getElementById('ms-start').value = d.start_time || '08:00';
    document.getElementById('ms-end').value = d.end_time || '18:00';
  } catch (e) { /* defaults bleiben */ }
}
async function saveMinerSettings() {
  const body = { miner_automation: {
    automation_enabled: document.getElementById('ms-enabled').checked,
    surplus_threshold: parseFloat(document.getElementById('ms-surplus').value),
    draw_threshold: parseFloat(document.getElementById('ms-draw').value),
    battery_threshold: parseFloat(document.getElementById('ms-battery').value),
    price_threshold: parseFloat(document.getElementById('ms-price').value),
    start_time: document.getElementById('ms-start').value,
    end_time: document.getElementById('ms-end').value,
  }};
  const r = await fetch('/api/miners/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) { closeModal('miner-settings-modal'); showToast('Automatisierung gespeichert', 'success'); }
}

/* ── General Settings ───────────────────────────────────────────── */
async function loadSettings() {
  try {
    const r = await fetch('/api/settings');
    const d = await r.json();
    document.getElementById('set-bat-low').value = d.notification_battery_low || 20;
    document.getElementById('set-bat-full').value = d.notification_battery_full || 85;
    document.getElementById('set-high-import').value = d.notification_high_import || 2000;
    document.getElementById('set-cheap-price').value = d.price_notification_threshold || 0.20;
    document.getElementById('set-fronius-ip').value = d.fronius_ip || '192.168.178.100';
    document.getElementById('set-buyback').value = d.electricity_buyback_price || 0.07;
    document.getElementById('set-install-cost').value = d.pv_install_cost || 12000;
    document.getElementById('set-pv-kwp').value = d.pv_installed_kwp || 5;
    document.getElementById('set-surv-enabled').checked = String(d.surveillance_enabled) === '1';
    document.getElementById('set-daily-report-enabled').checked = String(d.daily_report_enabled) === '1';
    document.getElementById('set-daily-report-time').value = d.daily_report_time || '20:00';
    document.getElementById('daily-report-time-row').hidden = String(d.daily_report_enabled) !== '1';
    document.getElementById('set-morning-msg-enabled').checked = String(d.morning_message_enabled) === '1';
    document.getElementById('set-morning-msg-time').value = d.morning_message_time || '18:00';
    document.getElementById('set-morning-msg-optimism').value = d.morning_message_optimism || 60;
    document.getElementById('morning-msg-optimism-val').textContent = d.morning_message_optimism || 60;
    document.getElementById('morning-msg-options').hidden = String(d.morning_message_enabled) !== '1';
  } catch (e) { /* defaults bleiben */ }
}
async function saveSettings() {
  const body = {
    notification_battery_low: parseInt(document.getElementById('set-bat-low').value),
    notification_battery_full: parseInt(document.getElementById('set-bat-full').value),
    notification_high_import: parseInt(document.getElementById('set-high-import').value),
    price_notification_threshold: parseFloat(document.getElementById('set-cheap-price').value),
    fronius_ip: document.getElementById('set-fronius-ip').value,
    electricity_buyback_price: parseFloat(document.getElementById('set-buyback').value),
    pv_install_cost: parseFloat(document.getElementById('set-install-cost').value),
    pv_installed_kwp: parseFloat(document.getElementById('set-pv-kwp').value) || 0,
    surveillance_enabled: document.getElementById('set-surv-enabled').checked ? '1' : '0',
    daily_report_enabled: document.getElementById('set-daily-report-enabled').checked ? '1' : '0',
    daily_report_time: document.getElementById('set-daily-report-time').value || '20:00',
    morning_message_enabled: document.getElementById('set-morning-msg-enabled').checked ? '1' : '0',
    morning_message_time: document.getElementById('set-morning-msg-time').value || '18:00',
    morning_message_optimism: parseInt(document.getElementById('set-morning-msg-optimism').value) || 60,
  };
  const r = await fetch('/api/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json();
  if (d.success) {
    closeModal('settings-modal'); loadDashboard();
    showToast('Einstellungen gespeichert - Dashboard neu starten, damit die Personenerkennung greift', 'success');
  }
}

/* ── Notifications ──────────────────────────────────────────────── */
function updateNotifBadge() {
  const u = state.notifications.filter(n => !n.read);
  const b = document.getElementById('notif-badge');
  b.hidden = u.length === 0;
  b.textContent = u.length;
}
function renderNotifications() {
  const el = document.getElementById('notif-list');
  if (!state.notifications.length) { el.innerHTML = '<div class="empty-state">Keine Benachrichtigungen</div>'; return; }
  el.innerHTML = state.notifications.slice(0, 30).map(n => `
    <div class="notif-item ${n.type || ''} ${!n.read ? 'unread' : ''}">
      <div class="notif-title">${escapeHtml(n.title)}</div>
      <div class="notif-msg">${escapeHtml(n.message)}</div>
      <div class="notif-time">${new Date(n.timestamp).toLocaleString('de-AT')}</div>
    </div>`).join('');
}
async function markAllNotifRead() {
  await fetch('/api/notifications', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'mark_all_read' }) });
  state.notifications.forEach(n => n.read = true);
  updateNotifBadge(); renderNotifications(); showToast('Alle als gelesen markiert', 'info');
}

/* ── Hintergrund-Personalisierung ──────────────────────────────────
   Drei Modi: 'weather' (folgt der Wetterkategorie + Tag/Nacht), 'time'
   (folgt nur der Tageszeit), 'static' (feste Akzentfarbe), 'off' (einfarbig).
   Alles reines CSS (Klassen + ein Datenattribut), keine Partikel-Animation -
   bewusst subtil gehalten. */
const ACCENT_VAR_MAP = {
  amber: '--amber-soft', teal: '--teal-soft', violet: '--violet-soft',
  moss: '--moss-soft', rose: '--rose-soft',
};

let backgroundSettings = { mode: 'weather', accent: 'amber' };

function getTimeOfDay() {
  const h = new Date().getHours();
  if (h >= 5 && h < 10) return 'morning';
  if (h >= 10 && h < 17) return 'day';
  if (h >= 17 && h < 21) return 'evening';
  return 'night';
}

function applyBackgroundMode() {
  const layer = document.getElementById('bg-layer');
  if (!layer) return;
  layer.className = 'bg-mode-' + backgroundSettings.mode;
  layer.dataset.time = getTimeOfDay();
  if (backgroundSettings.mode === 'weather' && state.weather?.category) {
    layer.classList.add('bg-cat-' + state.weather.category);
  }
  if (backgroundSettings.mode === 'static') {
    const cssVar = ACCENT_VAR_MAP[backgroundSettings.accent] || ACCENT_VAR_MAP.amber;
    layer.style.setProperty('--bg-accent-color', `var(${cssVar})`);
  }
}

function updateBackgroundFromWeather(weatherData) {
  if (backgroundSettings.mode !== 'weather') return;
  const layer = document.getElementById('bg-layer');
  if (!layer || !weatherData?.category) return;
  layer.className = 'bg-mode-weather bg-cat-' + weatherData.category;
  layer.dataset.time = weatherData.is_day === false ? 'night' : getTimeOfDay();
}

function openBackgroundModal() {
  document.querySelectorAll('.bg-mode-card').forEach(c => c.classList.toggle('selected', c.dataset.mode === backgroundSettings.mode));
  document.querySelectorAll('.bg-accent-swatch').forEach(s => s.classList.toggle('selected', s.dataset.accent === backgroundSettings.accent));
  document.getElementById('bg-accent-group').hidden = backgroundSettings.mode !== 'static';
}

function selectBackgroundMode(mode) {
  backgroundSettings.mode = mode;
  document.querySelectorAll('.bg-mode-card').forEach(c => c.classList.toggle('selected', c.dataset.mode === mode));
  document.getElementById('bg-accent-group').hidden = mode !== 'static';
}

function selectBackgroundAccent(accent) {
  backgroundSettings.accent = accent;
  document.querySelectorAll('.bg-accent-swatch').forEach(s => s.classList.toggle('selected', s.dataset.accent === accent));
}

async function saveBackgroundSettings() {
  await fetch('/api/settings', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ background_mode: backgroundSettings.mode, background_accent: backgroundSettings.accent }),
  });
  applyBackgroundMode();
  closeModal('background-modal');
  showToast('Hintergrund aktualisiert', 'success');
}

// Tageszeit-Wechsel auch ohne Datenupdate alle 5 Minuten neu auswerten
setInterval(applyBackgroundMode, 5 * 60 * 1000);
// "vor X Min." im Ereignis-Widget auch ohne neues Ereignis frisch halten
setInterval(renderEventsWidget, 30 * 1000);

/* ── Navigation (Sidebar / Seiten) ──────────────────────────────── */
const PAGE_TITLES = {
  overview: 'Übersicht', energy: 'Energie', miner: 'Bitcoin Miner',
  cameras: 'Heimüberwachung', family: 'Familie',
};

function navigateTo(page) {
  if (!PAGE_TITLES[page]) return;
  currentPage = page;
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('active', p.id === 'page-' + page));
  document.querySelectorAll('.nav-item[data-page]').forEach(b => b.classList.toggle('active', b.dataset.page === page));
  document.getElementById('page-title').textContent = PAGE_TITLES[page];
  closeSidebarMobile();

  if (page === 'energy') {
    initEnergyPageCharts();
    loadEnergyPageData();
  }
  if (page === 'cameras') {
    loadHeimCameras();
    loadHeimEvents();
    loadHeimSettings();
    heimLoadGallery();
  }
  window.scrollTo({ top: 0, behavior: 'instant' in window ? 'instant' : 'auto' });
}

function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  if (window.innerWidth <= 900) {
    sidebar.classList.toggle('mobile-open');
    document.getElementById('sidebar-backdrop').classList.toggle('active', sidebar.classList.contains('mobile-open'));
  } else {
    sidebar.classList.toggle('collapsed');
  }
}

function closeSidebarMobile() {
  const sidebar = document.getElementById('sidebar');
  sidebar.classList.remove('mobile-open');
  document.getElementById('sidebar-backdrop').classList.remove('active');
}

/* ── Modal Helpers ──────────────────────────────────────────────── */
function openModal(id) {
  document.getElementById(id).classList.add('active');
  if (id === 'settings-modal') { loadSettings(); applyPersonalization(); }
  if (id === 'miner-settings-modal') loadMinerSettings();
  if (id === 'notif-modal') renderNotifications();
  if (id === 'add-miner-modal') resetAddMinerForm();
  if (id === 'background-modal') openBackgroundModal();
}
function closeModal(id) {
  document.getElementById(id).classList.remove('active');
  if (id === 'miner-detail-modal') mdCurrentMinerId = null;
}
document.querySelectorAll('.modal-overlay').forEach(m => {
  m.addEventListener('click', e => { if (e.target === m) m.classList.remove('active'); });
});

function openSurveillanceSettings() {
  openModal('settings-modal');
  const modal = document.getElementById('settings-modal');
  modal.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  modal.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-surveillance').classList.add('active');
  const btn = Array.from(modal.querySelectorAll('.tab-btn')).find(b => b.textContent.trim() === 'Überwachung');
  if (btn) btn.classList.add('active');
}

function switchTab(e, tabId, modalId) {
  const modal = document.getElementById(modalId);
  modal.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  modal.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  e.target.classList.add('active');
}

/* ── Toast ──────────────────────────────────────────────────────── */
function showToast(msg, type = 'info') {
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.innerHTML = `<span>${escapeHtml(msg)}</span>`;
  document.getElementById('toast-container').appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; setTimeout(() => t.remove(), 300); }, 3500);
}

/* ── Helpers ────────────────────────────────────────────────────── */
function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function escapeAttr(str) { return escapeHtml(str); }

/* ── Clock ──────────────────────────────────────────────────────── */
function tickHeaderClock() {
  const now = new Date();
  const el = document.getElementById('header-time');
  if (el) el.textContent = personalization.timeFormat === '12h'
    ? now.toLocaleTimeString('de-AT', { hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true })
    : now.toLocaleTimeString('de-AT');
  renderGreeting(now);
}
setInterval(tickHeaderClock, 1000);

/* ── Init ───────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initPersonalization();
  initCommandPalette();
  initTvMode();
  initCharts();
  // Erste Datenladung parallel statt sequentiell - das Overlay verschwindet,
  // sobald die wichtigeren Dashboard-Daten da sind; die Miner-Historie zieht
  // im Hintergrund nach, ohne den ersten sichtbaren Render zu verzögern.
  loadDashboard();
  loadMinerHistory();
  setInterval(loadDashboard, 30000);
  setInterval(loadMinerHistory, 60000);
  // Kein Snapshot-Polling mehr nötig - die Kamera-Kacheln zeigen echtes
  // Live-Video über /api/surveillance/stream/<name> (MJPEG), das sich von
  // selbst fortlaufend aktualisiert. renderCameraGrid() behält die
  // laufenden Streams bei solange sich Kameraliste/Status nicht ändern.
  heimRegisterServiceWorker();
});

/* ── Browser-Push-Benachrichtigungen ───────────────────────────────── */
function heimUrlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = atob(base64);
  return Uint8Array.from([...rawData].map(c => c.charCodeAt(0)));
}

async function heimRegisterServiceWorker() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
  try {
    await navigator.serviceWorker.register('/sw.js');
  } catch (e) { console.warn('Service Worker konnte nicht registriert werden:', e); }
}

async function heimLoadPushStatus() {
  const statusEl = document.getElementById('heim-push-status');
  const enableBtn = document.getElementById('heim-push-enable-btn');
  const disableBtn = document.getElementById('heim-push-disable-btn');
  if (!statusEl) return;

  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    statusEl.textContent = 'Dieser Browser unterstützt keine Push-Benachrichtigungen.';
    if (enableBtn) enableBtn.hidden = true;
    return;
  }

  try {
    const r = await fetch('/api/push/status');
    const d = await r.json();
    if (!d.available) {
      statusEl.textContent = 'Auf dem Server fehlt das Paket "pywebpush" (pip install pywebpush) — Browser-Push ist noch nicht aktiv.';
      if (enableBtn) enableBtn.hidden = true;
      return;
    }

    const reg = await navigator.serviceWorker.ready.catch(() => null);
    const sub = reg ? await reg.pushManager.getSubscription() : null;
    if (sub) {
      statusEl.textContent = `Für diesen Browser aktiviert. Insgesamt ${d.subscriptions} Gerät(e) abonniert.`;
      if (enableBtn) enableBtn.hidden = true;
      if (disableBtn) disableBtn.hidden = false;
    } else {
      statusEl.textContent = `Noch nicht für diesen Browser aktiviert. Insgesamt ${d.subscriptions} Gerät(e) abonniert.`;
      if (enableBtn) enableBtn.hidden = false;
      if (disableBtn) disableBtn.hidden = true;
    }
  } catch (e) { /* ignore */ }
}

async function heimEnableBrowserPush() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    showToast('Dieser Browser unterstützt keine Push-Benachrichtigungen', 'error');
    return;
  }
  try {
    const permission = await Notification.requestPermission();
    if (permission !== 'granted') {
      showToast('Benachrichtigungen wurden nicht erlaubt', 'error');
      return;
    }
    const keyRes = await fetch('/api/push/public-key');
    const keyData = await keyRes.json();
    if (!keyData.available) {
      showToast('Browser-Push ist serverseitig nicht verfügbar (pywebpush fehlt)', 'error');
      return;
    }
    const reg = await navigator.serviceWorker.ready;
    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: heimUrlBase64ToUint8Array(keyData.public_key),
    });
    await fetch('/api/push/subscribe', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(sub.toJSON()) });
    showToast('Browser-Benachrichtigungen aktiviert', 'success');
    heimLoadPushStatus();
  } catch (e) {
    console.error(e);
    showToast('Aktivieren fehlgeschlagen', 'error');
  }
}

async function heimDisableBrowserPush() {
  try {
    const reg = await navigator.serviceWorker.ready;
    const sub = await reg.pushManager.getSubscription();
    if (sub) {
      await fetch('/api/push/unsubscribe', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ endpoint: sub.endpoint }) });
      await sub.unsubscribe();
    }
    showToast('Browser-Benachrichtigungen deaktiviert', 'success');
    heimLoadPushStatus();
  } catch (e) {
    console.error(e);
    showToast('Deaktivieren fehlgeschlagen', 'error');
  }
}

async function heimTestBrowserPush() {
  const r = await fetch('/api/push/test', { method: 'POST' });
  const d = await r.json();
  if (d.success) showToast(`Test-Push an ${d.sent} Gerät(e) gesendet`, 'success');
  else showToast(d.error || 'Kein aktives Abo', 'error');
}

/* ══════════════════════════════════════════════════════════════════
   PERSÖNLICHER BEREICH — Name, Akzentfarbe, Dichte, Uhrzeitformat,
   Schnellübersicht-Widgets (Auswahl + Reihenfolge). Alles rein
   clientseitig in localStorage, wirkt sofort ohne Neuladen und
   unabhängig von der Server-Konfiguration.
   ══════════════════════════════════════════════════════════════════ */
const PERSONALIZATION_KEY = 'smarthome_personalization_v1';
const ACCENT_PRESETS = {
  amber:  { c: '#e8a34c', soft: 'rgba(232,163,76,0.16)' },
  teal:   { c: '#4cc7c2', soft: 'rgba(76,199,194,0.16)' },
  violet: { c: '#a78bd8', soft: 'rgba(167,139,216,0.16)' },
  moss:   { c: '#7bbf6e', soft: 'rgba(123,191,110,0.16)' },
  rose:   { c: '#e2685f', soft: 'rgba(226,104,95,0.16)' },
};
let personalization = {
  name: '',
  accent: 'amber',
  density: 'comfortable',      // 'comfortable' | 'compact'
  timeFormat: '24h',           // '24h' | '12h'
  widgets: DEFAULT_WIDGETS.slice(),   // 4 Einträge, je einer aus WIDGET_DEFS - Inhalt & Reihenfolge der Schnellübersicht-Kacheln
};

function loadPersonalization() {
  try {
    const raw = localStorage.getItem(PERSONALIZATION_KEY);
    if (raw) personalization = { ...personalization, ...JSON.parse(raw) };
  } catch (e) { /* defaults */ }
  // Alte/beschädigte Werte abfedern: immer genau 4 gültige Widget-Typen.
  if (!Array.isArray(personalization.widgets) || personalization.widgets.length !== 4) {
    personalization.widgets = DEFAULT_WIDGETS.slice();
  }
  personalization.widgets = personalization.widgets.map(k => WIDGET_DEFS[k] ? k : 'persons');
}
function savePersonalization() {
  try { localStorage.setItem(PERSONALIZATION_KEY, JSON.stringify(personalization)); } catch (e) {}
}

function applyPersonalization() {
  const preset = ACCENT_PRESETS[personalization.accent] || ACCENT_PRESETS.amber;
  document.documentElement.style.setProperty('--user-accent', preset.c);
  document.documentElement.style.setProperty('--user-accent-soft', preset.soft);
  document.body.classList.toggle('density-compact', personalization.density === 'compact');
  renderGreeting(new Date());

  // Persönlich-Tab-Felder spiegeln, falls Modal offen/gefüllt wird
  const nameInput = document.getElementById('set-user-name');
  if (nameInput) nameInput.value = personalization.name || '';
  document.querySelectorAll('#accent-swatch-row .accent-swatch').forEach(b => {
    b.classList.toggle('active', b.dataset.accent === personalization.accent);
  });
  document.querySelectorAll('#density-toggle .seg-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.val === personalization.density);
  });
  document.querySelectorAll('#timeformat-toggle .seg-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.val === personalization.timeFormat);
  });
  populateWidgetSelects();
  syncWidgetSelects();
}

function renderGreeting(now) {
  const el = document.getElementById('header-greeting');
  if (!el) return;
  const h = now.getHours();
  let phrase = 'Willkommen zurück';
  if (h < 5) phrase = 'Noch spät unterwegs';
  else if (h < 11) phrase = 'Guten Morgen';
  else if (h < 14) phrase = 'Schönen Mittag';
  else if (h < 18) phrase = 'Guten Nachmittag';
  else if (h < 23) phrase = 'Guten Abend';
  else phrase = 'Noch spät unterwegs';
  const name = (personalization.name || '').trim();
  el.textContent = name ? `${phrase}, ${name}` : phrase;
}

function setPersonalizationAccent(key) {
  personalization.accent = key;
  applyPersonalization();
  savePersonalization();
}
function setPersonalizationDensity(val) {
  personalization.density = val;
  applyPersonalization();
  savePersonalization();
}
function setPersonalizationTimeFormat(val) {
  personalization.timeFormat = val;
  applyPersonalization();
  savePersonalization();
}
function savePersonalTab() {
  const nameInput = document.getElementById('set-user-name');
  personalization.name = nameInput ? nameInput.value.trim().slice(0, 40) : personalization.name;
  savePersonalization();
  applyPersonalization();
}

// Füllt die vier "Widget N"-Dropdowns in Einstellungen → Persönlich einmalig
// mit allen verfügbaren Datentypen aus WIDGET_DEFS.
function populateWidgetSelects() {
  for (let i = 0; i < 4; i++) {
    const sel = document.getElementById(`set-widget-${i}`);
    if (!sel || sel.options.length) continue; // schon befüllt
    sel.innerHTML = Object.keys(WIDGET_DEFS).map(key =>
      `<option value="${key}">${escapeHtml(WIDGET_DEFS[key].label)}</option>`
    ).join('');
  }
}
// Spiegelt personalization.widgets in die vier Dropdowns.
function syncWidgetSelects() {
  currentWidgetOrder().forEach((key, i) => {
    const sel = document.getElementById(`set-widget-${i}`);
    if (sel) sel.value = key;
  });
}
// Wird beim Ändern eines der vier Dropdowns aufgerufen - wirkt sofort auf
// die Übersichtsseite, kein Neuladen/Speichern-Klick nötig.
function setPersonalizationWidget(slotIndex, key) {
  if (!WIDGET_DEFS[key]) return;
  const widgets = currentWidgetOrder().slice();
  widgets[slotIndex] = key;
  personalization.widgets = widgets;
  savePersonalization();
  renderQuickstatSlots();
}

function initPersonalization() {
  loadPersonalization();
  applyPersonalization();
  renderQuickstatSlots();
}

/* ── Kachel-Reihenfolge per Drag & Drop (Schnellübersicht) ─────────
   Rein clientseitig persistiert - jedes Familienmitglied kann sich die
   vier Kacheln der Übersichtsseite nach eigenem Geschmack anordnen.
   Die Reihenfolge landet direkt in personalization.widgets, genau wie bei
   einer Auswahl über die Dropdowns in Einstellungen → Persönlich. */
function initQuickstatReorder() {
  const row = document.getElementById('quickstats-row');
  if (!row) return;
  const items = Array.from(row.children);
  items.forEach(el => {
    el.setAttribute('draggable', 'true');
    el.classList.add('draggable-card');
    el.addEventListener('dragstart', () => el.classList.add('dragging'));
    el.addEventListener('dragend', () => {
      el.classList.remove('dragging');
      persistQuickstatOrder();
    });
  });
  row.addEventListener('dragover', (e) => {
    e.preventDefault();
    const dragging = row.querySelector('.dragging');
    if (!dragging) return;
    const after = Array.from(row.querySelectorAll('.quickstat:not(.dragging)')).find(sib => {
      const box = sib.getBoundingClientRect();
      return e.clientX < box.left + box.width / 2;
    });
    if (after) row.insertBefore(dragging, after);
    else row.appendChild(dragging);
  });
}
function persistQuickstatOrder() {
  const row = document.getElementById('quickstats-row');
  if (!row) return;
  const order = Array.from(row.children).map(el => el.dataset.qsKey);
  personalization.widgets = order;
  savePersonalization();
  syncWidgetSelects();
  // Kompletter Rebuild, damit die Slot-IDs (qs-slot-0..3) wieder zur neuen
  // visuellen Reihenfolge passen - sonst würden künftige Live-Updates
  // (z.B. neue Hashrate) in der falschen Kachel landen.
  renderQuickstatSlots();
}

/* ══════════════════════════════════════════════════════════════════
   BEFEHLSPALETTE (Ctrl/Cmd+K) — schnelle Navigation & Aktionen ohne
   Maus, mit Fuzzy-Filter über Titel und Stichworte.
   ══════════════════════════════════════════════════════════════════ */
const COMMAND_ACTIONS = [
  { title: 'Übersicht öffnen', keywords: 'overview start home', run: () => navigateTo('overview'), shortcut: 'G Ü' },
  { title: 'Energie öffnen', keywords: 'energy pv solar strom', run: () => navigateTo('energy'), shortcut: 'G E' },
  { title: 'Miner öffnen', keywords: 'miner mining bitcoin hashrate', run: () => navigateTo('miner'), shortcut: 'G M' },
  { title: 'Heimüberwachung öffnen', keywords: 'kamera cameras überwachung security', run: () => navigateTo('cameras'), shortcut: 'G K' },
  { title: 'Familie öffnen', keywords: 'family kalender einkaufsliste notizen', run: () => navigateTo('family'), shortcut: 'G F' },
  { title: 'Benachrichtigungen anzeigen', keywords: 'notifications alerts', run: () => openModal('notif-modal') },
  { title: 'Einstellungen öffnen', keywords: 'settings config konfiguration', run: () => openModal('settings-modal') },
  { title: 'Hintergrund personalisieren', keywords: 'background design theme farbe', run: () => openModal('background-modal') },
  { title: 'Miner hinzufügen', keywords: 'add miner new', run: () => openModal('add-miner-modal') },
  { title: 'Neuer Termin', keywords: 'calendar termin event', run: () => openModal('calendar-modal') },
  { title: 'Alle Miner starten', keywords: 'miner start resume all', run: () => controlAllMiners('resume') },
  { title: 'Alle Miner stoppen', keywords: 'miner stop pause all', run: () => controlAllMiners('pause') },
  { title: 'Dashboard aktualisieren', keywords: 'refresh reload update', run: () => { loadDashboard(); showToast('Dashboard aktualisiert', 'info'); } },
  { title: 'Wandmontage-Modus umschalten', keywords: 'tv wall mount fullscreen wandmontage tablet kiosk', run: () => toggleTvMode(), shortcut: 'T' },
];

function initCommandPalette() {
  const overlay = document.getElementById('command-palette');
  if (!overlay) return;
  const input = document.getElementById('command-input');
  const list = document.getElementById('command-list');

  function renderList(filter) {
    const q = (filter || '').toLowerCase().trim();
    const matches = COMMAND_ACTIONS.filter(a =>
      !q || a.title.toLowerCase().includes(q) || a.keywords.includes(q)
    );
    list.innerHTML = matches.length
      ? matches.map((a, i) => `
        <div class="command-item ${i === 0 ? 'active' : ''}" data-index="${i}">
          <span>${escapeHtml(a.title)}</span>
          ${a.shortcut ? `<span class="command-shortcut">${escapeHtml(a.shortcut)}</span>` : ''}
        </div>`).join('')
      : '<div class="empty-state">Keine Treffer</div>';
    list.dataset.matches = JSON.stringify(matches.map(m => m.title));
  }

  function runByTitle(title) {
    const action = COMMAND_ACTIONS.find(a => a.title === title);
    closeCommandPalette();
    if (action) action.run();
  }

  list.addEventListener('click', (e) => {
    const item = e.target.closest('.command-item');
    if (item) runByTitle(item.querySelector('span').textContent);
  });

  input.addEventListener('input', () => renderList(input.value));
  input.addEventListener('keydown', (e) => {
    const items = Array.from(list.querySelectorAll('.command-item'));
    let idx = items.findIndex(i => i.classList.contains('active'));
    if (e.key === 'ArrowDown') { e.preventDefault(); idx = Math.min(idx + 1, items.length - 1); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); idx = Math.max(idx - 1, 0); }
    else if (e.key === 'Enter') { e.preventDefault(); if (items[idx]) runByTitle(items[idx].querySelector('span').textContent); return; }
    else if (e.key === 'Escape') { closeCommandPalette(); return; }
    else return;
    items.forEach(i => i.classList.remove('active'));
    if (items[idx]) items[idx].classList.add('active');
  });

  window.openCommandPalette = function () {
    renderList('');
    input.value = '';
    overlay.classList.add('open');
    setTimeout(() => input.focus(), 30);
  };
  window.closeCommandPalette = function () {
    overlay.classList.remove('open');
  };
  overlay.addEventListener('click', (e) => { if (e.target === overlay) closeCommandPalette(); });

  document.addEventListener('keydown', (e) => {
    const inField = ['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement?.tagName);
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      overlay.classList.contains('open') ? closeCommandPalette() : openCommandPalette();
    } else if (e.key === '/' && !inField) {
      e.preventDefault();
      openCommandPalette();
    }
  });
}

/* ── "g" + Buchstabe Tastaturkürzel für schnelle Navigation ────────── */
(function setupGotoShortcuts() {
  let pendingG = false;
  const map = { 'ü': 'overview', 'o': 'overview', 'e': 'energy', 'm': 'miner', 'k': 'cameras', 'f': 'family' };
  document.addEventListener('keydown', (e) => {
    const inField = ['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement?.tagName);
    if (inField) return;
    const key = e.key.toLowerCase();
    if (key === 'g' && !e.metaKey && !e.ctrlKey) { pendingG = true; setTimeout(() => pendingG = false, 900); return; }
    if (pendingG && map[key]) { pendingG = false; navigateTo(map[key]); return; }
    if (key === 't' && !e.metaKey && !e.ctrlKey && !e.altKey) { toggleTvMode(); }
  });
})();

/* ── TV-/Wandmontage-Modus ──────────────────────────────────────────
   Für ein an der Wand montiertes Tablet: Sidebar aus, größere Schrift.
   Zustand landet in localStorage, damit ein fest montiertes Tablet nach
   einem Reload (z.B. nächtlicher Neustart) automatisch wieder im
   TV-Modus startet. Nutzt zusätzlich die Fullscreen-API des Browsers,
   sofern verfügbar/erlaubt (schlägt z.B. in manchen eingebetteten
   Browsern fehl - wird dann still ignoriert, der Rest funktioniert trotzdem). */
const TV_MODE_KEY = 'smarthome_tv_mode_v1';

function applyTvMode(on) {
  document.body.classList.toggle('tv-mode', on);
  const btn = document.getElementById('tv-mode-btn');
  if (btn) {
    btn.classList.toggle('active', on);
    btn.title = on ? 'Wandmontage-Modus verlassen (Taste \'t\')' : 'Wandmontage-Modus (Taste \'t\')';
  }
}

function toggleTvMode() {
  const on = !document.body.classList.contains('tv-mode');
  applyTvMode(on);
  try { localStorage.setItem(TV_MODE_KEY, on ? '1' : '0'); } catch (e) {}
  if (on && document.documentElement.requestFullscreen) {
    document.documentElement.requestFullscreen().catch(() => {});
  } else if (!on && document.fullscreenElement && document.exitFullscreen) {
    document.exitFullscreen().catch(() => {});
  }
  showToast(on ? 'Wandmontage-Modus aktiviert' : 'Wandmontage-Modus beendet', 'info');
}

function initTvMode() {
  let on = false;
  try { on = localStorage.getItem(TV_MODE_KEY) === '1'; } catch (e) {}
  applyTvMode(on);
}

/* ── Kleiner Hilfslauf: Zahlen sanft hochzählen statt hart zu springen ─ */
function animateCountUp(el, newValue, decimals = 0) {
  if (!el) return;
  const prev = parseFloat(el.dataset.rawValue || '0');
  const next = parseFloat(newValue);
  if (isNaN(next)) return;
  el.dataset.rawValue = next;
  if (isNaN(prev) || Math.abs(next - prev) < 0.001) return;
  const duration = 500;
  const start = performance.now();
  function step(ts) {
    const p = Math.min(1, (ts - start) / duration);
    const eased = 1 - Math.pow(1 - p, 3);
    const val = prev + (next - prev) * eased;
    el.textContent = val.toFixed(decimals);
    if (p < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

'''

# ---------- Hauptanwendung (vormals app.py) ----------
# Referenzen auf "db", "fronius", "miners_mod", ... werden für den
# app.py-Code unten auf die bereits erzeugten Modul-Namensräume gelegt
# (genau wie es die ursprünglichen "from backend import ..."-Imports
# getan haben).
db = _database_module
fronius = _fronius_module
miners_mod = _miners_module
external_apis = _external_apis_module
surveillance = _surveillance_module
savings_mod = _savings_module
energy_analytics = _energy_analytics_module
scheduler = _scheduler_module

SERVICE_WORKER_JS = r'''
/* Service Worker für Browser-Push-Benachrichtigungen (Web Push).
   Bewusst minimal: zeigt eingehende Push-Nachrichten als System-
   Benachrichtigung an und öffnet beim Klick das Dashboard. */
self.addEventListener('push', event => {
  let payload = { title: 'SmartHome Dashboard', body: 'Neue Benachrichtigung', url: '/' };
  try { payload = { ...payload, ...event.data.json() }; } catch (e) { /* Text statt JSON */ }
  event.waitUntil(
    self.registration.showNotification(payload.title, {
      body: payload.body,
      icon: '/static/icon-192.png',
      badge: '/static/icon-192.png',
      data: { url: payload.url || '/' },
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(windowClients => {
      for (const client of windowClients) {
        if ('focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(url);
    })
  );
});
'''

"""
app.py
Hauptanwendung des SmartHome Dashboards.
Start: python app.py  (läuft auf http://0.0.0.0:5000)
"""
import logging
import os
from flask import Flask, jsonify, request, Response
from flask_socketio import SocketIO


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("smarthome.app")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, static_folder=None, template_folder=None)
app.config["SECRET_KEY"] = os.environ.get("SMARTHOME_SECRET", "change-me-in-prod")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


# ── Browser-Push-Benachrichtigungen (Web Push, VAPID) ──────────────────────
# Eigenständiger Weg zusätzlich zu E-Mail/ntfy/Telegram: Push direkt in den
# Browser, ganz ohne Cloud-Dienst eines Drittanbieters (Firebase o.ä.) - nur
# der offene Web-Push-Standard. Braucht das Paket "pywebpush"
# (pip install pywebpush); ist es nicht installiert, bleibt die Funktion
# einfach ausgeblendet/deaktiviert, der Rest vom Dashboard läuft normal weiter.
try:
    from pywebpush import webpush, WebPushException
    _WEBPUSH_AVAILABLE = True
except ImportError:
    _WEBPUSH_AVAILABLE = False
    log.warning("Paket 'pywebpush' nicht installiert - Browser-Push-Benachrichtigungen sind "
                "deaktiviert (pip install pywebpush, dann Dashboard neu starten).")

VAPID_KEY_PATH = os.path.join(os.path.dirname(db.DB_PATH), "vapid_private.pem")
_vapid_public_key_b64 = None


def _ensure_vapid_keys():
    """Erzeugt beim allerersten Start ein VAPID-Schlüsselpaar (für Web Push)
    und legt den privaten Schlüssel unter instance/vapid_private.pem ab -
    danach wird er einfach wiederverwendet. Kein externer Dienst nötig."""
    global _vapid_public_key_b64
    if not _WEBPUSH_AVAILABLE:
        return
    try:
        from py_vapid import Vapid02
        from cryptography.hazmat.primitives import serialization
        import base64 as _b64

        os.makedirs(os.path.dirname(VAPID_KEY_PATH), exist_ok=True)
        key_existed = os.path.exists(VAPID_KEY_PATH)
        # Vapid02.from_file() ist eine Classmethod (kein Instanzmethoden-Aufruf!) -
        # sie liest den Schlüssel ein, falls die Datei existiert, oder erzeugt und
        # speichert automatisch ein neues Schlüsselpaar, falls nicht. Wichtig: das
        # Rückgabeobjekt muss zugewiesen werden, sonst bleiben private_key/public_key
        # auf der ursprünglichen Instanz leer (führte zuvor zum NoneType-Fehler).
        vapid = Vapid02.from_file(VAPID_KEY_PATH)
        if not key_existed:
            log.info("Neues VAPID-Schlüsselpaar für Browser-Push erzeugt (%s).", VAPID_KEY_PATH)

        raw = vapid.public_key.public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        _vapid_public_key_b64 = _b64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    except Exception as e:
        log.warning("VAPID-Schlüssel konnten nicht geladen/erzeugt werden - Browser-Push bleibt "
                     "deaktiviert: %s", e)


_ensure_vapid_keys()


def send_browser_push(title, message, url="/"):
    """Sendet eine Push-Benachrichtigung an alle abonnierten Browser. Ungültig
    gewordene Abos (z.B. Browser abgemeldet/deinstalliert) werden dabei
    automatisch aus der Datenbank entfernt."""
    if not _WEBPUSH_AVAILABLE or not _vapid_public_key_b64:
        return 0
    subs = db.get_push_subscriptions()
    if not subs:
        return 0
    payload = json.dumps({"title": title, "body": message, "url": url})
    sent = 0
    for sub in subs:
        subscription_info = {
            "endpoint": sub["endpoint"],
            "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]},
        }
        try:
            webpush(
                subscription_info=subscription_info,
                data=payload,
                vapid_private_key=VAPID_KEY_PATH,
                vapid_claims={"sub": f"mailto:{db.get_setting('admin_email', 'admin@example.local')}"},
                ttl=300,
            )
            sent += 1
        except WebPushException as e:
            status = getattr(e.response, "status_code", None)
            if status in (404, 410):  # Abo nicht mehr gültig -> aufräumen
                db.remove_push_subscription(sub["endpoint"])
            else:
                log.warning("Push an Abo %s fehlgeschlagen: %s", sub["endpoint"][:40], e)
        except Exception as e:
            log.warning("Push an Abo %s fehlgeschlagen: %s", sub["endpoint"][:40], e)
    return sent


# Verknüpft den Scheduler (siehe _surveillance_loop) mit dem Push-Versand,
# damit bei einer neuen Erkennung automatisch auch eine Browser-Push-
# Benachrichtigung rausgeht (zusätzlich zu E-Mail/ntfy/Telegram).
scheduler.push_callback = send_browser_push if _WEBPUSH_AVAILABLE else None


# ── Seiten ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.route("/static/dashboard.css")
def _serve_dashboard_css():
    return Response(DASHBOARD_CSS, mimetype="text/css")


@app.route("/static/dashboard.js")
def _serve_dashboard_js():
    return Response(DASHBOARD_JS, mimetype="application/javascript")


# ── Dashboard Gesamtdaten (initialer Load) ────────────────────────────────
@app.route("/api/dashboard-data")
def dashboard_data():
    """
    Liest ausschließlich aus dem Scheduler-Cache (siehe backend/scheduler.py::LATEST)
    statt erneut alle externen APIs/Miner/Kameras synchron abzufragen. Das macht
    den initialen Page-Load sehr schnell und unabhängig von langsamen oder
    nicht erreichbaren externen Diensten - der Scheduler hält die Werte ohnehin
    im Hintergrund aktuell (Solar alle 10s, Miner alle 15s, etc.).
    """
    settings = db.get_all_settings()

    solar = scheduler.get_latest("solar")
    energy_prices = scheduler.get_latest("energy_prices")
    weather = scheduler.get_latest("weather")
    btc_price = scheduler.get_latest("btc_price")
    miners_with_stats = scheduler.get_latest("miners", [])
    surveillance_data = scheduler.get_latest("surveillance")

    # Solarprognose: rein rechnerisch aus der Wettervorhersage (Globalstrahlung)
    # + installierter kWp-Leistung, keine zusätzliche externe Abfrage nötig.
    solar_forecast = None
    try:
        installed_kwp = float(settings.get("pv_installed_kwp", 0) or 0)
        if installed_kwp > 0 and weather and weather.get("hourly"):
            solar_forecast = external_apis.compute_solar_forecast(weather["hourly"], installed_kwp)
    except Exception:
        solar_forecast = None

    # Fallback nur für den allerersten Request direkt nach Programmstart, bevor
    # der erste Scheduler-Durchlauf fertig ist (normalerweise <1s Verzögerung).
    if solar is None:
        solar = fronius.get_full_solar_data(settings.get("fronius_ip", "192.168.178.100"))
    if not miners_with_stats:
        miner_rows = db.get_miners()
        miners_with_stats = [{**m, **miners_mod.get_miner_stats(m)} for m in miner_rows]
    if surveillance_data is None:
        surveillance_data = surveillance.get_dashboard_data()

    grid_price = float(settings.get("grid_reference_price", 0.25)) if "grid_reference_price" in settings else (
        energy_prices["current_price"] if energy_prices else 0.25
    )
    cost_savings = savings_mod.compute_savings(grid_price=grid_price)

    energy_history = db.get_energy_history(hours=24)
    btc_history = db.get_btc_history(hours=24)

    chart_data = {
        "pv_power": [{"x": h["timestamp"][11:16], "y": h["pv_power"]} for h in energy_history],
        "house_load": [{"x": h["timestamp"][11:16], "y": h["house_load"]} for h in energy_history],
        "grid_import": [{"x": h["timestamp"][11:16], "y": h["grid_import"]} for h in energy_history],
        "bitcoin": [{"x": h["timestamp"][11:16], "y": h["price_eur"]} for h in btc_history],
    }

    # Live-Vorschau des Tagesberichts für das personalisierbare "Tagesbericht"-
    # Widget auf der Übersichtsseite (siehe WIDGET_DEFS im Frontend). Nutzt
    # dieselbe Aufbau-Funktion wie die tägliche Benachrichtigung, unabhängig
    # davon, ob/wann der Bericht heute schon automatisch verschickt wurde.
    # Beide Funktionen leben im scheduler-Modul-Namespace (siehe SCHEDULER_SOURCE),
    # daher der Aufruf über "scheduler." statt als nackter Funktionsname.
    try:
        daily_report_preview = scheduler._build_daily_report_message()
    except Exception as e:
        log.warning("Tagesbericht-Vorschau (Widget) fehlgeschlagen: %s", e)
        daily_report_preview = "Tagesbericht nicht verfügbar."

    # Morgennachricht: Vorschau auf den kommenden Tag (Wetter morgens/mittags/
    # abends + geschätzte PV-Erzeugung mit einstellbarem Optimismusgrad),
    # ebenfalls als personalisierbares Widget wählbar.
    try:
        morning_message = scheduler._build_morning_message()
    except Exception as e:
        log.warning("Morgennachricht-Vorschau (Widget) fehlgeschlagen: %s", e)
        morning_message = {"available": False, "text": "Morgennachricht nicht verfügbar.", "label": ""}

    return jsonify({
        "solar": solar,
        "energy_prices": energy_prices,
        "weather": weather,
        "btc_price": btc_price,
        "miners": miners_with_stats,
        "surveillance": surveillance_data,
        "notifications": db.get_notifications(),
        "shopping_list": db.get_shopping_list(),
        "calendar": db.get_calendar_events(),
        "notes": db.get_notes(),
        "cost_savings": cost_savings,
        "chart_data": chart_data,
        "settings": settings,
        "solar_forecast": solar_forecast,
        "daily_report_preview": daily_report_preview,
        "morning_message": morning_message,
    })


# ── Miner CRUD ─────────────────────────────────────────────────────────────
@app.route("/api/miners", methods=["GET", "POST"])
def miners_collection():
    if request.method == "GET":
        rows = db.get_miners()
        result = [{**m, **miners_mod.get_miner_stats(m)} for m in rows]
        return jsonify(result)

    data = request.get_json(force=True) or {}
    if not data.get("name") or not data.get("ip"):
        return jsonify({"success": False, "error": "Name und IP sind Pflichtfelder"}), 400
    try:
        miner_id = db.add_miner(data)
        db.add_miner_event(miner_id, "created", f"Miner \"{data['name']}\" wurde angelegt")
        return jsonify({"success": True, "id": miner_id})
    except Exception as e:
        log.exception("Fehler beim Anlegen des Miners")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/miners/<int:miner_id>", methods=["GET", "PUT", "DELETE"])
def miner_item(miner_id):
    if request.method == "GET":
        miner = db.get_miner(miner_id)
        if not miner:
            return jsonify({"error": "Miner nicht gefunden"}), 404
        stats = miners_mod.get_miner_stats(miner)
        return jsonify({**miner, **stats})

    if request.method == "DELETE":
        miner = db.get_miner(miner_id)
        db.delete_miner(miner_id)
        return jsonify({"success": True})

    data = request.get_json(force=True) or {}
    db.update_miner(miner_id, data)
    db.add_miner_event(miner_id, "updated", "Einstellungen wurden geändert")
    return jsonify({"success": True})


@app.route("/api/miners/<int:miner_id>/status")
def miner_status(miner_id):
    miner = db.get_miner(miner_id)
    if not miner:
        return jsonify({"error": "Miner nicht gefunden"}), 404
    stats = miners_mod.get_miner_stats(miner)
    return jsonify(stats)


@app.route("/api/miners/<int:miner_id>/history")
def miner_detail_history(miner_id):
    hours = request.args.get("hours", 24, type=int)
    return jsonify(db.get_miner_stats_history(miner_id, hours=hours))


@app.route("/api/miners/<int:miner_id>/events")
def miner_detail_events(miner_id):
    limit = request.args.get("limit", 50, type=int)
    return jsonify(db.get_miner_events(miner_id, limit=limit))


@app.route("/api/miners/<int:miner_id>/toggle", methods=["POST"])
def miner_toggle(miner_id):
    miner = db.get_miner(miner_id)
    if not miner:
        return jsonify({"success": False, "error": "Miner nicht gefunden"}), 404
    data = request.get_json(force=True) or {}
    command = data.get("command", "resume")
    ok = miners_mod.set_miner_power(miner, turn_on=(command == "resume"))
    if ok:
        from datetime import datetime
        db.update_miner(miner_id, {
            "last_status": "running" if command == "resume" else "paused",
            "last_state_change": datetime.now().isoformat(),
        })
        db.add_miner_event(
            miner_id, "started" if command == "resume" else "stopped",
            f"Manuell {'gestartet' if command == 'resume' else 'gestoppt'}",
        )
    return jsonify({"success": ok})


@app.route("/api/miners/control-all", methods=["POST"])
def miners_control_all():
    data = request.get_json(force=True) or {}
    command = data.get("command", "resume")
    miners_rows = db.get_miners()
    from datetime import datetime
    results = []
    for m in miners_rows:
        ok = miners_mod.set_miner_power(m, turn_on=(command == "resume"))
        if ok:
            db.update_miner(m["id"], {
                "last_status": "running" if command == "resume" else "paused",
                "last_state_change": datetime.now().isoformat(),
            })
            db.add_miner_event(
                m["id"], "started" if command == "resume" else "stopped",
                f"Manuell {'gestartet' if command == 'resume' else 'gestoppt'} (Alle-Miner-Aktion)",
            )
        results.append(ok)
    return jsonify({"success": any(results) or not miners_rows, "results": results})


@app.route("/api/miners/history")
def miners_history():
    return jsonify(db.get_miner_history(hours=24))


# ── Energie-Detailanalysen ───────────────────────────────────────────────
@app.route("/api/energy/week-comparison")
def energy_week_comparison():
    return jsonify(energy_analytics.get_week_comparison())


@app.route("/api/energy/month-overview")
def energy_month_overview():
    days = request.args.get("days", 30, type=int)
    return jsonify(energy_analytics.get_month_overview(days=days))


@app.route("/api/energy/daily-profile")
def energy_daily_profile():
    days = request.args.get("days", 14, type=int)
    return jsonify(energy_analytics.get_daily_profile(days=days))


@app.route("/api/energy/pv-yield-history")
def energy_pv_yield_history():
    days = request.args.get("days", 30, type=int)
    return jsonify(energy_analytics.get_pv_yield_history(days=days))


@app.route("/api/energy/kpis")
def energy_kpis():
    return jsonify(energy_analytics.get_energy_kpis())


@app.route("/api/miners/settings", methods=["GET", "POST"])
def miners_settings():
    if request.method == "GET":
        s = db.get_all_settings()
        return jsonify({
            "automation_enabled": s.get("miner_automation_enabled", "1") == "1",
            "surplus_threshold": float(s.get("miner_surplus_threshold", 2000)),
            "draw_threshold": float(s.get("miner_draw_threshold", 500)),
            "battery_threshold": float(s.get("miner_battery_threshold", 20)),
            "price_threshold": float(s.get("miner_price_threshold", 0.30)),
            "start_time": s.get("miner_start_time", "08:00"),
            "end_time": s.get("miner_end_time", "18:00"),
        })

    data = request.get_json(force=True) or {}
    ma = data.get("miner_automation", {})
    db.set_settings({
        "miner_automation_enabled": "1" if ma.get("automation_enabled") else "0",
        "miner_surplus_threshold": ma.get("surplus_threshold", 2000),
        "miner_draw_threshold": ma.get("draw_threshold", 500),
        "miner_battery_threshold": ma.get("battery_threshold", 20),
        "miner_price_threshold": ma.get("price_threshold", 0.30),
        "miner_start_time": ma.get("start_time", "08:00"),
        "miner_end_time": ma.get("end_time", "18:00"),
    })
    return jsonify({"success": True})


# ── Heimüberwachung (vollständig nativ auf demselben Server/Port - kein
#    zweiter Prozess, kein zweiter Port, kein externer Link. Siehe
#    backend/heimueberwachung_engine.py für die Erkennungs-Engine selbst) ──
@app.route("/api/surveillance/still/<name>")
def surveillance_still(name):
    """Einzelbild einer Heimüberwachungs-Kamera (per Name), direkt aus dem
    eingebetteten Erfassungs-Thread - kein zweiter Server, kein Netzwerk-Call."""
    data = surveillance.get_still_bytes(name)
    if data is None:
        return Response(status=503)
    return Response(data, mimetype="image/jpeg", headers={"Cache-Control": "no-store"})


@app.route("/api/surveillance/stream/<name>")
def surveillance_stream(name):
    """Echter Live-Stream (MJPEG) einer Heimüberwachungs-Kamera - dieselbe
    Technik wie in der eigenständigen Heimüberwachung (dort /stream/<name>),
    jetzt aber nativ im Dashboard eingebettet: kein zweiter Server/Port,
    kein Umweg über einzelne, alle paar Sekunden nachgeladene Standbilder."""
    if not surveillance.is_embedded_available():
        return Response(status=503)
    return Response(
        surveillance.stream_mjpeg(name),
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-store"},
    )


@app.route("/api/heim/cameras", methods=["GET", "POST"])
def heim_cameras():
    if request.method == "GET":
        return jsonify(surveillance.list_cameras_full())
    data = request.get_json(force=True) or {}
    name, rtsp_url = (data.get("name") or "").strip(), (data.get("rtsp_url") or "").strip()
    if not name or not rtsp_url:
        return jsonify({"success": False, "error": "Name und RTSP-URL sind Pflichtfelder"}), 400
    ok, msg = surveillance.add_camera(name, rtsp_url)
    return jsonify({"success": ok, "error": None if ok else msg})


@app.route("/api/heim/cameras/<name>", methods=["PUT", "DELETE"])
def heim_camera_item(name):
    if request.method == "DELETE":
        ok, msg = surveillance.remove_camera(name)
        return jsonify({"success": ok, "error": None if ok else msg})
    data = request.get_json(force=True) or {}
    rtsp_url = (data.get("rtsp_url") or "").strip()
    if not rtsp_url:
        return jsonify({"success": False, "error": "RTSP-URL darf nicht leer sein"}), 400
    ok, msg = surveillance.update_camera(name, rtsp_url)
    return jsonify({"success": ok, "error": None if ok else msg})


@app.route("/api/heim/cameras/<name>/zones", methods=["GET", "POST"])
def heim_camera_zones(name):
    """Zonen einer einzelnen Kamera abrufen/speichern - Koordinaten sind
    normiert (0.0-1.0), damit sie unabhängig von der tatsächlichen
    Kameraauflösung im Zonen-Editor (Browser-Canvas) funktionieren."""
    if request.method == "GET":
        zones = surveillance.get_camera_zones(name)
        if zones is None:
            return jsonify({"success": False, "error": "Kamera nicht gefunden."}), 404
        return jsonify({"success": True, "zones": zones})
    data = request.get_json(force=True) or {}
    ok, msg = surveillance.set_camera_zones(name, data.get("zones", []))
    return jsonify({"success": ok, "error": None if ok else msg})


@app.route("/api/heim/events")
def heim_events():
    """Ohne 'date': letzte N Ereignisse (Standard-Listenansicht, neueste zuerst).
    Mit 'date' (YYYY-MM-DD): ALLE Ereignisse dieses Kalendertags, unabhängig von
    'limit' - wird von der Zeitleiste und der Bewegungs-Heatmap gebraucht, die
    ein vollständiges Bild eines Tages brauchen statt nur der letzten paar
    Einträge. Der Ereignis-Feed hält ohnehin nur eine begrenzte Zahl an
    Ereignissen vor (siehe storage.max_events_in_feed), ein hohes Limit hier
    ist also unkritisch.
    """
    date_str = request.args.get("date")
    if date_str:
        try:
            target_prefix = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
        except ValueError:
            return jsonify([])
        all_events = surveillance.list_events(limit=100000)
        return jsonify([e for e in all_events if str(e.get("time", "")).startswith(target_prefix)])
    limit = request.args.get("limit", 50, type=int)
    return jsonify(surveillance.list_events(limit))


@app.route("/api/heim/detection-classes", methods=["GET", "POST"])
def heim_detection_classes():
    if request.method == "GET":
        return jsonify({
            "available": surveillance.get_available_classes(),
            "selected": surveillance.get_selected_classes(),
        })
    data = request.get_json(force=True) or {}
    surveillance.set_selected_classes(data.get("classes", []))
    return jsonify({"success": True})


@app.route("/api/heim/gallery", methods=["GET", "DELETE"])
def heim_gallery():
    """Fotos, die bei erkannten Objekten automatisch gespeichert wurden.
    Es werden ausschließlich Bilder aufgenommen (die Video-Aufnahme ist
    bewusst deaktiviert - siehe CONFIG['recording']['enabled'] in der Engine)."""
    if request.method == "GET":
        limit = request.args.get("limit", 200, type=int)
        camera = request.args.get("camera") or None
        date_from = request.args.get("from") or None
        date_to = request.args.get("to") or None
        return jsonify(surveillance.list_gallery(limit=limit, camera=camera, date_from=date_from, date_to=date_to))
    data = request.get_json(force=True) or {}
    if data.get("action") == "clear":
        deleted = surveillance.clear_gallery()
        return jsonify({"success": True, "deleted": deleted})
    filenames = data.get("filenames")
    if filenames:
        ok_count, fail_count = surveillance.delete_snapshots(filenames)
        return jsonify({"success": fail_count == 0, "deleted": ok_count, "failed": fail_count})
    filename = (data.get("filename") or "").strip()
    if not filename:
        return jsonify({"success": False, "error": "Dateiname fehlt"}), 400
    ok, msg = surveillance.delete_snapshot(filename)
    return jsonify({"success": ok, "error": msg})


@app.route("/api/heim/gallery/<path:filename>")
def heim_gallery_image(filename):
    data = surveillance.get_snapshot_bytes(filename)
    if data is None:
        return Response(status=404)
    return Response(data, mimetype="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})


@app.route("/api/heim/gallery/settings", methods=["GET", "POST"])
def heim_gallery_settings():
    """Automatisches Aufräumen der Galerie: max. Alter in Tagen und/oder
    max. Anzahl Fotos (0 = jeweils kein Limit)."""
    if request.method == "GET":
        return jsonify(surveillance.get_gallery_settings())
    data = request.get_json(force=True) or {}
    result = surveillance.set_gallery_settings(
        max_photo_age_days=data.get("max_photo_age_days"),
        max_photos=data.get("max_photos"),
    )
    if result is None:
        return jsonify({"success": False, "error": "Personenerkennung läuft nicht."}), 503
    return jsonify({"success": True, **result})


@app.route("/api/heim/notifications/status")
def heim_notifications_status():
    return jsonify(surveillance.get_notification_status())


@app.route("/api/heim/notifications/settings", methods=["GET", "POST"])
def heim_notifications_settings():
    """Konfiguration des Benachrichtigungswegs (E-Mail/ntfy/Telegram/Konsole).
    Passwörter/Tokens werden beim GET nie zurückgegeben, nur ob eines gesetzt
    ist - ein leeres Feld beim Speichern lässt ein vorhandenes unangetastet."""
    if request.method == "GET":
        return jsonify(surveillance.get_notification_settings())
    data = request.get_json(force=True) or {}
    result = surveillance.set_notification_settings(data)
    if result is None:
        return jsonify({"success": False, "error": "Personenerkennung ist nicht verfügbar."}), 503
    return jsonify({"success": True, **result})


@app.route("/api/heim/notifications/snooze", methods=["POST"])
def heim_notifications_snooze():
    data = request.get_json(force=True) or {}
    return jsonify(surveillance.snooze_notifications(data.get("minutes", 0)))


@app.route("/api/heim/notifications/test", methods=["POST"])
def heim_notifications_test():
    ok, msg = surveillance.send_test_notification()
    return jsonify({"success": ok, "message": msg})


# ── Browser-Push (Web Push) ────────────────────────────────────────────────
@app.route("/sw.js")
def _serve_service_worker():
    # Muss unter der Root-Domain liegen (nicht /static/...), damit der
    # Service-Worker-Scope die ganze Seite abdeckt und Push-Events überall
    # empfangen kann.
    return Response(SERVICE_WORKER_JS, mimetype="application/javascript")


@app.route("/api/push/public-key")
def push_public_key():
    return jsonify({"available": _WEBPUSH_AVAILABLE and bool(_vapid_public_key_b64),
                     "public_key": _vapid_public_key_b64 or ""})


@app.route("/api/push/subscribe", methods=["POST"])
def push_subscribe():
    if not _WEBPUSH_AVAILABLE:
        return jsonify({"success": False, "error": "pywebpush ist nicht installiert (pip install pywebpush)."}), 503
    data = request.get_json(force=True) or {}
    endpoint = data.get("endpoint")
    keys = data.get("keys") or {}
    if not endpoint or not keys.get("p256dh") or not keys.get("auth"):
        return jsonify({"success": False, "error": "Ungültiges Abo."}), 400
    db.add_push_subscription(endpoint, keys["p256dh"], keys["auth"], request.headers.get("User-Agent"))
    return jsonify({"success": True, "count": db.count_push_subscriptions()})


@app.route("/api/push/unsubscribe", methods=["POST"])
def push_unsubscribe():
    data = request.get_json(force=True) or {}
    endpoint = data.get("endpoint")
    if endpoint:
        db.remove_push_subscription(endpoint)
    return jsonify({"success": True, "count": db.count_push_subscriptions()})


@app.route("/api/push/status")
def push_status():
    return jsonify({"available": _WEBPUSH_AVAILABLE, "subscriptions": db.count_push_subscriptions()})


@app.route("/api/push/test", methods=["POST"])
def push_test():
    if not _WEBPUSH_AVAILABLE:
        return jsonify({"success": False, "error": "pywebpush ist nicht installiert."}), 503
    sent = send_browser_push("Test-Benachrichtigung", "Browser-Push funktioniert - SmartHome Dashboard", "/")
    if sent == 0:
        return jsonify({"success": False, "error": "Kein aktives Abo gefunden oder Versand fehlgeschlagen."})
    return jsonify({"success": True, "sent": sent})


# ── Familie: Einkaufsliste ────────────────────────────────────────────────
@app.route("/api/family/shopping-list", methods=["GET", "POST", "PUT", "DELETE"])
def shopping_list():
    if request.method == "GET":
        return jsonify(db.get_shopping_list())
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        item = (data.get("item") or "").strip()
        if not item:
            return jsonify({"success": False, "error": "Artikel darf nicht leer sein"}), 400
        item_id = db.add_shopping_item(item, data.get("added_by", ""))
        return jsonify({"success": True, "id": item_id})
    if request.method == "PUT":
        data = request.get_json(force=True) or {}
        if data.get("action") == "complete":
            db.complete_shopping_item(data["id"])
        return jsonify({"success": True})
    if request.method == "DELETE":
        data = request.get_json(force=True) or {}
        if data.get("action") == "clear_completed":
            db.clear_completed_shopping()
        elif "id" in data:
            db.delete_shopping_item(data["id"])
        return jsonify({"success": True})


# ── Familie: Kalender ──────────────────────────────────────────────────────
@app.route("/api/family/calendar", methods=["GET", "POST", "DELETE"])
def family_calendar():
    if request.method == "GET":
        return jsonify(db.get_calendar_events())
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        if not data.get("title") or not data.get("start_time"):
            return jsonify({"success": False, "error": "Titel und Startzeit sind Pflicht"}), 400
        event_id = db.add_calendar_event(data["title"], data.get("description", ""), data["start_time"])
        return jsonify({"success": True, "id": event_id})
    if request.method == "DELETE":
        data = request.get_json(force=True) or {}
        db.delete_calendar_event(data["id"])
        return jsonify({"success": True})


# ── Familie: Notizen ───────────────────────────────────────────────────────
@app.route("/api/family/notes", methods=["GET", "POST"])
def family_notes():
    if request.method == "GET":
        return jsonify(db.get_notes())
    data = request.get_json(force=True) or {}
    db.set_note(data.get("category", "general"), data.get("content", ""))
    return jsonify({"success": True})


# ── Benachrichtigungen ─────────────────────────────────────────────────────
@app.route("/api/notifications", methods=["GET", "PUT"])
def notifications():
    if request.method == "GET":
        return jsonify(db.get_notifications())
    data = request.get_json(force=True) or {}
    if data.get("action") == "mark_all_read":
        db.mark_all_notifications_read()
    return jsonify({"success": True})


# ── Allgemeine Einstellungen ───────────────────────────────────────────────
@app.route("/api/settings", methods=["GET", "POST"])
def settings_endpoint():
    if request.method == "GET":
        return jsonify(db.get_all_settings())
    data = request.get_json(force=True) or {}
    db.set_settings(data)
    return jsonify({"success": True})


# ── Health-Check ────────────────────────────────────────────────────────────
# Für Docker HEALTHCHECK, Uptime-Kuma, Watchtower o.ä. - bewusst ohne
# API-Key-Pflicht und ohne DB-Zugriff, damit der Check auch dann noch
# antwortet, wenn z.B. die Datenbank kurzzeitig gesperrt ist.
@app.route("/api/health")
def health_check():
    return jsonify({"status": "ok", "service": "smarthome-dashboard",
                     "demo_mode": os.environ.get("DEMO_MODE") == "1"})


# ── CSV-Export ──────────────────────────────────────────────────────────────
# Exportiert die wichtigsten Verlaufsdaten als CSV, z.B. für eine eigene
# Auswertung in Excel/LibreOffice/Pandas oder als Backup.
def _csv_response(rows, fieldnames, filename):
    import csv
    import io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    resp = Response(buf.getvalue(), mimetype="text/csv")
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp


@app.route("/api/export/energy.csv")
def export_energy_csv():
    hours = request.args.get("hours", default=24, type=int)
    rows = db.get_energy_history(hours=hours)
    return _csv_response(
        rows, ["timestamp", "pv_power", "house_load", "grid_import", "battery_soc"],
        "energy_history.csv",
    )


@app.route("/api/export/daily-summary.csv")
def export_daily_summary_csv():
    days = request.args.get("days", default=90, type=int)
    rows = db.get_daily_summaries(days=days)
    return _csv_response(
        rows,
        ["day", "pv_kwh", "self_consumed_kwh", "exported_kwh", "imported_kwh",
         "house_kwh", "avg_battery_soc"],
        "daily_energy_summary.csv",
    )


@app.route("/api/export/miner-stats/<int:miner_id>.csv")
def export_miner_stats_csv(miner_id):
    hours = request.args.get("hours", default=24, type=int)
    rows = db.get_miner_stats_history(miner_id, hours=hours)
    return _csv_response(
        rows, ["timestamp", "hashrate", "temperature", "power_watts", "status"],
        f"miner_{miner_id}_stats.csv",
    )


# ── SocketIO Connect/Disconnect Logging ───────────────────────────────────
@socketio.on("connect")
def handle_connect():
    log.info("Client verbunden: %s", request.sid)


@socketio.on("disconnect")
def handle_disconnect():
    log.info("Client getrennt: %s", request.sid)


def create_app():
    db.init_db()
    if os.environ.get("DEMO_MODE") == "1":
        log.info("DEMO_MODE aktiv - Dashboard läuft mit erfundenen Beispieldaten, es wird "
                 "nicht versucht echte Hardware (Fronius/Miner) anzusprechen, sofern keine "
                 "Geräte eingetragen sind.")
    _start_surveillance_if_enabled()
    scheduler.start_background_threads(socketio)
    return app


def _start_surveillance_if_enabled():
    """Bettet die Personenerkennung (Heimüberwachung) im selben Prozess/Port
    ein, falls in den Einstellungen aktiviert. Läuft NICHT als zweiter
    Server - siehe backend/heimueberwachung_engine.py::start_embedded().
    Schlägt der Start fehl (z.B. fehlende Pakete ultralytics/opencv, oder
    Modell nicht vorhanden), läuft das restliche Dashboard trotzdem normal
    weiter - die Kameras-Seite zeigt dann einen entsprechenden Hinweis."""
    if str(db.get_setting("surveillance_enabled", "0")) != "1":
        log.info("Personenerkennung (Heimüberwachung) ist in den Einstellungen deaktiviert.")
        return
    try:
        try:
            heim = _heimueberwachung_engine_module  # Version "alles in einer Datei"
        except NameError:
            from backend import heimueberwachung_engine as heim  # normale Mehr-Datei-Version
        heim.start_embedded()
        log.info("Personenerkennung eingebettet - läuft nativ im Reiter 'Heimüberwachung', kein zweiter Port.")
    except ModuleNotFoundError as e:
        log.warning(
            "Personenerkennung konnte nicht gestartet werden - Paket fehlt (%s). "
            "Installiere mit: pip install ultralytics opencv-python-headless", e,
        )
    except Exception as e:
        log.exception("Personenerkennung konnte nicht gestartet werden: %s", e)


if __name__ == "__main__":
    create_app()
    log.info("SmartHome Dashboard startet auf http://0.0.0.0:5000")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)

