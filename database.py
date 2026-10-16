# -*- coding: utf-8 -*-
"""База даних: підписки (сигнали + радник), налаштування, користувачі, історія угод.

Дві окремі платні послуги:
  'signals' — розсилка сигналів (stars_price / sub_days)
  'advisor' — команда /pump (калькулятор плеча/ліквідації) — доп. плата на місяць
"""
import os
import sqlite3
import time
from contextlib import contextmanager

import config


def _ensure_db_dir():
    """На Railway файловая система ephemeral — кладём БД в постоянный volume,
    если он примонтирован (DB_PATH указывает внутрь /data или иного volume)."""
    d = os.path.dirname(config.DB_PATH)
    if d:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass


@contextmanager
def db():
    _ensure_db_dir()
    conn = sqlite3.connect(config.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _migrate(c):
    """М'які міграції старої БД: subs отримує колонку plan + складений PK (uid, plan),
    щоб юзер міг тримати дві підписки одночасно (signals і radar)."""
    cols = [r["name"] for r in c.execute("PRAGMA table_info(subs)")]
    if "plan" not in cols:
        # стара таблиця: uid PRIMARY KEY -> перебудовуємо з новим PK
        c.executescript("""
            CREATE TABLE subs_new(
                uid INTEGER, expires_ts REAL,
                plan TEXT NOT NULL DEFAULT 'signals',
                PRIMARY KEY (uid, plan)
            );
            INSERT INTO subs_new(uid, expires_ts, plan)
                SELECT uid, expires_ts, 'signals' FROM subs;
            DROP TABLE subs;
            ALTER TABLE subs_new RENAME TO subs;
        """)
    else:
        # план уже є — перевіряємо, чи старий PK (тільки uid) тобто unique on uid
        idx = c.execute("PRAGMA index_list(subs)").fetchall()
        pk_cols = None
        for i in idx:
            if i["unique"]:
                info = c.execute(f"PRAGMA index_info('{i['name']}')").fetchall()
                names = {r["name"] for r in info}
                if names == {"uid"}:
                    pk_cols = "uid_only"
        if pk_cols == "uid_only":
            c.executescript("""
                CREATE TABLE subs_new(
                    uid INTEGER, expires_ts REAL,
                    plan TEXT NOT NULL DEFAULT 'signals',
                    PRIMARY KEY (uid, plan)
                );
                INSERT INTO subs_new(uid, expires_ts, plan) SELECT uid, expires_ts, plan FROM subs;
                DROP TABLE subs;
                ALTER TABLE subs_new RENAME TO subs;
            """)


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            uid INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            joined_ts REAL
        );
        CREATE TABLE IF NOT EXISTS subs(
            uid INTEGER,
            expires_ts REAL,
            plan TEXT NOT NULL DEFAULT 'signals',
            PRIMARY KEY (uid, plan)          -- один юзер може тримати дві підписки: signals + radar
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            symbol TEXT,
            side TEXT,
            duration TEXT,
            entry REAL, stop_loss REAL, tp1 REAL, tp2 REAL,
            confidence INTEGER,
            status TEXT DEFAULT 'open',
            closed_ts REAL,
            pnl_pct REAL DEFAULT 0
        );
        """)
        _migrate(c)
        defaults = {
            "price_stars": config.DEFAULT_PRICE_STARS,
            "sub_days": config.SUB_DAYS,
            "radar_price_stars": config.RADAR_PRICE_STARS,
            "radar_days": config.RADAR_DAYS,
            "max_signals_per_scan": config.MAX_SIGNALS_PER_SCAN,
            "min_confidence": config.MIN_CONFIDENCE,
            "scan_interval_min": config.SCAN_INTERVAL_MIN,
        }
        for k, v in defaults.items():
            if not c.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone():
                c.execute("INSERT INTO settings(key,value) VALUES(?,?)", (k, str(v)))


# ---------- універсальні налаштування ----------

def get_setting(key: str, default: int = 0) -> int:
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        try:
            return int(row["value"]) if row else default
        except Exception:
            return default


def set_setting(key: str, val: int):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(int(val))))


def get_price() -> int:
    return get_setting("price_stars", config.DEFAULT_PRICE_STARS)


def set_price(stars: int):
    set_setting("price_stars", stars)


def get_sub_days() -> int:
    return get_setting("sub_days", config.SUB_DAYS)


def set_sub_days(days: int):
    set_setting("sub_days", days)


def get_radar_price() -> int:
    return get_setting("radar_price_stars", config.RADAR_PRICE_STARS)


def set_radar_price(stars: int):
    set_setting("radar_price_stars", stars)


def get_radar_days() -> int:
    return get_setting("radar_days", config.RADAR_DAYS)


def set_radar_days(days: int):
    set_setting("radar_days", days)


def get_max_per_scan() -> int:
    return get_setting("max_signals_per_scan", config.MAX_SIGNALS_PER_SCAN)


def set_max_per_scan(v: int):
    set_setting("max_signals_per_scan", v)


def get_min_conf() -> int:
    return get_setting("min_confidence", config.MIN_CONFIDENCE)


def set_min_conf(v: int):
    set_setting("min_confidence", v)


def get_scan_interval() -> int:
    return get_setting("scan_interval_min", config.SCAN_INTERVAL_MIN)


def set_scan_interval(v: int):
    set_setting("scan_interval_min", v)


# ---------- користувачі ----------

def register_user(uid: int, username: str | None, first_name: str | None):
    with db() as c:
        c.execute("INSERT OR IGNORE INTO users(uid,username,first_name,joined_ts) VALUES(?,?,?,?)",
                  (uid, username, first_name, time.time()))


def all_users() -> list[int]:
    with db() as c:
        return [r["uid"] for r in c.execute("SELECT uid FROM users")]


# ---------- підписки (plan: 'signals' | 'radar') ----------

def add_sub(uid: int, days: int, plan: str = "signals"):
    now = time.time()
    with db() as c:
        row = c.execute("SELECT expires_ts FROM subs WHERE uid=? AND plan=?", (uid, plan)).fetchone()
        base = max(now, row["expires_ts"]) if row else now
        c.execute("INSERT OR REPLACE INTO subs(uid,expires_ts,plan) VALUES(?,?,?)",
                  (uid, base + days * 86400, plan))


def is_active(uid: int, plan: str = "signals") -> bool:
    with db() as c:
        row = c.execute("SELECT expires_ts FROM subs WHERE uid=? AND plan=?", (uid, plan)).fetchone()
        return bool(row and row["expires_ts"] > time.time())


def active_users(plan: str = "signals") -> list[int]:
    now = time.time()
    with db() as c:
        return [r["uid"] for r in
                c.execute("SELECT uid FROM subs WHERE plan=? AND expires_ts>?", (plan, now))]


def sub_info(uid: int, plan: str = "signals"):
    with db() as c:
        row = c.execute("SELECT expires_ts FROM subs WHERE uid=? AND plan=?", (uid, plan)).fetchone()
    if not row:
        return None
    return max(0.0, row["expires_ts"] - time.time())


def revoke(uid: int, plan: str = "signals"):
    with db() as c:
        c.execute("DELETE FROM subs WHERE uid=? AND plan=?", (uid, plan))


# ---------- історія сигналів / результатів ----------

def save_signal(sig) -> int:
    """Зберігаємо сигнал (об'єкт analyzer.Signal) -> id."""
    with db() as c:
        cur = c.execute(
            "INSERT INTO signals(ts,symbol,side,duration,entry,stop_loss,tp1,tp2,confidence,status)"
            " VALUES(?,?,?,?,?,?,?,?,?,'open')",
            (sig.ts, sig.symbol_full, sig.side, sig.duration, sig.entry,
             sig.stop_loss, sig.tp1, sig.tp2, sig.confidence))
        return cur.lastrowid


def open_signals(limit_per_symbol: int = 3, max_age_h: int = 72) -> list[dict]:
    """Відкриті сигнали (для трекера). Обмежуємо дублі по одній монеті та віком."""
    cutoff = time.time() - max_age_h * 3600
    with db() as c:
        rows = c.execute(
            "SELECT * FROM signals WHERE status='open' AND ts>? ORDER BY ts DESC LIMIT 500",
            (cutoff,)).fetchall()
    out, seen = [], {}
    for r in rows:
        key = (r["symbol"], r["side"])
        seen[key] = seen.get(key, 0) + 1
        if seen[key] <= limit_per_symbol:
            out.append(dict(r))
    return out


def close_signal(sid: int, status: str, pnl_pct: float):
    with db() as c:
        c.execute("UPDATE signals SET status=?, closed_ts=?, pnl_pct=? WHERE id=?",
                  (status, time.time(), pnl_pct, sid))


def results_since(ts: float) -> dict:
    """Статистика закритих угод після моменту ts."""
    with db() as c:
        rows = c.execute(
            "SELECT status, COUNT(*) n, COALESCE(SUM(pnl_pct),0) s FROM signals "
            "WHERE closed_ts>=? GROUP BY status", (ts,)).fetchall()
        total_open = c.execute("SELECT COUNT(*) n FROM signals WHERE status='open'").fetchone()["n"]
    res = {"tp1": 0, "tp2": 0, "sl": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "total": 0}
    for r in rows:
        res[r["status"]] = r["n"]
        res["total"] += r["n"]
        res["sum_pnl"] += r["s"]
        if r["status"] in ("tp1", "tp2"):
            res["wins"] += r["n"]
        elif r["status"] == "sl":
            res["losses"] += r["n"]
    res["open"] = total_open
    res["winrate"] = round(res["wins"] / res["total"] * 100, 1) if res["total"] else 0.0
    return res


def count_all_signals() -> int:
    with db() as c:
        return c.execute("SELECT COUNT(*) n FROM signals").fetchone()["n"]
