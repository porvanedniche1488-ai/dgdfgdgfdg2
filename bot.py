# -*- coding: utf-8 -*-
"""
Telegram-бот: сигнали Binance Futures + платна підписка в Telegram Stars.

Команди користувача:
  /start  - привітання
  /help   - довідка
  /buy    - купити пропуск (оплата зірками Telegram)
  /status - статус підписки
  /signal - поточний топ-сигнал (для активних)

Платна команда /pump (RADAR): бот питає монету -> баланс -> плече, і дає план угоди:
  розмір позиції, ціну ліквідації, скільки $ заробиш на Тейк 1 / Тейк 2, ризик-оцінку.
Платна команда /result: скільки угод дійшло до тейків/стопів за день/тиждень/місяць
  + ЗАГАЛЬНИЙ результат у %.

Трекер результатів: бот стежить за кожною відправленою монетою і пише в чат БЕЗКОШТОВНО:
  ✅ «дійшла до ТЕЙК 1 — можна забирати», 🎯 «закрила ТЕЙК 2», 🛑 «дійшла до СТОП-ЛОСА».
Щодня о 22:00 (Київ): адміну — повний звіт (кількість спрацювань + загальний заробіток),
платникам — скорочений (скільки угод спрацювало).

Керування потоком угод (адмін): /setinterval <хв> — як часто прилітають сигнали,
/setmax <N> — скільки монет за розсилку (разом = ~N*1440/interval сигналів на добу).

Команди адміна (ADMIN_ID):
  /setprice <зірки>     - ціна пропуску
  /setdays <днів>       - тривалість пропуску
  /grant <uid> [днів]   - видач/подовження доступу вручну
  /revoke <uid>         - зняти доступ
  /broadcast <текст>    - розсилка всім активним
  /stats                - статистика
  /scan                 - примусовий скан ринку + розсилка
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters,
    CallbackQueryHandler, PreCheckoutQueryHandler, ConversationHandler,
)

import config
import database as db

db.init_db()  # гарантуємо наявність таблиць до побудови HELP_TEXT

from analyzer import Analyzer, format_signal

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("bot")

# глобальні посилання — щоб scheduler міг надсилати повідомлення
_app: Application | None = None
_sched: AsyncIOScheduler | None = None
_scan_lock = asyncio.Lock()   # захист від паралельних сканів (/scan під час cron)
_track_lock = asyncio.Lock()  # трекер результатів не збігається сам із собою


async def new_analyzer() -> Analyzer:
    """Кожен скан отримує свіжий http-клієнт і закриває його після роботи.
    Це усуває помилки 'client closed' / завислі з'єднання після тривалої роботи."""
    return Analyzer(httpx.AsyncClient(timeout=20))

HELP_TEXT = (
    "🤖 <b>Binance Signals Bot</b>\n\n"
    "<b>📡 СИГНАЛИ</b> (пропуск ⭐️{p1} / {d1} дн.):\n"
    "Аналізує Binance Futures і надсилає готові угоди: 🟢 лонг / 🔴 шорт "
    "(довгі й коротькі), вхід, стоп-лос, 2 тейки, шанс %.\n"
    "Раз на {iv} хв — до {mx} різних монет прямо в чат. Тільки за підпискою.\n\n"
    "<b>🛰 RADAR</b> (окрема доплата ⭐️{p2} / {d2} дн.):\n"
    "• /pump — калькулятор угоди: питаш монету → свій баланс → плече, а бот дає\n"
    "  безпечне плече, розмір позиції, ЦІНУ ЛІКВІДАЦІЇ і скільки реальних $\n"
    "  ти заробиш на Тейк 1 і Тейк 2 (і скільки втратиш на стопі)\n"
    "• /result — скільки заробили наші сигнали за день/тиждень/місяць\n"
    "  + ЗАГАЛЬНИЙ результат у кінці\n\n"
    "<b>Безкоштовно всім:</b>\n"
    "• Бот сам стежить за кожною монетою і пише: ✅ «дійшла до ТЕЙК 1 — забирай»,\n"
    "  🎯 «закрила ТЕЙК 2» або 🛑 «дійшла до СТОП-ЛОСА»\n"
    "• Щоденний підсумок о {rh}:00 (Київ) — скільки угод спрацювало\n\n"
    "<b>Команди:</b> /buy — сигнали · /radar — радар · /signal — угоди зараз · "
    "/status — мій доступ · /help — довідка\n\n"
    "💡 Вигідно брати обидва: сигнал каже КУДИ йти, радар — ЯКИМ плечем.\n"
    "⚠️ Не фінансова порада."
)  # ціни/дні підставляються динамічно при кожній відправці


async def is_admin(uid: int) -> bool:
    return uid == config.ADMIN_ID


async def has_access(uid: int) -> bool:
    """Сигнали: адмін — завжди безкоштовно; всі решта — тільки з платною підпискою."""
    return uid == config.ADMIN_ID or db.is_active(uid, "signals")


async def has_radar(uid: int) -> bool:
    """RADAR (/pump, /result): окрема доплата зірками; адміну безкоштовно."""
    return uid == config.ADMIN_ID or db.is_active(uid, "radar")


def _fmt_num(v: float) -> str:
    return f"{v:.6g}"


# ---------------- user commands ----------------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    db.register_user(u.id, u.username, u.first_name)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐️ СИГНАЛИ — купити пропуск", callback_data="buy")],
        [InlineKeyboardButton("🛰 RADAR — доплатити (/pump)", callback_data="buy_radar")],
    ])
    if u.id == config.ADMIN_ID:
        await update.message.reply_text(
            f"Привіт, {u.first_name}! 👑 Ти адмін — ВСЕ безкоштовно і назавжди.\n\n"
            f"📡 Сигнали: до {db.get_max_per_scan()} монет раз на {db.get_scan_interval()} хв (/signal — зараз)\n"
            f"🛰 Радар: /pump — питай будь-яку монету (баланс → плече → ліквідація → скільки заробиш)\n"
            f"🏆 Результати: /result — скільки заробили наші сигнали + загальний підсумок\n"
            f"📊 Звіт щодня о {config.DAILY_REPORT_HOUR_UTC3}:00 (Київ). Адмін-команди: /help",
            reply_markup=kb,
        )
    else:
        sig_on = db.is_active(u.id, "signals")
        rad_on = db.is_active(u.id, "radar")
        state = [
            "✅ СИГНАЛИ активні" if sig_on
            else f"🔒 СИГНАЛИ: ⭐️{db.get_price()} / {db.get_sub_days()} дн. — /buy",
            "✅ RADAR активний" if rad_on
            else f"🔒 RADAR (/pump, /result): ⭐️{db.get_radar_price()} / {db.get_radar_days()} дн. — кнопка нижче",
        ]
        await update.message.reply_text(
            f"Привіт, {u.first_name}! 👋\n\n"
            f"Я даю готові угоди по Binance Futures: лонг/шорт, стоп-лос, 2 тейки, шанс —\n"
            f"раз на {db.get_scan_interval()} хв, до {db.get_max_per_scan()} монет.\n"
            "І сам кажу всім безкоштовно, коли монета дійшла до тейка чи стопу. 🎯\n\n"
            + "\n".join(state) +
            "\n\n⛔️ Сигнали та /pump — ТІЛЬКИ за підпискою.\n"
            "⚠️ Не фінансова порада. Торгувай обачно.",
            reply_markup=kb,
        )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # текст формуємо заново — щоб ціни/дні завжди були актуальні після /setprice
    text = HELP_TEXT.format(p1=db.get_price(), d1=db.get_sub_days(),
                            p2=db.get_radar_price(), d2=db.get_radar_days(),
                            iv=db.get_scan_interval(), mx=db.get_max_per_scan(),
                            rh=config.DAILY_REPORT_HOUR_UTC3)
    if update.effective_user.id == config.ADMIN_ID:
        text += ("\n\n👑 <b>Адмін-команди:</b>\n"
                 "/setprice 50 — ціна СИГНАЛІВ · /setdays 30 — їх тривалість\n"
                 "/setradarprice 30 — ціна RADAR · /setradardays 30\n"
                 "/setinterval 30 — хвилин між розсилками (потік угод на добу)\n"
                 "/setmax 8 — монет за розсилку (≈ {pd} сигналів на добу зараз)\n"
                 "/setminconf 65 — мінімальний шанс %\n"
                 "/grant uid [днів] [signals|radar] · /revoke uid [signals|radar]\n"
                 "/scan — терміновий скан + розсилка\n"
                 "/broadcast текст · /stats").replace("{pd}", str(round(24*60/db.get_scan_interval()*db.get_max_per_scan())))
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id == config.ADMIN_ID:
        await update.message.reply_text("👑 Ти адмін — все безкоштовно назавжди.")
        return
    l1 = db.sub_info(u.id, "signals")
    l2 = db.sub_info(u.id, "radar")
    lines = [
        f"📡 СИГНАЛИ: ✅ ще {l1/86400:.1f} дн." if l1
        else f"📡 СИГНАЛИ: ❌ — /buy ⭐️{db.get_price()}",
        f"🛰 RADAR: ✅ ще {l2/86400:.1f} дн." if l2
        else f"🛰 RADAR: ❌ — /radar ⭐️{db.get_radar_price()}",
    ]
    await update.message.reply_text("\n".join(lines))


async def _send_invoice(ctx, uid: int, plan: str):
    """Кидаємо invoice в Telegram Stars під обраний план (signals|radar)."""
    if plan == "signals":
        price, days, title = db.get_price(), db.get_sub_days(), "Пропуск СИГНАЛИ"
        desc = "Розсилка угод Binance Futures раз на " \
               f"{db.get_scan_interval()} хв + /signal"
    else:
        price, days, title = db.get_radar_price(), db.get_radar_days(), "Пропуск RADAR"
        desc = "/pump калькулятор угоди (баланс/плече/ліквідація) + /result статистика"
    await ctx.bot.send_invoice(
        chat_id=uid,
        title=f"{title} на {days} днів",
        description=desc,
        payload=f"{plan}_{uid}_{int(time.time())}",
        provider_token="",           # порожньо для Telegram Stars
        currency="XTR",              # XTR = Telegram Stars
        prices=[LabeledPrice(label=title, amount=price)],
    )


async def cb_buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_invoice(ctx, q.from_user.id, "signals")


async def cb_buy_radar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_invoice(ctx, q.from_user.id, "radar")


async def cmd_buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _send_invoice(ctx, update.effective_user.id, "signals")


async def cmd_radar_buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _send_invoice(ctx, update.effective_user.id, "radar")


async def on_precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)


async def on_successful_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    uid = update.effective_user.id
    plan = "radar" if sp.invoice_payload.startswith("radar_") else "signals"
    days = db.get_radar_days() if plan == "radar" else db.get_sub_days()
    db.add_sub(uid, days, plan)
    db.register_user(uid, update.effective_user.username, update.effective_user.first_name)
    log.info("Payment success uid=%s plan=%s stars=%s", uid, plan, sp.total_amount)
    if plan == "radar":
        await update.message.reply_text(
            f"🎉 Оплачено! RADAR активовано на {days} днів.\n"
            "Пиши /pump BTC (або /pump SOL 100 20) — і радар порахує угоду: "
            "плече, ліквідацію і скільки ти заробиш. 🛰\n"
            "Статистика заробітку: /result day|week|month"
        )
    else:
        await update.message.reply_text(
            f"🎉 Оплачено! СИГНАЛИ активовано на {days} днів.\n"
            f"Перший пакет — найближчим сканом, далі раз на {db.get_scan_interval()} хв.\n"
            "Бот також сам напише, коли монета дійде до тейка чи стопу. 🎯"
        )


async def cmd_signal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ручний запит сигналів. Тільки адмін (безкоштовно) або платні підписники.
    Повертає до 5 різних монет — не одну."""
    u = update.effective_user
    if not await has_access(u.id):
        await update.message.reply_text(
            f"🔒 Сигнали доступні тільки за підпискою.\n/buy — ⭐️{db.get_price()} на {db.get_sub_days()} днів."
        )
        return
    async with _scan_lock:
        msg = await update.message.reply_text("⏳ Сканую ринок…")
        analyzer = await new_analyzer()
        try:
            sigs = await analyzer.scan_market(config.TOP_N_SYMBOLS, min_confidence=db.get_min_conf())
        except Exception as e:
            log.error("manual scan failed: %s", e)
            await msg.edit_text("⚠️ Binance зараз не відповідає. Спробуй через хвилину.")
            return
        finally:
            await analyzer.client.aclose()
        if not sigs:
            await msg.edit_text("Зараз сильних сетапів немає. Чекай наступного скану або /signal пізніше.")
            return
        n = db.get_max_per_scan()
        tag = " 👑(адмін, безкоштовно)" if u.id == config.ADMIN_ID else ""
        await msg.edit_text(f"📊 ТОП-{min(len(sigs), n)} угод просто зараз{tag}:")
        for s in sigs[:n]:
            db.save_signal(s)  # трекер буде стежити за цією монетою і напише результат
            await asyncio.sleep(0.1)
            await ctx.bot.send_message(u.id, format_signal(s))


async def cmd_signals(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Те саме, що /signal — псевдонім."""
    await cmd_signal(update, ctx)


# ---------------- /pump RADAR calculator (paid: radar plan) ----------------
# Бот питає: 1) монету 2) баланс 3) плече — і видає готовий план угоди.

PUMP_ASSET, PUMP_BALANCE, PUMP_LEV = range(3)
MAX_LEV = 125


def _parse_asset(text: str) -> str | None:
    t = (text or "").strip().upper().replace("/", "").replace("-", "")
    if not t or not t.isalnum():
        return None
    return t if t.endswith("USDT") else t + "USDT"


async def pump_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    return ConversationHandler.END


async def _pump_report(asset: str, balance: float, lev: float) -> str | None:
    """План угоди по актуальному сигналу бота для монети (або загальна механіка)."""
    analyzer = await new_analyzer()
    try:
        price = await analyzer.last_price(asset)
        sig = await analyzer.analyze_symbol(asset)
    finally:
        await analyzer.client.aclose()
    if not price:
        return None

    pos = balance * lev                    # розмір позиції в $
    qty = pos / price                      # кількість монет
    coin = asset.replace("USDT", "")
    liq_mult = (1 - 1 / lev) if lev > 1 else 0.999

    lines = [
        f"🛰 <b>RADAR • {coin}/USDT</b>",
        f"📌 Ринок зараз: <b>${_fmt_num(price)}</b>",
        "",
        f"💵 Твій баланс: ${balance:g} · плече x{lev:g}",
        f"📦 Розмір позиції: ${pos:,.2f}",
        f"🧮 Кількість: {_fmt_num(qty)} {coin}",
    ]

    if lev <= 3:
        risk_note = "🟢 Дуже безпечно — ліквідація дуже далеко"
    elif lev <= 10:
        risk_note = "🟢 Безпечне плече"
    elif lev <= 25:
        risk_note = "🟡 Помірний ризик"
    elif lev <= 50:
        risk_note = "🟠 Високий ризик"
    else:
        risk_note = "🔴 Екстремально — один різкий рух і все"

    if sig:
        side_ru = "ЛОНГ 🟢" if sig.side == "LONG" else "ШОРТ 🔴"
        liq = price * liq_mult if sig.side == "LONG" else price * (2 - liq_mult)
        sl_pct = abs(sig.entry - sig.stop_loss) / sig.entry * 100
        tp1_pct = abs(sig.tp1 - sig.entry) / sig.entry * 100
        tp2_pct = abs(sig.tp2 - sig.entry) / sig.entry * 100
        loss = pos * sl_pct / 100
        g1, g2 = pos * tp1_pct / 100, pos * tp2_pct / 100
        lines += [
            "",
            f"🤖 Сигнал бота зараз: <b>{side_ru}</b> (шанс ~{sig.confidence}%)",
            f"💰 Вхід: ${_fmt_num(sig.entry)}",
            f"🛑 Стоп-лос: ${_fmt_num(sig.stop_loss)}  (-{sl_pct:.2f}% руху)",
            f"🎯 Тейк 1: ${_fmt_num(sig.tp1)}  (+{tp1_pct:.2f}%)",
            f"🎯🎯 Тейк 2: ${_fmt_num(sig.tp2)}  (+{tp2_pct:.2f}%)",
            "",
            f"⚡️ Ціна ліквідації: <b>${_fmt_num(liq)}</b>",
            f"📉 Якщо дійде до стопу: втрати ≈ -${loss:,.2f} ({loss/balance*100:.0f}% балансу)",
            f"💵 На Тейк 1: +${g1:,.2f} · На Тейк 2: +${g2:,.2f}",
            f"📊 Ризик/прибуток: 1:{(g1/loss if loss else 0):.1f}",
        ]
    else:
        liq_up = price * (2 - liq_mult)
        liq_dn = price * liq_mult
        lines += [
            "",
            "🤖 Сильного сетапу по цій монеті зараз немає — це просто механіка.",
            f"⚡️ Ліквідація при LONG: ≈${_fmt_num(liq_dn)} · при SHORT: ≈${_fmt_num(liq_up)}",
            f"📉 Рух проти тебе на {100/lev:.1f}% = злиття всього балансу!",
        ]
    lines += ["", risk_note, "⚠️ Не фінансова порада."]
    return "\n".join(lines)


async def _pump_answer(target_msg, asset, balance, lev):
    msg = await target_msg.reply_text("🛰 Рахую…")
    text = await _pump_report(asset, balance, lev)
    if text is None:
        await msg.edit_text(f"❌ Тікера {asset} немає на Binance Futures. Перевір назву (BTC, ETH, SOL…).")
        return
    await msg.edit_text(text, parse_mode=ParseMode.HTML)


async def cmd_pump(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not await has_radar(u.id):
        await update.message.reply_text(
            f"🛰 <b>RADAR</b> — окремий платний інструмент:\n"
            f"питаш монету + свій баланс → бот дає БЕЗПЕЧНЕ плече, ціну ліквідації,\n"
            f"розмір позиції і скільки реальних $ заробиш на Тейк 1 і Тейк 2.\n\n"
            f"🎫 Доплата: ⭐️{db.get_radar_price()} на {db.get_radar_days()} днів.",
            parse_mode=ParseMode.HTML,
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⭐️ Купити RADAR", callback_data="buy_radar")]])
        await update.message.reply_text("Жми:", reply_markup=kb)
        return ConversationHandler.END
    args = ctx.args
    if args:
        asset = _parse_asset(args[0])
        balance = lev = None
        try:
            if len(args) >= 2:
                balance = float(args[1].replace("$", "").replace(",", "."))
                assert 1 <= balance <= 10_000_000
            if len(args) >= 3:
                lev = float(args[2].lower().replace("x", ""))
                assert 1 <= lev <= MAX_LEV
        except Exception:
            balance = lev = None
        if asset and balance and lev:
            await _pump_answer(update.message, asset, balance, lev)
            return ConversationHandler.END
        if asset and balance:
            ctx.user_data["pump_asset"], ctx.user_data["pump_balance"] = asset, balance
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(x, callback_data=f"plev_{x}") for x in ["2", "5", "10", "20"]],
                [InlineKeyboardButton(x, callback_data=f"plev_{x}") for x in ["50", "75", "100", "125"]],
            ])
            await update.message.reply_text(
                f"1️⃣ Монета: <b>{asset.replace('USDT','')}</b> ✅\n"
                f"2️⃣ Баланс: <b>${balance:g}</b> ✅\n\n"
                f"3️⃣ Яке плече? (1–{MAX_LEV}, кнопка або число):",
                parse_mode=ParseMode.HTML, reply_markup=kb)
            return PUMP_LEV
        if asset:
            ctx.user_data["pump_asset"] = asset
            await update.message.reply_text(
                f"1️⃣ Монета: <b>{asset.replace('USDT','')}</b> ✅\n\n"
                "2️⃣ 💵 Який твій баланс у USDT? (напиши число, напр. 100)",
                parse_mode=ParseMode.HTML)
            return PUMP_BALANCE
    await update.message.reply_text(
        "🛰 RADAR увімкнено!\n\n1️⃣ Напиши монету (напр. BTC, ETH, SOL, DOGE):\n"
        "(можна одразу: /pump BTC 100 20 — монета, баланс, плече)")
    return PUMP_ASSET


async def pump_asset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    asset = _parse_asset(update.message.text)
    if not asset:
        await update.message.reply_text("Не зрозумів. Напиши тікер, напр. BTC або SOL:")
        return PUMP_ASSET
    ctx.user_data["pump_asset"] = asset
    await update.message.reply_text(
        f"1️⃣ Монета: <b>{asset.replace('USDT','')}</b> ✅\n\n"
        "2️⃣ 💵 Який твій баланс у USDT? (напр. 100)",
        parse_mode=ParseMode.HTML)
    return PUMP_BALANCE


async def pump_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        bal = float((update.message.text or "").replace("$", "").replace(",", ".").strip())
        assert 1 <= bal <= 10_000_000
    except Exception:
        await update.message.reply_text("Напиши баланс числом, напр. 100 (або /pumpcancel):")
        return PUMP_BALANCE
    ctx.user_data["pump_balance"] = bal
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(x, callback_data=f"plev_{x}") for x in ["2", "5", "10", "20"]],
        [InlineKeyboardButton(x, callback_data=f"plev_{x}") for x in ["50", "75", "100", "125"]],
    ])
    await update.message.reply_text(
        f"2️⃣ Баланс: <b>${bal:g}</b> ✅\n\n"
        f"3️⃣ Яке плече? (1–{MAX_LEV}, кнопка або число):",
        parse_mode=ParseMode.HTML, reply_markup=kb)
    return PUMP_LEV


async def pump_lev_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    lev = float(q.data.split("_")[1])
    asset = ctx.user_data.get("pump_asset")
    bal = ctx.user_data.get("pump_balance")
    if not asset or not bal:
        await q.message.reply_text("Сесія застаріла — напиши /pump ще раз.")
        return ConversationHandler.END
    await _pump_answer(q.message, asset, bal, lev)
    ctx.user_data.clear()
    return ConversationHandler.END


async def pump_lev_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        lev = float((update.message.text or "").lower().replace("x", "").strip())
        assert 1 <= lev <= MAX_LEV
    except Exception:
        await update.message.reply_text(f"Плече — число 1–{MAX_LEV} (або /pumpcancel):")
        return PUMP_LEV
    asset = ctx.user_data.get("pump_asset")
    bal = ctx.user_data.get("pump_balance")
    if not asset or not bal:
        await update.message.reply_text("Сесія застаріла — напиши /pump ще раз.")
        return ConversationHandler.END
    await _pump_answer(update.message, asset, bal, lev)
    ctx.user_data.clear()
    return ConversationHandler.END


# ---------------- /result — скільки заробили (paid: radar plan) ----------------

PERIODS = {"day": 1, "week": 7, "month": 30}


async def cmd_result(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not await has_radar(u.id):
        await update.message.reply_text(
            f"🏆 /result — частина RADAR. Доплата ⭐️{db.get_radar_price()} "
            f"на {db.get_radar_days()} днів: /radar"
        )
        return
    period = (ctx.args[0].lower() if ctx.args else "week")
    days = PERIODS.get(period, 7)
    res = db.results_since(time.time() - days * 86400)
    if res["total"] == 0:
        await update.message.reply_text(
            f"За останні {days} дн. закритих угод ще немає — результати з'являться, коли "
            "монети дійдуть до тейків/стопів (бот напише про це в чат безкоштовно)."
        )
        return
    await update.message.reply_text(
        f"🏆 РЕЗУЛЬТАТИ СИГНАЛІВ • останні {days} дн.\n\n"
        f"✅ Тейк 1 забрано: {res['tp1']}\n"
        f"🎯 Тейк 2 взято: {res['tp2']}\n"
        f"🛑 Стоп-лос: {res['sl']}\n"
        f"⏳ Ще в роботі: {res['open']}\n"
        f"📈 Winrate: {res['winrate']}%\n\n"
        f"💰 ЗАГАЛЬНИЙ РЕЗУЛЬТАТ: {res['sum_pnl']:+.1f}% руху ціни по всіх закритих угодах"
    )


# ---------------- admin commands ----------------

async def cmd_setprice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0])
        assert 1 <= val <= 100000
    except Exception:
        await update.message.reply_text("Формат: /setprice 50  (ціна в зірках)")
        return
    db.set_price(val)
    await update.message.reply_text(f"💰 Нова ціна пропуску: ⭐️{val}")


async def cmd_setdays(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0])
        assert 1 <= val <= 3650
    except Exception:
        await update.message.reply_text("Формат: /setdays 30")
        return
    db.set_sub_days(val)
    await update.message.reply_text(f"📅 Тривалість пропуску: {val} днів")


async def cmd_setradarprice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0]); assert 1 <= val <= 100000
    except Exception:
        await update.message.reply_text("Формат: /setradarprice 30  (ціна RADAR в зірках)")
        return
    db.set_radar_price(val)
    await update.message.reply_text(f"💰 Нова ціна RADAR: ⭐️{val}")


async def cmd_setradardays(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0]); assert 1 <= val <= 3650
    except Exception:
        await update.message.reply_text("Формат: /setradardays 30")
        return
    db.set_radar_days(val)
    await update.message.reply_text(f"📅 RADAR на {val} днів")


async def cmd_setinterval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Як часто прилітають сигнали (хв) = скільки угод на добу."""
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0]); assert 5 <= val <= 720
    except Exception:
        await update.message.reply_text(
            f"Формат: /setinterval 30 (зараз {db.get_scan_interval()} хв)")
        return
    db.set_scan_interval(val)
    _reschedule_scans()
    per_day = round(24 * 60 / val * db.get_max_per_scan())
    await update.message.reply_text(
        f"⚙️ Скан раз на {val} хв ≈ {per_day} сигналів на добу "
        f"(при {db.get_max_per_scan()} монет за скан).")


async def cmd_grant(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        uid = int(ctx.args[0])
        days = int(ctx.args[1]) if len(ctx.args) > 1 else None
        plan = ctx.args[2] if len(ctx.args) > 2 else "signals"
        assert plan in ("signals", "radar")
    except Exception:
        await update.message.reply_text("Формат: /grant 123456789 [днів] [signals|radar]")
        return
    d = days or (db.get_radar_days() if plan == "radar" else db.get_sub_days())
    db.add_sub(uid, d, plan)
    await update.message.reply_text(f"✅ {plan}: видано {d} дн. користувачу {uid}")


async def cmd_revoke(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    try:
        uid = int(ctx.args[0])
        plan = ctx.args[1] if len(ctx.args) > 1 else "signals"
        assert plan in ("signals", "radar")
    except Exception:
        await update.message.reply_text("Формат: /revoke 123456789 [signals|radar]")
        return
    db.revoke(uid, plan)
    await update.message.reply_text(f"🚫 {plan} знято для {uid}")


async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    text = " ".join(ctx.args)
    if not text:
        await update.message.reply_text("Формат: /broadcast Привіт усім!")
        return
    users = set(db.active_users("signals")) | set(db.active_users("radar")) | {config.ADMIN_ID}
    sent, failed = 0, 0
    for uid in users:
        try:
            await ctx.bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"📣 Розіслано: {sent}, помилок: {failed}")


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    total = len(db.all_users())
    s1 = len(db.active_users("signals")); s2 = len(db.active_users("radar"))
    res = db.results_since(time.time() - 7 * 86400)
    await update.message.reply_text(
        f"📊 Всього юзерів: {total}\n"
        f"📡 СИГНАЛИ активних: {s1}\n🛰 RADAR активних: {s2}\n"
        f"💰 Ціни: сигнали ⭐️{db.get_price()}/{db.get_sub_days()}дн · радар ⭐️{db.get_radar_price()}/{db.get_radar_days()}дн\n"
        f"⚙️ Інтервал {db.get_scan_interval()} хв · {db.get_max_per_scan()} монет/скан · шанс ≥{db.get_min_conf()}%\n"
        f"≈ {round(24*60/db.get_scan_interval()*db.get_max_per_scan())} сигналів на добу\n"
        f"🏆 За тиждень: ✅{res['wins']} / 🛑{res['losses']} · WR {res['winrate']}% · "
        f"загальний {res['sum_pnl']:+.1f}% · відкрито {res['open']}"
    )


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    await update.message.reply_text("🔍 Запускаю скан…")
    # не чекаємо — скан у фоні, інакше Telegram показує «бот не відповідає»
    asyncio.create_task(run_scan_and_broadcast(source="admin/scan"))


async def cmd_setmax(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Адмін: скільки монет розсилати за один скан."""
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0])
        assert 1 <= val <= 20
    except Exception:
        await update.message.reply_text(f"Формат: /setmax 8 (зараз {db.get_max_per_scan()})")
        return
    db.set_max_per_scan(val)
    per_day = round(24 * 60 / db.get_scan_interval() * val)
    await update.message.reply_text(f"⚙️ {val} монет за скан ≈ {per_day} сигналів на добу")


async def cmd_setminconf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Адмін: мінімальний шанс %, нижчий сигнал не надсилається."""
    if not await is_admin(update.effective_user.id):
        return
    try:
        val = int(ctx.args[0])
        assert 50 <= val <= 95
    except Exception:
        await update.message.reply_text(f"Формат: /setminconf 65 (зараз {db.get_min_conf()}%)")
        return
    db.set_min_conf(val)
    await update.message.reply_text(f"⚙️ Мінімальний шанс сигналу: {val}%")


# ---------------- scheduler: signals to subscribers ----------------

async def post_init(app):
    """Ініціалізація після старту: планувальник скану ринку + health-сервер."""
    global _app
    # у python-telegram-bot v21+ post_init може отримати ApplicationBuilder, а не Application
    application = getattr(app, "application", None) or app
    _app = application
    sched = AsyncIOScheduler(timezone="UTC")
    sched.add_job(scan_job, trigger="interval", minutes=config.SCAN_INTERVAL_MIN)
    sched.start()
    log.info("Scheduler started every %d min", config.SCAN_INTERVAL_MIN)
    try:
        await start_health_server()
    except Exception as e:
        log.warning("Health server failed to start: %s", e)


async def run_scan_and_broadcast(source: str = "cron"):
    """Скан ринку -> список різних монет (до MAX_SIGNALS_PER_SCAN) -> розсилка.
    Адмін (ADMIN_ID) отримує безкоштовно; всі решта — тільки платні підписники."""
    if _app is None:
        return
    async with _scan_lock:  # одночасно лише один скан (cron + /scan не збиваються)
        analyzer = await new_analyzer()
        try:
            sigs = await analyzer.scan_market(config.TOP_N_SYMBOLS,
                                              min_confidence=config.MIN_CONFIDENCE)
        except Exception as e:
            log.error("[%s] scan failed: %s", source, e)
            await analyzer.client.aclose()
            return
        finally:
            pass
        await analyzer.client.aclose()

        if not sigs:
            log.info("[%s] no signals this round", source)
            return

        sigs = sigs[:config.MAX_SIGNALS_PER_SCAN]

        # кому надсилати: платні підписники + адмін (безкоштовно)
        recipients = set(db.active_users())
        recipients.add(config.ADMIN_ID)
        log.info("[%s] broadcasting %d different coin(s) to %d user(s)",
                 source, len(sigs), len(recipients))

        for uid in recipients:
            header = f"📡 Сигнали {time.strftime('%d.%m %H:%M UTC')} • {len(sigs)} шт."
            if uid == config.ADMIN_ID:
                header += " 👑(адмін — безкоштовно)"
            try:
                await _app.bot.send_message(uid, header)
            except Exception as e:
                log.warning("[%s] send header to %s failed: %s", source, uid, e)
                continue
            for s in sigs:
                try:
                    await _app.bot.send_message(uid, format_signal(s))
                    await asyncio.sleep(0.15)  # антифлуд
                except Exception as e:
                    log.warning("[%s] send to %s failed: %s", source, uid, e)


async def scan_job():
    """Періодичне завдання (раз на SCAN_INTERVAL_MIN хв)."""
    await run_scan_and_broadcast("cron")


async def err_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error("Update error: %s", ctx.error)


async def start_health_server():
    """Мікро HTTP-сервер з /health — щоб Railway бачив, що бот живий,
    і не вбивав процес (підмінна змінна PORT задається автоматично)."""
    from aiohttp import web

    async def health(request):
        return web.json_response({"status": "ok", "bot": "binance-signals"})

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080")))
    await site.start()
    log.info("Health server on port %s", os.environ.get("PORT", "8080"))


def main():
    db.init_db()
    app = Application.builder().token(config.BOT_TOKEN).build()
    app.post_init = post_init

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("buy", cmd_buy))
    app.add_handler(CommandHandler("signal", cmd_signal))
    app.add_handler(CommandHandler("signals", cmd_signals))
    app.add_handler(CallbackQueryHandler(cb_buy, pattern="^buy$"))

    # admin
    app.add_handler(CommandHandler("setprice", cmd_setprice))
    app.add_handler(CommandHandler("setdays", cmd_setdays))
    app.add_handler(CommandHandler("setmax", cmd_setmax))
    app.add_handler(CommandHandler("setminconf", cmd_setminconf))
    app.add_handler(CommandHandler("grant", cmd_grant))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("scan", cmd_scan))

    # payments
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_successful_payment))
    from telegram.ext import PreCheckoutQueryHandler
    app.add_handler(PreCheckoutQueryHandler(on_precheckout))

    app.add_error_handler(err_handler)

    print("Bot is running… Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
