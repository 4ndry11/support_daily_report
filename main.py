# -*- coding: utf-8 -*-
import os
import requests
from datetime import datetime, timedelta, timezone, time  # ← добавлен time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import gspread
from google.oauth2.service_account import Credentials

# =========================
# TZ helpers (стабильно для pandas)
# =========================
def get_kyiv_tz():
    """
    Возвращает tz-объект, который pandas корректно переваривает:
    1) pytz 'Europe/Kiev' (лучший вариант для pandas),
    2) zoneinfo 'Europe/Kyiv',
    3) fixed UTC+03:00 (без DST как крайний случай).
    """
    try:
        import pytz  # предпочтительно для pandas
        return pytz.timezone("Europe/Kiev")
    except Exception:
        pass
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("Europe/Kyiv")
    except Exception:
        pass
    return timezone(timedelta(hours=3))  # без переходов, но стабильно

KYIV_TZ = get_kyiv_tz()

# =========================
# НАЛАШТУВАННЯ (ENV + секретный файл)
# =========================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # спрячь в Render → Environment
WORKSHEET_NAME = "Лист1"                      # можно оставить в коде
TOKEN = os.getenv("TOKEN")                    # спрячь в Render → Environment

# Секретный файл с ключами Google (Render → Secret Files: filename = gsheets.json)
GOOGLE_JSON_PATH = "/etc/secrets/gsheets.json"

# Кому слать отчёт (жёсткий список)
CHAT_IDS = [727013047, 6555660815, 718885452]

# =========================
# ТОЧНИЙ ПЕРЕЛІК КАТЕГОРІЙ (як у таблиці)
# =========================
CATEGORIES = {
    "CL1": "Дзвінки дрібні",
    "CL2": "Дзвінки середні",
    "CL3": "Дзвінки довготривалі",
    "SMS": "СМС",
    "SEC": "СБ (супровід)",
    "CNF": "Конференція",
    "NEW": "Перший контакт",
    "HS1": "Опрацювання історії легке",
    "HS2": "Опрацювання історії середнє",
    "HS3": "Опрацювання історії складне",
    "REP": "Повторне звернення"
}
CAT_CALL_SMALL  = CATEGORIES["CL1"]
CAT_CALL_MEDIUM = CATEGORIES["CL2"]
CAT_CALL_LONG   = CATEGORIES["CL3"]
CAT_CHAT        = CATEGORIES["SMS"]
CAT_CONF        = CATEGORIES["CNF"]
CAT_SB          = CATEGORIES["SEC"]

# =========================
# ДАТИ — РАХУЄМО СТРОГО ВЧОРА (за Києвом)
# =========================
def now_kyiv():
    try:
        return datetime.now(tz=KYIV_TZ)
    except Exception:
        return datetime.now(timezone.utc).astimezone(KYIV_TZ)

_now = now_kyiv()
report_day = (_now - timedelta(days=1)).date()                      # ← ВЧОРА
start_date = datetime.combine(report_day, time(0, 0), tzinfo=KYIV_TZ)      # 00:00 вчера
end_date_exclusive = start_date + timedelta(days=1)                        # 00:00 сегодня (исключительно)

# =========================
# ІНФРА
# =========================
def get_gspread_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_JSON_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return gspread.authorize(creds)

def send_message(text, chat_ids):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    for chat_id in chat_ids:
        try:
            requests.post(url, data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=30)
        except Exception as e:
            print(f"send_message error for {chat_id}: {e}")

def send_photo(image_path, chat_ids):
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    for chat_id in chat_ids:
        try:
            with open(image_path, "rb") as photo:
                requests.post(url, data={"chat_id": chat_id}, files={"photo": photo}, timeout=60)
        except Exception as e:
            print(f"send_photo error for {chat_id}: {e}")

# =========================
# ЗАВАНТАЖЕННЯ ДАНИХ
# =========================
gc = get_gspread_client()
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(WORKSHEET_NAME)
rows = ws.get_all_records()
df = pd.DataFrame(rows)

# Очікувані назви стовпців
rename_map = {
    "Дата/час": "datetime",
    "Співробітник": "employee",
    "Категорія": "category",
    "Телефон клієнта": "phone",
    "Коментар": "comment",
    "Статус": "status",
}
df = df.rename(columns=rename_map)

# =========================
# Парс дат: ВХОД В UTC → КОНВЕРСИЯ В КИЕВ
# (если в таблице уже Киевское время без TZ, можно заменить на tz_localize — см. комментарий ниже)
# =========================
dt_utc = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
df["dt_kyiv"] = dt_utc.dt.tz_convert(KYIV_TZ)
df = df.dropna(subset=["dt_kyiv"])

# --- Если ваши значения в таблице уже в местном времени без часового пояса (а не UTC) — используйте это вместо блока выше:
# dt_local = pd.to_datetime(df["datetime"], errors="coerce")           # без utc=True
# df["dt_kyiv"] = dt_local.dt.tz_localize(KYIV_TZ, nonexistent='shift_forward', ambiguous='NaT')
# df = df.dropna(subset=["dt_kyiv"])

# Фільтр: тільки ВЧОРА по Києву (напіввідкритий інтервал)
mask_day = (df["dt_kyiv"] >= start_date) & (df["dt_kyiv"] < end_date_exclusive)
day_df = df.loc[mask_day].copy()

# Нормалізація тексту
for col in ["employee", "category", "phone", "status", "comment"]:
    if col in day_df.columns:
        day_df[col] = day_df[col].astype(str).str.strip()

# “Виконано”
done_df = day_df[day_df["status"].str.lower() == "виконано"].copy()

# =========================
# МЕТРИКИ (денні)
# =========================
total_tasks = len(done_df)

phone_counts = done_df["phone"].value_counts()
repeat_rate = round((phone_counts[phone_counts > 1].sum() / phone_counts.sum()) * 100, 2) if phone_counts.sum() else 0

tasks_by_employee = (
    done_df.groupby("employee")["status"].count()
    .sort_values(ascending=False).rename("tasks_done").reset_index()
)

uniq_clients_by_employee = (
    done_df.groupby("employee")["phone"].nunique(dropna=True)
    .sort_values(ascending=False).rename("unique_clients").reset_index()
)

cats = (
    done_df.groupby("category")["status"].count()
    .sort_values(ascending=False).rename("tasks").reset_index()
)

top_clients = (
    done_df.groupby("phone")["status"].count()
    .sort_values(ascending=False).head(3).rename("events").reset_index()
)

# Дзвінки/години/чати/конференції/СБ (строго по категоріях)
calls_small  = int((done_df["category"] == CAT_CALL_SMALL).sum())
calls_medium = int((done_df["category"] == CAT_CALL_MEDIUM).sum())
calls_long   = int((done_df["category"] == CAT_CALL_LONG).sum())
total_calls  = calls_small + calls_medium + calls_long

AVG_MIN_SMALL, AVG_MIN_MEDIUM, AVG_MIN_LONG = 10, 30, 50
total_minutes = calls_small*AVG_MIN_SMALL + calls_medium*AVG_MIN_MEDIUM + calls_long*AVG_MIN_LONG
total_hours = round(total_minutes / 60, 2)

total_chats = int((done_df["category"] == CAT_CHAT).sum())
total_conferences = int((done_df["category"] == CAT_CONF).sum())

sb_df = done_df[done_df["category"] == CAT_SB]
sb_unique_clients = sb_df["phone"].nunique(dropna=True)

# =========================
# ЛІНІЙНИЙ ГРАФІК: звернення по годинах (усі події ВЧОРА, за Києвом)
# =========================
# часовая сетка 00:00..23:00 (вчера)
hidx = pd.date_range(start=start_date, end=end_date_exclusive - timedelta(hours=1),
                     freq="H", tz=KYIV_TZ)
events_by_hour = (
    day_df.set_index("dt_kyiv").sort_index()
          .resample("H").size()
).reindex(hidx, fill_value=0)
hour_labels = hidx.strftime("%H:%M")

# =========================
# ДАШБОРД (1 большой line + 2 вертикальные bar)
# =========================
from matplotlib.gridspec import GridSpec

fig = plt.figure(figsize=(16, 9))
gs = fig.add_gridspec(2, 2, height_ratios=[1.4, 1.0], hspace=0.4, wspace=0.25)
fig.suptitle(f"Підтримка • Денний звіт {start_date.strftime('%d.%m.%Y')} (час Києва)", fontsize=18, fontweight="bold")

# ВЕРХ: лінія
ax0 = fig.add_subplot(gs[0, :])
ax0.plot(hour_labels, events_by_hour.values, marker="o")
ax0.set_title("Звернення по годинах (усі події, час Києва)")
ax0.set_xlabel("Час")
ax0.set_ylabel("К-сть звернень")
ax0.set_xticks(range(0, len(hour_labels), max(1, len(hour_labels)//8)))
ax0.tick_params(axis='x', rotation=45)

# НИЗ ЛІВО: Унікальні клієнти по співробітнику
ax1 = fig.add_subplot(gs[1, 0])
x1 = np.arange(len(uniq_clients_by_employee))
ax1.bar(x1, uniq_clients_by_employee["unique_clients"])
ax1.set_xticks(x1)
ax1.set_xticklabels(uniq_clients_by_employee["employee"], rotation=45, ha="right")
ax1.set_title("Унікальні клієнти по співробітнику")
ax1.set_ylabel("К-сть унікальних телефонів")
for i, v in enumerate(uniq_clients_by_employee["unique_clients"]):
    ax1.text(i, v + 0.05, str(int(v)), ha='center', va='bottom')

# НИЗ ПРАВО: Розподіл задач за категоріями
ax2 = fig.add_subplot(gs[1, 1])
x2 = np.arange(len(cats))
ax2.bar(x2, cats["tasks"])
ax2.set_xticks(x2)
ax2.set_xticklabels(cats["category"], rotation=45, ha="right")
ax2.set_title("Розподіл задач за категоріями")
ax2.set_ylabel("К-сть задач")
for i, v in enumerate(cats["tasks"]):
    ax2.text(i, v + 0.05, str(int(v)), ha='center', va='bottom')

fig.tight_layout(rect=[0, 0.03, 1, 0.96])
dashboard_img = "support_daily_report.png"
fig.savefig(dashboard_img, dpi=200, bbox_inches="tight")
plt.close(fig)

# =========================
# ТЕКСТ
# =========================
max_tasks = tasks_by_employee["tasks_done"].max() if len(tasks_by_employee) else 0
min_tasks = tasks_by_employee["tasks_done"].min() if len(tasks_by_employee) else 0

def badge(tasks):
    b = []
    if tasks == max_tasks and tasks > 0:
        b.append("🏆")
    if tasks == min_tasks and tasks > 0 and max_tasks != min_tasks:
        b.append("🔴")
    return " ".join(b)

u_map = dict(zip(uniq_clients_by_employee["employee"], uniq_clients_by_employee["unique_clients"]))
emp_lines = []
for _, r in tasks_by_employee.iterrows():
    emp = r["employee"]; t = int(r["tasks_done"]); u = int(u_map.get(emp, 0))
    emp_lines.append(f"• <b>{emp}</b> — задач: <b>{t}</b> {badge(t)} | унікальних клієнтів: <b>{u}</b>")
employees_inline_text = "\n".join(emp_lines)

cat_lines = [f"• <b>{row['category']}</b>: {int(row['tasks'])}" for _, row in cats.iterrows()]
cats_inline_text = "\n".join(cat_lines)

top_lines = [f"• <b>{row['phone']}</b>: {int(row['events'])}" for _, row in top_clients.iterrows()]
top_inline_text = "\n".join(top_lines)

kpi_text = (
    f"📊 <b>Денний звіт підтримки</b> ({start_date.strftime('%d.%m.%Y')} — час Києва)\n\n"
    f"✅ Всього виконано задач: <b>{total_tasks}</b>\n"
    f"🔁 Частка повторних звернень (за день): <b>{repeat_rate}%</b>\n\n"
    f"☎️ <b>Дзвінки</b>: всього <b>{total_calls}</b> "
    f"(короткі: <b>{calls_small}</b>, середні: <b>{calls_medium}</b>, довготривалі: <b>{calls_long}</b>)\n"
    f"⏱️ <b>Годин у розмові</b> (оцінка): <b>{total_hours} год</b>\n"
    f"💬 <b>Чати</b>: <b>{total_chats}</b>\n"
    f"🎥 <b>Проведені конференції</b>: <b>{total_conferences}</b>\n"
    f"🧩 <b>СБ (супровід)</b> — унікальних клієнтів: <b>{sb_unique_clients}</b>\n\n"
    f"👥 <b>По співробітниках</b>:\n{employees_inline_text}\n\n"
    f"🏷️ <b>Категорії (розподіл задач)</b>:\n{cats_inline_text}\n\n"
    f"📱 <b>Топ-3 клієнтів за зверненнями</b>:\n{top_inline_text}\n\n"
    f"📈 Лінійний графік звернень по годинах — див. на дашборді (час Києва)."
)

# =========================
# ВІДПРАВКА: спочатку дашборд, потім текст
# =========================
send_photo(dashboard_img, CHAT_IDS)
send_message(kpi_text, CHAT_IDS)
