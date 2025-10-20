# -*- coding: utf-8 -*-
import os
import re
import requests
from datetime import datetime, timedelta, timezone, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any

# =========================
# TZ helpers (стабильно для pandas)
# =========================
def get_kyiv_tz():
    try:
        import pytz
        return pytz.timezone("Europe/Kiev")   # для pandas это самый беспроблемный вариант
    except Exception:
        pass
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("Europe/Kyiv")
    except Exception:
        pass
    return timezone(timedelta(hours=3))       # fallback без DST

KYIV_TZ = get_kyiv_tz()

# =========================
# НАЛАШТУВАННЯ (ENV + секретний файл)
# =========================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
WORKSHEET_NAME = "Лист1"
TOKEN = os.getenv("TOKEN")  # Telegram Bot Token (HTTP API)
GOOGLE_JSON_PATH = "/etc/secrets/gsheets.json"

# Основные чаты для звіту підтримки
CHAT_IDS = [int(x) for x in os.getenv("CHAT_IDS", "727013047,6555660815,718885452").split(",") if x.strip()]

# === Новое: Bitrix для дней рождения ===
BITRIX_CONTACT_URL = os.getenv("BITRIX_CONTACT_URL")  # .../crm.contact.list.json
BITRIX_USERS_URL   = os.getenv("BITRIX_USERS_URL")    # .../user.get.json
BITRIX_DEALS_URL   = os.getenv("BITRIX_DEALS_URL")    # .../crm.deal.list.json
BITRIX_STAGES_URL  = os.getenv("BITRIX_STAGES_URL")   # .../crm.status.list.json (опционально, для названий стадий)

# === Новое: отдельные чаты для ДР (опционально). Если не указано — используем CHAT_IDS.
BIRTHDAYS_CHAT_IDS = [int(x) for x in os.getenv("BIRTHDAYS_CHAT_IDS", "").split(",") if x.strip()]
if not BIRTHDAYS_CHAT_IDS:
    BIRTHDAYS_CHAT_IDS = CHAT_IDS

# =========================
# Категории: КОД -> НАЗВАНИЕ
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
    "REP": "Повторне звернення",
}
NAME2CODE = {v: k for k, v in CATEGORIES.items()}

# =========================
# ДАТЫ — звіт строго за ВЧОРА (Київ)
# =========================
def now_kyiv():
    try:
        return datetime.now(tz=KYIV_TZ)
    except Exception:
        return datetime.now(timezone.utc).astimezone(KYIV_TZ)

_now = now_kyiv()
report_day = (_now - timedelta(days=1)).date()                           # вчора
start_date = datetime.combine(report_day, time(0, 0), tzinfo=KYIV_TZ)    # 00:00 Київ
end_date_exclusive = start_date + timedelta(days=1)                      # напіввідкритий інтервал

# =========================
# Інфра
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
# Bitrix helpers для ДР
# =========================
def b24_paged_get(url: str, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Пагинация Bitrix24: ?start=N, собираем весь result/items."""
    items: List[Dict[str, Any]] = []
    start = 0
    while True:
        params = dict(base_params or {})
        params["start"] = start
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"❌ Bitrix request failed ({url}): {e}")
            break

        chunk = data.get("result", [])
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        if not chunk:
            # если вернулась ошибка API (например, INVALID_CREDENTIALS)
            if "error" in data:
                print(f"❌ Bitrix error: {data.get('error')} {data.get('error_description')}")
            break

        items.extend(chunk)
        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
    return items

def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p or "")

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if not digits:
        return ""
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+" + digits

def today_month_day():
    n = now_kyiv()
    return n.month, n.day

def parse_b24_date(d: str):
    """'YYYY-MM-DD' -> (month, day) | None"""
    if not d:
        return None
    s = d.strip()[:10]
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.month, dt.day
    except Exception:
        return None

def b24_get_employees_birthday_today() -> List[Dict[str, Any]]:
    """Сотрудники с ДР сегодня (PERSONAL_BIRTHDAY), фильтруем ACTIVE на клиенте."""
    if not BITRIX_USERS_URL:
        print("⚠ BITRIX_USERS_URL not set; skip employees birthdays")
        return []
    month_today, day_today = today_month_day()
    items = b24_paged_get(
        BITRIX_USERS_URL,
        {"SELECT[]": ["ID", "NAME", "LAST_NAME", "PERSONAL_BIRTHDAY", "ACTIVE"]}
    )
    result = []
    for u in items or []:
        is_active = str(u.get("ACTIVE")).upper() in ("Y", "TRUE", "1")
        if not is_active:
            continue
        md = parse_b24_date(u.get("PERSONAL_BIRTHDAY"))
        if md and md == (month_today, day_today):
            full_name = f"{(u.get('NAME') or '').strip()} {(u.get('LAST_NAME') or '').strip()}".strip() or "Без імені"
            result.append({"id": u.get("ID"), "name": full_name})
    result.sort(key=lambda x: x["name"].lower())
    return result

def b24_get_clients_birthday_today() -> List[Dict[str, Any]]:
    """Клиенты с ДР сегодня (BIRTHDATE) + нормализованные телефоны."""
    if not BITRIX_CONTACT_URL:
        print("⚠ BITRIX_CONTACT_URL not set; skip clients birthdays")
        return []
    month_today, day_today = today_month_day()
    items = b24_paged_get(
        BITRIX_CONTACT_URL,
        {"filter[!BIRTHDATE]": "", "select[]": ["ID", "NAME", "SECOND_NAME", "LAST_NAME", "BIRTHDATE", "PHONE", "DATE_CREATE", "ASSIGNED_BY_ID"]}
    )
    result = []
    for c in items or []:
        md = parse_b24_date(c.get("BIRTHDATE"))
        if not md or md != (month_today, day_today):
            continue
        # Полное ФИО: Фамилия Имя Отчество
        name_parts = [
            (c.get('LAST_NAME') or '').strip(),
            (c.get('NAME') or '').strip(),
            (c.get('SECOND_NAME') or '').strip()
        ]
        full_name = " ".join([p for p in name_parts if p]) or "Без імені"
        phones = []
        for ph in c.get("PHONE", []) or []:
            val = normalize_phone(ph.get("VALUE", ""))
            if val:
                phones.append(val)
        # уникальные телефоны
        seen, uniq = set(), []
        for p in phones:
            k = clean_phone(p)
            if k not in seen:
                seen.add(k)
                uniq.append(p)
        result.append({
            "id": c.get("ID"),
            "name": full_name,
            "phones": uniq,
            "date_create": c.get("DATE_CREATE", ""),
            "assigned_by_id": c.get("ASSIGNED_BY_ID", "")
        })
    result.sort(key=lambda x: x["name"].lower())
    return result

def b24_get_deals_for_contacts(contact_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Получить все сделки для списка контактов, сгруппированные по CONTACT_ID."""
    if not BITRIX_DEALS_URL or not contact_ids:
        return {}

    # Битрикс не поддерживает фильтр по нескольким CONTACT_ID напрямую,
    # поэтому получаем все сделки и фильтруем на клиенте
    deals = b24_paged_get(
        BITRIX_DEALS_URL,
        {"select[]": ["ID", "TITLE", "CATEGORY_ID", "STAGE_ID", "STAGE_SEMANTIC_ID",
                      "DATE_CREATE", "DATE_MODIFY", "ASSIGNED_BY_ID", "CONTACT_ID"]}
    )

    # Группируем сделки по CONTACT_ID
    contact_deals: Dict[str, List[Dict[str, Any]]] = {}
    contact_id_set = set(str(cid) for cid in contact_ids)

    for deal in deals or []:
        # В Битрикс24 CONTACT_ID может быть массивом или одним значением
        contact_id = deal.get("CONTACT_ID")
        if isinstance(contact_id, list):
            deal_contacts = [str(c) for c in contact_id if c]
        else:
            deal_contacts = [str(contact_id)] if contact_id else []

        for cid in deal_contacts:
            if cid in contact_id_set:
                if cid not in contact_deals:
                    contact_deals[cid] = []
                contact_deals[cid].append(deal)

    return contact_deals

def parse_b24_datetime(dt_str: str):
    """Парсинг даты Bitrix24 формата 'YYYY-MM-DDTHH:MM:SS+03:00'."""
    if not dt_str:
        return None
    try:
        # Пробуем разные форматы
        for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return datetime.strptime(dt_str[:19], fmt.replace("%z", ""))
            except:
                continue
        return None
    except Exception:
        return None

def days_since(dt_str: str) -> int:
    """Сколько дней прошло с даты."""
    dt = parse_b24_datetime(dt_str)
    if not dt:
        return 0
    delta = now_kyiv().replace(tzinfo=None) - dt
    return max(0, delta.days)

def categorize_client_by_deals(deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Категоризация клиента по сделкам.

    Воронки:
    - 7: Досудебка
    - 1: Початок шлях до суду
    - 2: Суд

    Возвращает:
    {
        "is_our_client": bool,  # есть ли сделка в целевых воронках
        "deals_info": list,      # информация о сделках в целевых воронках
        "funnel_names": dict     # маппинг ID воронки -> название
    }
    """
    TARGET_FUNNELS = {"7": "Досудебка", "1": "Початок шлях до суду", "2": "Суд"}

    our_deals = []
    for deal in deals:
        category_id = str(deal.get("CATEGORY_ID", ""))
        if category_id in TARGET_FUNNELS:
            stage_id = deal.get("STAGE_ID", "Невідомо")
            date_modify = deal.get("DATE_MODIFY", "")
            days_in_stage = days_since(date_modify)

            our_deals.append({
                "funnel_id": category_id,
                "funnel_name": TARGET_FUNNELS[category_id],
                "stage_id": stage_id,
                "days_in_stage": days_in_stage,
                "assigned_by_id": deal.get("ASSIGNED_BY_ID", "")
            })

    return {
        "is_our_client": len(our_deals) > 0,
        "deals_info": our_deals,
        "funnel_names": TARGET_FUNNELS
    }

def get_user_name_by_id(user_id: str, users_cache: Dict[str, str]) -> str:
    """Получить имя пользователя по ID из кеша."""
    return users_cache.get(str(user_id), f"ID:{user_id}")

def build_users_cache() -> Dict[str, str]:
    """Построить кеш ID пользователя -> Имя."""
    if not BITRIX_USERS_URL:
        return {}

    users = b24_paged_get(
        BITRIX_USERS_URL,
        {"SELECT[]": ["ID", "NAME", "LAST_NAME"]}
    )

    cache = {}
    for u in users or []:
        uid = str(u.get("ID", ""))
        name = f"{(u.get('NAME') or '').strip()} {(u.get('LAST_NAME') or '').strip()}".strip()
        if uid and name:
            cache[uid] = name

    return cache

def build_stages_cache() -> Dict[str, str]:
    """Построить кеш STAGE_ID -> Название стадии.

    В Bitrix24 стадии хранятся в crm.status.list с ENTITY_ID вида:
    - DEAL_STAGE - общие стадии
    - DEAL_STAGE_1, DEAL_STAGE_2, DEAL_STAGE_7 - стадии для конкретных воронок
    """
    if not BITRIX_STAGES_URL:
        print("⚠ BITRIX_STAGES_URL not set; stage names will show as IDs")
        return {}

    try:
        # Получаем все статусы для сделок
        statuses = b24_paged_get(
            BITRIX_STAGES_URL,
            {"filter[ENTITY_ID]": "DEAL_STAGE%"}  # фильтр по маске для всех воронок
        )

        cache = {}
        for status in statuses or []:
            status_id = status.get("STATUS_ID", "")
            name = status.get("NAME", "")
            if status_id and name:
                cache[str(status_id)] = name

        return cache
    except Exception as e:
        print(f"⚠ Failed to load stages cache: {e}")
        return {}

def format_birthday_message() -> str:
    """Форматирование сообщения о днях рождения с разделением на своих и потенциальных клиентов."""
    employees = b24_get_employees_birthday_today()
    clients = b24_get_clients_birthday_today()

    if not employees and not clients:
        return "📅 На сьогодні днів народження немає."

    lines = ["🎂 Щоденна перевірка днів народження:"]

    # Сотрудники
    if employees:
        lines.append("\n👥 Співробітники:")
        for e in employees:
            lines.append(f"• {e['name']}")

    # Клиенты
    if clients:
        # Получаем ID всех контактов с ДР
        contact_ids = [c["id"] for c in clients]

        # Получаем сделки для этих контактов
        contact_deals = b24_get_deals_for_contacts(contact_ids)

        # Кеш пользователей для отображения имен ответственных
        users_cache = build_users_cache()

        # Кеш стадий для отображения названий стадий
        stages_cache = build_stages_cache()

        # Разделяем клиентов на две категории
        our_clients = []
        potential_clients = []

        for c in clients:
            contact_id = str(c["id"])
            deals = contact_deals.get(contact_id, [])
            category = categorize_client_by_deals(deals)

            client_info = {
                "contact": c,
                "category": category,
                "contact_manager": get_user_name_by_id(c.get("assigned_by_id", ""), users_cache),
                "date_create": c.get("date_create", "")
            }

            if category["is_our_client"]:
                our_clients.append(client_info)
            else:
                potential_clients.append(client_info)

        # Форматируем НАШИХ клиентов (с расширенной информацией)
        if our_clients:
            lines.append("\n✅ <b>Наші клієнти</b> (є сделки в цільових воронках):")
            for ci in our_clients:
                c = ci["contact"]
                phones_str = ", ".join(c["phones"]) if c["phones"] else "(тел. відсутній)"

                lines.append(f"\n📋 <b>{c['name']}</b>")
                lines.append(f"   📞 {phones_str}")
                lines.append(f"   🆔 <a href='https://your-bitrix-domain.bitrix24.ua/crm/contact/details/{c['id']}/'>Контакт #{c['id']}</a>")
                lines.append(f"   👨‍💼 Менеджер: {ci['contact_manager']}")

                # Информация о сделках
                for deal_info in ci["category"]["deals_info"]:
                    lawyer = get_user_name_by_id(deal_info["assigned_by_id"], users_cache)

                    # Получаем человекочитаемое название стадии
                    stage_id = deal_info['stage_id']
                    stage_name = stages_cache.get(stage_id, stage_id)  # fallback на ID если не нашли

                    lines.append(f"   🗂️ Воронка: <b>{deal_info['funnel_name']}</b>")
                    lines.append(f"      • Стадія: {stage_name}")
                    lines.append(f"      • На стадії: {deal_info['days_in_stage']} днів")
                    lines.append(f"      • Відповідальний юрист: {lawyer}")

        # Форматируем ПОТЕНЦИАЛЬНЫХ клиентов (для повторной продажи)
        if potential_clients:
            lines.append("\n🎯 <b>Потенційні клієнти</b> (немає сделок в цільових воронках — можна спробувати продати!):")
            for ci in potential_clients:
                c = ci["contact"]
                phones_str = ", ".join(c["phones"]) if c["phones"] else "(тел. відсутній)"

                # Вычисляем сколько дней с создания контакта
                days_since_create = days_since(ci["date_create"])

                lines.append(f"• <b>{c['name']}</b> — {phones_str}")
                lines.append(f"  <a href='https://your-bitrix-domain.bitrix24.ua/crm/contact/details/{c['id']}/'>Контакт #{c['id']}</a> | Створено: {days_since_create} днів тому | Менеджер: {ci['contact_manager']}")

    return "\n".join(lines)

# =========================
# Завантаження даних підтримки
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
# ДАТЫ: у таблиці UTC -> у звіті Київ
# =========================
dt_any_utc = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
df["dt_kyiv"] = dt_any_utc.dt.tz_convert(KYIV_TZ)
df = df.dropna(subset=["dt_kyiv"])

# Нормалізація текстових полів
for col in ["employee", "category", "phone", "status", "comment"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# Нормалізуємо категорії (код + назва)
def to_code(val: str) -> str:
    v = str(val).strip()
    if v in CATEGORIES:   # це код
        return v
    if v in NAME2CODE:    # було назва
        return NAME2CODE[v]
    return v

df["category_code"] = df["category"].apply(to_code)
df["category_name"] = df["category_code"].map(CATEGORIES).fillna(df["category"])

# Фільтр: тільки ВЧОРА (Київ)
mask_day = (df["dt_kyiv"] >= start_date) & (df["dt_kyiv"] < end_date_exclusive)
day_df = df.loc[mask_day].copy()

# “Виконано”
done_df = day_df[day_df["status"].str.lower() == "виконано"].copy()

# =========================
# Метрики (денні)
# =========================
total_tasks = len(done_df)

# Загальна частка повторних звернень (подій)
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
    done_df.groupby("category_name")["status"].count()
    .sort_values(ascending=False).rename("tasks").reset_index()
)

top_clients = (
    done_df.groupby("phone")["status"].count()
    .sort_values(ascending=False).head(3).rename("events").reset_index()
)

# Лічильники по кодам
calls_small  = int((done_df["category_code"] == "CL1").sum())
calls_medium = int((done_df["category_code"] == "CL2").sum())
calls_long   = int((done_df["category_code"] == "CL3").sum())
total_calls  = calls_small + calls_medium + calls_long

AVG_MIN_SMALL, AVG_MIN_MEDIUM, AVG_MIN_LONG = 10, 30, 50
total_minutes = calls_small*AVG_MIN_SMALL + calls_medium*AVG_MIN_MEDIUM + calls_long*AVG_MIN_LONG
total_hours = round(total_minutes / 60, 2)

total_chats = int((done_df["category_code"] == "SMS").sum())
total_conferences = int((done_df["category_code"] == "CNF").sum())
sb_df = done_df[done_df["category_code"] == "SEC"]
sb_unique_clients = int(sb_df["phone"].nunique(dropna=True))

# =========================
# === НОВЕ: повторні звернення по співробітниках
# =========================
THRESHOLD_REPEAT = 30  # поріг, %

emp_phone = (
    done_df.groupby(["employee", "phone"])["status"]
    .count()
    .rename("events")
    .reset_index()
)
tot_clients = emp_phone.groupby("employee")["phone"].nunique().rename("total_clients")
rep_clients = (
    emp_phone[emp_phone["events"] >= 2]
    .groupby("employee")["phone"].nunique()
    .rename("repeat_clients")
)
repeat_by_employee = (
    pd.concat([tot_clients, rep_clients], axis=1)
    .fillna(0)
    .reset_index()
)
repeat_by_employee["repeat_share_pct"] = (
    (repeat_by_employee["repeat_clients"] / repeat_by_employee["total_clients"].replace({0: np.nan})) * 100
).fillna(0).round(2)

emp_summary = (
    tasks_by_employee
    .merge(uniq_clients_by_employee, on="employee", how="outer")
    .merge(repeat_by_employee, on="employee", how="outer")
    .fillna(0)
)
emp_summary = emp_summary.sort_values(["repeat_share_pct", "tasks_done"], ascending=[False, False]).reset_index(drop=True)

# =========================
# ЛІНІЙНИЙ ГРАФІК АКТИВНОСТІ (вчора, Київ)
# =========================
hidx = pd.date_range(start=start_date, end=end_date_exclusive - timedelta(hours=1), freq="H", tz=KYIV_TZ)
hour_floor = day_df["dt_kyiv"].dt.tz_convert(KYIV_TZ).dt.floor("H")
events_by_hour = (
    hour_floor.value_counts()
    .reindex(hidx, fill_value=0)
    .sort_index()
)
if events_by_hour.values.sum() == 0 and len(day_df) > 0:
    by_hour_int = (day_df["dt_kyiv"].dt.hour.value_counts().reindex(range(24), fill_value=0))
    hour_labels = [f"{h:02d}:00" for h in range(24)]
    events_values = by_hour_int.values
else:
    hour_labels = [d.strftime("%H:%M") for d in hidx]
    events_values = events_by_hour.values

peak_idx = np.argsort(-events_values)[:3]
valley_idx = np.argsort(events_values)[:1]

# =========================
# Дашборд
# =========================
from matplotlib.gridspec import GridSpec

fig = plt.figure(figsize=(16, 9))
gs = fig.add_gridspec(2, 2, height_ratios=[1.4, 1.0], hspace=0.4, wspace=0.25)
fig.suptitle(f"Підтримка • Денний звіт {start_date.strftime('%d.%m.%Y')} (час Києва)", fontsize=18, fontweight="bold")

ax0 = fig.add_subplot(gs[0, :])
ax0.plot(hour_labels, events_values, marker="o")
ax0.set_title("Звернення по годинах (усі події, час Києва)")
ax0.set_xlabel("Час")
ax0.set_ylabel("К-сть звернень")
ax0.set_xticks(range(0, len(hour_labels), max(1, len(hour_labels)//8)))
ax0.tick_params(axis='x', rotation=45)
ax0.set_ylim(bottom=0, top=max(1, int(max(events_values) * 1.2)))
for i in peak_idx:
    ax0.annotate(f"пік: {events_values[i]}", (i, events_values[i]),
                 textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)
for i in valley_idx:
    ax0.annotate(f"мін: {events_values[i]}", (i, events_values[i]),
                 textcoords="offset points", xytext=(0, -12), ha="center", fontsize=9)

ax1 = fig.add_subplot(gs[1, 0])
x1 = np.arange(len(emp_summary))
ax1.bar(x1, emp_summary["unique_clients"])
ax1.set_xticks(x1)
ax1.set_xticklabels(emp_summary["employee"], rotation=45, ha="right")
ax1.set_title("Унікальні клієнти по співробітнику")
ax1.set_ylabel("К-сть унікальних телефонів")
for i, v in enumerate(emp_summary["unique_clients"]):
    ax1.text(i, v + 0.05, str(int(v)), ha='center', va='bottom')

ax2 = fig.add_subplot(gs[1, 1])
x2 = np.arange(len(cats))
ax2.bar(x2, cats["tasks"])
ax2.set_xticks(x2)
ax2.set_xticklabels(cats["category_name"], rotation=45, ha="right")
ax2.set_title("Розподіл задач за категоріями")
ax2.set_ylabel("К-сть задач")
for i, v in enumerate(cats["tasks"]):
    ax2.text(i, v + 0.05, str(int(v)), ha='center', va='bottom')

fig.tight_layout(rect=[0, 0.03, 1, 0.96])
dashboard_img = "support_daily_report.png"
fig.savefig(dashboard_img, dpi=200, bbox_inches="tight")
plt.close(fig)

# =========================
# Текст звіту підтримки
# =========================
max_tasks = tasks_by_employee["tasks_done"].max() if len(tasks_by_employee) else 0
min_tasks = tasks_by_employee["tasks_done"].min() if len(tasks_by_employee) else 0

def badge_tasks(tasks):
    b = []
    if tasks == max_tasks and tasks > 0:
        b.append("🏆")
    if tasks == min_tasks and tasks > 0 and max_tasks != min_tasks:
        b.append("🔴")
    return " ".join(b)

u_map = dict(zip(emp_summary["employee"], emp_summary["unique_clients"]))

emp_lines = []
for _, r in emp_summary.iterrows():
    emp = r["employee"]
    t = int(r.get("tasks_done", 0))
    u = int(r.get("unique_clients", 0))
    emp_lines.append(f"• <b>{emp}</b> — задач: <b>{t}</b> {badge_tasks(t)} | унікальних клієнтів: <b>{u}</b>")
employees_inline_text = "\n".join(emp_lines)

rep_lines = []
for _, r in emp_summary.iterrows():
    emp = r["employee"]
    total_c = int(r.get("total_clients", 0))
    repeat_c = int(r.get("repeat_clients", 0))
    share = float(r.get("repeat_share_pct", 0.0))
    flag = "🔴" if share > THRESHOLD_REPEAT else "🟢"
    rep_lines.append(f"• <b>{emp}</b> — повторні клієнти: <b>{share}%</b> ({repeat_c} з {total_c}) {flag}")
repeat_inline_text = "\n".join(rep_lines)

cat_lines = [f"• <b>{row['category_name']}</b>: {int(row['tasks'])}" for _, row in cats.iterrows()]
cats_inline_text = "\n".join(cat_lines)

top_lines = [f"• <b>{row['phone']}</b>: {int(row['events'])}" for _, row in top_clients.iterrows()]
top_inline_text = "\n".join(top_lines)

kpi_text = (
    f"📊 <b>Денний звіт підтримки</b> ({start_date.strftime('%d.%m.%Y')} — час Києва)\n\n"
    f"✅ Всього виконано задач: <b>{total_tasks}</b>\n"
    f"🔁 Частка повторних звернень (за день, по подіях): <b>{repeat_rate}%</b>\n\n"
    f"☎️ <b>Дзвінки</b>: всього <b>{total_calls}</b> "
    f"(короткі: <b>{calls_small}</b>, середні: <b>{calls_medium}</b>, довготривалі: <b>{calls_long}</b>)\n"
    f"⏱️ <b>Годин у розмові</b> (оцінка): <b>{total_hours} год</b>\n"
    f"💬 <b>Чати</b>: <b>{total_chats}</b>\n"
    f"🎥 <b>Проведені конференції</b>: <b>{total_conferences}</b>\n"
    f"🧩 <b>СБ (супровід)</b> — унікальних клієнтів: <b>{sb_unique_clients}</b>\n\n"
    f"👥 <b>По співробітниках</b>:\n{employees_inline_text}\n\n"
    f"🔁 <b>Повторні звернення по співробітниках</b> "
    f"(клієнти з ≥2 зверненнями; поріг: {THRESHOLD_REPEAT}%):\n{repeat_inline_text}\n\n"
    f"🏷️ <b>Категорії (розподіл задач)</b>:\n{cats_inline_text}\n\n"
    f"📱 <b>Топ-3 клієнтів за зверненнями</b>:\n{top_inline_text}\n\n"
    f"📈 Лінійний графік звернень по годинах — див. на дашборді (час Києва)."
)

# =========================
# Відправка: 1) звіт підтримки
# =========================
send_photo(dashboard_img, CHAT_IDS)
send_message(kpi_text, CHAT_IDS)

# =========================
# Відправка: 2) окремий блок "Дні народження"
# =========================
birthday_text = format_birthday_message()
send_message(birthday_text, BIRTHDAYS_CHAT_IDS)
