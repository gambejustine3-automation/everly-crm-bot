from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = Flask(__name__)

# ─────────────────────────────────────────────
# ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
BOT_TOKEN                = os.environ.get("BOT_TOKEN")
PIPELINE_BOT_TOKEN       = os.environ.get("PIPELINE_BOT_TOKEN")
DASHBOARD_BOT_TOKEN      = os.environ.get("DASHBOARD_BOT_TOKEN")
CHAT_ID                  = os.environ.get("CHAT_ID")
SPREADSHEET_ID           = os.environ.get("SPREADSHEET_ID")

DASHBOARD_API            = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"
PIPELINE_API             = f"https://api.telegram.org/bot{PIPELINE_BOT_TOKEN}"
CLIENT_API               = f"https://api.telegram.org/bot{BOT_TOKEN}"

SCOPES                   = ["https://www.googleapis.com/auth/spreadsheets"]

CAL_API_KEY              = os.environ.get("CAL_API_KEY")
CAL_EVENT_TYPE_ID        = os.environ.get("CAL_EVENT_TYPE_ID")

# Zapier Webhooks
CLOSE_LEAD_WEBHOOK       = os.environ.get("CLOSE_LEAD_WEBHOOK")
PROPOSAL_ZAPIER_WEBHOOK  = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
CONTRACT_ZAPIER_WEBHOOK  = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
DEPOSIT_PAID_WEBHOOK     = os.environ.get("DEPOSIT_PAID_WEBHOOK")
DELIVER_GALLERY_WEBHOOK  = os.environ.get("DELIVER_GALLERY_WEBHOOK")
RETENTION_WEBHOOK        = os.environ.get("RETENTION_WEBHOOK")

# FIX: System 1.5B Call Outcome Email Routing webhook.
# This MUST be the Zapier Catch Hook URL from the s1_5b Zap.
# Without this, post-call follow-up emails (no-show, reschedule,
# completed_continue) are NEVER sent to clients.
CALL_OUTCOME_WEBHOOK     = os.environ.get("CALL_OUTCOME_WEBHOOK")

# External Resources
GOOGLE_REVIEW_LINK       = os.environ.get("GOOGLE_REVIEW_LINK", "https://g.page/r/YOUR_PLACE_ID/review")

# ─────────────────────────────────────────────
# PH TIME HELPER
# ─────────────────────────────────────────────
def ph_now():
    return datetime.utcnow() + timedelta(hours=8)

# ─────────────────────────────────────────────
# STAGE DEFINITIONS
# ─────────────────────────────────────────────
PIPELINE_STAGES = [
    "Inquiry", "Discovery Call Booked", "Discovery Call Completed",
    "Proposal Sent", "Contracted", "Active Project", "Delivered",
    "Retention", "Closed Won", "Closed Lost"
]

# Projects sheet uses a separate stage schema from Pipeline Tracker.
# Do NOT mix these two stage sets — they track different things.
#   Pipeline Tracker = sales/relationship stage
#   Projects         = production/delivery stage
PROJECT_STAGES = [
    "Pre-Production", "Active", "Post-Production", "Delivered", "Completed", "Closed"
]

STATUS_EMOJI = {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}

# Uncertain budgets require /setbudget confirmation before System 3A fires.
UNCERTAIN_BUDGETS = {"$10,000+", "TBD", "10000+", "$10,000+ ", "10,000+"}

OUTCOME_LABELS = {
    "completed_continue":  "✅ Completed — moving to Proposal",
    "completed_stop":      "🛑 Closed as Not a Fit",
    "no_show":             "❌ Marked as No Show",
    "reschedule":          "🔄 Marked for Rescheduling",
    "reschedule_oncall":   "🔁 Rescheduled On-Call",
    "booked_for_client":   "📅 Booked For Client"
}

OUTCOME_MAP = {
    "completed_continue":  {"current_stage": "Discovery Call Completed", "call_status": "Completed",                     "next_action": "Client's Approval"},
    "completed_stop":      {"current_stage": "Closed Lost",              "call_status": "Completed",                     "next_action": "Archive Lead"},
    "no_show":             {"current_stage": "Discovery Call Booked",    "call_status": "No Show",                       "next_action": "Follow up / Reschedule"},
    "reschedule":          {"current_stage": "Discovery Call Booked",    "call_status": "Rescheduling",                  "next_action": "Send new Cal.com link"},
    "reschedule_oncall":   {"current_stage": "Discovery Call Booked",    "call_status": "Rescheduling — Client Request", "next_action": "Send new Cal.com link"},
    "booked_for_client":   {"current_stage": "Discovery Call Booked",    "call_status": "Booked by Victoria",            "next_action": "Confirm with client"}
}

# These outcomes trigger System 1.5B to send a follow-up email to the client.
# "completed_stop" closes the lead — no email sent.
# "booked_for_client" is an internal booking — no automated email needed.
OUTCOMES_REQUIRING_EMAIL = {"completed_continue", "no_show", "reschedule", "reschedule_oncall"}

# ─────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────
def get_sheets_service():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_sheet_with_headers(range_name):
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=range_name
        ).execute()
        values = result.get("values", [])
        if not values:
            return [], {}
        headers = values[0]
        rows = values[1:]
        col = {name: i for i, name in enumerate(headers)}
        return rows, col
    except Exception as e:
        print(f"[READ ERROR] {range_name}: {e}")
        return [], {}

def write_sheet(range_name, values):
    try:
        service = get_sheets_service()
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range_name,
            valueInputOption="RAW", body={"values": values}
        ).execute()
        return True
    except Exception as e:
        print(f"[WRITE ERROR] {range_name}: {e}")
        return False

def _write_back(sheet_name, range_name, lookup_col_name, lookup_value, updates):
    rows, col = read_sheet_with_headers(range_name)
    found_row_idx = -1
    for i, row in enumerate(rows):
        if safe_get(row, col, lookup_col_name) == lookup_value:
            found_row_idx = i
            break

    if found_row_idx == -1:
        print(f"[WRITE BACK ERROR] {lookup_value} not found in {sheet_name} for column {lookup_col_name}")
        return False

    target_row = rows[found_row_idx]
    updated_values = list(target_row)

    for key, value in updates.items():
        if key in col:
            # Extend the row list if necessary
            while len(updated_values) <= col[key]:
                updated_values.append("")
            updated_values[col[key]] = value
        else:
            print(f"[WRITE BACK WARNING] Column '{key}' not found in {sheet_name}. Skipping.")

    update_range = f"{sheet_name}!{get_col_letter(0)}{found_row_idx + 2}:{get_col_letter(len(updated_values) - 1)}{found_row_idx + 2}"
    return write_sheet(update_range, [updated_values])

def safe_get(row, col, key):
    if key not in col or len(row) <= col[key]:
        return "—"
    val = row[col[key]]
    return val if val else "—"

def get_col_letter(idx):
    result = ""
    while idx >= 0:
        result = chr(65 + (idx % 26)) + result
        idx = (idx // 26) - 1
    return result

def fire_webhook(url, payload):
    if not url:
        print("[WEBHOOK] URL not set.")
        return False
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[WEBHOOK ERROR] {url}: {e}")
        return False

def answer_callback(cb_id, text, use_pipeline=False):
    api = PIPELINE_API if use_pipeline else DASHBOARD_API
    requests.post(f"{api}/answerCallbackQuery", json={
        "callback_query_id": cb_id,
        "text": text
    })

# ─────────────────────────────────────────────
# TELEGRAM SEND/EDIT HELPERS
# ─────────────────────────────────────────────
def send_msg(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)
    print(f"[DASHBOARD SEND] {r.status_code}: {r.text[:200]}")
    return r

def edit_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id, "message_id": message_id,
        "text": text, "parse_mode": "Markdown",
        "reply_markup": reply_markup if reply_markup else {"inline_keyboard": []}
    }
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

def edit_pipeline_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id, "message_id": message_id,
        "text": text, "parse_mode": "Markdown",
        "reply_markup": reply_markup if reply_markup else {"inline_keyboard": []}
    }
    requests.post(f"{PIPELINE_API}/editMessageText", json=payload)

def send_pipeline_msg(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = requests.post(f"{PIPELINE_API}/sendMessage", json=payload)
    return r

def delete_pipeline_msg(chat_id, message_id):
    try:
        requests.post(f"{PIPELINE_API}/deleteMessage", json={
            "chat_id": chat_id, "message_id": message_id
        }, timeout=5)
    except Exception as e:
        print(f"[DELETE MSG] Failed: {e}")

def delete_msg(chat_id, message_id):
    try:
        requests.post(f"{DASHBOARD_API}/deleteMessage", json={
            "chat_id": chat_id, "message_id": message_id
        }, timeout=5)
    except Exception as e:
        print(f"[DELETE MSG] Failed: {e}")

def send_client_msg(chat_id, text):
    requests.post(f"{CLIENT_API}/sendMessage", json={"chat_id": chat_id, "text": text})

def smart_send(chat_id, text, markup=None, msg_id=None, use_pipeline=False):
    if msg_id:
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, text, markup)
        else:
            edit_msg(chat_id, msg_id, text, markup)
    else:
        if use_pipeline:
            return send_pipeline_msg(chat_id, text, markup)
        else:
            return send_msg(chat_id, text, markup)

# ─────────────────────────────────────────────
# CONFIG HELPERS
# ─────────────────────────────────────────────
def get_briefing_time():
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Config!C2"
        ).execute()
        values = result.get("values", [])
        if values and values[0] and values[0][0]:
            val = str(values[0][0]).strip()
            if ":" in val:
                return val
    except Exception as e:
        print(f"[CONFIG] Error reading briefing_time: {e}")
    return "09:00"

def write_briefing_time(time_str):
    try:
        service = get_sheets_service()
        check = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Config!C1"
        ).execute()
        c1_vals = check.get("values", [])
        if not c1_vals or not c1_vals[0]:
            write_sheet("Config!C1", [["briefing_time"]])
        write_sheet("Config!C2", [[time_str]])
    except Exception as e:
        print(f"[CONFIG] Error writing briefing_time: {e}")

# ─────────────────────────────────────────────
# SCHEDULER — Daily Jobs
# ─────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone="Asia/Manila")

def run_daily_jobs():
    print(f"[SCHEDULER] Running daily jobs at {ph_now().strftime('%Y-%m-%d %H:%M')}")
    send_daily_briefing()
    check_retention_completions()

def reschedule_briefing(time_str):
    try:
        hour, minute = map(int, time_str.split(":"))
        scheduler.reschedule_job('daily_jobs', trigger='cron', hour=hour, minute=minute)
        print(f"[SCHEDULER] Rescheduled to {time_str} PH time")
    except Exception as e:
        print(f"[SCHEDULER] Reschedule error: {e}")

def init_scheduler():
    if scheduler.running:
        return
    try:
        briefing_time = get_briefing_time()
        hour, minute = map(int, briefing_time.split(":"))
    except Exception:
        hour, minute = 9, 0
    scheduler.add_job(run_daily_jobs, 'cron', hour=hour, minute=minute,
                      id='daily_jobs', replace_existing=True)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
    print(f"[SCHEDULER] Started — daily jobs at {hour:02d}:{minute:02d} PH time")

# ─────────────────────────────────────────────
# DAILY BRIEFING
# ─────────────────────────────────────────────
def send_daily_briefing():
    today = ph_now()
    today_disp = today.strftime("%B %d, %Y")
    lines = [f"🌅 *Good morning! Daily Briefing — {today_disp}*\n"]
    buttons = []
    has_items = False

    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")

    # ── 1. Overdue Balances ──
    overdue = []
    for r in proj_rows:
        stage = safe_get(r, proj_col, "Current_Stage")
        if stage in ("Completed", "Closed", "Closed Won", "Closed Lost"):
            continue
        balance_paid = safe_get(r, proj_col, "Balance_Paid")
        if balance_paid.upper() == "TRUE":
            continue
        due_str = safe_get(r, proj_col, "Balance_Due_Date")
        if due_str == "—":
            continue
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    due_dt = datetime.strptime(due_str.strip(), fmt)
                    if due_dt.date() < today.date():
                        days_overdue = (today.date() - due_dt.date()).days
                        overdue.append((r, days_overdue))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if overdue:
        has_items = True
        overdue.sort(key=lambda x: x[1], reverse=True)
        lines.append(f"🔴 *Overdue Balances ({len(overdue)})*")
        for r, days in overdue[:3]:
            name = safe_get(r, proj_col, "Client_Name")
            lid  = safe_get(r, proj_col, "Lead_ID")
            bal  = safe_get(r, proj_col, "Balance")
            due  = safe_get(r, proj_col, "Balance_Due_Date")
            lines.append(f" • *{name}* — ${bal} overdue by {days} day(s) (due {due})")
            buttons.append([{"text": f"💰 {name} — Overdue Balance", "callback_data": f"view_project|{lid}"}])

    # ── 2. Galleries Not Yet Delivered ──
    undelivered = []
    for r in proj_rows:
        stage = safe_get(r, proj_col, "Current_Stage")
        if stage not in ("Post-Production", "Active"):
            continue
        event_str = safe_get(r, proj_col, "Event_Date")
        if event_str == "—":
            continue
        try:
            for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    event_dt = datetime.strptime(event_str.strip(), fmt)
                    days_since = (today.date() - event_dt.date()).days
                    if days_since > 0:
                        undelivered.append((r, days_since))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if undelivered:
        has_items = True
        undelivered.sort(key=lambda x: x[1], reverse=True)
        lines.append(f"\n📸 *Gallery Not Yet Delivered ({len(undelivered)})*")
        for r, days in undelivered[:3]:
            name  = safe_get(r, proj_col, "Client_Name")
            lid   = safe_get(r, proj_col, "Lead_ID")
            stage = safe_get(r, proj_col, "Current_Stage")
            lines.append(f" • *{name}* — {days} day(s) since event | Stage: {stage}")
            buttons.append([{"text": f"📸 {name} — Deliver Gallery", "callback_data": f"view_project|{lid}"}])

    # ── 3. Projects Stuck in a Stage (14+ days past Next_Action_Date) ──
    pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    stuck = []
    terminal = {"Closed Won", "Closed Lost", "Retention", "Delivered"}
    for r in pipe_rows:
        stage = safe_get(r, pipe_col, "Current_Stage")
        if stage in terminal:
            continue
        nad_str = safe_get(r, pipe_col, "Next_Action_Date")
        if nad_str == "—":
            continue
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    nad_dt    = datetime.strptime(nad_str.strip(), fmt)
                    days_past = (today.date() - nad_dt.date()).days
                    if days_past >= 14:
                        stuck.append((r, days_past))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if stuck:
        has_items = True
        stuck.sort(key=lambda x: x[1], reverse=True)
        lines.append(f"\n⏳ *Stuck Projects ({len(stuck)})*")
        for r, days in stuck[:3]:
            name  = safe_get(r, pipe_col, "Client_Name")
            lid   = safe_get(r, pipe_col, "Lead_ID")
            stage = safe_get(r, pipe_col, "Current_Stage")
            lines.append(f" • *{name}* — {stage} for {days} day(s)")
            buttons.append([{"text": f"⏳ {name} — Stuck", "callback_data": f"view_pipe|{lid}"}])

    # ── 4. Upcoming Shoots This Week ──
    upcoming = []
    for r in lead_rows:
        event_str = safe_get(r, lead_col, "Event_Date")
        if event_str == "—":
            continue
        try:
            for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    event_dt   = datetime.strptime(event_str.strip(), fmt)
                    days_until = (event_dt.date() - today.date()).days
                    if 0 <= days_until <= 7:
                        upcoming.append((r, days_until))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if upcoming:
        has_items = True
        upcoming.sort(key=lambda x: x[1])
        lines.append(f"\n📅 *Upcoming Shoots This Week ({len(upcoming)})*")
        for r, days in upcoming[:3]:
            name  = safe_get(r, lead_col, "Full_Name")
            lid   = safe_get(r, lead_col, "Lead_ID")
            etype = safe_get(r, lead_col, "Event_Type")
            label = "Today" if days == 0 else f"in {days} day(s)"
            lines.append(f" • *{name}* — {etype} {label}")
            buttons.append([{"text": f"📅 {name} — {etype}", "callback_data": f"view_lead|{lid}"}])

    # ── 5. Retention Windows Closing (day 5–7 of 7) ──
    closing = []
    for r in proj_rows:
        stage = safe_get(r, proj_col, "Current_Stage")
        if stage != "Delivered":
            continue
        review_sent = safe_get(r, proj_col, "Review")
        if review_sent.upper() == "TRUE":
            continue
        delivery_str = safe_get(r, proj_col, "Delivery_Date")
        if delivery_str == "—":
            continue
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    delivery_dt = datetime.strptime(delivery_str.strip(), fmt)
                    days_since  = (today.date() - delivery_dt.date()).days
                    if 5 <= days_since <= 7:
                        closing.append((r, days_since))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if closing:
        has_items = True
        lines.append(f"\n⭐ *Retention Windows Closing ({len(closing)})*")
        for r, days in closing[:3]:
            name = safe_get(r, proj_col, "Client_Name")
            lid  = safe_get(r, proj_col, "Lead_ID")
            lines.append(f" • *{name}* — Day {days} of 7")
            buttons.append([{"text": f"⭐ {name} — Run Retention", "callback_data": f"trigger_retention|{lid}"}])

    if not has_items:
        lines.append("✅ *All clear — no urgent items today.*\nEnjoy your day!")

    markup = {"inline_keyboard": buttons} if buttons else None
    send_msg(CHAT_ID, "\n".join(lines), markup)

# ─────────────────────────────────────────────
# AUTO-COMPLETE RETENTION
# ─────────────────────────────────────────────
def check_retention_completions():
    today = ph_now()
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    for r in proj_rows:
        stage = safe_get(r, proj_col, "Current_Stage")
        if stage != "Delivered":
            continue
        review_sent_date_str = safe_get(r, proj_col, "Review_Sent_Date")
        if review_sent_date_str == "—":
            continue

        lead_id     = safe_get(r, proj_col, "Lead_ID")
        client_name = safe_get(r, proj_col, "Client_Name")

        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    sent_dt    = datetime.strptime(review_sent_date_str.strip(), fmt)
                    days_since = (today.date() - sent_dt.date()).days
                    if days_since >= 7:
                        _auto_complete_project(lead_id, client_name)
                    break
                except ValueError:
                    continue
        except Exception as e:
            print(f"[RETENTION CHECK] Error for {lead_id}: {e}")

def _auto_complete_project(lead_id, client_name):
    today_str = ph_now().strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage": "Completed"
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Closed Won",
        "Last_Action":      "Auto-completed — retention window closed",
        "Next_Action":      "—",
        "Next_Action_Date": today_str
    })

    msg = (
        f"✅ *Project Auto-Completed*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name} | Lead: `{lead_id}`\n"
        f"📅 {today_str}\n\n"
        f"Retention window closed (7 days).\n"
        f"Project → *Completed* | Pipeline → *Closed Won*"
    )
    send_msg(CHAT_ID, msg)
    send_pipeline_msg(CHAT_ID, msg)
    print(f"[AUTO-COMPLETE] {lead_id} — {client_name}")

# ─────────────────────────────────────────────
# CAL.COM API HELPERS
# ─────────────────────────────────────────────
def cancel_cal_booking_for_lead(lead_id):
    if not CAL_API_KEY:
        return False, "no_api_key"
    try:
        r = requests.get(
            "https://api.cal.com/v2/bookings",
            headers={"Authorization": f"Bearer {CAL_API_KEY}", "cal-api-version": "2024-08-13"},
            timeout=10
        )
        r.raise_for_status()
        data     = r.json().get("data", [])
        bookings = data if isinstance(data, list) else data.get("bookings", [])
        target_uid = None
        now_utc    = datetime.now(timezone.utc)
        for b in bookings:
            booking_lead_id = b.get("bookingFieldsResponses", {}).get("lead_id")
            if booking_lead_id != lead_id:
                continue
            if b.get("status") != "accepted":
                continue
            start_str = b.get("start", "")
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                if start_dt <= now_utc:
                    continue
            except Exception:
                pass
            target_uid = b.get("uid")
            break
        if not target_uid:
            return False, "not_found"
        cr = requests.post(
            f"https://api.cal.com/v2/bookings/{target_uid}/cancel",
            headers={
                "Authorization": f"Bearer {CAL_API_KEY}",
                "cal-api-version": "2024-08-13",
                "Content-Type": "application/json"
            },
            json={"cancellationReason": "Rescheduled by host"},
            timeout=10
        )
        cr.raise_for_status()
        return True, target_uid
    except Exception as e:
        print(f"[CAL CANCEL ERROR] {e}")
        return False, str(e)

def fetch_cal_bookings(date_str):
    if not CAL_API_KEY:
        return []
    try:
        r = requests.get(
            "https://api.cal.com/v2/bookings",
            headers={"Authorization": f"Bearer {CAL_API_KEY}", "cal-api-version": "2024-08-13"},
            params={
                "afterStart": f"{date_str}T00:00:00+08:00",
                "beforeEnd":  f"{date_str}T23:59:59+08:00",
                "status":     "upcoming"
            },
            timeout=10
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        return data if isinstance(data, list) else data.get("bookings", [])
    except Exception as e:
        print(f"[CAL ERROR] {e}")
        return []

def parse_cal_booking(booking):
    attendees    = booking.get("attendees", [])
    client_name  = attendees[0].get("name", "Unknown") if attendees else "Unknown"
    client_email = attendees[0].get("email", "—") if attendees else "—"
    responses    = booking.get("bookingFieldsResponses", {})
    metadata     = booking.get("metadata", {})
    lead_id      = responses.get("lead_id") or metadata.get("lead_id") or "—"
    start_raw    = booking.get("start", booking.get("startTime", ""))
    try:
        dt_utc   = datetime.strptime(start_raw[:19], "%Y-%m-%dT%H:%M:%S")
        dt_local = dt_utc + timedelta(hours=8)
        time_str = dt_local.strftime("%I:%M %p")
    except Exception:
        time_str = start_raw
    meeting_url = booking.get("videoCallData", {}).get("url") or booking.get("location", "—") or "—"
    return {
        "id":           booking.get("id"),
        "uid":          booking.get("uid", ""),
        "client_name":  client_name,
        "client_email": client_email,
        "lead_id":      lead_id,
        "time":         time_str,
        "status":       booking.get("status", "accepted"),
        "meeting_url":  meeting_url
    }

# ─────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────
def handle_start_command(chat_id):
    text = (
        "📸 *Welcome to Everly & Co. CRM Dashboard*\n\n"
        "Use /menu for quick navigation buttons\n"
        "Use /help to see all commands and what they do"
    )
    send_msg(chat_id, text)

def handle_help_command(chat_id):
    text = (
        "📖 *COMMAND REFERENCE*\n\n"
        "📋 *Leads & Pipeline*\n"
        "`/leads` — View your latest 10 leads with status\n"
        "`/hot` — Show only HOT priority leads\n"
        "`/pipeline` — See all pipeline stages at a glance\n"
        "`/search <name or email>` — Find a specific lead\n"
        "`/updateemail <Lead_ID> <new_email>` — Update lead email\n\n"
        "📅 *Schedule*\n"
        "`/schedule` — Today's discovery calls from Cal.com\n"
        "`/tomorrow` — Tomorrow's discovery calls\n"
        "`/today` — Today's photography shoots\n\n"
        "👤 *Clients & Projects*\n"
        "`/client <ID>` — Full client card with LTV and booking history\n"
        "`/project` — All active projects and their current stage\n\n"
        "🛠 *Admin*\n"
        "`/resetleadcounter` — Reset lead ID counter to 0 (use before demos)\n"
        "`/retention <Lead_ID>` — Trigger post-delivery review + retention sequence\n"
        "`/setbudget <Lead_ID> <amount>` — Confirm exact amount for \\$10k+/TBD leads before sending contract\n\n"
        "⏰ *Briefing Settings*\n"
        "`/briefing` — Send today's briefing right now\n"
        "`/setbriefingtime HH:MM` — Change daily briefing time (24hr, PH time)\n"
        "  Example: `/setbriefingtime 08:30`\n\n"
        "Use /menu for quick navigation buttons."
    )
    send_msg(chat_id, text)

def handle_menu_command(chat_id):
    text = "*Main Menu*\nWhat would you like to do?"
    markup = {
        "inline_keyboard": [
            [{"text": "📋 Leads",    "callback_data": "nav_leads"},    {"text": "🔥 Hot Leads", "callback_data": "nav_hot"}],
            [{"text": "📊 Pipeline", "callback_data": "nav_pipe"},     {"text": "🗓 Schedule",   "callback_data": "nav_schedule"}],
            [{"text": "📂 Projects", "callback_data": "nav_projects"}, {"text": "⚙️ Admin",     "callback_data": "nav_admin"}]
        ]
    }
    send_msg(chat_id, text, markup)

# FIX: nav_admin handler was missing — the Admin button in the menu did nothing.
def handle_admin_menu(chat_id, msg_id=None, use_pipeline=False):
    text = (
        "⚙️ *Admin Panel*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage system settings and maintenance tasks."
    )
    markup = {
        "inline_keyboard": [
            [{"text": "📋 View Leads",       "callback_data": "nav_leads"},
             {"text": "📊 View Pipeline",    "callback_data": "nav_pipe"}],
            [{"text": "⏰ Send Briefing Now", "callback_data": "admin_briefing"},
             {"text": "🔄 Reset Lead Counter","callback_data": "admin_reset_counter_confirm"}],
            [{"text": "📅 Set Briefing Time", "callback_data": "admin_set_briefing_prompt"}]
        ]
    }
    smart_send(chat_id, text, markup, msg_id, use_pipeline)

def handle_leads_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    lines = ["📋 *Latest Leads*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    for r in rows[-10:]:
        lid    = safe_get(r, col, "Lead_ID")
        name   = safe_get(r, col, "Full_Name")
        status = safe_get(r, col, "Lead_Status")
        emoji  = STATUS_EMOJI.get(status, "")
        lines.append(f"• {emoji} *{name}* (`{lid}`) — {status}")
        buttons.append([{"text": f"👤 {name}", "callback_data": f"view_lead|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_hot_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    lines = ["🔥 *Hot Leads*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    hot_leads = [r for r in rows if safe_get(r, col, "Lead_Status") == "HOT"]
    if not hot_leads:
        lines.append("No hot leads at the moment. Keep hustling! 💪")
    for r in hot_leads:
        lid  = safe_get(r, col, "Lead_ID")
        name = safe_get(r, col, "Full_Name")
        lines.append(f"• 🔴 *{name}* (`{lid}`)")
        buttons.append([{"text": f"👤 {name}", "callback_data": f"view_lead|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_pipeline_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    lines = ["📊 *Pipeline Overview*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    for r in rows:
        lid         = safe_get(r, col, "Lead_ID")
        name        = safe_get(r, col, "Client_Name")
        stage       = safe_get(r, col, "Current_Stage")
        next_action = safe_get(r, col, "Next_Action")
        lines.append(f"• *{name}* (`{lid}`) — {stage} | Next: {next_action}")
        buttons.append([{"text": f"📈 {name} — {stage}", "callback_data": f"view_pipe|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_project_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Projects!A1:Z200")
    lines = ["📂 *Active Projects*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    active_projects = [
        r for r in rows
        if safe_get(r, col, "Current_Stage") not in ("Completed", "Closed", "Closed Won", "Closed Lost")
    ]
    if not active_projects:
        lines.append("No active projects at the moment. Time to close some deals! 🚀")
    for r in active_projects:
        lid   = safe_get(r, col, "Lead_ID")
        name  = safe_get(r, col, "Client_Name")
        stage = safe_get(r, col, "Current_Stage")
        lines.append(f"• *{name}* (`{lid}`) — {stage}")
        buttons.append([{"text": f"🛠 {name} — {stage}", "callback_data": f"view_project|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_schedule_command(chat_id, msg_id=None, use_pipeline=False):
    today_str = ph_now().strftime("%Y-%m-%d")
    bookings  = fetch_cal_bookings(today_str)
    lines     = [f"📅 *Today's Discovery Calls — {ph_now().strftime('%B %d, %Y')}*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons   = []
    if not bookings:
        lines.append("No calls scheduled for today. Time to book some! 📞")
    for b in bookings:
        parsed = parse_cal_booking(b)
        lines.append(f"• {parsed['time']} — *{parsed['client_name']}* (`{parsed['lead_id']}`)")
        if parsed['meeting_url'] != "—":
            buttons.append([{"text": f"📞 {parsed['client_name']} — Join Call", "url": parsed['meeting_url']}])
        buttons.append([{"text": f"✅ Log Call Outcome for {parsed['client_name']}", "callback_data": f"call_menu|{parsed['lead_id']}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_tomorrow_command(chat_id, msg_id=None, use_pipeline=False):
    tomorrow     = ph_now() + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    bookings     = fetch_cal_bookings(tomorrow_str)
    lines        = [f"🗓 *Tomorrow's Discovery Calls — {tomorrow.strftime('%B %d, %Y')}*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons      = []
    if not bookings:
        lines.append("No calls scheduled for tomorrow. Get ready for a productive day! 💪")
    for b in bookings:
        parsed = parse_cal_booking(b)
        lines.append(f"• {parsed['time']} — *{parsed['client_name']}* (`{parsed['lead_id']}`)")
        if parsed['meeting_url'] != "—":
            buttons.append([{"text": f"📞 {parsed['client_name']} — Join Call", "url": parsed['meeting_url']}])
        buttons.append([{"text": f"✅ Log Call Outcome for {parsed['client_name']}", "callback_data": f"call_menu|{parsed['lead_id']}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_today_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    today     = ph_now()
    lines     = [f"📸 *Today's Photography Shoots — {today.strftime('%B %d, %Y')}*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons   = []
    today_shoots = []
    for r in rows:
        event_date_str = safe_get(r, col, "Event_Date")
        if event_date_str == "—":
            continue
        try:
            for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    event_dt = datetime.strptime(event_date_str.strip(), fmt)
                    if event_dt.date() == today.date():
                        today_shoots.append(r)
                    break
                except ValueError:
                    continue
        except Exception:
            pass
    if not today_shoots:
        lines.append("No shoots scheduled for today. Enjoy the break! 🏖️")
    for r in today_shoots:
        lid   = safe_get(r, col, "Lead_ID")
        name  = safe_get(r, col, "Full_Name")
        etype = safe_get(r, col, "Event_Type")
        lines.append(f"• *{name}* — {etype}")
        buttons.append([{"text": f"📸 {name} — View Lead", "callback_data": f"view_lead|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_search_command(chat_id, query, msg_id=None, use_pipeline=False):
    if not query:
        smart_send(chat_id, "Usage: `/search <name or email>`", None, msg_id, use_pipeline)
        return
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    matching_leads = []
    for r in lead_rows:
        name  = safe_get(r, lead_col, "Full_Name").lower()
        email = safe_get(r, lead_col, "Email").lower()
        if query.lower() in name or query.lower() in email:
            matching_leads.append(r)
    lines = [f"🔍 *Search Results for '{query}'*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    if not matching_leads:
        lines.append("No leads found matching your query.")
    for r in matching_leads:
        lid    = safe_get(r, lead_col, "Lead_ID")
        name   = safe_get(r, lead_col, "Full_Name")
        status = safe_get(r, lead_col, "Lead_Status")
        emoji  = STATUS_EMOJI.get(status, "")
        lines.append(f"• {emoji} *{name}* (`{lid}`) — {status}")
        buttons.append([{"text": f"👤 {name}", "callback_data": f"view_lead|{lid}"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, "\n".join(lines), markup, msg_id, use_pipeline)

def handle_client_command(chat_id, client_id, msg_id=None, use_pipeline=False):
    if not client_id:
        smart_send(chat_id, "Usage: `/client <Client_ID>`", None, msg_id, use_pipeline)
        return
    _show_client(chat_id, msg_id, client_id, method="edit", use_pipeline=use_pipeline)

# ─────────────────────────────────────────────
# VIEW HELPERS
# ─────────────────────────────────────────────
def _show_lead(chat_id, msg_id, lead_id, use_pipeline=False):
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    if not lead_row:
        smart_send(chat_id, f"❌ Lead `{lead_id}` not found.", None, msg_id, use_pipeline)
        return

    name               = safe_get(lead_row, lead_col, "Full_Name")
    email              = safe_get(lead_row, lead_col, "Email")
    phone              = safe_get(lead_row, lead_col, "Phone")
    status             = safe_get(lead_row, lead_col, "Lead_Status")
    budget             = safe_get(lead_row, lead_col, "Budget")
    event_type         = safe_get(lead_row, lead_col, "Event_Type")
    event_date         = safe_get(lead_row, lead_col, "Event_Date")
    source             = safe_get(lead_row, lead_col, "Source")
    ai_summary         = safe_get(lead_row, lead_col, "AI_Summary")
    recommended_action = safe_get(lead_row, lead_col, "Recommended_Action")

    text = (
        f"👤 *Lead Details*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Lead: `{lead_id}`\n"
        f"Name: *{name}*\n"
        f"Email: {email}\n"
        f"Phone: {phone}\n"
        f"Status: {STATUS_EMOJI.get(status, '')} *{status}*\n"
        f"Budget: {budget}\n"
        f"Event: {event_type} on {event_date}\n"
        f"Source: {source}\n"
        f"AI Summary: {ai_summary}\n"
        f"Recommended Action: {recommended_action}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    buttons = [
        [{"text": "🔥 Mark HOT",  "callback_data": f"upd_lead|{lead_id}|HOT"},
         {"text": "🟡 Mark WARM", "callback_data": f"upd_lead|{lead_id}|WARM"},
         {"text": "🔵 Mark COLD", "callback_data": f"upd_lead|{lead_id}|COLD"}],
        [{"text": "📊 View Pipeline", "callback_data": f"view_pipe|{lead_id}"}],
        [{"text": "📝 Send Proposal", "callback_data": f"send_proposal|{lead_id}"}]
    ]
    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline)

def _show_pipeline(chat_id, msg_id, lead_id, use_pipeline=False):
    pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    pipe_row = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == lead_id), None)
    if not pipe_row:
        smart_send(chat_id, f"❌ Pipeline entry for `{lead_id}` not found.", None, msg_id, use_pipeline)
        return

    client_name      = safe_get(pipe_row, pipe_col, "Client_Name")
    current_stage    = safe_get(pipe_row, pipe_col, "Current_Stage")
    last_action      = safe_get(pipe_row, pipe_col, "Last_Action")
    next_action      = safe_get(pipe_row, pipe_col, "Next_Action")
    next_action_date = safe_get(pipe_row, pipe_col, "Next_Action_Date")
    call_status      = safe_get(pipe_row, pipe_col, "Call_Status")
    proposal_status  = safe_get(pipe_row, pipe_col, "Proposal_Status")
    proposal_doc_url = safe_get(pipe_row, pipe_col, "Proposal_Doc_Url")
    proposal_sent    = safe_get(pipe_row, pipe_col, "Proposal_Sent_Date")

    if proposal_doc_url != "—":
        proposal_line = f"[View Proposal]({proposal_doc_url}) — {proposal_status} ({proposal_sent})"
    elif proposal_status != "—":
        proposal_line = f"{proposal_status} ({proposal_sent})"
    else:
        proposal_line = "—"

    text = (
        f"📊 *Pipeline Details*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Lead: `{lead_id}`\n"
        f"Client: *{client_name}*\n"
        f"Current Stage: *{current_stage}*\n"
        f"Last Action: {last_action}\n"
        f"Next Action: {next_action} (by {next_action_date})\n"
        f"Call Status: {call_status}\n"
        f"Proposal: {proposal_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    buttons = [
        [{"text": "👤 View Lead", "callback_data": f"view_lead|{lead_id}"}],
    ]
    if current_stage == "Discovery Call Completed":
        buttons.append([{"text": "📝 Send Proposal", "callback_data": f"send_proposal|{lead_id}"}])
    if current_stage in ("Proposal Sent", "Contracted"):
        buttons.append([{"text": "📝 Send Contract", "callback_data": f"send_contract|{lead_id}"}])

    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline)

def _show_project(chat_id, msg_id, lead_id, use_pipeline=False):
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    proj_row = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == lead_id), None)
    if not proj_row:
        smart_send(chat_id, f"❌ Project for `{lead_id}` not found.", None, msg_id, use_pipeline)
        return

    client_name      = safe_get(proj_row, proj_col, "Client_Name")
    project_id       = safe_get(proj_row, proj_col, "Project_ID")
    current_stage    = safe_get(proj_row, proj_col, "Current_Stage")
    package          = safe_get(proj_row, proj_col, "Package")
    total_value      = safe_get(proj_row, proj_col, "Total_Price")
    deposit_paid     = safe_get(proj_row, proj_col, "Deposit_Paid")
    balance          = safe_get(proj_row, proj_col, "Balance")
    balance_due_date = safe_get(proj_row, proj_col, "Balance_Due_Date")
    balance_paid     = safe_get(proj_row, proj_col, "Balance_Paid")
    gallery_link     = safe_get(proj_row, proj_col, "Gallery_Folder_URL")
    delivery_date    = safe_get(proj_row, proj_col, "Delivery_Date")
    review_sent      = safe_get(proj_row, proj_col, "Review")
    upsell_sent      = safe_get(proj_row, proj_col, "Upsell_Sent")

    deposit_label   = "✅ Yes" if deposit_paid.upper() == "TRUE"  else "❌ No"
    balance_label   = "✅ Yes" if balance_paid.upper() == "TRUE"  else "❌ No"
    review_label    = "✅ Yes" if review_sent.upper()  == "TRUE"  else "❌ No"
    upsell_label    = "✅ Yes" if upsell_sent.upper()  == "TRUE"  else "❌ No"
    gallery_display = f"[View Gallery]({gallery_link})" if gallery_link != "—" else "—"

    text = (
        f"📂 *Project Details*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"Client: *{client_name}*\n"
        f"Current Stage: *{current_stage}*\n"
        f"Package: {package}\n"
        f"Total Value: ${total_value}\n"
        f"Deposit Paid: {deposit_label}\n"
        f"Balance: ${balance} (Due: {balance_due_date})\n"
        f"Balance Paid: {balance_label}\n"
        f"Gallery: {gallery_display}\n"
        f"Delivery Date: {delivery_date}\n"
        f"Review Sent: {review_label}\n"
        f"Upsell Sent: {upsell_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    buttons = [
        [{"text": "👤 View Lead",     "callback_data": f"view_lead|{lead_id}"}],
        [{"text": "📊 View Pipeline", "callback_data": f"view_pipe|{lead_id}"}],
    ]

    # FIX: Show "Mark Balance Paid" on the project card for Delivered projects
    # where balance has not yet been marked. Previously only the gallery delivery
    # Telegram card had this button. Now the operator can do it from here too.
    if current_stage == "Delivered" and balance_paid.upper() != "TRUE":
        buttons.append([{"text": "💳 Mark Balance Paid", "callback_data": f"balance_paid|{lead_id}"}])

    if current_stage == "Delivered" and review_sent.upper() != "TRUE":
        buttons.append([{"text": "⭐ Run Retention", "callback_data": f"trigger_retention_confirm|{lead_id}"}])

    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline)

def _show_client(chat_id, msg_id, client_id, method="edit", use_pipeline=False):
    client_rows, client_col = read_sheet_with_headers("Clients!A1:H200")
    client_row = next(
        (r for r in client_rows if safe_get(r, client_col, "Client_ID") == client_id),
        None
    )
    if not client_row:
        smart_send(chat_id, f"❌ Client `{client_id}` not found.", None, msg_id, use_pipeline)
        return

    name       = safe_get(client_row, client_col, "Name")
    email      = safe_get(client_row, client_col, "Email")
    phone      = safe_get(client_row, client_col, "Phone")
    created_at = safe_get(client_row, client_col, "Created_At")
    ltv        = safe_get(client_row, client_col, "LTV")
    bookings   = safe_get(client_row, client_col, "Bookings")
    tier       = safe_get(client_row, client_col, "Client_Tier")
    tier_emoji = {"VIP": "⭐", "Premium": "💎", "Standard": "🔹"}.get(tier, "👤")

    text = (
        f"👤 *Client Card*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Client ID: `{client_id}`\n"
        f"Name: *{name}*\n"
        f"Email: {email}\n"
        f"Phone: {phone}\n"
        f"Member Since: {created_at}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{tier_emoji} Tier: *{tier}*\n"
        f"💰 Lifetime Value: *${ltv}*\n"
        f"📅 Total Bookings: *{bookings}*\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    buttons = []
    for r in proj_rows:
        if safe_get(r, proj_col, "Client_ID") == client_id:
            lid   = safe_get(r, proj_col, "Lead_ID")
            stage = safe_get(r, proj_col, "Current_Stage")
            pid   = safe_get(r, proj_col, "Project_ID")
            if stage not in ("Completed", "Closed", "Closed Lost"):
                buttons.append([{"text": f"📂 {pid} — {stage}", "callback_data": f"view_project|{lid}"}])

    smart_send(chat_id, text, {"inline_keyboard": buttons} if buttons else None, msg_id, use_pipeline)

def _show_call_menu(chat_id, msg_id, lead_id, use_pipeline_edit=False):
    text = f"✅ *Log Call Outcome for `{lead_id}`*\n━━━━━━━━━━━━━━━━━━━━\nWhat was the result of the discovery call?"
    buttons = [
        [{"text": OUTCOME_LABELS["completed_continue"],  "callback_data": f"confirm_call|{lead_id}|completed_continue"}],
        [{"text": OUTCOME_LABELS["completed_stop"],      "callback_data": f"confirm_call|{lead_id}|completed_stop"}],
        [{"text": OUTCOME_LABELS["no_show"],             "callback_data": f"confirm_call|{lead_id}|no_show"}],
        [{"text": OUTCOME_LABELS["reschedule"],          "callback_data": f"confirm_call|{lead_id}|reschedule"}],
        [{"text": OUTCOME_LABELS["reschedule_oncall"],   "callback_data": f"confirm_call|{lead_id}|reschedule_oncall"}],
        [{"text": OUTCOME_LABELS["booked_for_client"],   "callback_data": f"confirm_call|{lead_id}|booked_for_client"}],
        [{"text": "❌ Cancel", "callback_data": f"view_lead|{lead_id}"}]
    ]
    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline_edit)

def _confirm_call_out(chat_id, msg_id, lead_id, outcome_key, use_pipeline_edit=False):
    outcome_label = OUTCOME_LABELS.get(outcome_key, "Unknown Outcome")
    text = (
        f"⚠️ *Confirm Call Outcome*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Lead: `{lead_id}`\n"
        f"Outcome: *{outcome_label}*\n\n"
        f"Are you sure you want to apply this outcome?"
    )
    buttons = [
        [{"text": "✅ Yes — Confirm", "callback_data": f"call_out|{lead_id}|{outcome_key}"}],
        [{"text": "❌ No — Go Back",  "callback_data": f"call_menu|{lead_id}"}]
    ]
    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline_edit)

def _execute_call_out(chat_id, msg_id, lead_id, outcome_key, cb_id, use_pipeline=False):
    outcome_data = OUTCOME_MAP.get(outcome_key)
    if not outcome_data:
        answer_callback(cb_id, "Invalid outcome key.", use_pipeline)
        smart_send(chat_id, "❌ Error: Invalid call outcome.", None, msg_id, use_pipeline)
        return

    current_stage = outcome_data["current_stage"]
    call_status   = outcome_data["call_status"]
    next_action   = outcome_data["next_action"]
    today_str     = ph_now().strftime("%Y-%m-%d")

    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    current_stage,
        "Call_Status":      call_status,
        "Last_Action":      f"Call Outcome: {call_status}",
        "Next_Action":      next_action,
        "Next_Action_Date": today_str
    })

    # Cancel the Cal.com booking if the lead is rescheduling.
    if outcome_key in ("reschedule", "reschedule_oncall"):
        success, cal_response = cancel_cal_booking_for_lead(lead_id)
        if success:
            answer_callback(cb_id, "✅ Call outcome logged. Cal.com booking cancelled.", use_pipeline)
        else:
            answer_callback(cb_id, f"✅ Call outcome logged. Cal cancel failed: {cal_response}", use_pipeline)
    else:
        answer_callback(cb_id, "✅ Call outcome logged.", use_pipeline)

    # FIX: Fire System 1.5B to send the appropriate follow-up email to the client.
    # Previously this webhook was NEVER fired, so clients never received post-call
    # emails (no-show recovery, reschedule link, or post-call follow-up).
    # CALL_OUTCOME_WEBHOOK must be set to the System 1.5B Zapier Catch Hook URL.
    if outcome_key in OUTCOMES_REQUIRING_EMAIL:
        fired_email = fire_webhook(CALL_OUTCOME_WEBHOOK, {
            "lead_id": lead_id,
            "action":  outcome_key
        })
        if not fired_email:
            print(f"[CALL OUTCOME] 1.5B email webhook failed for {lead_id} / {outcome_key}")

    # Close the lead in CRM if this is a permanent stop.
    if outcome_key == "completed_stop":
        fire_webhook(CLOSE_LEAD_WEBHOOK, {"lead_id": lead_id, "outcome": outcome_key})

    if outcome_key == "completed_continue":
        _show_pipeline(chat_id, msg_id, lead_id, use_pipeline=use_pipeline)
    else:
        smart_send(chat_id, f"✅ Call outcome for `{lead_id}` set to *{call_status}*.", None, msg_id, use_pipeline)

def _confirm_contract(chat_id, msg_id, lead_id, use_pipeline=False):
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row    = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    client_name = safe_get(lead_row, lead_col, "Full_Name")       if lead_row else lead_id
    package     = safe_get(lead_row, lead_col, "Primary_Package") if lead_row else "—"

    text = (
        f"📝 *Confirm Send Contract*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Lead: `{lead_id}` — {client_name}\n"
        f"Package: {package}\n\n"
        f"Are you sure you want to send the contract?"
    )
    buttons = [
        [{"text": "✅ Yes — Send Contract", "callback_data": f"contract_yes|{lead_id}"}],
        [{"text": "❌ No — Close Lead",     "callback_data": f"contract_no|{lead_id}"}]
    ]
    smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline)

# ─────────────────────────────────────────────
# CLIENT STATS
# ─────────────────────────────────────────────
def _update_client_stats(lead_id):
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T500")
    current_lead = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    if not current_lead:
        return {"ltv": 0, "tier": "Standard", "bookings": 0}

    client_email    = safe_get(current_lead, lead_col, "Email")
    client_lead_ids = set()

    if client_email != "—":
        for row in lead_rows:
            email_val   = safe_get(row, lead_col, "Email")
            row_lead_id = safe_get(row, lead_col, "Lead_ID")
            if row_lead_id == "—":
                continue
            if email_val != "—" and email_val.lower() == client_email.lower():
                client_lead_ids.add(row_lead_id)
    else:
        client_lead_ids.add(lead_id)

    client_lead_ids.add(lead_id)

    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    total_ltv      = 0
    total_bookings = 0

    for row in proj_rows:
        row_lead_id = safe_get(row, proj_col, "Lead_ID")
        if row_lead_id not in client_lead_ids:
            continue
        stage = safe_get(row, proj_col, "Current_Stage")
        if stage in ("Closed Lost",):
            continue
        total_bookings += 1
        total_val_raw = safe_get(row, proj_col, "Total_Price")
        total_val_str = total_val_raw.replace("$", "").replace(",", "").strip()
        try:
            if total_val_str and (total_val_str[0].isdigit() or
                    (total_val_str.startswith("-") and len(total_val_str) > 1)):
                total_ltv += float(total_val_str)
        except ValueError:
            pass

    tier = "Standard"
    if total_ltv >= 5000:
        tier = "VIP"
    elif total_ltv >= 2500:
        tier = "Premium"

    return {"ltv": int(total_ltv), "tier": tier, "bookings": total_bookings}

# ─────────────────────────────────────────────
# EXECUTE RETENTION
# ─────────────────────────────────────────────
def _execute_retention(lead_id, processing_msg_id=None):
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    if not lead_row:
        print(f"[RETENTION] Lead {lead_id} not found.")
        return {"fired": False}

    pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    pipe_row = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == lead_id), None)

    project_id   = safe_get(pipe_row, pipe_col, "Project_ID") if pipe_row else "—"
    client_name  = safe_get(lead_row, lead_col, "Full_Name")
    client_email = safe_get(lead_row, lead_col, "Email")
    event_type   = safe_get(lead_row, lead_col, "Event_Type")

    stats    = _update_client_stats(lead_id)
    new_ltv  = stats["ltv"]
    new_tier = stats["tier"]
    bookings = stats["bookings"]

    # Update Clients sheet
    try:
        client_rows, client_col = read_sheet_with_headers("Clients!A1:H200")
        target_client = None

        if client_email != "—":
            target_client = next(
                (r for r in client_rows
                 if safe_get(r, client_col, "Email").lower() == client_email.lower()),
                None
            )
        if not target_client:
            client_id_on_lead = safe_get(lead_row, lead_col, "Client_ID")
            if client_id_on_lead != "—":
                target_client = next(
                    (r for r in client_rows
                     if safe_get(r, client_col, "Client_ID") == client_id_on_lead),
                    None
                )
        if target_client:
            cid = safe_get(target_client, client_col, "Client_ID")
            _write_back("Clients", "Clients!A1:H200", "Client_ID", cid, {
                "LTV":         str(new_ltv),
                "Bookings":    str(bookings),
                "Client_Tier": new_tier
            })
            print(f"[RETENTION] Clients sheet updated — {cid} | LTV ${new_ltv} | {new_tier}")
        else:
            print(f"[RETENTION] Client record not found for lead {lead_id}")
    except Exception as e:
        print(f"[RETENTION] Error writing client stats: {e}")

    today_str = ph_now().strftime("%Y-%m-%d")

    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Retention",
        "Last_Action":      "Review Request Sent",
        "Next_Action":      "Await Review — Rebooking in 7 days",
        "Next_Action_Date": today_str
    })

    fired = fire_webhook(RETENTION_WEBHOOK, {
        "lead_id":      lead_id,
        "message_id":   str(processing_msg_id) if processing_msg_id else None,
        "project_id":   project_id,
        "client_name":  client_name,
        "client_email": client_email,
        "event_type":   event_type,
        "new_ltv":      new_ltv,
        "new_tier":     new_tier,
        "bookings":     bookings
    })

    return {
        "fired":        fired,
        "client_name":  client_name,
        "client_email": client_email,
        "project_id":   project_id,
        "new_ltv":      new_ltv,
        "new_tier":     new_tier,
        "bookings":     bookings,
        "message_id":   processing_msg_id
    }

def _execute_deliver_gallery(chat_id, msg_id, lead_id, cb_id, use_pipeline=False):
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == lead_id), None)
    client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
    project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
    today_str   = ph_now().strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage":  "Post-Production",
        "Shoot_Complete": "TRUE"
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Active Project",
        "Last_Action":      "Shoot Completed — Gallery in Progress",
        "Next_Action":      "Deliver Gallery",
        "Next_Action_Date": today_str
    })

    fired = fire_webhook(DELIVER_GALLERY_WEBHOOK, {
        "lead_id":     lead_id,
        "project_id":  project_id,
        "client_name": client_name
    })

    answer_callback(cb_id, "📸 Gallery delivery triggered!", use_pipeline)
    text = (
        f"📸 *Gallery Delivery Triggered*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 {today_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'✅ Webhook fired — gallery delivery sequence started.' if fired else '⚠️ Webhook failed — check DELIVER_GALLERY_WEBHOOK in Railway.'}"
    )
    smart_send(chat_id, text, None, msg_id, use_pipeline)

# ─────────────────────────────────────────────
# SHARED CALLBACK HANDLER
# ─────────────────────────────────────────────
def handle_callbacks(data, use_pipeline=False):
    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    cb        = data["callback_query"]
    chat_id   = cb["message"]["chat"]["id"]
    msg_id    = cb["message"]["message_id"]
    cb_data   = cb.get("data", "")
    parts     = cb_data.split("|")
    action    = parts[0]
    target_id = parts[1] if len(parts) > 1 else None

    if action == "none":
        answer_callback(cb["id"], "No action", use_pipeline)
        return jsonify({"status": "ok"})

    elif action == "view_lead":
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "view_pipe":
        _show_pipeline(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "view_project":
        _show_project(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "view_client":
        _show_client(chat_id, msg_id, target_id, method="edit", use_pipeline=use_pipeline)

    elif action == "nav_leads":
        handle_leads_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "nav_hot":
        handle_hot_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "nav_pipe":
        handle_pipeline_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "nav_projects":
        handle_project_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "nav_schedule":
        if target_id == "tomorrow":
            handle_tomorrow_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)
        else:
            handle_schedule_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "nav_today":
        handle_today_command(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    # FIX: nav_admin was completely missing — clicking the Admin button in the
    # main menu did nothing. Now it opens the Admin Panel.
    elif action == "nav_admin":
        handle_admin_menu(chat_id, msg_id=msg_id, use_pipeline=use_pipeline)

    # Admin panel sub-actions
    elif action == "admin_briefing":
        answer_callback(cb["id"], "Sending briefing...", use_pipeline)
        send_daily_briefing()

    elif action == "admin_reset_counter_confirm":
        text = (
            "⚠️ *Reset Lead Counter*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "This will reset the Lead ID counter to 0.\n"
            "The next lead created will be `LED-0001`.\n\n"
            "⚠️ *Only do this before demos or testing.*\n"
            "Are you sure?"
        )
        buttons = [
            [{"text": "✅ Yes — Reset Counter", "callback_data": "admin_reset_counter_execute"}],
            [{"text": "❌ Cancel",               "callback_data": "nav_admin"}]
        ]
        smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id, use_pipeline)

    elif action == "admin_reset_counter_execute":
        write_sheet("Config!A2", [[0]])
        write_sheet("Config!B2", [["0001"]])
        answer_callback(cb["id"], "✅ Counter reset!", use_pipeline)
        smart_send(chat_id, "✅ Lead counter reset to 0. Next lead will be LED-0001.", None, msg_id, use_pipeline)

    elif action == "admin_set_briefing_prompt":
        text = (
            "⏰ *Set Daily Briefing Time*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Type the command below with your desired time:\n\n"
            "`/setbriefingtime HH:MM`\n\n"
            "Example: `/setbriefingtime 08:30`\n"
            "Uses 24-hour PH time format."
        )
        smart_send(chat_id, text, None, msg_id, use_pipeline)

    elif action == "upd_lead" and len(parts) > 2:
        new_status = parts[2]
        _write_back("Leads", "Leads!A1:T200", "Lead_ID", target_id, {"Lead_Status": new_status})
        answer_callback(cb["id"], f"✅ Status updated to {new_status}", use_pipeline)
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "upd_pipe" and len(parts) > 2:
        new_stage = parts[2]
        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    new_stage,
            "Last_Action":      f"Stage moved to {new_stage}",
            "Next_Action_Date": today_str
        })
        answer_callback(cb["id"], f"✅ Stage → {new_stage}", use_pipeline)
        _show_pipeline(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "upd_proj" and len(parts) > 2:
        new_stage = parts[2]
        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {"Current_Stage": new_stage})
        answer_callback(cb["id"], f"✅ Project stage → {new_stage}", use_pipeline)
        _show_project(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "call_menu":
        _show_call_menu(chat_id, msg_id, target_id, use_pipeline_edit=use_pipeline)

    elif action == "confirm_call" and len(parts) > 2:
        _confirm_call_out(chat_id, msg_id, target_id, parts[2], use_pipeline_edit=use_pipeline)

    elif action == "call_out" and len(parts) > 2:
        _execute_call_out(chat_id, msg_id, target_id, parts[2], cb["id"], use_pipeline=use_pipeline)

    elif action == "send_proposal":
        fired    = fire_webhook(PROPOSAL_ZAPIER_WEBHOOK, {"lead_id": target_id})
        answer_callback(cb["id"], "📄 Proposal triggered", use_pipeline)
        msg_text = (
            f"📄 Proposal triggered for `{target_id}`\nCheck your email and Telegram for confirmation."
            if fired else
            f"⚠️ Webhook failed — check PROPOSAL_ZAPIER_WEBHOOK in Railway."
        )
        smart_send(chat_id, msg_text, None, msg_id, use_pipeline)

    elif action in ("send_contract", "confirm_contract"):
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "contract_yes":
        # Gate: Budget must be confirmed before System 3A fires.
        # If budget is uncertain, prompt operator to use /setbudget first.
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        budget   = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""
        name     = safe_get(lead_row, lead_col, "Full_Name")      if lead_row else target_id

        if budget in UNCERTAIN_BUDGETS:
            answer_callback(cb["id"], "Budget unclear — enter exact amount first", use_pipeline)
            prompt_text = (
                f"💰 *Budget Confirmation Required*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Lead: `{target_id}` — *{name}*\n"
                f"Budget on file: *{budget}*\n\n"
                f"This budget is unclear and cannot be used for the contract.\n"
                f"Please type the confirmed total amount:\n\n"
                f"`/setbudget {target_id} <amount>`\n\n"
                f"*Example:* `/setbudget {target_id} 13000`"
            )
            smart_send(chat_id, prompt_text, None, msg_id, use_pipeline)
        else:
            fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {"lead_id": target_id})
            answer_callback(cb["id"], "✅ Contract triggered", use_pipeline)
            confirmed_text = (
                f"✅ *Contract Triggered — System 3A*\n"
                f"Lead: `{target_id}`\n\n"
                f"• Contract sent for e-signature\n"
                f"• Watch Telegram for confirmation"
            ) if fired else (
                f"⚠️ *Webhook failed for `{target_id}`*\n"
                f"Check CONTRACT_ZAPIER_WEBHOOK in Railway."
            )
            smart_send(chat_id, confirmed_text, None, msg_id, use_pipeline)

    elif action == "contract_no":
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    "Closed Lost",
            "Last_Action":      "Contract declined",
            "Next_Action":      "Archive Lead",
            "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
        })
        answer_callback(cb["id"], "❌ Contract declined. Lead closed.", use_pipeline)
        smart_send(chat_id, f"❌ Lead `{target_id}` closed due to contract decline.", None, msg_id, use_pipeline)

    elif action == "budget_contract_yes" and len(parts) > 2:
        lead_id      = target_id
        total_amount = parts[2]

        try:
            total = int(float(total_amount))
        except (ValueError, TypeError):
            answer_callback(cb["id"], "❌ Invalid amount — could not parse.", use_pipeline)
            return jsonify({"status": "ok"})

        deposit = round(total * 0.30)
        balance = total - deposit

        _write_back("Leads", "Leads!A1:T200", "Lead_ID", lead_id, {"Budget": str(total)})
        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
            "Total_Price": str(total),
            "Deposit":     str(deposit),
            "Balance":     str(balance)
        })

        fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {
            "lead_id":     lead_id,
            "total_price": str(total),
            "deposit":     str(deposit),
            "balance":     str(balance)
        })

        answer_callback(cb["id"], "✅ Contract triggered with confirmed budget", use_pipeline)
        confirmed_text = (
            f"✅ *Contract Triggered — System 3A*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Lead: `{lead_id}` | Budget: *${total:,}*\n"
            f"💳 Deposit (30%): *${deposit:,}*\n"
            f"📊 Balance (70%): *${balance:,}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• Contract sent for e-signature\n"
            f"• Watch Telegram for confirmation"
        ) if fired else (
            f"⚠️ *Webhook failed for `{lead_id}`*\n"
            f"Check CONTRACT_ZAPIER_WEBHOOK in Railway."
        )
        smart_send(chat_id, confirmed_text, None, msg_id, use_pipeline)

    elif action == "budget_contract_edit":
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        name     = safe_get(lead_row, lead_col, "Full_Name") if lead_row else target_id
        budget   = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""
        prompt_text = (
            f"💰 *Budget Confirmation Required*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Lead: `{target_id}` — *{name}*\n"
            f"Budget on file: *{budget}*\n\n"
            f"Type the confirmed total:\n\n"
            f"`/setbudget {target_id} <amount>`\n\n"
            f"*Example:* `/setbudget {target_id} 13000`"
        )
        smart_send(chat_id, prompt_text, None, msg_id, use_pipeline)

    elif action == "deposit_paid_confirm":
        # FIX: This used to EXECUTE the deposit action directly without confirmation.
        # Now it shows a proper confirmation dialog first.
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        deposit     = safe_get(proj_row, proj_col, "Deposit")      if proj_row else "—"
        deposit_paid_flag = safe_get(proj_row, proj_col, "Deposit_Paid") if proj_row else "—"

        # Idempotency guard: don't allow double-marking.
        if deposit_paid_flag.upper() == "TRUE":
            answer_callback(cb["id"], "Already marked as paid!", use_pipeline)
            smart_send(chat_id,
                f"ℹ️ Deposit for `{target_id}` is already marked as paid.",
                None, msg_id, use_pipeline
            )
            return jsonify({"status": "ok"})

        answer_callback(cb["id"], "Confirm deposit payment 👇", use_pipeline)
        text = (
            f"⚠️ *Confirm Deposit Payment*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{client_name}*\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"💰 Deposit Amount: *${deposit}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Have you confirmed this payment in Zoho?\n\n"
            f"Pressing *Yes* will:\n"
            f"  • Mark the deposit as paid in Google Sheets\n"
            f"  • Move the project to Pre-Production stage\n"
            f"  • Unlock the pre-production workflow"
        )
        buttons = [
            [{"text": "✅ Yes — Mark Deposit Paid", "callback_data": f"deposit_paid_execute|{target_id}|{msg_id}"}],
            [{"text": "❌ Cancel",                  "callback_data": f"view_project|{target_id}"}]
        ]
        send_pipeline_msg(CHAT_ID, text, {"inline_keyboard": buttons})

    elif action == "deposit_paid_execute":
        today_str      = ph_now().strftime("%Y-%m-%d")
        invoice_msg_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        deposit     = safe_get(proj_row, proj_col, "Deposit")      if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")      if proj_row else "—"

        # Idempotency guard.
        deposit_paid_flag = safe_get(proj_row, proj_col, "Deposit_Paid") if proj_row else "—"
        if deposit_paid_flag.upper() == "TRUE":
            answer_callback(cb["id"], "Already marked as paid!", use_pipeline)
            edit_pipeline_msg(chat_id, msg_id, (
                f"⚠️ *Confirm Deposit Payment*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 {client_name} | Lead: `{target_id}`\n\n"
                f"ℹ️ *Already confirmed — deposit was already marked as paid.*"
            ))
            return jsonify({"status": "ok"})

        # FIX: Projects stage after deposit was "Active Project" which is not a valid
        # Projects stage. Correct stage is "Pre-Production" (project confirmed, work begins).
        # Pipeline Tracker correctly uses "Active Project" as its own stage schema.
        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {
            "Deposit_Paid":  "TRUE",
            "Current_Stage": "Pre-Production"     # was incorrectly "Active Project"
        })
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    "Active Project",
            "Last_Action":      "Deposit Payment Confirmed",
            "Next_Action":      "Begin Pre-Production Workflow",
            "Next_Action_Date": today_str
        })

        answer_callback(cb["id"], "💰 Deposit marked as paid!", use_pipeline)

        # Lock the original invoice message — remove the button.
        if invoice_msg_id:
            edit_pipeline_msg(chat_id, invoice_msg_id, (
                f"🧾 *Deposit Invoice Sent*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 {client_name}\n"
                f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ *Deposit confirmed paid — {today_str}*"
            ))

        # Lock the confirmation dialog.
        edit_pipeline_msg(chat_id, msg_id, (
            f"⚠️ *Confirm Deposit Payment*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n\n"
            f"✅ *Confirmed — deposit marked as paid.*"
        ))

        # Send fresh success card with System 4 trigger button.
        send_pipeline_msg(CHAT_ID, (
            f"💰 *Deposit Payment Confirmed — System 3C*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{client_name}*\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"💵 Deposit Paid: *${deposit}*\n"
            f"📊 Balance Remaining: *${balance}*\n"
            f"📅 Confirmed: {today_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Deposit confirmed paid.\n"
            f"Project moved to *Pre-Production* stage.\n"
            f"Pre-production workflow is now unlocked.\n\n"
            f"When the shoot is done, tap below to trigger gallery delivery."
        ), {"inline_keyboard": [[
            {"text": "🎬 Trigger System 4 — Gallery Delivery", "callback_data": f"trigger_system4_confirm|{target_id}"}
        ]]})

    elif action == "deliver_gallery_confirm":
        _execute_deliver_gallery(chat_id, msg_id, target_id, cb["id"], use_pipeline=use_pipeline)

    # ── System 4: Step 1 of 2 — Confirmation prompt ──────────────────────────
    # The operator taps "Trigger System 4" on the System 3C card.
    # We show a clear confirmation dialog before doing anything irreversible.
    elif action == "trigger_system4_confirm":
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        event_date  = safe_get(proj_row, proj_col, "Event_Date")  if proj_row else "—"

        answer_callback(cb["id"], "Confirm gallery delivery below 👇", use_pipeline)
        text = (
            f"🎬 *Confirm — Trigger System 4 Gallery Delivery*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{client_name}*\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"📅 Event Date: {event_date}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"This will:\n"
            f"  • Mark shoot as complete in Google Sheets\n"
            f"  • Move project to Post-Production stage\n"
            f"  • Send the gallery delivery email to the client\n"
            f"  • Create the balance payment invoice in Zoho\n\n"
            f"⚠️ Only tap *Yes* after the shoot is fully done.\n"
            f"This action cannot be undone."
        )
        buttons = [
            [{"text": "✅ Yes — Trigger Gallery Delivery", "callback_data": f"trigger_system4_execute|{target_id}|{msg_id}"}],
            [{"text": "❌ Cancel — Not Ready Yet",          "callback_data": f"none"}]
        ]
        send_pipeline_msg(CHAT_ID, text, {"inline_keyboard": buttons})

    # ── System 4: Step 2 of 2 — Execute ──────────────────────────────────────
    # Operator confirmed. Fire the webhook, remove buttons from BOTH the
    # System 3C card and the confirmation dialog so neither can be re-pressed.
    elif action == "trigger_system4_execute":
        system3c_msg_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        today_str   = ph_now().strftime("%Y-%m-%d")

        # Write Sheets first before firing webhook.
        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {
            "Current_Stage":  "Post-Production",
            "Shoot_Complete": "TRUE"
        })
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    "Active Project",
            "Last_Action":      "Shoot Completed — Gallery in Progress",
            "Next_Action":      "Deliver Gallery",
            "Next_Action_Date": today_str
        })

        fired = fire_webhook(DELIVER_GALLERY_WEBHOOK, {
            "lead_id":     target_id,
            "project_id":  project_id,
            "client_name": client_name
        })

        answer_callback(cb["id"], "🎬 System 4 triggered!", use_pipeline)

        # Lock the System 3C card — remove the trigger button so it cannot be pressed again.
        if system3c_msg_id:
            edit_pipeline_msg(chat_id, system3c_msg_id, (
                f"💰 *Deposit Payment Confirmed — System 3C*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 *{client_name}*\n"
                f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ Deposit confirmed paid.\n"
                f"🎬 Gallery delivery triggered — {today_str}"
            ))

        # Lock the confirmation dialog — strip its buttons.
        edit_pipeline_msg(chat_id, msg_id, (
            f"🎬 *Confirm — Trigger System 4 Gallery Delivery*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n\n"
            f"✅ *Confirmed — gallery delivery sequence triggered.*"
        ))

        # Send result card.
        send_pipeline_msg(CHAT_ID, (
            f"🎬 *System 4 — Gallery Delivery Triggered*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{client_name}*\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"📅 Triggered: {today_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{'✅ Webhook fired — gallery delivery email is on its way to the client.' if fired else '⚠️ Webhook failed — check DELIVER_GALLERY_WEBHOOK in Railway.'}\n\n"
            f"📊 Project → *Post-Production*\n"
            f"A balance payment invoice will be created in Zoho automatically."
        ))

    elif action == "balance_paid":
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name")     if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")          if proj_row else "—"
        due_date    = safe_get(proj_row, proj_col, "Balance_Due_Date") if proj_row else "—"
        balance_paid_flag = safe_get(proj_row, proj_col, "Balance_Paid") if proj_row else "—"

        # Idempotency guard.
        if balance_paid_flag.upper() == "TRUE":
            answer_callback(cb["id"], "Already marked as paid!", use_pipeline)
            smart_send(chat_id,
                f"ℹ️ Balance for `{target_id}` is already marked as paid.",
                None, msg_id, use_pipeline
            )
            return jsonify({"status": "ok"})

        answer_callback(cb["id"], "Confirm balance payment below 👇", use_pipeline)
        text = (
            f"💳 *Confirm Balance Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n\n"
            f"💰 Amount: *${balance}*\n"
            f"📅 Due: {due_date}\n\n"
            f"⚠️ Confirm you have received the full balance payment in Zoho?"
        )
        buttons = [[
            {"text": "✅ Yes — Mark Paid", "callback_data": f"balance_paid_confirm|{target_id}|{msg_id}"},
            {"text": "❌ Cancel",           "callback_data": f"view_project|{target_id}"}
        ]]
        send_pipeline_msg(CHAT_ID, text, {"inline_keyboard": buttons})

    elif action == "balance_paid_confirm":
        today_str      = ph_now().strftime("%Y-%m-%d")
        gallery_msg_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None

        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name")     if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")      if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")          if proj_row else "—"
        due_date    = safe_get(proj_row, proj_col, "Balance_Due_Date") if proj_row else "—"
        balance_paid_flag = safe_get(proj_row, proj_col, "Balance_Paid") if proj_row else "—"

        # Idempotency guard.
        if balance_paid_flag.upper() == "TRUE":
            answer_callback(cb["id"], "Already marked as paid!", use_pipeline)
            edit_pipeline_msg(chat_id, msg_id, (
                f"💳 *Confirm Balance Paid*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 {client_name} | Lead: `{target_id}`\n\n"
                f"ℹ️ *Already confirmed — balance was already marked as paid.*"
            ))
            return jsonify({"status": "ok"})

        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {
            "Balance_Paid": "TRUE"
        })
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Last_Action":      "Balance Received",
            "Next_Action":      "Run Retention Sequence",
            "Next_Action_Date": today_str
        })
        answer_callback(cb["id"], "💰 Balance marked as paid!", use_pipeline)

        if gallery_msg_id:
            edit_pipeline_msg(chat_id, gallery_msg_id, (
                f"📸 *System 4 — Gallery Delivered*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 {client_name}\n"
                f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ Gallery delivered. Balance payment confirmed."
            ))

        edit_pipeline_msg(chat_id, msg_id, (
            f"💳 *Confirm Balance Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n\n"
            f"💰 Amount: *${balance}*\n"
            f"📅 Due: {due_date}\n\n"
            f"✅ *Confirmed — balance marked as paid.*"
        ))

        send_pipeline_msg(CHAT_ID, (
            f"✅ *System 4 — Balance Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name}\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"💵 Amount: ${balance}\n"
            f"📅 Received: {today_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Balance marked as paid.\n"
            f"Ready to send the retention sequence to the client?"
        ), {"inline_keyboard": [[{"text": "⭐ Run Retention", "callback_data": f"trigger_retention_confirm|{target_id}"}]]})

    elif action == "trigger_retention_confirm":
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row     = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        client_email = safe_get(lead_row, lead_col, "Email") if lead_row else "—"
        text = (
            f"⭐ *Confirm Retention Sequence*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n"
            f"✉️ Sending to: {client_email}\n\n"
            f"This will:\n"
            f"• Send a review request email to the client\n"
            f"• Schedule a rebooking email for +7 days\n"
            f"• Update client LTV and tier in the Clients sheet\n\n"
            f"⚠️ Confirm?"
        )
        buttons = [[
            {"text": "✅ Yes — Send It", "callback_data": f"trigger_retention|{target_id}"},
            {"text": "❌ Cancel",         "callback_data": f"view_project|{target_id}"}
        ]]
        smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "trigger_retention":
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row     = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        client_email = safe_get(lead_row, lead_col, "Email") if lead_row else "—"

        stripped_text = (
            f"⭐ *Confirm Retention Sequence*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n"
            f"✉️ Sending to: {client_email}\n\n"
            f"✅ *Confirmed — retention sequence initiated.*"
        )
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, stripped_text)
        else:
            edit_msg(chat_id, msg_id, stripped_text)

        answer_callback(cb["id"], "⭐ Retention sequence initiated.", use_pipeline)

        result = _execute_retention(target_id, processing_msg_id=None)
        if not result["fired"]:
            send_pipeline_msg(CHAT_ID, "⚠️ Webhook failed — check RETENTION_WEBHOOK in Railway.")

    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# MAIN WEBHOOK — DASHBOARD BOT
# ─────────────────────────────────────────────
@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json

    if "message" in data:
        msg     = data["message"]
        text    = msg.get("text", "").strip()
        chat_id = msg["chat"]["id"]

        if text == "/start":
            handle_start_command(chat_id)
        elif text == "/help":
            handle_help_command(chat_id)
        elif text == "/menu":
            handle_menu_command(chat_id)
        elif text == "/leads":
            handle_leads_command(chat_id)
        elif text == "/hot":
            handle_hot_command(chat_id)
        elif text == "/today":
            handle_today_command(chat_id)
        elif text == "/schedule":
            handle_schedule_command(chat_id)
        elif text == "/tomorrow":
            handle_tomorrow_command(chat_id)
        elif text == "/pipeline":
            handle_pipeline_command(chat_id)
        elif text == "/project":
            handle_project_command(chat_id)
        elif text == "/briefing":
            send_daily_briefing()
        elif text.startswith("/setbriefingtime"):
            parts = text.split()
            if len(parts) != 2:
                send_msg(chat_id,
                    "⏰ Usage: `/setbriefingtime HH:MM`\n"
                    "Example: `/setbriefingtime 08:30`\n\n"
                    "Uses 24-hour PH time format."
                )
                return jsonify({"status": "ok"})
            time_str = parts[1]
            try:
                hour, minute = map(int, time_str.split(":"))
                assert 0 <= hour <= 23 and 0 <= minute <= 59
            except Exception:
                send_msg(chat_id,
                    "❌ Invalid time format.\n"
                    "Use 24hr format: `/setbriefingtime 09:00`"
                )
                return jsonify({"status": "ok"})
            write_briefing_time(time_str)
            reschedule_briefing(time_str)
            send_msg(chat_id,
                f"⏰ *Daily briefing rescheduled*\n"
                f"New time: *{time_str} PH time*\n\n"
                f"Use `/briefing` to send it right now."
            )
        elif text.startswith("/search"):
            handle_search_command(chat_id, text[len("/search"):].strip())
        elif text.startswith("/client"):
            handle_client_command(chat_id, text[len("/client"):].strip())
        elif text.startswith("/updateemail"):
            parts = text.split()
            if len(parts) != 3:
                send_msg(chat_id, "Usage: `/updateemail <Lead_ID> <new_email>`")
                return jsonify({"status": "ok"})
            lead_id, new_email = parts[1], parts[2]
            rows, col = read_sheet_with_headers("Leads!A1:T200")
            found = False
            for i, row in enumerate(rows):
                if safe_get(row, col, "Lead_ID") == lead_id:
                    col_idx = col.get("Email")
                    if col_idx is None:
                        send_msg(chat_id, "⚠️ Email column not found.")
                        return jsonify({"status": "ok"})
                    write_sheet(f"Leads!{get_col_letter(col_idx)}{i + 2}", [[new_email]])
                    send_msg(chat_id, f"✅ Email updated for {lead_id} → {new_email}")
                    found = True
                    break
            if not found:
                send_msg(chat_id, f"❌ Lead {lead_id} not found.")
        elif text == "/resetleadcounter":
            write_sheet("Config!A2", [[0]])
            write_sheet("Config!B2", [["0001"]])
            send_msg(chat_id, "✅ Lead counter reset to 0. Next lead will be LED-0001.")
        elif text.startswith("/retention"):
            parts = text.split()
            if len(parts) != 2:
                send_msg(chat_id, "Usage: `/retention <Lead_ID>`")
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            result  = _execute_retention(lead_id, processing_msg_id=None)
            if not result["fired"]:
                send_msg(chat_id, "⚠️ Webhook failed — check RETENTION_WEBHOOK in Railway.")
        elif text.startswith("/setbudget"):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                send_msg(chat_id,
                    "💰 Usage: `/setbudget <Lead_ID> <amount>`\n"
                    "Example: `/setbudget LED-0002 13000`"
                )
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            try:
                total = int(float(parts[2].replace("$", "").replace(",", "").strip()))
                if total <= 0:
                    raise ValueError("Amount must be positive")
            except ValueError:
                send_msg(chat_id,
                    "❌ Invalid amount — numbers only, no symbols.\n"
                    "Example: `/setbudget LED-0002 13000`"
                )
                return jsonify({"status": "ok"})
            deposit = round(total * 0.30)
            balance = total - deposit
            lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
            lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
            if not lead_row:
                send_msg(chat_id, f"❌ Lead `{lead_id}` not found.")
                return jsonify({"status": "ok"})
            name = safe_get(lead_row, lead_col, "Full_Name")
            pkg  = safe_get(lead_row, lead_col, "Primary_Package")
            text_msg = (
                f"💰 *Confirm Contract Amount*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Lead: `{lead_id}` — *{name}*\n"
                f"Package: {pkg}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Total:          *${total:,}*\n"
                f"💳 Deposit (30%): *${deposit:,}*\n"
                f"📊 Balance (70%): *${balance:,}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"These amounts will be written to Google Sheets and\n"
                f"sent to System 3A to generate the contract.\n\n"
                f"Confirm and fire System 3A?"
            )
            buttons = [
                [{"text": "✅ Confirm & Fire System 3A", "callback_data": f"budget_contract_yes|{lead_id}|{total}"}],
                [{"text": "✏️ Change Amount",            "callback_data": f"budget_contract_edit|{lead_id}"}],
                [{"text": "❌ Cancel",                   "callback_data": f"view_pipe|{lead_id}"}]
            ]
            send_msg(chat_id, text_msg, {"inline_keyboard": buttons})
        else:
            send_msg(chat_id, "❓ Unknown command. Type /help to see all commands.")
        return jsonify({"status": "ok"})

    return handle_callbacks(data, use_pipeline=False)

# ─────────────────────────────────────────────
# PIPELINE TRACKER BOT WEBHOOK
# ─────────────────────────────────────────────
@app.route("/pipeline_dashboard", methods=["POST"])
def pipeline_dashboard():
    data = request.json

    if "message" in data:
        msg     = data["message"]
        text    = msg.get("text", "").strip()
        chat_id = msg["chat"]["id"]

        if text.startswith("/setbudget"):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                send_pipeline_msg(chat_id,
                    "💰 Usage: `/setbudget <Lead_ID> <amount>`\n"
                    "Example: `/setbudget LED-0002 13000`"
                )
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            try:
                total = int(float(parts[2].replace("$", "").replace(",", "").strip()))
                if total <= 0:
                    raise ValueError("Amount must be positive")
            except ValueError:
                send_pipeline_msg(chat_id,
                    "❌ Invalid amount — numbers only.\n"
                    "Example: `/setbudget LED-0002 13000`"
                )
                return jsonify({"status": "ok"})
            deposit = round(total * 0.30)
            balance = total - deposit
            lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
            lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
            if not lead_row:
                send_pipeline_msg(chat_id, f"❌ Lead `{lead_id}` not found.")
                return jsonify({"status": "ok"})
            name = safe_get(lead_row, lead_col, "Full_Name")
            pkg  = safe_get(lead_row, lead_col, "Primary_Package")
            text_msg = (
                f"💰 *Confirm Contract Amount*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Lead: `{lead_id}` — *{name}*\n"
                f"Package: {pkg}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Total:          *${total:,}*\n"
                f"💳 Deposit (30%): *${deposit:,}*\n"
                f"📊 Balance (70%): *${balance:,}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"These amounts will be written to Google Sheets and\n"
                f"sent to System 3A to generate the contract.\n\n"
                f"Confirm and fire System 3A?"
            )
            buttons = [
                [{"text": "✅ Confirm & Fire System 3A", "callback_data": f"budget_contract_yes|{lead_id}|{total}"}],
                [{"text": "✏️ Change Amount",            "callback_data": f"budget_contract_edit|{lead_id}"}],
                [{"text": "❌ Cancel",                   "callback_data": f"view_pipe|{lead_id}"}]
            ]
            send_pipeline_msg(chat_id, text_msg, {"inline_keyboard": buttons})
            return jsonify({"status": "ok"})

    return handle_callbacks(data, use_pipeline=True)

# ─────────────────────────────────────────────
# ZAPIER NOTIFY ROUTES
# ─────────────────────────────────────────────
@app.route("/notify", methods=["POST"])
def notify():
    data    = request.json
    message = data.get("message", "")
    if message:
        send_client_msg(CHAT_ID, message)
    return jsonify({"status": "ok"})

@app.route("/pipeline_notify", methods=["POST"])
def pipeline_notify():
    data         = request.json
    lead_id      = data.get("lead_id",         "—")
    client_name  = data.get("client_name",     "—")
    client_email = data.get("client_email",    "—")
    lead_status  = data.get("lead_status",     "—")
    urgency      = data.get("urgency_score",   "—")
    package      = data.get("primary_package", "—")
    summary      = data.get("ai_summary",      "—")
    call_time    = data.get("call_time",       "—")

    text = (
        f"📅 *DISCOVERY CALL BOOKED*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"✉️ {client_email}\n"
        f"🆔 Lead: {lead_id}\n"
        f"📅 Call: {call_time}\n"
        f"🎯 {lead_status} | ⚡ {urgency}/10\n"
        f"📦 {package}\n"
        f"🧠 {summary}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"What was the result of the discovery call?"
    )
    buttons = [
        [{"text": "✅ Completed — Continue",  "callback_data": f"confirm_call|{lead_id}|completed_continue"}],
        [{"text": "🛑 Completed — Not a Fit", "callback_data": f"confirm_call|{lead_id}|completed_stop"}],
        [{"text": "❌ No Show",               "callback_data": f"confirm_call|{lead_id}|no_show"}],
        [{"text": "🔄 Reschedule",            "callback_data": f"confirm_call|{lead_id}|reschedule"}],
        [{"text": "🔁 Rescheduled On-Call",   "callback_data": f"confirm_call|{lead_id}|reschedule_oncall"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

@app.route("/proposal_notify", methods=["POST"])
def proposal_notify():
    data          = request.json
    lead_id       = data.get("lead_id",       "—")
    project_id    = data.get("project_id",    "—")
    client_name   = data.get("client_name",   "—")
    proposal_link = data.get("proposal_link", "—")
    event_type    = data.get("event_type",    "—")
    event_date    = data.get("event_date",    "—")
    package       = data.get("package",       "—")

    text = (
        f"📤 *Proposal Sent*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 Event: {event_type} — {event_date}\n"
        f"📦 Package: {package}\n"
        f"📄 [View Proposal Doc]({proposal_link})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"When the client confirms, tap below to send the contract."
    )
    buttons = [[{"text": "📝 Send Contract", "callback_data": f"confirm_contract|{lead_id}"}]]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# System 3B post-signature notification.
# IMPORTANT: The Zapier s3b.json step 17 does NOT include invoice_link in its
# payload. This field will display as "—" until that Zapier step is updated to
# include the Zoho invoice URL. The rest of the card is fully functional.
@app.route("/invoice_sent", methods=["POST"])
def invoice_sent():
    data         = request.json
    lead_id      = data.get("lead_id",      "—")
    lead_name    = data.get("lead_name",    "—")
    project_id   = data.get("project_id",   "—")
    package      = data.get("package",      "—")
    deposit      = data.get("deposit",      "—")
    invoice_date = data.get("invoice_date", "—")
    invoice_link = data.get("invoice_link", "—")
    due_date     = data.get("due_date",     "—")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Invoice_Sent": "TRUE",
        "Invoice_Date": invoice_date
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Active Project",
        "Last_Action":      "Deposit Invoice Sent",
        "Next_Action":      "Await Deposit Payment",
        "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
    })

    invoice_display = f"[View Invoice]({invoice_link})" if invoice_link != "—" else "_(link not provided)_"

    text = (
        f"🧾 *Deposit Invoice Sent — System 3B*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📦 Package: {package}\n"
        f"💰 Deposit Due: *${deposit}*\n"
        f"📅 Invoice Date: {invoice_date}\n"
        f"📅 Due Date: {due_date}\n"
        f"📄 {invoice_display}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Deposit invoice sent to client.\n"
        f"Tap the button below once you confirm\n"
        f"payment has been received in Zoho."
    )
    buttons = [[{"text": "✅ Mark Deposit Paid", "callback_data": f"deposit_paid_confirm|{lead_id}"}]]

    r = requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

@app.route("/deposit_confirmed", methods=["POST"])
def deposit_confirmed():
    data         = request.json
    lead_id      = data.get("lead_id",        "—")
    lead_name    = data.get("lead_name",      "—")
    project_id   = data.get("project_id",     "—")
    deposit_amt  = data.get("deposit_amount", "—")
    payment_date = data.get("payment_date",   "—")

    # FIX: Projects stage after deposit was "Active Project" which is not a
    # valid Projects stage. Correct stage is "Pre-Production".
    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Deposit_Paid":  "TRUE",
        "Current_Stage": "Pre-Production"     # was incorrectly "Active Project"
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Active Project",
        "Last_Action":      "Deposit Payment Received",
        "Next_Action":      "Pre-Production Prep",
        "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
    })

    text = (
        f"✅ *Deposit Payment Received*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"💰 Amount: ${deposit_amt}\n"
        f"📅 Received: {payment_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Deposit payment confirmed in Zoho.\n"
        f"Project is now in *Pre-Production* stage."
    )
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"
    })
    return jsonify({"status": "ok"})

@app.route("/gallery_notify", methods=["POST"])
def gallery_notify():
    data          = request.json
    lead_id       = data.get("lead_id",       "—")
    client_name   = data.get("lead_name",     "—")
    project_id    = data.get("project_id",    "—")
    gallery_link  = data.get("gallery_url",   "—")
    delivery_date = data.get("delivery_date", "—")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage":      "Delivered",
        "Gallery_Folder_URL": gallery_link,
        "Delivery_Date":      delivery_date,
        "Shoot_Complete":     "TRUE"
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Delivered",
        "Last_Action":      "Gallery Delivered",
        "Next_Action":      "Await Balance Payment / Run Retention",
        "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
    })

    text = (
        f"📸 *System 4 — Gallery Delivered*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"🔗 [View Gallery]({gallery_link})\n"
        f"📅 Delivered: {delivery_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Gallery delivered to client.\n"
        f"Project moved to *Delivered* stage.\n"
        f"Tap below to confirm balance payment when received."
    )
    buttons = [[{"text": "💳 Mark Balance Paid", "callback_data": f"balance_paid|{lead_id}"}]]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# System 5 Review Request.
# CRITICAL: Does NOT set Current_Stage = "Completed".
# Projects must stay "Delivered" so APScheduler finds the row
# after 7 days and auto-completes via check_retention_completions().
@app.route("/retention_notify", methods=["POST"])
def retention_notify():
    data        = request.json
    lead_id     = data.get("lead_id",     "—")
    message_id  = data.get("message_id")
    client_name = data.get("client_name", "—")
    project_id  = data.get("project_id",  "—")
    event_type  = data.get("event_type",  "—")
    new_ltv     = data.get("new_ltv",     "—")
    new_tier    = data.get("new_tier",    "—")
    bookings    = data.get("bookings",    "—")

    tier_emoji = {"VIP": "⭐", "Premium": "💎", "Standard": "🔹"}.get(str(new_tier), "👤")
    today_str  = ph_now().strftime("%Y-%m-%d")

    # Write review flags only — do NOT set Current_Stage = "Completed".
    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Review":           "TRUE",
        "Review_Sent_Date": today_str
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Last_Action":      "Review Request Email Sent",
        "Next_Action":      "Await 7-day Window — Rebooking Email Queued",
        "Next_Action_Date": today_str
    })

    text = (
        f"⭐ *Everly & Co. — System 5 Review + Rebooking Sequence (Review Request)*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name} | {tier_emoji} {new_tier}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📸 Event: {event_type}\n\n"
        f"📊 *Updated Client Stats:*\n"
        f"  💰 Lifetime Value: ${new_ltv}\n"
        f"  📅 Total Bookings: {bookings}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Review request email sent to client.\n"
        f"📧 Rebooking upsell auto-fires via Zapier in +7 days.\n"
        f"🔄 Project auto-completes in 7 days if no manual action.\n\n"
        f"🏁 *Status: Complete*"
    )

    msg_id_int = None
    if message_id:
        try:
            msg_id_int = int(message_id)
        except (ValueError, TypeError):
            pass
    if msg_id_int:
        delete_pipeline_msg(CHAT_ID, msg_id_int)

    send_pipeline_msg(CHAT_ID, text)
    return jsonify({"status": "ok"})

# System 5 Rebooking Upsell.
# NOTE: The s5.json Zapier step 8 currently fires to /retention_5b_notify.
# This route also accepts that path via the alias below so both URLs work.
@app.route("/retention_rebooking_notify", methods=["POST"])
@app.route("/retention_5b_notify", methods=["POST"])   # alias for s5.json compatibility
def retention_rebooking_notify():
    data        = request.json
    lead_id     = data.get("lead_id",     "—")
    client_name = data.get("client_name", "—")
    project_id  = data.get("project_id",  "—")
    event_type  = data.get("event_type",  "—")

    today_str = ph_now().strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Upsell_Sent": "TRUE"
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Closed Won",
        "Last_Action":      "Rebooking Email Sent",
        "Next_Action":      "Monitor for Rebook",
        "Next_Action_Date": today_str
    })

    text = (
        f"📧 *Everly & Co. — Rebooking Sequence Complete — Rebooking Email Sent*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📸 Event: {event_type}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Rebooking upsell email delivered to client.\n"
        f"📊 Pipeline → *Closed Won*"
    )
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
try:
    init_scheduler()
except Exception as e:
    print(f"[STARTUP] Scheduler init error: {e}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
