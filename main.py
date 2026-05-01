from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build

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
RETENTION_5B_WEBHOOK     = os.environ.get("RETENTION_5B_WEBHOOK")

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
PROJECT_STAGES = [
    "Pre-Production", "Active", "Post-Production", "Delivered", "Completed", "Closed"
]
STATUS_EMOJI = {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}

UNCERTAIN_BUDGETS = {"$10,000+", "TBD", "10000+", "$10,000+ "}

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

def safe_get(row, col, key):
    if key not in col or len(row) <= col[key]:
        return "—"
    return row[col[key]] if row[col[key]] else "—"

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
# FIX: edit_pipeline_msg now includes parse_mode — was silently failing before
# ─────────────────────────────────────────────
def send_msg(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)
    print(f"[DASHBOARD SEND] {r.status_code}: {r.text[:200]}")
    return r

def edit_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

def edit_pipeline_msg(chat_id, message_id, text, reply_markup=None):
    # FIX: Added parse_mode — previously missing, causing all pipeline edits to silently fail
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(f"{PIPELINE_API}/editMessageText", json=payload)

def send_pipeline_msg(chat_id, text, reply_markup=None):
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(f"{PIPELINE_API}/sendMessage", json=payload)

def send_client_msg(chat_id, text):
    requests.post(f"{CLIENT_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

# ─────────────────────────────────────────────
# SMART SEND/EDIT HELPER
# Chooses edit vs send based on whether msg_id is present
# Chooses pipeline vs dashboard API based on use_pipeline flag
# ─────────────────────────────────────────────
def smart_send(chat_id, text, markup=None, msg_id=None, use_pipeline=False):
    """
    If msg_id is provided → edit in place (no new message spam).
    If not → send new message.
    Respects use_pipeline flag for correct bot selection.
    """
    if msg_id:
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, text, markup)
        else:
            edit_msg(chat_id, msg_id, text, markup)
    else:
        if use_pipeline:
            send_pipeline_msg(chat_id, text, markup)
        else:
            send_msg(chat_id, text, markup)

# ─────────────────────────────────────────────
# CAL.COM API HELPERS
# ─────────────────────────────────────────────
def cancel_cal_booking_for_lead(lead_id):
    if not CAL_API_KEY:
        print("[CAL CANCEL] CAL_API_KEY not set.")
        return False, "no_api_key"
    try:
        r = requests.get(
            "https://api.cal.com/v2/bookings",
            headers={
                "Authorization": f"Bearer {CAL_API_KEY}",
                "cal-api-version": "2024-08-13"
            },
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
            print(f"[CAL CANCEL] No upcoming accepted booking found for lead {lead_id}")
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
        print(f"[CAL CANCEL] Cancelled booking {target_uid} for lead {lead_id}")
        return True, target_uid

    except Exception as e:
        print(f"[CAL CANCEL ERROR] {e}")
        return False, str(e)


def fetch_cal_bookings(date_str):
    if not CAL_API_KEY:
        print("[CAL] CAL_API_KEY not set.")
        return []
    try:
        date_from = f"{date_str}T00:00:00+08:00"
        date_to   = f"{date_str}T23:59:59+08:00"
        r = requests.get(
            "https://api.cal.com/v2/bookings",
            headers={
                "Authorization": f"Bearer {CAL_API_KEY}",
                "cal-api-version": "2024-08-13"
            },
            params={
                "afterStart": date_from,
                "beforeEnd":  date_to,
                "status":     "upcoming"
            },
            timeout=10
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        if isinstance(data, list):
            return data
        return data.get("bookings", [])
    except Exception as e:
        print(f"[CAL ERROR] {e}")
        return []


def parse_cal_booking(booking):
    attendees    = booking.get("attendees", [])
    client_name  = attendees[0].get("name", "Unknown") if attendees else "Unknown"
    client_email = attendees[0].get("email", "—") if attendees else "—"

    responses = booking.get("bookingFieldsResponses", {})
    metadata  = booking.get("metadata", {})
    lead_id   = responses.get("lead_id") or metadata.get("lead_id") or "—"

    start_raw = booking.get("start", booking.get("startTime", ""))
    try:
        dt_utc   = datetime.strptime(start_raw[:19], "%Y-%m-%dT%H:%M:%S")
        dt_local = dt_utc + timedelta(hours=8)
        time_str = dt_local.strftime("%I:%M %p")
    except Exception:
        time_str = start_raw

    meeting_url = (
        booking.get("videoCallData", {}).get("url")
        or booking.get("location", "—")
        or "—"
    )
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
# FIX: All nav handlers now accept msg_id + use_pipeline for in-place editing
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
        "`/setbudget <Lead_ID> <amount>` — Confirm exact amount for $10k+/TBD leads before sending contract\n\n"
        "💡 *Tips*\n"
        "• Tap any lead or client button to drill in\n"
        "• Update lead status (HOT/WARM/COLD) from the lead card\n"
        "• Log call outcomes right after a discovery call\n"
        "• Deliver galleries from the Projects tab — no typing needed"
    )
    send_msg(chat_id, text)

def handle_menu_command(chat_id):
    text = "📸 *Everly & Co. CRM — Quick Menu*"
    buttons = [
        [
            {"text": "📋 Leads",          "callback_data": "nav_leads|none"},
            {"text": "🔴 Hot Leads",      "callback_data": "nav_hot|none"}
        ],
        [
            {"text": "📊 Pipeline",       "callback_data": "nav_pipe|none"},
            {"text": "🗂 Projects",       "callback_data": "nav_projects|none"}
        ],
        [
            {"text": "📅 Today's Calls",  "callback_data": "nav_schedule|today"},
            {"text": "📅 Tomorrow",       "callback_data": "nav_schedule|tomorrow"}
        ],
        [
            {"text": "📸 Today's Shoots", "callback_data": "nav_today|none"}
        ]
    ]
    send_msg(chat_id, text, {"inline_keyboard": buttons})


# FIX: Added msg_id + use_pipeline params — nav callbacks now edit in-place
def handle_leads_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    if not rows:
        return smart_send(chat_id, "📭 No leads found.", msg_id=msg_id, use_pipeline=use_pipeline)
    lines = ["📋 *LEADS OVERVIEW* (latest 10)\n"]
    buttons = []
    for row in rows[:10]:
        name   = safe_get(row, col, "Full_Name")
        lid    = safe_get(row, col, "Lead_ID")
        status = safe_get(row, col, "Lead_Status")
        emoji  = STATUS_EMOJI.get(status.upper(), "⚪")
        lines.append(f"• {emoji} *{name}* (`{lid}`) — {status}")
        buttons.append([{"text": f"{emoji} {name}", "callback_data": f"view_lead|{lid}"}])
    buttons.append([
        {"text": "🔴 HOT Only", "callback_data": "nav_hot|none"},
        {"text": "📊 Pipeline", "callback_data": "nav_pipe|none"}
    ])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)


# FIX: Added msg_id + use_pipeline params
def handle_hot_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    hot_rows = [r for r in rows if safe_get(r, col, "Lead_Status").upper() == "HOT"]
    if not hot_rows:
        return smart_send(chat_id, "🔴 No HOT leads right now.", msg_id=msg_id, use_pipeline=use_pipeline)
    lines = [f"🔴 *HOT LEADS* ({len(hot_rows)} total)\n"]
    buttons = []
    for row in hot_rows[:10]:
        name  = safe_get(row, col, "Full_Name")
        lid   = safe_get(row, col, "Lead_ID")
        event = safe_get(row, col, "Event_Type")
        date  = safe_get(row, col, "Event_Date")
        lines.append(f"• 🔴 *{name}* (`{lid}`)\n  └ {event} on {date}")
        buttons.append([{"text": f"🔴 {name}", "callback_data": f"view_lead|{lid}"}])
    buttons.append([{"text": "⬅️ All Leads", "callback_data": "nav_leads|none"}])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)


# FIX: Added msg_id + use_pipeline params
def handle_today_command(chat_id, msg_id=None, use_pipeline=False):
    today_str     = ph_now().strftime("%Y-%m-%d")
    today_display = ph_now().strftime("%B %d, %Y")
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    today_rows = [r for r in rows if today_str in safe_get(r, col, "Event_Date")]
    lines = [f"📸 *TODAY'S SHOOTS* — {today_display}\n"]
    buttons = []
    if today_rows:
        for row in today_rows:
            name  = safe_get(row, col, "Full_Name")
            lid   = safe_get(row, col, "Lead_ID")
            event = safe_get(row, col, "Event_Type")
            lines.append(f"• 📸 *{name}* — {event}\n  ID: `{lid}`")
            buttons.append([{"text": f"📸 {name}", "callback_data": f"view_lead|{lid}"}])
    else:
        lines.append("No shoots scheduled for today.")
    buttons.append([
        {"text": "📅 Today's Calls", "callback_data": "nav_schedule|today"},
        {"text": "⬅️ All Leads",     "callback_data": "nav_leads|none"}
    ])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)


# FIX: Added msg_id + use_pipeline params
def handle_schedule_command(chat_id, date_str=None, date_label=None, msg_id=None, use_pipeline=False):
    now = ph_now()
    if not date_str:
        date_str   = now.strftime("%Y-%m-%d")
        date_label = now.strftime("%B %d, %Y")
    bookings_raw = fetch_cal_bookings(date_str)
    bookings = [
        parse_cal_booking(b) for b in bookings_raw
        if b.get("status", "").lower() in ("accepted", "upcoming", "pending", "booked")
    ]
    lines   = [f"📅 *DISCOVERY CALLS — {date_label}*\n"]
    buttons = []
    if bookings:
        lines.append(f"_{len(bookings)} call(s) scheduled_\n")
        for b in bookings:
            lead_id = b["lead_id"]
            lines.append(
                f"🕐 *{b['time']}* — {b['client_name']}\n"
                f"  └ Lead: `{lead_id}` | {b['client_email']}"
            )
            row_buttons = []
            if lead_id and lead_id != "—":
                row_buttons.append({"text": f"👤 {b['client_name']}", "callback_data": f"view_lead|{lead_id}"})
            if b["meeting_url"] and b["meeting_url"] != "—":
                row_buttons.append({"text": "🔗 Join Call", "url": b["meeting_url"]})
            if row_buttons:
                buttons.append(row_buttons)
            if lead_id and lead_id != "—":
                buttons.append([{"text": "📞 Log Call Outcome", "callback_data": f"call_menu|{lead_id}"}])
    else:
        lines.append("No discovery calls scheduled.")
    today_str = now.strftime("%Y-%m-%d")
    if date_str == today_str:
        buttons.append([
            {"text": "➡️ Tomorrow",       "callback_data": "nav_schedule|tomorrow"},
            {"text": "📸 Today's Shoots", "callback_data": "nav_today|none"}
        ])
    else:
        buttons.append([
            {"text": "⬅️ Today",     "callback_data": "nav_schedule|today"},
            {"text": "📋 All Leads", "callback_data": "nav_leads|none"}
        ])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)


# FIX: Added msg_id + use_pipeline params
def handle_tomorrow_command(chat_id, msg_id=None, use_pipeline=False):
    tomorrow     = ph_now() + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    tomorrow_lbl = tomorrow.strftime("%B %d, %Y")
    handle_schedule_command(
        chat_id,
        date_str=tomorrow_str,
        date_label=f"Tomorrow — {tomorrow_lbl}",
        msg_id=msg_id,
        use_pipeline=use_pipeline
    )

def handle_search_command(chat_id, query):
    if not query:
        return send_msg(chat_id, "🔍 Usage: `/search <name or email>`")
    query_lower = query.lower()
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    matches = [
        r for r in rows
        if query_lower in safe_get(r, col, "Full_Name").lower()
        or query_lower in safe_get(r, col, "Email").lower()
    ]
    if not matches:
        return send_msg(chat_id, f"🔍 No results found for: *{query}*")
    lines = [f"🔍 *Search Results for \"{query}\"* ({len(matches)} found)\n"]
    buttons = []
    for row in matches[:10]:
        name   = safe_get(row, col, "Full_Name")
        lid    = safe_get(row, col, "Lead_ID")
        email  = safe_get(row, col, "Email")
        status = safe_get(row, col, "Lead_Status")
        emoji  = STATUS_EMOJI.get(status.upper(), "⚪")
        lines.append(f"• {emoji} *{name}* (`{lid}`)\n  └ {email}")
        buttons.append([{"text": f"{emoji} {name}", "callback_data": f"view_lead|{lid}"}])
    buttons.append([{"text": "⬅️ All Leads", "callback_data": "nav_leads|none"}])
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})


# FIX: Pipeline command properly edits in-place when msg_id provided
def handle_pipeline_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    if not rows:
        return smart_send(chat_id, "📭 Pipeline is empty.", msg_id=msg_id, use_pipeline=use_pipeline)
    lines = ["📊 *PIPELINE SNAPSHOT*\n"]
    buttons = []
    for row in rows[:10]:
        client = safe_get(row, col, "Client_Name")
        lid    = safe_get(row, col, "Lead_ID")
        stage  = safe_get(row, col, "Current_Stage")
        next_a = safe_get(row, col, "Next_Action")
        lines.append(f"• *{client}* (`{lid}`)\n  └ {stage} → _{next_a}_")
        buttons.append([{"text": f"📊 {client}", "callback_data": f"view_pipe|{lid}"}])
    buttons.append([
        {"text": "📋 All Leads", "callback_data": "nav_leads|none"},
        {"text": "🗂 Projects",  "callback_data": "nav_projects|none"}
    ])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)


# FIX: Added msg_id + use_pipeline params
def handle_project_command(chat_id, msg_id=None, use_pipeline=False):
    rows, col = read_sheet_with_headers("Projects!A1:Z200")
    if not rows:
        return smart_send(chat_id, "📭 No projects found.", msg_id=msg_id, use_pipeline=use_pipeline)
    active = [r for r in rows if safe_get(r, col, "Current_Stage") not in ("Closed", "Completed")]
    lines = [f"🗂 *ACTIVE PROJECTS* ({len(active)} total)\n"]
    buttons = []
    for row in active[:10]:
        pid    = safe_get(row, col, "Project_ID")
        client = safe_get(row, col, "Client_Name")
        stage  = safe_get(row, col, "Current_Stage")
        edate  = safe_get(row, col, "Event_Date")
        lid    = safe_get(row, col, "Lead_ID")
        lines.append(f"• 🗂 *{client}* (`{pid}`)\n  └ {stage} | Event: {edate}")
        buttons.append([{"text": f"🗂 {client}", "callback_data": f"view_project|{lid}"}])
    if not active:
        lines.append("No active projects at the moment.")
    buttons.append([{"text": "📊 Pipeline", "callback_data": "nav_pipe|none"}])
    smart_send(chat_id, "\n".join(lines), {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)

def handle_client_command(chat_id, client_id):
    if not client_id:
        return send_msg(chat_id, "👤 Usage: `/client <Client_ID>`\nExample: `/client C-1234567890`")
    _show_client(chat_id, None, client_id)

# ─────────────────────────────────────────────
# VIEW HANDLERS
# ─────────────────────────────────────────────
def _show_lead(chat_id, msg_id, target_id, method="edit", use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == target_id), None)
    if not row:
        return send_msg(chat_id, f"❌ Lead `{target_id}` not found.")
    status = safe_get(row, col, "Lead_Status")
    emoji  = STATUS_EMOJI.get(status.upper(), "⚪")
    text = (
        f"👤 *{safe_get(row, col, 'Full_Name')}*\n"
        f"ID: `{target_id}` | {emoji} {status}\n\n"
        f"📅 Event: {safe_get(row, col, 'Event_Type')} — {safe_get(row, col, 'Event_Date')}\n"
        f"📍 Venue: {safe_get(row, col, 'Venue')}\n"
        f"👥 Guests: {safe_get(row, col, 'Guest_Count')}\n"
        f"💰 Budget: {safe_get(row, col, 'Budget')}\n"
        f"📦 Package: {safe_get(row, col, 'Primary_Package')}\n"
        f"📱 Phone: {safe_get(row, col, 'Phone')}\n"
        f"✉️ Email: {safe_get(row, col, 'Email')}\n\n"
        f"🧠 *AI Summary:*\n{safe_get(row, col, 'AI_Summary')}\n\n"
        f"✅ *Recommended:* {safe_get(row, col, 'Recommended_Action')}"
    )
    client_id = safe_get(row, col, "Client_ID") if "Client_ID" in col else None
    buttons = [
        [
            {"text": "🔴 HOT",  "callback_data": f"upd_lead|{target_id}|HOT"},
            {"text": "🟡 WARM", "callback_data": f"upd_lead|{target_id}|WARM"},
            {"text": "🔵 COLD", "callback_data": f"upd_lead|{target_id}|COLD"}
        ],
        [
            {"text": "📊 Pipeline", "callback_data": f"view_pipe|{target_id}"},
            {"text": "🗂 Project",  "callback_data": f"view_project|{target_id}"}
        ],
        [
            {"text": "👤 Client Card", "callback_data": f"view_client|{client_id}"} if client_id and client_id != "—" else {"text": "👤 No Client Data", "callback_data": "none"}
        ],
        [{"text": "⬅️ Back to Leads", "callback_data": "nav_leads|none"}]
    ]
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, text, markup, msg_id=msg_id if method == "edit" else None, use_pipeline=use_pipeline)


def _show_pipeline(chat_id, msg_id, target_id, method="edit", use_pipeline=False):
    rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == target_id), None)
    if not row:
        return send_msg(chat_id, f"❌ Pipeline entry for `{target_id}` not found.")
    curr_stage = safe_get(row, col, "Current_Stage")
    text = (
        f"📊 *Pipeline: {safe_get(row, col, 'Client_Name')}*\n"
        f"Lead ID: `{target_id}`\n"
        f"Project ID: `{safe_get(row, col, 'Project_ID')}`\n\n"
        f"📍 Current Stage: *{curr_stage}*\n"
        f"🕐 Last Action: {safe_get(row, col, 'Last_Action')}\n"
        f"➡️ Next Action: {safe_get(row, col, 'Next_Action')}\n"
        f"📅 Due: {safe_get(row, col, 'Next_Action_Date')}\n"
        f"📞 Call Status: {safe_get(row, col, 'Call_Status')}\n"
        f"📝 Proposal: {safe_get(row, col, 'Proposal_Status')}"
    )

    # FIX: Only show next pipeline stages when the current stage is actually in PIPELINE_STAGES
    # Previously: None + 1 would crash; or stage not found would show wrong stages
    curr_idx    = PIPELINE_STAGES.index(curr_stage) if curr_stage in PIPELINE_STAGES else None
    next_stages = PIPELINE_STAGES[curr_idx + 1: curr_idx + 4] if curr_idx is not None else []

    buttons = [[{"text": f"➡️ {s}", "callback_data": f"upd_pipe|{target_id}|{s}"}] for s in next_stages]

    # Contextual action button — one clear CTA per stage
    action_row = []
    if curr_stage == "Discovery Call Booked":
        action_row = [{"text": "📞 Log Call Outcome",        "callback_data": f"call_menu|{target_id}"}]
    elif curr_stage == "Discovery Call Completed":
        action_row = [{"text": "📄 Send Proposal",           "callback_data": f"send_proposal|{target_id}"}]
    elif curr_stage == "Proposal Sent":
        action_row = [{"text": "📝 Send Contract",           "callback_data": f"send_contract|{target_id}"}]
    elif curr_stage == "Contracted":
        action_row = [{"text": "💰 Mark Deposit Paid",       "callback_data": f"deposit_paid|{target_id}"}]
    elif curr_stage == "Active Project":
        action_row = [{"text": "📸 Mark Shoot Complete",     "callback_data": f"shoot_complete|{target_id}"}]
    elif curr_stage == "Post-Production":
        action_row = [{"text": "✅ Gallery Ready to Ship?",  "callback_data": f"gallery_ready|{target_id}"}]
    elif curr_stage == "Delivered":
        action_row = [{"text": "⭐ Run Retention",           "callback_data": f"trigger_retention|{target_id}"}]
    if action_row:
        buttons.append(action_row)

    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{target_id}"},
        {"text": "🗂 Project",   "callback_data": f"view_project|{target_id}"}
    ])
    buttons.append([{"text": "⬅️ Back to Pipeline", "callback_data": "nav_pipe|none"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, text, markup, msg_id=msg_id if method == "edit" else None, use_pipeline=use_pipeline)


def _show_project(chat_id, msg_id, lead_id, method="edit", use_pipeline=False):
    rows, col = read_sheet_with_headers("Projects!A1:Z200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == lead_id), None)
    if not row:
        return send_msg(chat_id, f"❌ No project found for Lead `{lead_id}`.")
    curr_stage      = safe_get(row, col, "Current_Stage")
    balance_val     = safe_get(row, col, "Balance")
    deposit_paid    = safe_get(row, col, "Deposit_Paid")
    balance_due     = safe_get(row, col, "Balance_Due_Date")
    balance_paid    = safe_get(row, col, "Balance_Paid")
    text = (
        f"🗂 *Project: {safe_get(row, col, 'Client_Name')}*\n"
        f"Project ID: `{safe_get(row, col, 'Project_ID')}`\n"
        f"Lead ID: `{lead_id}`\n\n"
        f"📅 Event Date: {safe_get(row, col, 'Event_Date')}\n"
        f"📦 Package: {safe_get(row, col, 'Package')}\n"
        f"💰 Total: {safe_get(row, col, 'Total_Price')}\n"
        f"💵 Deposit: {safe_get(row, col, 'Deposit')} | Paid: {deposit_paid}\n"
        f"📊 Balance: {balance_val} | Due: {balance_due} | Paid: {balance_paid}\n\n"
        f"📍 Stage: *{curr_stage}*\n"
        f"📝 Contract: {safe_get(row, col, 'Contract_Sent')} ({safe_get(row, col, 'Contract_Date')})\n"
        f"🖼️ Gallery: {safe_get(row, col, 'Gallery_Folder_URL')}\n"
        f"📦 Delivered: {safe_get(row, col, 'Delivery_Date')}\n"
        f"⭐ Review Sent: {safe_get(row, col, 'Review_Sent') if 'Review_Sent' in col else safe_get(row, col, 'Review')}"
    )

    # Days since event (positive = past, negative = future) — useful distinction for Victoria
    event_date_raw   = safe_get(row, col, "Event_Date")
    days_since_event = None
    try:
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                event_dt = datetime.strptime(event_date_raw.strip(), fmt)
                days_since_event = (ph_now() - event_dt).days
                break
            except ValueError:
                continue
    except Exception:
        pass

    # Append stage-specific status line
    if curr_stage == "Post-Production" and days_since_event is not None:
        if days_since_event >= 0:
            text += f"\n\n⏳ *In post-production — {days_since_event} day(s) since the event.*"
        else:
            text += f"\n\n📅 *Event is in {abs(days_since_event)} day(s). Not happened yet.*"

    buttons = []
    if curr_stage == "Active":
        buttons.append([{"text": "📸 Mark Shoot Complete",  "callback_data": f"shoot_complete|{lead_id}"}])
    elif curr_stage == "Post-Production":
        buttons.append([{"text": "✅ Gallery Ready to Ship?", "callback_data": f"gallery_ready|{lead_id}"}])
    elif curr_stage == "Delivered":
        buttons.append([{"text": "⭐ Run Retention",  "callback_data": f"trigger_retention|{lead_id}"}])
        buttons.append([{"text": "➡️ Mark Completed", "callback_data": f"upd_proj|{lead_id}|Completed"}])

    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{lead_id}"},
        {"text": "📊 Pipeline",  "callback_data": f"view_pipe|{lead_id}"}
    ])
    buttons.append([{"text": "⬅️ Projects List", "callback_data": "nav_projects|none"}])
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, text, markup, msg_id=msg_id if method == "edit" else None, use_pipeline=use_pipeline)


def _show_client(chat_id, msg_id, client_id, method="send"):
    rows, col = read_sheet_with_headers("Clients!A1:H200")
    row = next((r for r in rows if safe_get(r, col, "Client_ID") == client_id), None)
    if not row:
        return send_msg(chat_id, f"❌ Client `{client_id}` not found.")
    tier       = safe_get(row, col, "Client_Tier")
    tier_emoji = {"VIP": "⭐", "Premium": "💎", "Standard": "🔹"}.get(tier, "👤")
    text = (
        f"{tier_emoji} *Client: {safe_get(row, col, 'Name')}*\n"
        f"ID: `{client_id}` | Tier: {tier}\n\n"
        f"✉️ Email: {safe_get(row, col, 'Email')}\n"
        f"📱 Phone: {safe_get(row, col, 'Phone')}\n"
        f"📅 Since: {safe_get(row, col, 'Created_At')}\n\n"
        f"📊 *Lifetime Stats:*\n"
        f"  📸 Bookings: {safe_get(row, col, 'Bookings')}\n"
        f"  💰 LTV: {safe_get(row, col, 'LTV')}"
    )
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    client_leads = [r for r in lead_rows if safe_get(r, lead_col, "Client_ID") == client_id]
    buttons = []
    if client_leads:
        text += f"\n\n📋 *Leads ({len(client_leads)}):*"
        for lr in client_leads[:5]:
            lid    = safe_get(lr, lead_col, "Lead_ID")
            etype  = safe_get(lr, lead_col, "Event_Type")
            status = safe_get(lr, lead_col, "Lead_Status")
            emoji  = STATUS_EMOJI.get(status.upper(), "⚪")
            text  += f"\n  • {emoji} `{lid}` — {etype}"
            buttons.append([{"text": f"{emoji} {etype} ({lid})", "callback_data": f"view_lead|{lid}"}])
    buttons.append([{"text": "⬅️ Back", "callback_data": "nav_leads|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)


def _show_call_menu(chat_id, msg_id, lead_id, use_pipeline_edit=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    row  = next((r for r in rows if safe_get(r, col, "Lead_ID") == lead_id), None)
    name = safe_get(row, col, "Full_Name") if row else lead_id
    text = (
        f"📞 *Call Outcome — {name}*\n"
        f"Lead: `{lead_id}`\n\n"
        f"What was the result of the discovery call?"
    )
    buttons = [
        [{"text": "✅ Completed — Continue",  "callback_data": f"confirm_call|{lead_id}|completed_continue"}],
        [{"text": "🛑 Completed — Not a Fit", "callback_data": f"confirm_call|{lead_id}|completed_stop"}],
        [{"text": "❌ No Show",               "callback_data": f"confirm_call|{lead_id}|no_show"}],
        [{"text": "🔄 Reschedule",            "callback_data": f"confirm_call|{lead_id}|reschedule"}],
        [{"text": "🔁 Rescheduled On-Call",   "callback_data": f"confirm_call|{lead_id}|reschedule_oncall"}],
        [{"text": "⬅️ Back to Lead",          "callback_data": f"view_lead|{lead_id}"}]
    ]
    markup = {"inline_keyboard": buttons}
    if use_pipeline_edit:
        edit_pipeline_msg(chat_id, msg_id, text, markup)
    else:
        edit_msg(chat_id, msg_id, text, markup)


def _confirm_call_out(chat_id, msg_id, lead_id, outcome, use_pipeline_edit=False):
    outcome_display = {
        "completed_continue": "✅ Completed — Continue",
        "completed_stop":     "🛑 Completed — Not a Fit",
        "no_show":            "❌ No Show",
        "reschedule":         "🔄 Reschedule",
        "reschedule_oncall":  "🔁 Rescheduled On-Call"
    }
    label = outcome_display.get(outcome, outcome)
    text = (
        f"⚠️ *Confirm Action*\n\n"
        f"Lead: `{lead_id}`\n"
        f"Action: *{label}*\n\n"
        f"Are you sure? This will update the pipeline and trigger follow-up emails."
    )
    buttons = [
        [
            {"text": "✅ Yes, confirm", "callback_data": f"call_out|{lead_id}|{outcome}"},
            {"text": "❌ Cancel",       "callback_data": f"call_menu|{lead_id}"}
        ]
    ]
    markup = {"inline_keyboard": buttons}
    if use_pipeline_edit:
        edit_pipeline_msg(chat_id, msg_id, text, markup)
    else:
        edit_msg(chat_id, msg_id, text, markup)


def _confirm_contract(chat_id, msg_id, lead_id, use_pipeline=False):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    row  = next((r for r in rows if safe_get(r, col, "Lead_ID") == lead_id), None)
    name = safe_get(row, col, "Full_Name") if row else lead_id
    pkg  = safe_get(row, col, "Primary_Package") if row else "—"

    pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    pipe_row   = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == lead_id), None)
    project_id = safe_get(pipe_row, pipe_col, "Project_ID") if pipe_row else "—"

    text = (
        f"📝 *Contract Decision — {name}*\n"
        f"Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"Package: {pkg}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Did the client confirm the proposal and is ready to sign?\n\n"
        f"✅ *YES — Send Contract*\n"
        f"  Fires System 3A:\n"
        f"  • Contract sent to client for e-signature\n"
        f"  • Pipeline updated → Contract Sent\n"
        f"  • Projects sheet updated\n"
        f"  • Telegram confirmation\n\n"
        f"❌ *NO — Close Lead*\n"
        f"  • Pipeline Tracker → Closed Lost\n"
        f"  • Lead archived automatically"
    )
    buttons = [
        [{"text": "✅ Yes — Send Contract & Fire System 3A", "callback_data": f"contract_yes|{lead_id}"}],
        [{"text": "❌ No — Close Lead",                      "callback_data": f"contract_no|{lead_id}"}],
        [{"text": "🔙 Back to Pipeline",                     "callback_data": f"view_pipe|{lead_id}"}]
    ]
    markup = {"inline_keyboard": buttons}
    if use_pipeline:
        edit_pipeline_msg(chat_id, msg_id, text, markup)
    else:
        edit_msg(chat_id, msg_id, text, markup)


def _execute_call_out(chat_id, msg_id, target_id, outcome, cb_id, use_pipeline=False):
    today_str = ph_now().strftime("%Y-%m-%d")
    mapping   = OUTCOME_MAP.get(outcome, {})

    _write_back(
        "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
        {
            "Current_Stage":    mapping.get("current_stage", "—"),
            "Call_Status":      mapping.get("call_status", "—"),
            "Last_Action":      "Discovery Call Completed",
            "Next_Action":      mapping.get("next_action", "—"),
            "Next_Action_Date": today_str
        }
    )

    label          = OUTCOME_LABELS.get(outcome, "Updated")
    confirmed_text = f"*{label}*\nLead: `{target_id}` — logged {today_str}"

    requests.post(f"{PIPELINE_API}/answerCallbackQuery", json={
        "callback_query_id": cb_id,
        "text": label
    })

    if use_pipeline:
        edit_pipeline_msg(chat_id, msg_id, confirmed_text)
    else:
        edit_msg(chat_id, msg_id, confirmed_text)

    if outcome in ("reschedule", "reschedule_oncall"):
        cancelled, result = cancel_cal_booking_for_lead(target_id)
        if cancelled:
            print(f"[CAL CANCEL] Successfully cancelled booking {result} for lead {target_id}")
        else:
            print(f"[CAL CANCEL] Warning: could not cancel booking for {target_id}: {result}")

    if outcome != "booked_for_client":
        fire_webhook(CLOSE_LEAD_WEBHOOK, {
            "lead_id":       target_id,
            "action":        outcome,
            "current_stage": mapping.get("current_stage"),
            "call_status":   mapping.get("call_status"),
            "timestamp":     today_str
        })

    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": f"✅ {label} for Lead {target_id}"
    })

# ─────────────────────────────────────────────
# WRITE-BACK HELPER
# ─────────────────────────────────────────────
def _write_back(sheet_range_prefix, sheet_header_range, id_col, target_id, updates: dict):
    rows, col = read_sheet_with_headers(sheet_header_range)
    for i, row in enumerate(rows, 2):
        if safe_get(row, col, id_col) == target_id:
            for col_name, new_value in updates.items():
                if col_name in col:
                    cl = get_col_letter(col[col_name])
                    write_sheet(f"{sheet_range_prefix}!{cl}{i}", [[new_value]])
            return True
    return False

# ─────────────────────────────────────────────
# SYSTEM 3C — DEPOSIT CONFIRMED HANDLER
# ─────────────────────────────────────────────
def _handle_deposit_confirmed(lead_id, lead_name="—", project_id="—"):
    today_str = ph_now().strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Deposit_Paid":  "TRUE",
        "Current_Stage": "Active"
    })

    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Active Project",
        "Last_Action":      "Deposit Received",
        "Next_Action":      "Prepare for Shoot",
        "Next_Action_Date": today_str
    })

    text = (
        f"💰 *Deposit Confirmed*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 Paid: {today_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Deposit marked as paid.\n"
        f"📊 Pipeline → *Active Project*\n"
        f"➡️ Next: Prepare for Shoot"
    )
    buttons = [
        [{"text": "🗂 View Project",  "callback_data": f"view_project|{lead_id}"}],
        [{"text": "📊 View Pipeline", "callback_data": f"view_pipe|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })

# ─────────────────────────────────────────────
# SYSTEM 4 — SHOOT COMPLETE HANDLER
# ─────────────────────────────────────────────
def _handle_shoot_complete(lead_id, lead_name="—", project_id="—"):
    today_str = ph_now().strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Shoot_Complete":  "TRUE",
        "Current_Stage":   "Post-Production"
    })

    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Post-Production",
        "Last_Action":      "Shoot Completed",
        "Next_Action":      "Edit & Deliver Gallery",
        "Next_Action_Date": today_str
    })

    text = (
        f"📸 *Shoot Marked Complete*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 Date: {today_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Shoot complete. Project → *Post-Production*\n"
        f"When gallery is ready, tap below to check."
    )
    buttons = [
        [{"text": "✅ Gallery Ready to Ship?", "callback_data": f"gallery_ready|{lead_id}"}],
        [{"text": "🗂 View Project",            "callback_data": f"view_project|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })

# ─────────────────────────────────────────────
# SYSTEM 4 — GALLERY READY TO SHIP CHECK
# ─────────────────────────────────────────────
def _show_gallery_ready_check(chat_id, msg_id, lead_id, use_pipeline=False):
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    proj_row = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == lead_id), None)
    if not proj_row:
        send_pipeline_msg(chat_id, f"❌ No project found for Lead `{lead_id}`.")
        return

    client_name    = safe_get(proj_row, proj_col, "Client_Name")
    event_date_raw = safe_get(proj_row, proj_col, "Event_Date")
    days_since     = None
    try:
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                event_dt   = datetime.strptime(event_date_raw.strip(), fmt)
                days_since = (ph_now() - event_dt).days
                break
            except ValueError:
                continue
    except Exception:
        pass

    if days_since is not None:
        if days_since >= 0:
            days_line = f"📅 {days_since} day(s) since the event."
        else:
            days_line = f"📅 Event is in {abs(days_since)} day(s) — hasn't happened yet."
    else:
        days_line = ""

    text = (
        f"📦 *Gallery Ready to Ship?*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Client: *{client_name}*\n"
        f"🆔 Lead: `{lead_id}`\n"
        f"{days_line}\n\n"
        f"Have you reviewed the edited photos and confirmed the gallery is ready to deliver?"
    )
    buttons = [
        [{"text": "✅ Yes — Ready to Deliver", "callback_data": f"gallery_ready_yes|{lead_id}"}],
        [{"text": "❌ No — Not Yet",           "callback_data": f"gallery_ready_no|{lead_id}"}]
    ]
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, text, markup, msg_id=msg_id, use_pipeline=use_pipeline)

# ─────────────────────────────────────────────
# SYSTEM 4 — GALLERY DELIVERY CONFIRMATION SCREEN
# ─────────────────────────────────────────────
def _show_deliver_gallery_confirm(chat_id, msg_id, lead_id, use_pipeline=False):
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    proj_row = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == lead_id), None)
    if not proj_row:
        send_pipeline_msg(chat_id, f"❌ No project found for Lead `{lead_id}`.")
        return

    client_name  = safe_get(proj_row, proj_col, "Client_Name")
    project_id   = safe_get(proj_row, proj_col, "Project_ID")
    gallery_url  = safe_get(proj_row, proj_col, "Gallery_Folder_URL")
    balance      = safe_get(proj_row, proj_col, "Balance")
    event_date   = safe_get(proj_row, proj_col, "Event_Date")

    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row     = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    client_email = safe_get(lead_row, lead_col, "Email") if lead_row else "—"

    due_date = (ph_now() + timedelta(days=14)).strftime("%Y-%m-%d")

    if gallery_url == "—" or not gallery_url:
        smart_send(
            chat_id,
            f"⚠️ *No Gallery URL Found*\n"
            f"Lead: `{lead_id}` | {client_name}\n\n"
            f"The Gallery_Folder_URL column is empty in the Projects sheet.\n"
            f"This should have been created automatically in System 3B.\n"
            f"Fix the URL in the sheet before delivering.",
            msg_id=msg_id,
            use_pipeline=use_pipeline
        )
        return

    text = (
        f"🖼️ *Confirm Gallery Delivery*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Client: *{client_name}*\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 Event: {event_date}\n"
        f"✉️ Sending to: {client_email}\n\n"
        f"📁 Gallery: {gallery_url}\n\n"
        f"💳 *This will also:*\n"
        f"  • Share the Drive folder with the client\n"
        f"  • Send the gallery delivery email\n"
        f"  • Create & send Zoho balance invoice\n"
        f"  • Balance: *${balance}* | Due: {due_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ This cannot be undone. Confirm?"
    )
    buttons = [
        [{"text": "✅ Yes — Deliver Gallery & Send Invoice", "callback_data": f"confirm_deliver|{lead_id}"}],
        [{"text": "❌ Cancel",                               "callback_data": f"view_project|{lead_id}"}]
    ]
    markup = {"inline_keyboard": buttons}
    smart_send(chat_id, text, markup, msg_id=msg_id, use_pipeline=use_pipeline)

# ─────────────────────────────────────────────
# SYSTEM 4 — GALLERY DELIVERY EXECUTOR
# FIX: Removed "Run Retention" + "View Pipeline" from success message — clean, one path forward
# ─────────────────────────────────────────────
def _execute_deliver_gallery(chat_id, msg_id, lead_id, cb_id, use_pipeline=False):
    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    proj_row = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == lead_id), None)
    if not proj_row:
        answer_callback(cb_id, "❌ Project not found", use_pipeline)
        return

    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)

    client_name  = safe_get(proj_row, proj_col, "Client_Name")
    project_id   = safe_get(proj_row, proj_col, "Project_ID")
    gallery_url  = safe_get(proj_row, proj_col, "Gallery_Folder_URL")
    balance      = safe_get(proj_row, proj_col, "Balance")
    client_email = safe_get(lead_row, lead_col, "Email")     if lead_row else "—"
    lead_name    = safe_get(lead_row, lead_col, "Full_Name") if lead_row else "—"

    today_str    = ph_now().strftime("%Y-%m-%d")
    due_date_str = (ph_now() + timedelta(days=14)).strftime("%Y-%m-%d")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage":    "Delivered",
        "Delivery_Date":    today_str,
        "Balance_Due_Date": due_date_str
    })

    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Delivered",
        "Last_Action":      "Gallery Delivered",
        "Next_Action":      "Send Retention Email",
        "Next_Action_Date": today_str
    })

    fired = fire_webhook(DELIVER_GALLERY_WEBHOOK, {
        "lead_id":          lead_id,
        "lead_name":        lead_name,
        "project_id":       project_id,
        "gallery_url":      gallery_url,
        "client_name":      client_name,
        "client_email":     client_email,
        "balance":          balance,
        "balance_due_date": due_date_str,
        "delivery_date":    today_str
    })

    answer_callback(cb_id, "🖼️ Gallery delivery triggered!", use_pipeline)

    if fired:
        success_text = (
            f"🖼️ *Gallery Delivered — System 4 Fired*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name}\n"
            f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
            f"📅 Delivered: {today_str}\n"
            f"📁 [View Gallery]({gallery_url})\n\n"
            f"✅ Drive folder shared with client.\n"
            f"📧 Gallery delivery email queued.\n"
            f"💳 Balance invoice sent via Zoho.\n"
            f"📊 Balance due: *${balance}* by {due_date_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Pipeline → *Delivered*\n"
            f"Zapier will send a confirmation — then run retention."
        )
        # FIX: Removed "Run Retention" + "View Pipeline" from here — they'll appear in /gallery_notify
        buttons = [
            [{"text": "🗂 View Project", "callback_data": f"view_project|{lead_id}"}]
        ]
        smart_send(chat_id, success_text, {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)
    else:
        fail_text = (
            f"⚠️ *Webhook Failed — System 4 Did Not Fire*\n"
            f"Lead: `{lead_id}`\n\n"
            f"Sheets have been updated but Zapier was not triggered.\n"
            f"Check DELIVER_GALLERY_WEBHOOK in Railway env vars and retry."
        )
        smart_send(chat_id, fail_text, msg_id=msg_id, use_pipeline=use_pipeline)

# ─────────────────────────────────────────────
# SYSTEM 5A — CLIENT STATS CALCULATOR
# ─────────────────────────────────────────────
def _update_client_stats(lead_id):
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row  = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
    if not lead_row:
        return None
    client_id = safe_get(lead_row, lead_col, "Client_ID")

    proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
    client_projects = [r for r in proj_rows if safe_get(r, proj_col, "Client_ID") == client_id]

    total_ltv = 0
    bookings  = len(client_projects)
    for p in client_projects:
        try:
            total_ltv += float(
                str(safe_get(p, proj_col, "Total_Price"))
                .replace(",", "").replace("$", "") or 0
            )
        except ValueError:
            pass

    if bookings >= 3 or total_ltv >= 50000:
        tier = "VIP"
    elif bookings >= 2 or total_ltv >= 20000:
        tier = "Premium"
    else:
        tier = "Standard"

    _write_back("Clients", "Clients!A1:H200", "Client_ID", client_id, {
        "LTV":         str(int(total_ltv)),
        "Bookings":    str(bookings),
        "Client_Tier": tier
    })

    return {"ltv": int(total_ltv), "bookings": bookings, "tier": tier}

# ─────────────────────────────────────────────
# SHARED CALLBACK HANDLER
# FIX: All nav_ callbacks now pass msg_id → edits in-place instead of sending new messages
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
        _show_client(chat_id, msg_id, target_id, method="edit")

    # FIX: All nav_ callbacks now pass msg_id so they edit in-place — no new message spam
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

    elif action == "upd_lead" and len(parts) > 2:
        new_status = parts[2]
        _write_back("Leads", "Leads!A1:T200", "Lead_ID", target_id,
                    {"Lead_Status": new_status})
        answer_callback(cb["id"], f"✅ Status updated to {new_status}", use_pipeline)
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "upd_pipe" and len(parts) > 2:
        new_stage = parts[2]
        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
                    {"Current_Stage": new_stage, "Last_Action": f"Stage moved to {new_stage}",
                     "Next_Action_Date": today_str})
        answer_callback(cb["id"], f"✅ Stage → {new_stage}", use_pipeline)
        _show_pipeline(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "upd_proj" and len(parts) > 2:
        new_stage = parts[2]
        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id,
                    {"Current_Stage": new_stage})
        answer_callback(cb["id"], f"✅ Project stage → {new_stage}", use_pipeline)
        _show_project(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "call_menu":
        _show_call_menu(chat_id, msg_id, target_id, use_pipeline_edit=use_pipeline)

    elif action == "confirm_call" and len(parts) > 2:
        outcome = parts[2]
        _confirm_call_out(chat_id, msg_id, target_id, outcome, use_pipeline_edit=use_pipeline)

    elif action == "call_out" and len(parts) > 2:
        outcome = parts[2]
        _execute_call_out(chat_id, msg_id, target_id, outcome, cb["id"], use_pipeline=use_pipeline)

    elif action == "send_proposal":
        fired = fire_webhook(PROPOSAL_ZAPIER_WEBHOOK, {"lead_id": target_id})
        answer_callback(cb["id"], "📄 Proposal triggered", use_pipeline)
        msg_text = (
            f"📄 Proposal generation triggered for `{target_id}`\nCheck your email and Telegram for confirmation."
            if fired else
            f"⚠️ Webhook failed — check PROPOSAL_ZAPIER_WEBHOOK in Railway."
        )
        send_msg(chat_id, msg_text)

    elif action == "send_contract":
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "confirm_contract":
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "contract_yes":
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        budget   = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""
        name     = safe_get(lead_row, lead_col, "Full_Name") if lead_row else target_id

        if budget in UNCERTAIN_BUDGETS:
            answer_callback(cb["id"], "Budget unclear — enter exact amount below", use_pipeline)
            prompt_text = (
                f"💰 *Budget Confirmation Required*\n"
                f"Lead: `{target_id}` — {name}\n"
                f"Budget on file: *{budget}*\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Type the confirmed total contract amount in the chat:\n\n"
                f"`/setbudget {target_id} <amount>`\n\n"
                f"Example:\n"
                f"`/setbudget {target_id} 13000`"
            )
            if use_pipeline:
                edit_pipeline_msg(chat_id, msg_id, prompt_text)
            else:
                edit_msg(chat_id, msg_id, prompt_text)
        else:
            fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {"lead_id": target_id})
            answer_callback(cb["id"], "✅ Contract triggered — System 3A firing", use_pipeline)
            confirmed_text = (
                f"✅ *Contract Triggered — System 3A*\n"
                f"Lead: `{target_id}`\n\n"
                f"• Contract sent to client for e-signature\n"
                f"• Pipeline Tracker will auto-update → Contract Sent\n"
                f"• Watch Telegram for confirmation"
            ) if fired else (
                f"⚠️ *Webhook failed for `{target_id}`*\n"
                f"Check CONTRACT_ZAPIER_WEBHOOK in Railway and retry."
            )
            if use_pipeline:
                edit_pipeline_msg(chat_id, msg_id, confirmed_text)
            else:
                edit_msg(chat_id, msg_id, confirmed_text)

    elif action == "contract_no":
        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage":    "Closed Lost",
                "Last_Action":      "Closed — Client Did Not Confirm Proposal",
                "Next_Action":      "Archive Lead",
                "Next_Action_Date": today_str
            }
        )
        answer_callback(cb["id"], "❌ Lead closed — Pipeline updated", use_pipeline)
        closed_text = (
            f"❌ *Lead Closed — No Contract*\n"
            f"Lead: `{target_id}`\n\n"
            f"Pipeline Tracker updated:\n"
            f"• Current Stage → Closed Lost\n"
            f"• Last Action → Closed — Client Did Not Confirm Proposal\n"
            f"• Date: {today_str}\n\n"
            f"Lead has been archived."
        )
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, closed_text)
        else:
            edit_msg(chat_id, msg_id, closed_text)

    elif action == "budget_contract_yes":
        try:
            total = int(parts[2])
        except (IndexError, ValueError):
            answer_callback(cb["id"], "❌ Invalid amount — use /setbudget to retry", use_pipeline)
            return jsonify({"status": "ok"})
        deposit = round(total * 0.30)
        balance = total - deposit
        fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {
            "lead_id":     target_id,
            "total_price": str(total),
            "deposit":     str(deposit),
            "balance":     str(balance)
        })
        answer_callback(cb["id"], "✅ System 3A fired with confirmed amounts", use_pipeline)
        confirmed_text = (
            f"✅ *Contract Triggered — System 3A*\n"
            f"Lead: `{target_id}`\n\n"
            f"💵 Total:        *${total:,}*\n"
            f"💳 Deposit 30%: *${deposit:,}*\n"
            f"📊 Balance 70%: *${balance:,}*\n\n"
            f"• Contract sent to client for e-signature\n"
            f"• Watch Telegram for confirmation"
        ) if fired else (
            f"⚠️ *Webhook failed for `{target_id}`*\n"
            f"Check CONTRACT_ZAPIER_WEBHOOK in Railway and retry."
        )
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, confirmed_text)
        else:
            edit_msg(chat_id, msg_id, confirmed_text)

    elif action == "budget_contract_edit":
        answer_callback(cb["id"], "Enter a new amount", use_pipeline)
        prompt_text = (
            f"✏️ *Re-enter Budget Amount*\n"
            f"Lead: `{target_id}`\n\n"
            f"Type the corrected amount:\n"
            f"`/setbudget {target_id} <amount>`\n\n"
            f"Example:\n`/setbudget {target_id} 13000`"
        )
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, prompt_text)
        else:
            edit_msg(chat_id, msg_id, prompt_text)

    # ── SYSTEM 3C — Deposit Paid ──
    elif action == "deposit_paid":
        pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
        pipe_row   = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == target_id), None)
        project_id = safe_get(pipe_row, pipe_col, "Project_ID") if pipe_row else "—"
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row   = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        lead_name  = safe_get(lead_row, lead_col, "Full_Name") if lead_row else "—"

        answer_callback(cb["id"], "💰 Deposit confirmed — updating records", use_pipeline)
        _handle_deposit_confirmed(target_id, lead_name, project_id)

    # ── SYSTEM 4 — Shoot Complete ──
    elif action == "shoot_complete":
        pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
        pipe_row   = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == target_id), None)
        project_id = safe_get(pipe_row, pipe_col, "Project_ID") if pipe_row else "—"
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row   = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        lead_name  = safe_get(lead_row, lead_col, "Full_Name") if lead_row else "—"

        answer_callback(cb["id"], "📸 Shoot marked complete", use_pipeline)
        _handle_shoot_complete(target_id, lead_name, project_id)

    # ── SYSTEM 4 — Gallery Ready to Ship? (Yes/No check) ──
    elif action == "gallery_ready":
        answer_callback(cb["id"], "Checking project...", use_pipeline)
        _show_gallery_ready_check(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    # ── SYSTEM 4 — Gallery Ready: Yes → show delivery confirmation ──
    elif action == "gallery_ready_yes":
        answer_callback(cb["id"], "Loading delivery details...", use_pipeline)
        _show_deliver_gallery_confirm(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    # ── SYSTEM 4 — Gallery Ready: No → back to project ──
    elif action == "gallery_ready_no":
        answer_callback(cb["id"], "Got it — back to project.", use_pipeline)
        _show_project(chat_id, msg_id, target_id, method="edit", use_pipeline=use_pipeline)

    # ── SYSTEM 4 — Deliver Gallery (Execute after double confirm) ──
    elif action == "confirm_deliver":
        _execute_deliver_gallery(chat_id, msg_id, target_id, cb["id"], use_pipeline=use_pipeline)

    # ── SYSTEM 5A — Trigger Retention ──
    elif action == "trigger_retention":
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
        pipe_row     = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == target_id), None)
        project_id   = safe_get(pipe_row, pipe_col, "Project_ID")  if pipe_row else "—"
        client_name  = safe_get(lead_row, lead_col, "Full_Name")   if lead_row else "—"
        client_email = safe_get(lead_row, lead_col, "Email")        if lead_row else "—"
        event_type   = safe_get(lead_row, lead_col, "Event_Type")   if lead_row else "—"

        stats    = _update_client_stats(target_id)
        new_ltv  = stats["ltv"]      if stats else "—"
        new_tier = stats["tier"]     if stats else "—"
        bookings = stats["bookings"] if stats else "—"

        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    "Retention",
            "Last_Action":      "Gallery Delivered",
            "Next_Action":      "Await Review",
            "Next_Action_Date": today_str
        })

        fired = fire_webhook(RETENTION_WEBHOOK, {
            "lead_id":      target_id,
            "project_id":   project_id,
            "client_name":  client_name,
            "client_email": client_email,
            "event_type":   event_type,
            "new_ltv":      new_ltv,
            "new_tier":     new_tier,
            "bookings":     bookings
        })

        answer_callback(cb["id"], "⭐ Retention triggered", use_pipeline)
        msg_text = (
            f"⭐ *Retention Sequence Triggered*\n"
            f"Lead: `{target_id}` | {client_name}\n\n"
            f"• Review request email queued\n"
            f"• Rebooking upsell scheduled for +7 days\n"
            f"• Client LTV updated → ${new_ltv} | {new_tier}\n"
            f"• Watch Telegram for confirmation"
        ) if fired else (
            f"⚠️ Webhook failed — check RETENTION_WEBHOOK in Railway."
        )
        send_pipeline_msg(chat_id, msg_text)

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
        elif text.startswith("/search"):
            query = text[len("/search"):].strip()
            handle_search_command(chat_id, query)
        elif text.startswith("/client"):
            cid = text[len("/client"):].strip()
            handle_client_command(chat_id, cid)
        elif text.startswith("/updateemail"):
            parts = text.split()
            if len(parts) != 3:
                send_msg(chat_id, "Usage: /updateemail <Lead_ID> <new_email>")
                return jsonify({"status": "ok"})
            lead_id, new_email = parts[1], parts[2]
            rows, col = read_sheet_with_headers("Leads!A1:T200")
            found = False
            for i, row in enumerate(rows):
                if safe_get(row, col, "Lead_ID") == lead_id:
                    row_num = i + 2
                    col_idx = col.get("Email")
                    if col_idx is None:
                        send_msg(chat_id, "⚠️ Email column not found in sheet.")
                        return jsonify({"status": "ok"})
                    col_letter = get_col_letter(col_idx)
                    write_sheet(f"Leads!{col_letter}{row_num}", [[new_email]])
                    send_msg(chat_id, f"✅ Email updated for {lead_id} → {new_email}")
                    found = True
                    break
            if not found:
                send_msg(chat_id, f"❌ Lead {lead_id} not found.")
        elif text == "/resetleadcounter":
            write_sheet("Config!A2", [[0]])
            send_msg(chat_id, "✅ Lead counter reset to 0. Next lead will be LED-0001.")
        elif text.startswith("/retention"):
            parts = text.split()
            if len(parts) != 2:
                send_msg(chat_id, "Usage: /retention <Lead_ID>")
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
            lead_row     = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
            pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
            pipe_row     = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == lead_id), None)
            project_id   = safe_get(pipe_row, pipe_col, "Project_ID")  if pipe_row else "—"
            client_name  = safe_get(lead_row, lead_col, "Full_Name")   if lead_row else "—"
            client_email = safe_get(lead_row, lead_col, "Email")        if lead_row else "—"
            event_type   = safe_get(lead_row, lead_col, "Event_Type")   if lead_row else "—"

            stats    = _update_client_stats(lead_id)
            new_ltv  = stats["ltv"]      if stats else "—"
            new_tier = stats["tier"]     if stats else "—"
            bookings = stats["bookings"] if stats else "—"

            today_str = ph_now().strftime("%Y-%m-%d")
            _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
                "Current_Stage":    "Retention",
                "Last_Action":      "Gallery Delivered",
                "Next_Action":      "Await Review",
                "Next_Action_Date": today_str
            })

            fired = fire_webhook(RETENTION_WEBHOOK, {
                "lead_id":      lead_id,
                "project_id":   project_id,
                "client_name":  client_name,
                "client_email": client_email,
                "event_type":   event_type,
                "new_ltv":      new_ltv,
                "new_tier":     new_tier,
                "bookings":     bookings
            })
            if fired:
                send_msg(chat_id,
                    f"⭐ *Retention sequence triggered for `{lead_id}`*\n"
                    f"Review request email queued.\n"
                    f"Rebooking upsell scheduled +7 days.\n"
                    f"Client LTV: ${new_ltv} | Tier: {new_tier}"
                )
            else:
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
            name = safe_get(lead_row, lead_col, "Full_Name") if lead_row else lead_id
            pkg  = safe_get(lead_row, lead_col, "Primary_Package") if lead_row else "—"
            text_msg = (
                f"💰 *Confirm Contract Amount*\n"
                f"Lead: `{lead_id}` — {name}\n"
                f"Package: {pkg}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Total:        *${total:,}*\n"
                f"💳 Deposit 30%: *${deposit:,}*\n"
                f"📊 Balance 70%: *${balance:,}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Fire System 3A with these amounts?"
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
                    "Example: `/setbudget LED-0001 13000`"
                )
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            try:
                total = int(float(parts[2].replace("$", "").replace(",", "").strip()))
            except ValueError:
                send_pipeline_msg(chat_id,
                    "❌ Invalid amount — numbers only.\n"
                    "Example: `/setbudget LED-0001 13000`"
                )
                return jsonify({"status": "ok"})
            deposit = round(total * 0.30)
            balance = total - deposit
            lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
            lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)
            name = safe_get(lead_row, lead_col, "Full_Name") if lead_row else lead_id
            pkg  = safe_get(lead_row, lead_col, "Primary_Package") if lead_row else "—"
            text_msg = (
                f"💰 *Confirm Contract Amount*\n"
                f"Lead: `{lead_id}` — {name}\n"
                f"Package: {pkg}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Total:        *${total:,}*\n"
                f"💳 Deposit 30%: *${deposit:,}*\n"
                f"📊 Balance 70%: *${balance:,}*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Fire System 3A with these amounts?"
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
    lead_id      = data.get("lead_id", "—")
    client_name  = data.get("client_name", "—")
    client_email = data.get("client_email", "—")
    lead_status  = data.get("lead_status", "—")
    urgency      = data.get("urgency_score", "—")
    package      = data.get("primary_package", "—")
    summary      = data.get("ai_summary", "—")
    call_time    = data.get("call_time", "—")

    text = (
        f"📅 DISCOVERY CALL BOOKED\n"
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
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

@app.route("/proposal_notify", methods=["POST"])
def proposal_notify():
    data          = request.json
    lead_id       = data.get("lead_id", "—")
    project_id    = data.get("project_id", "—")
    client_name   = data.get("client_name", "—")
    proposal_link = data.get("proposal_link", "—")
    event_type    = data.get("event_type", "—")
    event_date    = data.get("event_date", "—")
    package       = data.get("package", "—")

    text = (
        f"📤 *Proposal Sent*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Client: {client_name}\n"
        f"🆔 Lead ID: `{lead_id}`\n"
        f"🗂 Project ID: `{project_id}`\n"
        f"📅 Event: {event_type} — {event_date}\n"
        f"📦 Package: {package}\n"
        f"📄 [View Proposal Doc]({proposal_link})\n"
        f"📊 Stage: Proposal Sent\n"
        f"✅ Pipeline updated automatically.\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"When the client confirms, tap below to send the contract."
    )
    buttons = [
        [{"text": "📝 Send Contract", "callback_data": f"confirm_contract|{lead_id}"}],
        [{"text": "📊 View Pipeline", "callback_data": f"view_pipe|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# SYSTEM 3B — /invoice_sent
# ─────────────────────────────────────────────
@app.route("/invoice_sent", methods=["POST"])
def invoice_sent():
    data         = request.json
    lead_id      = data.get("lead_id",      "—")
    lead_name    = data.get("lead_name",    "—")
    project_id   = data.get("project_id",   "—")
    package      = data.get("package",      "—")
    deposit      = data.get("deposit",      "—")
    invoice_date = data.get("invoice_date", "—")
    due_date     = data.get("due_date",     "—")

    text = (
        f"💳 *Deposit Invoice Sent*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📦 Package: {package}\n"
        f"💵 Deposit Due: *${deposit}* by {due_date}\n"
        f"📅 Invoiced: {invoice_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Contract signed. Zoho invoice sent.\n"
        f"Tap below when payment is confirmed in Zoho."
    )
    buttons = [
        [{"text": "💰 Mark Deposit Paid", "callback_data": f"deposit_paid|{lead_id}"}],
        [{"text": "📊 View Pipeline",     "callback_data": f"view_pipe|{lead_id}"}],
        [{"text": "🗂 View Project",      "callback_data": f"view_project|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# SYSTEM 3C — /deposit_confirmed (HTTP fallback)
# ─────────────────────────────────────────────
@app.route("/deposit_confirmed", methods=["POST"])
def deposit_confirmed():
    data       = request.json
    lead_id    = data.get("lead_id",    "—")
    lead_name  = data.get("lead_name",  "—")
    project_id = data.get("project_id", "—")
    _handle_deposit_confirmed(lead_id, lead_name, project_id)
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# SYSTEM 4 — /gallery_notify
# Called by Zapier 4 after all steps complete
# ─────────────────────────────────────────────
@app.route("/gallery_notify", methods=["POST"])
def gallery_notify():
    data         = request.json
    lead_id      = data.get("lead_id",      "—")
    lead_name    = data.get("lead_name",    "—")
    project_id   = data.get("project_id",   "—")
    gallery_url  = data.get("gallery_url",  "—")
    delivery_date = data.get("delivery_date", ph_now().strftime("%Y-%m-%d"))

    text = (
        f"✅ *System 4 Complete — Gallery Delivered*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📅 Delivered: {delivery_date}\n"
        f"📁 [View Gallery]({gallery_url})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ All Zapier steps done.\n"
        f"• Drive folder shared with client\n"
        f"• Gallery email delivered\n"
        f"• Balance invoice sent via Zoho\n\n"
        f"Ready to run the retention sequence?"
    )
    buttons = [
        [{"text": "⭐ Run Retention",  "callback_data": f"trigger_retention|{lead_id}"}],
        [{"text": "🗂 View Project",   "callback_data": f"view_project|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# SYSTEM 5A — /retention_notify
# ─────────────────────────────────────────────
@app.route("/retention_notify", methods=["POST"])
def retention_notify():
    data        = request.json
    lead_id     = data.get("lead_id",     "—")
    client_name = data.get("client_name", "—")
    project_id  = data.get("project_id",  "—")
    event_type  = data.get("event_type",  "—")
    new_ltv     = data.get("new_ltv",     "—")
    new_tier    = data.get("new_tier",    "—")
    bookings    = data.get("bookings",    "—")

    tier_emoji = {"VIP": "⭐", "Premium": "💎", "Standard": "🔹"}.get(new_tier, "👤")

    today_str = ph_now().strftime("%Y-%m-%d")
    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage":    "Completed",
        "Review_Sent":      "TRUE",
        "Review_Sent_Date": today_str
    })

    text = (
        f"⭐ *Retention Sequence Triggered*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name} | {tier_emoji} {new_tier}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📸 Event: {event_type}\n\n"
        f"📊 *Updated Client Stats:*\n"
        f"  💰 Lifetime Value: ${new_ltv}\n"
        f"  📅 Total Bookings: {bookings}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Review request sent.\n"
        f"📧 Rebooking email queued for +7 days."
    )
    buttons = [
        [{"text": "👤 View Lead",    "callback_data": f"view_lead|{lead_id}"}],
        [{"text": "🗂 View Project", "callback_data": f"view_project|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# SYSTEM 5B — /retention_5b_notify
# ─────────────────────────────────────────────
@app.route("/retention_5b_notify", methods=["POST"])
def retention_5b_notify():
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
        f"📧 *Rebooking Email Sent — System 5B*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📸 Event: {event_type}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Rebooking upsell email delivered.\n"
        f"📊 Pipeline → *Closed Won*"
    )
    buttons = [
        [{"text": "👤 View Lead",    "callback_data": f"view_lead|{lead_id}"}],
        [{"text": "🗂 View Project", "callback_data": f"view_project|{lead_id}"}]
    ]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id":      CHAT_ID,
        "text":         text,
        "parse_mode":   "Markdown",
        "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
