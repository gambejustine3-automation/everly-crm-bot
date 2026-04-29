from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ─────────────────────────────────────────────
# ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
BOT_TOKEN                = os.environ.get("BOT_TOKEN")           # Everly&Co.ClientBot
PIPELINE_BOT_TOKEN       = os.environ.get("PIPELINE_BOT_TOKEN") # Everly&Co.PipelineTrackerBot
DASHBOARD_BOT_TOKEN      = os.environ.get("DASHBOARD_BOT_TOKEN")# Everly&Co.CRMDashboardBot
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
    "Proposal Sent", "Contracted", "Active Project", "Closed Won", "Closed Lost"
]
PROJECT_STAGES = [
    "Pre-Production", "Active", "Post-Production", "Delivered", "Completed", "Closed"
]
STATUS_EMOJI = {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}

# Budgets that require Victoria to confirm the exact amount before firing System 3A
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
    "completed_continue":  {"current_stage": "Discovery Call Completed", "call_status": "Completed",                     "next_action": "Send Proposal"},
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

def answer_callback(cb_id, text):
    requests.post(f"{DASHBOARD_API}/answerCallbackQuery", json={
        "callback_query_id": cb_id,
        "text": text
    })

# ─────────────────────────────────────────────
# TELEGRAM SEND/EDIT HELPERS
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
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
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
# CAL.COM API HELPERS
# ─────────────────────────────────────────────
def cancel_cal_booking_for_lead(lead_id):
    """Search Cal.com v2 for a booking matching this lead_id and cancel it."""
    if not CAL_API_KEY:
        print("[CAL CANCEL] CAL_API_KEY not set.")
        return False, "no_api_key"
    try:
        # Fetch upcoming bookings via v2 API
        r = requests.get(
            "https://api.cal.com/v2/bookings",
            headers={
                "Authorization": f"Bearer {CAL_API_KEY}",
                "cal-api-version": "2024-08-13"
            },
            params={"status": "upcoming"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        bookings = data if isinstance(data, list) else data.get("bookings", [])

        # Find booking whose responses or metadata contains this lead_id
        target_uid = None
        for b in bookings:
            print(f"[CAL DEBUG] booking responses: {b.get('responses')} | metadata: {b.get('metadata')}")
            responses = b.get("responses", {})
            metadata  = b.get("metadata", {})
            raw = responses.get("lead_id") or metadata.get("lead_id")
            booking_lead_id = (
                raw.get("value") if isinstance(raw, dict) else raw
            )
            if booking_lead_id == lead_id:
                target_uid = b.get("uid")
                break

        if not target_uid:
            print(f"[CAL CANCEL] No booking found for lead {lead_id}")
            return False, "not_found"

        # Cancel via v2 endpoint
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
    """Fetch bookings for a given date via Cal.com v2 API."""
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
        return r.json().get("data", {}).get("bookings", [])
    except Exception as e:
        print(f"[CAL ERROR] {e}")
        return []


def parse_cal_booking(booking):
    """Parse a Cal.com v2 booking object into a flat dict."""
    attendees    = booking.get("attendees", [])
    client_name  = attendees[0].get("name", "Unknown") if attendees else "Unknown"
    client_email = attendees[0].get("email", "—") if attendees else "—"
    responses    = booking.get("responses", {})
    metadata     = booking.get("metadata", {})
    lead_id = (
        responses.get("lead_id", {}).get("value")
        or metadata.get("lead_id")
        or "—"
    )
    start_raw = booking.get("start", booking.get("startTime", ""))
    try:
        # v2 returns ISO 8601 with timezone
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
        "`/deliver <Lead_ID> <drive_url>` — Trigger gallery delivery\n"
        "`/retention <Lead_ID>` — Trigger post-delivery review + retention sequence\n"
        "`/setbudget <Lead_ID> <amount>` — Confirm exact amount for $10k+/TBD leads before sending contract\n\n"
        "💡 *Tips*\n"
        "• Tap any lead or client button to drill in\n"
        "• Update lead status (HOT/WARM/COLD) from the lead card\n"
        "• Log call outcomes right after a discovery call\n"
        "• Move pipeline stages with one tap"
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

def handle_leads_command(chat_id):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    if not rows:
        return send_msg(chat_id, "📭 No leads found.")
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
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_hot_command(chat_id):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    hot_rows = [r for r in rows if safe_get(r, col, "Lead_Status").upper() == "HOT"]
    if not hot_rows:
        return send_msg(chat_id, "🔴 No HOT leads right now.")
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
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_today_command(chat_id):
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
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_schedule_command(chat_id, date_str=None, date_label=None):
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
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_tomorrow_command(chat_id):
    tomorrow     = ph_now() + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    tomorrow_lbl = tomorrow.strftime("%B %d, %Y")
    handle_schedule_command(chat_id, date_str=tomorrow_str, date_label=f"Tomorrow — {tomorrow_lbl}")

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

def handle_pipeline_command(chat_id, use_pipeline=False):
    rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    if not rows:
        return send_msg(chat_id, "📭 Pipeline is empty.")
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
    if use_pipeline:
        send_pipeline_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})
    else:
        send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_project_command(chat_id):
    rows, col = read_sheet_with_headers("Projects!A1:V200")
    if not rows:
        return send_msg(chat_id, "📭 No projects found.")
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
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

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
            {"text": "👤 Client Card", "callback_data": f"view_client|{client_id}"} if client_id else {"text": "👤 No Client Data", "callback_data": "none"}
        ],
        [{"text": "⬅️ Back to Leads", "callback_data": "nav_leads|none"}]
    ]
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, text, markup)
        else:
            edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

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
    curr_idx    = PIPELINE_STAGES.index(curr_stage) if curr_stage in PIPELINE_STAGES else -1
    next_stages = PIPELINE_STAGES[curr_idx + 1: curr_idx + 4]
    buttons     = [[{"text": f"➡️ {s}", "callback_data": f"upd_pipe|{target_id}|{s}"}] for s in next_stages]
    action_row = []
    if curr_stage == "Discovery Call Booked":
        action_row = [{"text": "📞 Call Outcome",      "callback_data": f"call_menu|{target_id}"}]
    elif curr_stage == "Discovery Call Completed":
        action_row = [{"text": "📄 Send Proposal",     "callback_data": f"send_proposal|{target_id}"}]
    elif curr_stage == "Proposal Sent":
        action_row = [{"text": "📝 Send Contract",     "callback_data": f"send_contract|{target_id}"}]
    elif curr_stage == "Contracted":
        action_row = [{"text": "💰 Mark Deposit Paid", "callback_data": f"deposit_paid|{target_id}"}]
    elif curr_stage == "Active Project":
        action_row = [{"text": "🖼️ Deliver Gallery",  "callback_data": f"deliver_gallery|{target_id}"}]
    if action_row:
        buttons.append(action_row)
    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{target_id}"},
        {"text": "🗂 Project",   "callback_data": f"view_project|{target_id}"}
    ])
    buttons.append([{"text": "⬅️ Back to Pipeline", "callback_data": "nav_pipe|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, text, markup)
        else:
            edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

def _show_project(chat_id, msg_id, lead_id, method="edit", use_pipeline=False):
    rows, col = read_sheet_with_headers("Projects!A1:V200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == lead_id), None)
    if not row:
        return send_msg(chat_id, f"❌ No project found for Lead `{lead_id}`.")
    curr_stage = safe_get(row, col, "Current_Stage")
    text = (
        f"🗂 *Project: {safe_get(row, col, 'Client_Name')}*\n"
        f"Project ID: `{safe_get(row, col, 'Project_ID')}`\n"
        f"Lead ID: `{lead_id}`\n\n"
        f"📅 Event Date: {safe_get(row, col, 'Event_Date')}\n"
        f"📦 Package: {safe_get(row, col, 'Package')}\n"
        f"💰 Total: {safe_get(row, col, 'Total_Price')}\n"
        f"💵 Deposit: {safe_get(row, col, 'Deposit')} | Paid: {safe_get(row, col, 'Deposit_Paid')}\n"
        f"📊 Balance: {safe_get(row, col, 'Balance')}\n\n"
        f"📍 Stage: *{curr_stage}*\n"
        f"📝 Contract: {safe_get(row, col, 'Contract_Sent')} ({safe_get(row, col, 'Contract_Date')})\n"
        f"🖼️ Gallery: {safe_get(row, col, 'Gallery_Folder_URL')}\n"
        f"📦 Delivered: {safe_get(row, col, 'Delivery_Date')}\n"
        f"⭐ Review: {safe_get(row, col, 'Review')}"
    )
    curr_idx    = PROJECT_STAGES.index(curr_stage) if curr_stage in PROJECT_STAGES else -1
    next_stages = PROJECT_STAGES[curr_idx + 1: curr_idx + 3]
    buttons     = [[{"text": f"➡️ {s}", "callback_data": f"upd_proj|{lead_id}|{s}"}] for s in next_stages]
    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{lead_id}"},
        {"text": "📊 Pipeline",  "callback_data": f"view_pipe|{lead_id}"}
    ])
    buttons.append([{"text": "⬅️ Projects List", "callback_data": "nav_projects|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        if use_pipeline:
            edit_pipeline_msg(chat_id, msg_id, text, markup)
        else:
            edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

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
    """Confirmation screen before firing System 3A or closing the lead."""
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

    # 1. Write pipeline update to Google Sheets
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

    # 2. Build response text
    label          = OUTCOME_LABELS.get(outcome, "Updated")
    confirmed_text = f"*{label}*\nLead: `{target_id}` — logged {today_str}"

    # 3. Answer Telegram callback FIRST — must happen within 5 seconds
    requests.post(f"{PIPELINE_API}/answerCallbackQuery", json={
        "callback_query_id": cb_id,
        "text": label
    })

    # 4. Edit the message immediately so buttons disappear
    if use_pipeline:
        edit_pipeline_msg(chat_id, msg_id, confirmed_text)
    else:
        edit_msg(chat_id, msg_id, confirmed_text)

    # 5. Cancel Cal.com booking — runs after Telegram is already updated
    if outcome in ("reschedule", "reschedule_oncall"):
        cancelled, result = cancel_cal_booking_for_lead(target_id)
        if not cancelled:
            print(f"[CAL CANCEL] Warning: could not cancel booking for {target_id}: {result}")

    # 6. Fire Zapier webhook for all outcomes except internal-only booked_for_client
    if outcome != "booked_for_client":
        fire_webhook(CLOSE_LEAD_WEBHOOK, {
            "lead_id":       target_id,
            "action":        outcome,
            "current_stage": mapping.get("current_stage"),
            "call_status":   mapping.get("call_status"),
            "timestamp":     today_str
        })

    # 7. Send confirmation message to Pipeline Bot chat
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

    if action == "view_lead":
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "view_pipe":
        _show_pipeline(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "view_project":
        _show_project(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "view_client":
        _show_client(chat_id, msg_id, target_id, method="edit")
    elif action == "nav_leads":
        handle_leads_command(chat_id)
    elif action == "nav_hot":
        handle_hot_command(chat_id)
    elif action == "nav_pipe":
        handle_pipeline_command(chat_id, use_pipeline=use_pipeline)
    elif action == "nav_projects":
        handle_project_command(chat_id)
    elif action == "nav_schedule":
        if target_id == "tomorrow":
            handle_tomorrow_command(chat_id)
        else:
            handle_schedule_command(chat_id)
    elif action == "nav_today":
        handle_today_command(chat_id)
    elif action == "upd_lead" and len(parts) > 2:
        new_status = parts[2]
        _write_back("Leads", "Leads!A1:T200", "Lead_ID", target_id,
                    {"Lead_Status": new_status})
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": f"✅ Status updated to {new_status}"
        })
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "upd_pipe" and len(parts) > 2:
        new_stage = parts[2]
        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
                    {"Current_Stage": new_stage, "Last_Action": f"Stage moved to {new_stage}",
                     "Next_Action_Date": today_str})
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": f"✅ Stage → {new_stage}"
        })
        _show_pipeline(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "upd_proj" and len(parts) > 2:
        new_stage = parts[2]
        _write_back("Projects", "Projects!A1:V200", "Lead_ID", target_id,
                    {"Current_Stage": new_stage})
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": f"✅ Project stage → {new_stage}"
        })
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
        if fired:
            send_msg(chat_id, f"📄 Proposal generation triggered for `{target_id}`\nCheck your email and Telegram for confirmation.")
        else:
            send_msg(chat_id, f"⚠️ Webhook failed — check PROPOSAL_ZAPIER_WEBHOOK in Railway.")
    elif action == "send_contract":
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "confirm_contract":
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)
    elif action == "contract_yes":
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        budget   = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""
        name     = safe_get(lead_row, lead_col, "Full_Name") if lead_row else target_id
        api      = PIPELINE_API if use_pipeline else DASHBOARD_API

        if budget in UNCERTAIN_BUDGETS:
            requests.post(f"{api}/answerCallbackQuery", json={
                "callback_query_id": cb["id"],
                "text": "Budget unclear — enter exact amount below"
            })
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
            requests.post(f"{api}/answerCallbackQuery", json={
                "callback_query_id": cb["id"],
                "text": "✅ Contract triggered — System 3A firing"
            })
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
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": "❌ Lead closed — Pipeline updated"
        })
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
        # callback_data: budget_contract_yes|{lead_id}|{total_int}
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        try:
            total = int(parts[2])
        except (IndexError, ValueError):
            requests.post(f"{api}/answerCallbackQuery", json={
                "callback_query_id": cb["id"],
                "text": "❌ Invalid amount — use /setbudget to retry"
            })
            return jsonify({"status": "ok"})
        deposit = round(total * 0.30)
        balance = total - deposit
        fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {
            "lead_id":     target_id,
            "total_price": str(total),
            "deposit":     str(deposit),
            "balance":     str(balance)
        })
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": "✅ System 3A fired with confirmed amounts"
        })
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
        api = PIPELINE_API if use_pipeline else DASHBOARD_API
        requests.post(f"{api}/answerCallbackQuery", json={
            "callback_query_id": cb["id"],
            "text": "Enter a new amount"
        })
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
    elif action == "deposit_paid":
        fired = fire_webhook(DEPOSIT_PAID_WEBHOOK, {"lead_id": target_id})
        if fired:
            send_msg(chat_id, f"💰 Deposit marked as paid for `{target_id}`\nSheets updated. Confirmation sent to client.")
        else:
            send_msg(chat_id, f"⚠️ Webhook failed — check DEPOSIT_PAID_WEBHOOK in Railway.")
    elif action == "deliver_gallery":
        send_msg(chat_id,
            f"📁 To deliver the gallery for `{target_id}`, send:\n\n"
            f"`/deliver {target_id} <google_drive_folder_url>`\n\n"
            f"Example:\n`/deliver {target_id} https://drive.google.com/drive/folders/...`"
        )

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
        elif text.startswith("/deliver"):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                send_msg(chat_id, "Usage: /deliver <Lead_ID> <google_drive_folder_url>")
                return jsonify({"status": "ok"})
            lead_id, gallery_url = parts[1], parts[2]
            fired = fire_webhook(DELIVER_GALLERY_WEBHOOK, {
                "lead_id":     lead_id,
                "gallery_url": gallery_url
            })
            if fired:
                send_msg(chat_id, f"🖼️ Gallery delivery triggered for `{lead_id}`\n📁 {gallery_url}\n\nClient will receive email + Zoho invoice shortly.")
            else:
                send_msg(chat_id, "⚠️ Webhook failed — check DELIVER_GALLERY_WEBHOOK in Railway.")
        elif text.startswith("/retention"):
            parts = text.split()
            if len(parts) != 2:
                send_msg(chat_id, "Usage: /retention <Lead_ID>")
                return jsonify({"status": "ok"})
            lead_id = parts[1]
            fired = fire_webhook(RETENTION_WEBHOOK, {"lead_id": lead_id})
            if fired:
                send_msg(chat_id, f"⭐ Retention sequence triggered for `{lead_id}`\nReview request email sent. Upsell follow-up queued.")
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
    """
    Called by Zapier System 2 after a proposal is sent.
    Posts to Pipeline Bot with a 'Send Contract' action button.
    Expected fields: lead_id, project_id, client_name, proposal_link,
                     event_type, event_date, package
    """
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
