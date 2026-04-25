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
BOT_TOKEN                = os.environ.get("BOT_TOKEN")
CHAT_ID                  = os.environ.get("CHAT_ID")
DASHBOARD_BOT_TOKEN      = os.environ.get("DASHBOARD_BOT_TOKEN")
SPREADSHEET_ID           = os.environ.get("SPREADSHEET_ID")
DASHBOARD_API            = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"
SCOPES                   = ["https://www.googleapis.com/auth/spreadsheets"]
CAL_API_KEY              = os.environ.get("CAL_API_KEY")         # ← NEW: Cal.com API key
CAL_EVENT_TYPE_ID        = os.environ.get("CAL_EVENT_TYPE_ID")   # ← NEW: your Discovery Call event type ID

# Zapier Webhooks
CLOSE_LEAD_WEBHOOK       = os.environ.get("CLOSE_LEAD_WEBHOOK")
PROPOSAL_ZAPIER_WEBHOOK  = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
CONTRACT_ZAPIER_WEBHOOK  = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
DEPOSIT_PAID_WEBHOOK     = os.environ.get("DEPOSIT_PAID_WEBHOOK")
DELIVER_GALLERY_WEBHOOK  = os.environ.get("DELIVER_GALLERY_WEBHOOK")

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
        print("[WEBHOOK] URL not set in environment variables.")
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
        "reply_markup": reply_markup
    }
    requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)

def edit_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": reply_markup
    }
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

# ─────────────────────────────────────────────
# CAL.COM API HELPER  ← NEW
# ─────────────────────────────────────────────
def fetch_cal_bookings(date_str):
    """
    Fetches bookings from Cal.com for a specific date.
    date_str format: "YYYY-MM-DD"
    Returns a list of booking dicts or empty list on failure.
    """
    if not CAL_API_KEY:
        print("[CAL] CAL_API_KEY not set.")
        return []
    try:
        # Build date range: full day in ISO format
        date_from = f"{date_str}T00:00:00+08:00"
        date_to   = f"{date_str}T23:59:59+08:00"
        url = "https://api.cal.com/v1/bookings"
        params = {
            "apiKey":   CAL_API_KEY,
            "dateFrom": date_from,
            "dateTo":   date_to,
            "status":   "upcoming"
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("bookings", [])
    except Exception as e:
        print(f"[CAL ERROR] {e}")
        return []

def parse_cal_booking(booking):
    """
    Extracts the key fields from a Cal.com booking object.
    Returns a clean dict.
    """
    attendees = booking.get("attendees", [])
    client_name  = attendees[0].get("name", "Unknown") if attendees else "Unknown"
    client_email = attendees[0].get("email", "—") if attendees else "—"

    # lead_id comes from the hidden field — stored in responses or metadata
    responses = booking.get("responses", {})
    metadata  = booking.get("metadata", {})
    lead_id   = (
        responses.get("lead_id", {}).get("value")
        or metadata.get("lead_id")
        or "—"
    )

    # Parse start time — Cal.com returns UTC ISO string
    start_raw = booking.get("startTime", "")
    try:
        # Convert to Asia/Manila (UTC+8)
        dt_utc   = datetime.strptime(start_raw, "%Y-%m-%dT%H:%M:%SZ")
        dt_local = dt_utc + timedelta(hours=8)
        time_str = dt_local.strftime("%I:%M %p")
    except Exception:
        time_str = start_raw

    return {
        "id":           booking.get("id"),
        "uid":          booking.get("uid", ""),
        "client_name":  client_name,
        "client_email": client_email,
        "lead_id":      lead_id,
        "time":         time_str,
        "status":       booking.get("status", "ACCEPTED"),
        "meeting_url":  booking.get("videoCallData", {}).get("url", "—")
    }

# ─────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────
def handle_start_command(chat_id):
    text = (
        "📸 *Everly & Co. CRM Dashboard*\n\n"
        "📋 *LEADS & PIPELINE*\n"
        "`/leads` — All leads (latest 10)\n"
        "`/hot` — HOT leads only\n"
        "`/pipeline` — Pipeline snapshot\n"
        "`/search <name or email>` — Search leads\n\n"
        "📅 *SCHEDULE*\n"
        "`/schedule` — Today's discovery calls\n"
        "`/tomorrow` — Tomorrow's calls\n"
        "`/today` — Today's photography events\n\n"
        "👤 *CLIENTS & PROJECTS*\n"
        "`/client <ID>` — View client card\n"
        "`/project` — All active projects\n"
    )
    send_msg(chat_id, text)

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
    buttons.append([{"text": "🔴 HOT Only", "callback_data": "nav_hot|none"},
                    {"text": "📊 Pipeline", "callback_data": "nav_pipe|none"}])
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
    """Shows photography events (shoots) happening today from the Leads sheet."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%B %d, %Y")
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    today_rows = [r for r in rows if today_str in safe_get(r, col, "Event_Date")]
    lines = [f"📅 *TODAY'S SHOOTS* — {today_display}\n"]
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
    buttons.append([{"text": "📅 Today's Calls", "callback_data": "nav_schedule|today"},
                    {"text": "⬅️ All Leads", "callback_data": "nav_leads|none"}])
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_schedule_command(chat_id, date_str=None, date_label=None):
    """
    Shows Cal.com discovery call bookings for a given date.
    date_str: "YYYY-MM-DD" — defaults to today
    date_label: display string e.g. "Today" or "Tomorrow"
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    if not date_label:
        date_label = datetime.now().strftime("%B %d, %Y")

    bookings_raw = fetch_cal_bookings(date_str)

    # Filter to only accepted/upcoming bookings
    bookings = [
        parse_cal_booking(b) for b in bookings_raw
        if b.get("status") in ("ACCEPTED", "upcoming", "PENDING")
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
            # Button row: view snapshot + join call
            row_buttons = []
            if lead_id and lead_id != "—":
                row_buttons.append({
                    "text": f"👤 {b['client_name']}",
                    "callback_data": f"view_lead|{lead_id}"
                })
            if b["meeting_url"] and b["meeting_url"] != "—":
                row_buttons.append({
                    "text": "🔗 Join Call",
                    "url": b["meeting_url"]
                })
            if row_buttons:
                buttons.append(row_buttons)

            # Call outcome buttons under each slot
            if lead_id and lead_id != "—":
                buttons.append([
                    {"text": "✅ Done", "callback_data": f"call_menu|{lead_id}"}
                ])
    else:
        lines.append("No discovery calls scheduled.")

    # Navigation row
    today_str    = datetime.now().strftime("%Y-%m-%d")
    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    tomorrow_lbl = (datetime.now() + timedelta(days=1)).strftime("%B %d, %Y")

    if date_str == today_str:
        buttons.append([
            {"text": "➡️ Tomorrow", "callback_data": f"nav_schedule|tomorrow"},
            {"text": "📅 Today's Shoots", "callback_data": "nav_today|none"}
        ])
    else:
        buttons.append([
            {"text": "⬅️ Today", "callback_data": "nav_schedule|today"},
            {"text": "📋 All Leads", "callback_data": "nav_leads|none"}
        ])

    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_tomorrow_command(chat_id):
    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    tomorrow_lbl = (datetime.now() + timedelta(days=1)).strftime("%B %d, %Y")
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

def handle_pipeline_command(chat_id):
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
    buttons.append([{"text": "📋 All Leads", "callback_data": "nav_leads|none"},
                    {"text": "🗂 Projects", "callback_data": "nav_projects|none"}])
    send_msg(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_project_command(chat_id):
    rows, col = read_sheet_with_headers("Projects!A1:V200")
    if not rows:
        return send_msg(chat_id, "📭 No projects found.")
    active = [
        r for r in rows
        if safe_get(r, col, "Current_Stage") not in ("Closed", "Completed")
    ]
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
def _show_lead(chat_id, msg_id, target_id, method="edit"):
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
            {"text": "🔴 HOT", "callback_data": f"upd_lead|{target_id}|HOT"},
            {"text": "🟡 WARM", "callback_data": f"upd_lead|{target_id}|WARM"},
            {"text": "🔵 COLD", "callback_data": f"upd_lead|{target_id}|COLD"}
        ],
        [
            {"text": "📞 Call Outcome", "callback_data": f"call_menu|{target_id}"},
            {"text": "📋 Pipeline", "callback_data": f"view_pipe|{target_id}"}
        ],
        [
            {"text": "🗂 Project", "callback_data": f"view_project|{target_id}"},
            {"text": "👤 Client", "callback_data": f"view_client|{client_id}"}
        ],
        [{"text": "⬅️ Back to Leads", "callback_data": "nav_leads|none"}]
    ]
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

def _show_pipeline(chat_id, msg_id, target_id, method="edit"):
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
    curr_idx = PIPELINE_STAGES.index(curr_stage) if curr_stage in PIPELINE_STAGES else -1
    next_stages = PIPELINE_STAGES[curr_idx + 1: curr_idx + 4]
    buttons = [[{"text": f"➡️ {s}", "callback_data": f"upd_pipe|{target_id}|{s}"}] for s in next_stages]

    action_row = []
    if curr_stage == "Discovery Call Booked":
        action_row = [{"text": "📞 Call Outcome", "callback_data": f"call_menu|{target_id}"}]
    elif curr_stage == "Discovery Call Completed":
        action_row = [{"text": "📄 Send Proposal", "callback_data": f"send_proposal|{target_id}"}]
    elif curr_stage == "Proposal Sent":
        action_row = [{"text": "📝 Send Contract", "callback_data": f"send_contract|{target_id}"}]
    elif curr_stage == "Contracted":
        action_row = [{"text": "💰 Mark Deposit Paid", "callback_data": f"deposit_paid|{target_id}"}]
    elif curr_stage == "Active Project":
        action_row = [{"text": "🖼️ Deliver Gallery", "callback_data": f"deliver_gallery|{target_id}"}]
    if action_row:
        buttons.append(action_row)

    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{target_id}"},
        {"text": "🗂 Project", "callback_data": f"view_project|{target_id}"}
    ])
    buttons.append([{"text": "⬅️ Back to Pipeline", "callback_data": "nav_pipe|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

def _show_project(chat_id, msg_id, lead_id, method="edit"):
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
    curr_idx = PROJECT_STAGES.index(curr_stage) if curr_stage in PROJECT_STAGES else -1
    next_stages = PROJECT_STAGES[curr_idx + 1: curr_idx + 3]
    buttons = [[{"text": f"➡️ {s}", "callback_data": f"upd_proj|{lead_id}|{s}"}] for s in next_stages]
    buttons.append([
        {"text": "👤 View Lead", "callback_data": f"view_lead|{lead_id}"},
        {"text": "📊 Pipeline", "callback_data": f"view_pipe|{lead_id}"}
    ])
    buttons.append([{"text": "⬅️ Projects List", "callback_data": "nav_projects|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

def _show_client(chat_id, msg_id, client_id, method="send"):
    rows, col = read_sheet_with_headers("Clients!A1:H200")
    row = next((r for r in rows if safe_get(r, col, "Client_ID") == client_id), None)
    if not row:
        return send_msg(chat_id, f"❌ Client `{client_id}` not found.")
    tier = safe_get(row, col, "Client_Tier")
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
            text += f"\n  • {emoji} `{lid}` — {etype}"
            buttons.append([{"text": f"{emoji} {etype} ({lid})", "callback_data": f"view_lead|{lid}"}])
    buttons.append([{"text": "⬅️ Back", "callback_data": "nav_leads|none"}])
    markup = {"inline_keyboard": buttons}
    if method == "edit" and msg_id:
        edit_msg(chat_id, msg_id, text, markup)
    else:
        send_msg(chat_id, text, markup)

def _show_call_menu(chat_id, msg_id, lead_id):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == lead_id), None)
    name = safe_get(row, col, "Full_Name") if row else lead_id
    text = (
        f"📞 *Call Outcome — {name}*\n"
        f"Lead: `{lead_id}`\n\n"
        f"What was the result of the discovery call?"
    )
    buttons = [
        [{"text": "✅ Completed — Continue",  "callback_data": f"call_out|{lead_id}|completed_continue"}],
        [{"text": "🛑 Completed — Not a Fit", "callback_data": f"call_out|{lead_id}|completed_stop"}],
        [{"text": "❌ No Show",               "callback_data": f"call_out|{lead_id}|no_show"}],
        [{"text": "🔄 Reschedule",            "callback_data": f"call_out|{lead_id}|reschedule"}],
        [{"text": "⬅️ Back to Lead",          "callback_data": f"view_lead|{lead_id}"}]
    ]
    edit_msg(chat_id, msg_id, text, {"inline_keyboard": buttons})

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
# MAIN WEBHOOK & CALLBACK ROUTER
# ─────────────────────────────────────────────
@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json

    # ── TEXT COMMANDS ──
    if "message" in data:
        msg     = data["message"]
        text    = msg.get("text", "").strip()
        chat_id = msg["chat"]["id"]

        if text in ("/start", "/help"):
            handle_start_command(chat_id)
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
        else:
            send_msg(chat_id, "❓ Unknown command. Type /help to see all commands.")

        return jsonify({"status": "ok"})

    # ── CALLBACK QUERIES ──
    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    cb        = data["callback_query"]
    chat_id   = cb["message"]["chat"]["id"]
    msg_id    = cb["message"]["message_id"]
    cb_data   = cb["data"]
    parts     = cb_data.split("|")
    action    = parts[0]
    target_id = parts[1] if len(parts) > 1 else None

    # ── VIEW ACTIONS ──
    if action == "view_lead":
        _show_lead(chat_id, msg_id, target_id)

    elif action == "view_pipe":
        _show_pipeline(chat_id, msg_id, target_id)

    elif action == "view_project":
        _show_project(chat_id, msg_id, target_id)

    elif action == "view_client":
        _show_client(chat_id, msg_id, target_id, method="edit")

    elif action == "call_menu":
        _show_call_menu(chat_id, msg_id, target_id)

    # ── WRITE-BACK: UPDATE LEAD STATUS ──
    elif action == "upd_lead":
        new_status = parts[2]
        success = _write_back(
            "Leads", "Leads!A1:T200", "Lead_ID", target_id,
            {"Lead_Status": new_status}
        )
        answer_callback(cb["id"], f"Status → {new_status} ✅" if success else "⚠️ Update failed")
        _show_lead(chat_id, msg_id, target_id)

    # ── WRITE-BACK: UPDATE PIPELINE STAGE ──
    elif action == "upd_pipe":
        new_stage = parts[2]
        today_str = datetime.now().strftime("%Y-%m-%d")
        success = _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage": new_stage,
                "Last_Action": f"Stage moved to {new_stage}",
                "Next_Action_Date": today_str
            }
        )
        answer_callback(cb["id"], f"Stage → {new_stage} ✅" if success else "⚠️ Update failed")
        _show_pipeline(chat_id, msg_id, target_id)

    # ── WRITE-BACK: UPDATE PROJECT STAGE ──
    elif action == "upd_proj":
        new_stage = parts[2]
        success = _write_back(
            "Projects", "Projects!A1:V200", "Lead_ID", target_id,
            {"Current_Stage": new_stage}
        )
        answer_callback(cb["id"], f"Project → {new_stage} ✅" if success else "⚠️ Update failed")
        _show_project(chat_id, msg_id, target_id)

    # ── CALL OUTCOME → ZAPIER WEBHOOK ──
    elif action == "call_out":
        outcome = parts[2]
        today_str = datetime.now().strftime("%Y-%m-%d")
        outcome_map = {
            "completed_continue": {
                "current_stage": "Discovery Call Completed",
                "call_status":   "Completed",
                "next_action":   "Send Proposal"
            },
            "completed_stop": {
                "current_stage": "Closed Lost",
                "call_status":   "Completed",
                "next_action":   "Archive Lead"
            },
            "no_show": {
                "current_stage": "Discovery Call Booked",
                "call_status":   "No Show",
                "next_action":   "Follow up / Reschedule"
            },
            "reschedule": {
                "current_stage": "Discovery Call Booked",
                "call_status":   "Rescheduling",
                "next_action":   "Send new Cal.com link"
            }
        }
        mapping = outcome_map.get(outcome, {})
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage": mapping.get("current_stage", "—"),
                "Call_Status":   mapping.get("call_status", "—"),
                "Last_Action":   "Discovery Call Completed",
                "Next_Action":   mapping.get("next_action", "—"),
                "Next_Action_Date": today_str
            }
        )
        webhook_payload = {
            "lead_id":       target_id,
            "action":        outcome,
            "current_stage": mapping.get("current_stage"),
            "call_status":   mapping.get("call_status"),
            "timestamp":     today_str
        }
        fired = fire_webhook(CLOSE_LEAD_WEBHOOK, webhook_payload)
        outcome_labels = {
            "completed_continue": "✅ Completed — moving to Proposal",
            "completed_stop":     "🛑 Closed as Not a Fit",
            "no_show":            "❌ Marked as No Show",
            "reschedule":         "🔄 Marked for Rescheduling"
        }
        answer_callback(cb["id"], outcome_labels.get(outcome, "Updated ✅"))
        _show_pipeline(chat_id, msg_id, target_id)

    # ── SEND PROPOSAL → ZAPIER ──
    elif action == "send_proposal":
        fired = fire_webhook(PROPOSAL_ZAPIER_WEBHOOK, {"lead_id": target_id})
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage":   "Proposal Sent",
                "Last_Action":     "Proposal Sent",
                "Next_Action":     "Follow up on Proposal",
                "Proposal_Status": "Sent"
            }
        )
        answer_callback(cb["id"], "📄 Proposal triggered! ✅" if fired else "⚠️ Webhook failed")
        _show_pipeline(chat_id, msg_id, target_id)

    # ── SEND CONTRACT → ZAPIER ──
    elif action == "send_contract":
        fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {"lead_id": target_id})
        today_str = datetime.now().strftime("%Y-%m-%d")
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage": "Contracted",
                "Last_Action":   "Contract Sent",
                "Next_Action":   "Await Deposit"
            }
        )
        _write_back(
            "Projects", "Projects!A1:V200", "Lead_ID", target_id,
            {
                "Contract_Sent":  "Yes",
                "Contract_Date":  today_str,
                "Current_Stage":  "Pre-Production"
            }
        )
        answer_callback(cb["id"], "📝 Contract triggered! ✅" if fired else "⚠️ Webhook failed")
        _show_pipeline(chat_id, msg_id, target_id)

    # ── DEPOSIT PAID → ZAPIER ──
    elif action == "deposit_paid":
        fired = fire_webhook(DEPOSIT_PAID_WEBHOOK, {"lead_id": target_id})
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage": "Active Project",
                "Last_Action":   "Deposit Received",
                "Next_Action":   "Block Calendar & Create Drive Folder"
            }
        )
        _write_back(
            "Projects", "Projects!A1:V200", "Lead_ID", target_id,
            {
                "Deposit_Paid":  "Yes",
                "Current_Stage": "Active"
            }
        )
        answer_callback(cb["id"], "💰 Deposit marked paid! ✅" if fired else "⚠️ Webhook failed")
        _show_pipeline(chat_id, msg_id, target_id)

    # ── DELIVER GALLERY → ZAPIER ──
    elif action == "deliver_gallery":
        fired = fire_webhook(DELIVER_GALLERY_WEBHOOK, {"lead_id": target_id})
        today_str = datetime.now().strftime("%Y-%m-%d")
        _write_back(
            "Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
            {
                "Current_Stage": "Closed Won",
                "Last_Action":   "Gallery Delivered",
                "Next_Action":   "Request Review"
            }
        )
        _write_back(
            "Projects", "Projects!A1:V200", "Lead_ID", target_id,
            {
                "Current_Stage":  "Delivered",
                "Delivery_Date":  today_str,
                "Shoot_Complete": "Yes"
            }
        )
        answer_callback(cb["id"], "🖼️ Gallery delivered! ✅" if fired else "⚠️ Webhook failed")
        _show_pipeline(chat_id, msg_id, target_id)

    # ── NAVIGATION ──
    elif action == "nav_leads":
        handle_leads_command(chat_id)
    elif action == "nav_pipe":
        handle_pipeline_command(chat_id)
    elif action == "nav_projects":
        handle_project_command(chat_id)
    elif action == "nav_hot":
        handle_hot_command(chat_id)
    elif action == "nav_today":
        handle_today_command(chat_id)
    elif action == "nav_schedule":
        # target_id here is "today" or "tomorrow"
        if target_id == "tomorrow":
            handle_tomorrow_command(chat_id)
        else:
            handle_schedule_command(chat_id)

    return jsonify({"status": "ok"})


# ─────────────────────────────────────────────
# NOTIFICATION BOT (inbound from Zapier)
# ─────────────────────────────────────────────
@app.route("/notify", methods=["POST"])
def notify():
    body = request.json
    message = body.get("message", "")
    if message and BOT_TOKEN and CHAT_ID:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
        )
    return jsonify({"status": "sent"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
