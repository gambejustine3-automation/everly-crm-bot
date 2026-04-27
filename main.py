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
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # Everly&Co.ClientBot
PIPELINE_BOT_TOKEN = os.environ.get("PIPELINE_BOT_TOKEN")  # Everly&Co.PipelineTrackerBot
DASHBOARD_BOT_TOKEN = os.environ.get("DASHBOARD_BOT_TOKEN")  # Everly&Co.CRMDashboardBot
CHAT_ID = os.environ.get("CHAT_ID")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
DASHBOARD_API = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"
PIPELINE_API = f"https://api.telegram.org/bot{PIPELINE_BOT_TOKEN}"
CLIENT_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CAL_API_KEY = os.environ.get("CAL_API_KEY")
CAL_EVENT_TYPE_ID = os.environ.get("CAL_EVENT_TYPE_ID")

# Zapier Webhooks
CLOSE_LEAD_WEBHOOK = os.environ.get("CLOSE_LEAD_WEBHOOK")
PROPOSAL_ZAPIER_WEBHOOK = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
DEPOSIT_PAID_WEBHOOK = os.environ.get("DEPOSIT_PAID_WEBHOOK")
DELIVER_GALLERY_WEBHOOK = os.environ.get("DELIVER_GALLERY_WEBHOOK")

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

# ─────────────────────────────────────────────
# TELEGRAM SEND HELPERS
# ─────────────────────────────────────────────
def send_msg(chat_id, text, reply_markup=None):
    """Send message using Dashboard Bot (primary)"""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": reply_markup
    }
    r = requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)
    print(f"[DASHBOARD SEND] {r.status_code}: {r.text[:200]}")
    return r

def send_pipeline_msg(chat_id, text, reply_markup=None):
    """Send via Pipeline Tracker Bot only when needed"""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": reply_markup
    }
    requests.post(f"{PIPELINE_API}/sendMessage", json=payload)

def edit_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": reply_markup
    }
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

def send_client_msg(chat_id, text):
    requests.post(f"{CLIENT_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

# ─────────────────────────────────────────────
# CAL.COM API HELPER
# ─────────────────────────────────────────────
def fetch_cal_bookings(date_str):
    if not CAL_API_KEY:
        print("[CAL] CAL_API_KEY not set.")
        return []
    try:
        date_from = f"{date_str}T00:00:00+08:00"
        date_to = f"{date_str}T23:59:59+08:00"
        url = "https://api.cal.com/v1/bookings"
        params = {
            "apiKey": CAL_API_KEY,
            "dateFrom": date_from,
            "dateTo": date_to,
            "status": "upcoming"
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("bookings", [])
    except Exception as e:
        print(f"[CAL ERROR] {e}")
        return []

def parse_cal_booking(booking):
    attendees = booking.get("attendees", [])
    client_name = attendees[0].get("name", "Unknown") if attendees else "Unknown"
    client_email = attendees[0].get("email", "—") if attendees else "—"
    responses = booking.get("responses", {})
    metadata = booking.get("metadata", {})
    lead_id = (
        responses.get("lead_id", {}).get("value")
        or metadata.get("lead_id")
        or "—"
    )
    start_raw = booking.get("startTime", "")
    try:
        dt_utc = datetime.strptime(start_raw, "%Y-%m-%dT%H:%M:%SZ")
        dt_local = dt_utc + timedelta(hours=8)
        time_str = dt_local.strftime("%I:%M %p")
    except Exception:
        time_str = start_raw
    return {
        "id": booking.get("id"),
        "uid": booking.get("uid", ""),
        "client_name": client_name,
        "client_email": client_email,
        "lead_id": lead_id,
        "time": time_str,
        "status": booking.get("status", "ACCEPTED"),
        "meeting_url": booking.get("videoCallData", {}).get("url", "—")
    }

# ─────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────
def handle_start_command(chat_id):
    text = "📸 *Welcome to Everly & Co. CRM Dashboard*\n\nUse /menu for quick navigation buttons\nUse /help to see all commands"
    send_msg(chat_id, text)

def handle_help_command(chat_id):
    text = (
        "📖 *COMMAND REFERENCE*\n\n"
        "📋 *Leads & Pipeline*\n"
        "`/leads` — View latest leads\n"
        "`/hot` — Show HOT leads\n"
        "`/pipeline` — Pipeline overview\n"
        "`/search <name or email>` — Find lead\n"
        "`/updateemail <Lead_ID> <new_email>` — Update lead email\n\n"
        "📅 *Schedule*\n"
        "`/schedule` — Today's calls\n"
        "`/tomorrow` — Tomorrow's calls\n"
        "`/today` — Today's shoots\n\n"
        "👤 *Clients & Projects*\n"
        "`/client <ID>` — Client card\n"
        "`/project` — Active projects"
    )
    send_msg(chat_id, text)

def handle_menu_command(chat_id):
    text = "📸 *Everly & Co. CRM — Quick Menu*"
    buttons = [
        [{"text": "📋 Leads", "callback_data": "nav_leads|none"}, {"text": "🔴 Hot Leads", "callback_data": "nav_hot|none"}],
        [{"text": "📊 Pipeline", "callback_data": "nav_pipe|none"}, {"text": "🗂 Projects", "callback_data": "nav_projects|none"}],
        [{"text": "📅 Today's Calls", "callback_data": "nav_schedule|today"}, {"text": "📅 Tomorrow", "callback_data": "nav_schedule|tomorrow"}],
        [{"text": "📸 Today's Shoots", "callback_data": "nav_today|none"}]
    ]
    send_msg(chat_id, text, {"inline_keyboard": buttons})

# ... [All other command handlers remain the same: handle_leads_command, handle_hot_command, handle_today_command, handle_schedule_command, handle_tomorrow_command, handle_search_command, handle_pipeline_command, handle_project_command, handle_client_command] ...

def handle_client_command(chat_id, client_id):
    if not client_id:
        return send_msg(chat_id, "👤 Usage: `/client <Client_ID>`")
    _show_client(chat_id, None, client_id)

# ─────────────────────────────────────────────
# VIEW HANDLERS (unchanged - abbreviated for brevity)
# ─────────────────────────────────────────────
def _show_lead(chat_id, msg_id, target_id, method="edit"):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    row = next((r for r in rows if safe_get(r, col, "Lead_ID") == target_id), None)
    if not row:
        return send_msg(chat_id, f"❌ Lead `{target_id}` not found.")
    # ... rest of _show_lead remains exactly as in your original code ...
    # (I kept it identical to avoid any risk)

# Note: For space, the full _show_lead, _show_pipeline, _show_project, _show_client, _show_call_menu, _write_back are unchanged from your original.

# ─────────────────────────────────────────────
# MAIN WEBHOOK ROUTER
# ─────────────────────────────────────────────
@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json
    if "message" in data:
        msg = data["message"]
        text = msg.get("text", "").strip()
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
            # FIXED: Clean handler + notification stays in Dashboard Bot
            parts = text.split()
            if len(parts) != 3:
                send_msg(chat_id, "Usage: /updateemail <Lead_ID> <new_email>", None)
                return jsonify({"status": "ok"})
            
            lead_id, new_email = parts[1], parts[2]
            rows, col = read_sheet_with_headers("Leads")
            found = False
            for i, row in enumerate(rows):
                if safe_get(row, col, "Lead_ID") == lead_id:
                    row_num = i + 2
                    col_idx = col.get("Email")
                    if col_idx is None:
                        send_msg(chat_id, "❌ Email column not found in sheet.", None)
                        return jsonify({"status": "ok"})
                    col_letter = get_col_letter(col_idx)
                    write_sheet(f"Leads!{col_letter}{row_num}", [[new_email]])
                    send_msg(chat_id, f"✅ Email updated for {lead_id} → {new_email}", None)
                    found = True
                    break
            
            if not found:
                send_msg(chat_id, f"❌ Lead {lead_id} not found.", None)
            return jsonify({"status": "ok"})

        else:
            send_msg(chat_id, "❓ Unknown command. Type /help to see all commands.")
        return jsonify({"status": "ok"})

    # Callback handling remains unchanged
    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    # ... [All your existing callback_query handling code stays exactly the same] ...

    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
