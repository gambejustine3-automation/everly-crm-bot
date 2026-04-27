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
BOT_TOKEN = os.environ.get("BOT_TOKEN")                    # Everly&Co.ClientBot
PIPELINE_BOT_TOKEN = os.environ.get("PIPELINE_BOT_TOKEN")  # Everly&Co.PipelineTrackerBot
DASHBOARD_BOT_TOKEN = os.environ.get("DASHBOARD_BOT_TOKEN")# Everly&Co.CRMDashboardBot
CHAT_ID = os.environ.get("CHAT_ID")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CAL_API_KEY = os.environ.get("CAL_API_KEY")
CAL_EVENT_TYPE_ID = os.environ.get("CAL_EVENT_TYPE_ID")

# Zapier Webhooks
CLOSE_LEAD_WEBHOOK = os.environ.get("CLOSE_LEAD_WEBHOOK")
PROPOSAL_ZAPIER_WEBHOOK = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
DEPOSIT_PAID_WEBHOOK = os.environ.get("DEPOSIT_PAID_WEBHOOK")
DELIVER_GALLERY_WEBHOOK = os.environ.get("DELIVER_GALLERY_WEBHOOK")

# Telegram Bot APIs
DASHBOARD_API = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"
PIPELINE_API = f"https://api.telegram.org/bot{PIPELINE_BOT_TOKEN}"
CLIENT_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ─────────────────────────────────────────────
# CONSTANTS
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
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def ph_now():
    """Return current time in Philippines (UTC+8)"""
    return datetime.utcnow() + timedelta(hours=8)


def get_sheets_service():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_sheet_with_headers(range_name):
    """Read Google Sheet and return rows + column index mapping"""
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
    """Write values to Google Sheet"""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        return True
    except Exception as e:
        print(f"[WRITE ERROR] {range_name}: {e}")
        return False


def safe_get(row, col, key):
    """Safely get value from sheet row"""
    if key not in col or len(row) <= col[key]:
        return "—"
    return row[col[key]] if row[col[key]] else "—"


def get_col_letter(idx):
    """Convert column index to Excel letter (0 → A, 26 → AA, etc.)"""
    result = ""
    while idx >= 0:
        result = chr(65 + (idx % 26)) + result
        idx = (idx // 26) - 1
    return result


def fire_webhook(url, payload):
    """Fire Zapier webhook"""
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


def answer_callback(cb_id, text, bot_api=None):
    """Answer Telegram callback query"""
    api = bot_api or DASHBOARD_API
    requests.post(f"{api}/answerCallbackQuery", json={
        "callback_query_id": cb_id,
        "text": text
    })


# ─────────────────────────────────────────────
# TELEGRAM SEND / EDIT HELPERS
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


def send_pipeline_msg(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": reply_markup
    }
    requests.post(f"{PIPELINE_API}/sendMessage", json=payload)


def send_client_msg(chat_id, text):
    requests.post(f"{CLIENT_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

# ─────────────────────────────────────────────
# CAL.COM HELPERS
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
        return r.json().get("bookings", [])
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

    # Convert time to PH time
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
# GOOGLE SHEETS WRITE-BACK HELPER
# ─────────────────────────────────────────────

def _write_back(sheet_range_prefix, sheet_header_range, id_col, target_id, updates: dict):
    rows, col = read_sheet_with_headers(sheet_header_range)
    for i, row in enumerate(rows, 2):  # +2 because row 1 is header
        if safe_get(row, col, id_col) == target_id:
            for col_name, new_value in updates.items():
                if col_name in col:
                    cl = get_col_letter(col[col_name])
                    write_sheet(f"{sheet_range_prefix}!{cl}{i}", [[new_value]])
            return True
    return False

# ─────────────────────────────────────────────
# COMMAND & VIEW HANDLERS
# ─────────────────────────────────────────────

# (All your handle_xxx and _show_xxx functions go here)
# For brevity, I kept only the structure. Paste your cleaned handlers below this section.

# Example of one cleaned handler:
def handle_start_command(chat_id):
    text = (
        "📸 *Welcome to Everly & Co. CRM Dashboard*\n\n"
        "Use /menu for quick navigation\n"
        "Use /help to see all available commands"
    )
    send_msg(chat_id, text)


# ... (include all other handlers: handle_help, handle_menu, handle_leads, etc.)

# ─────────────────────────────────────────────
# MAIN WEBHOOK ROUTE
# ─────────────────────────────────────────────

@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json

    # Handle regular messages
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
            query = text[len("/search "):].strip()
            handle_search_command(chat_id, query)
        elif text.startswith("/client"):
            cid = text[len("/client "):].strip()
            handle_client_command(chat_id, cid)
        else:
            send_msg(chat_id, "❓ Unknown command. Type /help to see all commands.")

        return jsonify({"status": "ok"})

    # Handle callback queries (inline buttons)
    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    cb = data["callback_query"]
    chat_id = cb["message"]["chat"]["id"]
    msg_id = cb["message"]["message_id"]
    cb_data = cb["data"]

    parts = cb_data.split("|")
    action = parts[0]
    target_id = parts[1] if len(parts) > 1 else None

    # Route callbacks to appropriate handlers
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
    # ... (all other callback actions: upd_lead, upd_pipe, call_out, send_proposal, etc.)

    return jsonify({"status": "ok"})


# ─────────────────────────────────────────────
# NOTIFICATION ROUTES
# ─────────────────────────────────────────────

@app.route("/notify", methods=["POST"])
def notify():
    """Receive notification from Zapier and forward via Client Bot"""
    body = request.json
    message = body.get("message", "")
    if message and BOT_TOKEN and CHAT_ID:
        requests.post(f"{CLIENT_API}/sendMessage", json={
            "chat_id": CHAT_ID,
            "text": message
        })
    return jsonify({"status": "sent"})


@app.route("/pipeline_notify", methods=["POST"])
def pipeline_notify():
    """Receive booking notification and send with call outcome buttons"""
    body = request.json
    # ... (your existing logic - cleaned)
    # (I can expand this if you want)

    return jsonify({"status": "sent"})


@app.route("/pipeline_dashboard", methods=["POST"])
def pipeline_dashboard():
    """Handle callbacks from Pipeline Tracker Bot"""
    # ... your existing logic
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
