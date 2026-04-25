from flask import Flask, request, jsonify
import requests
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ─────────────────────────────────────────────
# ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
BOT_TOKEN             = os.environ.get("BOT_TOKEN")
CHAT_ID               = os.environ.get("CHAT_ID")
DASHBOARD_BOT_TOKEN   = os.environ.get("DASHBOARD_BOT_TOKEN")
SPREADSHEET_ID        = os.environ.get("SPREADSHEET_ID")
DASHBOARD_API         = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"
SCOPES                = ["https://www.googleapis.com/auth/spreadsheets"]

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

# ─────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────
def get_sheets_service():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_sheet_with_headers(range_name):
    service = get_sheets_service()
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
    values = result.get("values", [])
    if not values: return [], {}
    headers = values[0]
    rows = values[1:]
    col = {name: i for i, name in enumerate(headers)}
    return rows, col

def write_sheet(range_name, values):
    service = get_sheets_service()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_name,
        valueInputOption="RAW", body={"values": values}
    ).execute()

def safe_get(row, col, key):
    if key not in col or len(row) <= col[key]: return "—"
    return row[col[key]] if row[col[key]] else "—"

def get_col_letter(idx):
    """Converts a zero-based column index to Excel-style column letter (A, B, C...)."""
    result = ""
    while idx >= 0:
        result = chr(65 + (idx % 26)) + result
        idx = (idx // 26) - 1
    return result

# ─────────────────────────────────────────────
# DASHBOARD BOT — HELPERS
# ─────────────────────────────────────────────
def send_dashboard_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": reply_markup}
    requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)

def edit_dashboard_message(chat_id, message_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown", "reply_markup": reply_markup}
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

# ─────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────
def handle_leads_command(chat_id):
    rows, col = read_sheet_with_headers("Leads!A1:T200")
    if not rows: return send_dashboard_message(chat_id, "📭 No leads found.")
    lines = ["📋 *LEADS OVERVIEW*\n"]
    buttons = []
    for row in rows[:10]:
        name, lid, status = safe_get(row, col, "Full_Name"), safe_get(row, col, "Lead_ID"), safe_get(row, col, "Lead_Status")
        emoji = {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}.get(status.upper(), "⚪")
        lines.append(f"• {emoji} *{name}* (`{lid}`)")
        buttons.append([{"text": f"{emoji} {name}", "callback_data": f"view_lead|{lid}"}])
    send_dashboard_message(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

def handle_pipeline_command(chat_id):
    rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    if not rows: return send_dashboard_message(chat_id, "📭 Pipeline is empty.")
    lines = ["📊 *PIPELINE SNAPSHOT*\n"]
    buttons = []
    for row in rows[:10]:
        client, lid, stage = safe_get(row, col, "Client_Name"), safe_get(row, col, "Lead_ID"), safe_get(row, col, "Current_Stage")
        lines.append(f"• *{client}* (`{lid}`)\n  └ Stage: {stage}")
        buttons.append([{"text": f"📊 {client}", "callback_data": f"view_pipe|{lid}"}])
    send_dashboard_message(chat_id, "\n".join(lines), {"inline_keyboard": buttons})

# ─────────────────────────────────────────────
# WEBHOOK & CALLBACK HANDLER
# ─────────────────────────────────────────────
@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json
    if "message" in data:
        text, chat_id = data["message"].get("text", ""), data["message"]["chat"]["id"]
        if text == "/leads": handle_leads_command(chat_id)
        elif text == "/pipeline": handle_pipeline_command(chat_id)
        return jsonify({"status": "ok"})

    if "callback_query" not in data: return jsonify({"status": "ignored"})
    
    cb = data["callback_query"]
    chat_id, msg_id, cb_data = cb["message"]["chat"]["id"], cb["message"]["message_id"], cb["data"]
    parts = cb_data.split("|")
    action, target_id = parts[0], parts[1]

    # --- VIEW LEAD ---
    if action == "view_lead":
        rows, col = read_sheet_with_headers("Leads!A1:T200")
        row = next((r for r in rows if safe_get(r, col, "Lead_ID") == target_id), None)
        if row:
            text = f"👤 *Lead: {safe_get(row, col, 'Full_Name')}*\nID: `{target_id}`\nStatus: {safe_get(row, col, 'Lead_Status')}\nEvent: {safe_get(row, col, 'Event_Type')}\nBudget: {safe_get(row, col, 'Budget')}\n\n🧠 *AI Summary:*\n{safe_get(row, col, 'AI_Summary')}"
            buttons = [
                [{"text": "🔴 HOT", "callback_data": f"upd_lead|{target_id}|HOT"}, {"text": "🟡 WARM", "callback_data": f"upd_lead|{target_id}|WARM"}, {"text": "🔵 COLD", "callback_data": f"upd_lead|{target_id}|COLD"}],
                [{"text": "⬅️ Back to Leads", "callback_data": "nav_leads|none"}]
            ]
            edit_dashboard_message(chat_id, msg_id, text, {"inline_keyboard": buttons})

    # --- VIEW PIPELINE ---
    elif action == "view_pipe":
        rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
        row = next((r for r in rows if safe_get(r, col, "Lead_ID") == target_id), None)
        if row:
            text = f"📊 *Pipeline: {safe_get(row, col, 'Client_Name')}*\nID: `{target_id}`\nCurrent Stage: *{safe_get(row, col, 'Current_Stage')}*\nNext Action: {safe_get(row, col, 'Next_Action')}"
            # Show next 3 stages as buttons
            curr_idx = PIPELINE_STAGES.index(safe_get(row, col, 'Current_Stage')) if safe_get(row, col, 'Current_Stage') in PIPELINE_STAGES else -1
            next_stages = PIPELINE_STAGES[curr_idx+1:curr_idx+4]
            buttons = [[{"text": f"➡️ {s}", "callback_data": f"upd_pipe|{target_id}|{s}"}] for s in next_stages]
            buttons.append([{"text": "⬅️ Back to Pipeline", "callback_data": "nav_pipe|none"}])
            edit_dashboard_message(chat_id, msg_id, text, {"inline_keyboard": buttons})

    # --- WRITE-BACK: UPDATE LEAD ---
    elif action == "upd_lead":
        new_status = parts[2]
        rows, col = read_sheet_with_headers("Leads!A1:T200")
        for i, row in enumerate(rows, 2):
            if safe_get(row, col, "Lead_ID") == target_id:
                write_sheet(f"Leads!{get_col_letter(col['Lead_Status'])}{i}", [[new_status]])
                break
        requests.post(f"{DASHBOARD_API}/answerCallbackQuery", json={"callback_query_id": cb["id"], "text": "Status updated! ✅"})
        # Redirection back to lead view
        data["callback_query"]["data"] = f"view_lead|{target_id}"
        return dashboard()

    # --- WRITE-BACK: UPDATE PIPELINE ---
    elif action == "upd_pipe":
        new_stage = parts[2]
        rows, col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
        for i, row in enumerate(rows, 2):
            if safe_get(row, col, "Lead_ID") == target_id:
                write_sheet(f"Pipeline Tracker!{get_col_letter(col['Current_Stage'])}{i}", [[new_stage]])
                break
        requests.post(f"{DASHBOARD_API}/answerCallbackQuery", json={"callback_query_id": cb["id"], "text": "Stage updated! ✅"})
        data["callback_query"]["data"] = f"view_pipe|{target_id}"
        return dashboard()

    # --- NAVIGATION ---
    elif action == "nav_leads": handle_leads_command(chat_id)
    elif action == "nav_pipe": handle_pipeline_command(chat_id)

    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
