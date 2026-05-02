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

def _write_back(sheet_name, range_name, lookup_col_name, lookup_value, updates):
    """
    Helper to write back updates to a Google Sheet based on a lookup value.
    """
    rows, col = read_sheet_with_headers(range_name)
    found_row_idx = -1
    for i, row in enumerate(rows):
        if safe_get(row, col, lookup_col_name) == lookup_value:
            found_row_idx = i
            break

    if found_row_idx == -1:
        print(f"[WRITE BACK ERROR] {lookup_value} not found in {sheet_name} for column {lookup_col_name}")
        return False

    # Prepare the row for update
    target_row = rows[found_row_idx]
    updated_values = [v for v in target_row] # Make a copy

    for key, value in updates.items():
        if key in col:
            updated_values[col[key]] = value
        else:
            print(f"[WRITE BACK WARNING] Column {key} not found in {sheet_name}. Skipping update for this key.")

    # Google Sheets API expects a list of lists for values
    update_range = f"{get_col_letter(0)}{found_row_idx + 2}:{get_col_letter(len(updated_values) - 1)}{found_row_idx + 2}"
    return write_sheet(update_range, [updated_values])

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
# ─────────────────────────────────────────────
def send_msg(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    else:
        # If reply_markup is explicitly None, remove the existing inline keyboard
        payload["reply_markup"] = {"inline_keyboard": []}
    r = requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)
    print(f"[DASHBOARD SEND] {r.status_code}: {r.text[:200]}")
    return r # Return the response object to get message_id

def edit_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    else:
        # If reply_markup is explicitly None, remove the existing inline keyboard
        payload["reply_markup"] = {"inline_keyboard": []}
    requests.post(f"{DASHBOARD_API}/editMessageText", json=payload)

def edit_pipeline_msg(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    else:
        # If reply_markup is explicitly None, remove the existing inline keyboard
        payload["reply_markup"] = {"inline_keyboard": []}
    requests.post(f"{PIPELINE_API}/editMessageText", json=payload)

def send_pipeline_msg(chat_id, text, reply_markup=None):
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    else:
        # If reply_markup is explicitly None, remove the existing inline keyboard
        payload["reply_markup"] = {"inline_keyboard": []}
    r = requests.post(f"{PIPELINE_API}/sendMessage", json=payload)
    return r # Return the response object to get message_id

def send_client_msg(chat_id, text):
    requests.post(f"{CLIENT_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

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
# CONFIG HELPERS — Briefing Time
# ─────────────────────────────────────────────
def get_briefing_time():
    try:
        rows, col = read_sheet_with_headers("Config!A1:B20")
        for row in rows:
            if safe_get(row, col, "Key") == "briefing_time":
                val = safe_get(row, col, "Value")
                if val and val != "—":
                    return val
    except Exception as e:
        print(f"[CONFIG] Error reading briefing_time: {e}")
    return "09:00"

def write_briefing_time(time_str):
    try:
        rows, col = read_sheet_with_headers("Config!A1:B20")
        for i, row in enumerate(rows, 2):
            if safe_get(row, col, "Key") == "briefing_time":
                write_sheet(f"Config!B{i}", [[time_str]])
                return
        next_row = len(rows) + 2
        write_sheet(f"Config!A{next_row}:B{next_row}", [["briefing_time", time_str]])
    except Exception as e:
        print(f"[CONFIG] Error writing briefing_time: {e}")

# ─────────────────────────────────────────────
# SCHEDULER — Daily Jobs
# ─────────────────────────────────────────────
def _execute_retention(lead_id):
    """
    Triggers the retention sequence by firing the Zapier webhook.
    Sends an initial 'processing' message and captures its message_id.
    """
    lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
    lead_row  = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == lead_id), None)

    if not lead_row:
        print(f"[RETENTION] Lead {lead_id} not found.")
        return {"fired": False}

    pipe_rows, pipe_col = read_sheet_with_headers("Pipeline Tracker!A1:L200")
    pipe_row  = next((r for r in pipe_rows if safe_get(r, pipe_col, "Lead_ID") == lead_id), None)

    project_id   = safe_get(pipe_row, pipe_col, "Project_ID")  if pipe_row  else "—"
    client_name  = safe_get(lead_row, lead_col, "Full_Name")   if lead_row  else "—"
    client_email = safe_get(lead_row, lead_col, "Email")       if lead_row  else "—"
    event_type   = safe_get(lead_row, lead_col, "Event_Type")  if lead_row  else "—"

    stats    = _update_client_stats(lead_id)
    new_ltv  = stats["ltv"]      if stats else 0
    new_tier = stats["tier"]     if stats else "Standard"
    bookings = stats["bookings"] if stats else 1

    today_str = ph_now().strftime("%Y-%m-%d")

    # Send initial processing message and capture message_id
    initial_msg_text = f"⭐ *Processing Retention Sequence for `{lead_id}` ({client_name})...*\n_Please wait, this may take a moment._"
    # Assuming send_pipeline_msg returns the response object from which message_id can be extracted
    sent_message_response = smart_send(CHAT_ID, initial_msg_text, use_pipeline=True)
    message_id = None
    if sent_message_response and sent_message_response.status_code == 200:
        try:
            message_id = sent_message_response.json().get("result", {}).get("message_id")
        except json.JSONDecodeError:
            print(f"[RETENTION] Could not decode JSON from Telegram response: {sent_message_response.text}")

    # Pipeline Tracker → Retention stage
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage":    "Retention",
        "Last_Action":      "Review Request Sent",
        "Next_Action":      "Await Review — Rebooking in 7 days",
        "Next_Action_Date": today_str
    })

    # Fire Zapier webhook (Everly & Co. — System 5 Review + Rebooking Sequence trigger) with message_id
    fired = fire_webhook(RETENTION_WEBHOOK, {
        "lead_id":      lead_id,
        "message_id":   message_id, # New parameter to pass to Zapier
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
        "message_id":   message_id # Return message_id for potential direct use if webhook fails
    }

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
        _show_client(chat_id, msg_id, target_id, method="edit")

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
        _write_back("Leads", "Leads!A1:T200", "Lead_ID", target_id, {"Lead_Status": new_status})
        answer_callback(cb["id"], f"✅ Status updated to {new_status}", use_pipeline)
        _show_lead(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "upd_pipe" and len(parts) > 2:
        new_stage = parts[2]
        today_str = ph_now().strftime("%Y-%m-%d")
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id,
                    {"Current_Stage": new_stage, "Last_Action": f"Stage moved to {new_stage}", "Next_Action_Date": today_str})
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
        fired = fire_webhook(PROPOSAL_ZAPIER_WEBHOOK, {"lead_id": target_id})
        answer_callback(cb["id"], "📄 Proposal triggered", use_pipeline)
        msg_text = (
            f"📄 Proposal triggered for `{target_id}`\nCheck your email and Telegram for confirmation."
            if fired else f"⚠️ Webhook failed — check PROPOSAL_ZAPIER_WEBHOOK in Railway."
        )
        smart_send(chat_id, msg_text, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action in ("send_contract", "confirm_contract"):
        _confirm_contract(chat_id, msg_id, target_id, use_pipeline=use_pipeline)

    elif action == "contract_yes":
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        budget   = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""
        name     = safe_get(lead_row, lead_col, "Full_Name") if lead_row else target_id

        if budget in UNCERTAIN_BUDGETS:
            answer_callback(cb["id"], "Budget unclear — enter exact amount", use_pipeline)
            prompt_text = (
                f"💰 *Budget Confirmation Required*\n"
                f"Lead: `{target_id}` — {name}\n"
                f"Budget on file: *{budget}*\n\n"
                f"Type the confirmed total:\n\n"
                f"`/setbudget {target_id} <amount>`\n"
                f"Example: `/setbudget {target_id} 13000`"
            )
            if use_pipeline:
                edit_pipeline_msg(chat_id, msg_id, prompt_text)
            else:
                edit_msg(chat_id, msg_id, prompt_text)
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
            if use_pipeline:
                edit_pipeline_msg(chat_id, msg_id, confirmed_text)
            else:
                edit_msg(chat_id, msg_id, confirmed_text)

    elif action == "contract_no":
        # Close lead logic (not fully detailed in original, assuming a simple update)
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage": "Closed Lost",
            "Last_Action": "Contract declined",
            "Next_Action": "Archive Lead",
            "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
        })
        answer_callback(cb["id"], "❌ Contract declined. Lead closed.", use_pipeline)
        smart_send(chat_id, f"❌ Lead `{target_id}` closed due to contract decline.", msg_id, use_pipeline)

    elif action == "budget_contract_yes" and len(parts) > 2:
        lead_id = target_id
        total_amount = parts[2]
        # Update lead budget in Sheets
        _write_back("Leads", "Leads!A1:T200", "Lead_ID", lead_id, {"Budget": total_amount})

        fired = fire_webhook(CONTRACT_ZAPIER_WEBHOOK, {"lead_id": lead_id})
        answer_callback(cb["id"], "✅ Contract triggered with confirmed budget", use_pipeline)
        confirmed_text = (
            f"✅ *Contract Triggered — System 3A*\n"
            f"Lead: `{lead_id}` (Budget: ${total_amount})\n\n"
            f"• Contract sent for e-signature\n"
            f"• Watch Telegram for confirmation"
        ) if fired else (
            f"⚠️ *Webhook failed for `{lead_id}`*\n"
            f"Check CONTRACT_ZAPIER_WEBHOOK in Railway."
        )
        smart_send(chat_id, confirmed_text, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "budget_contract_edit":
        # Re-show the setbudget prompt
        lead_rows, lead_col = read_sheet_with_headers("Leads!A1:T200")
        lead_row = next((r for r in lead_rows if safe_get(r, lead_col, "Lead_ID") == target_id), None)
        name = safe_get(lead_row, lead_col, "Full_Name") if lead_row else target_id
        budget = safe_get(lead_row, lead_col, "Budget").strip() if lead_row else ""

        prompt_text = (
            f"💰 *Budget Confirmation Required*\n"
            f"Lead: `{target_id}` — {name}\n"
            f"Budget on file: *{budget}*\n\n"
            f"Type the confirmed total:\n\n"
            f"`/setbudget {target_id} <amount>`\n"
            f"Example: `/setbudget {target_id} 13000`"
        )
        smart_send(chat_id, prompt_text, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "deposit_paid_confirm":
        today_str   = ph_now().strftime("%Y-%m-%d")
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")      if proj_row else "—"

        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {
            "Deposit_Paid": "TRUE"
        })
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Current_Stage":    "Active Project",
            "Last_Action":      "Deposit Received",
            "Next_Action":      "Begin Project Workflow",
            "Next_Action_Date": today_str
        })

        answer_callback(cb["id"], "💰 Deposit marked as paid!", use_pipeline)

        text = (
            f"💰 *Deposit Confirmed Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name}\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"💵 Amount: ${balance} (assuming balance was deposit amount for now)\n"
            f"📅 Received: {today_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Deposit marked as paid in the system.\n"
            f"Project moved to *Active Project* stage."
        )
        smart_send(chat_id, text, msg_id=msg_id, use_pipeline=use_pipeline)

    elif action == "deliver_gallery_confirm":
        _execute_deliver_gallery(chat_id, msg_id, target_id, cb["id"], use_pipeline=use_pipeline)

    # ── Balance Paid — confirmation screen ──
    elif action == "balance_paid":
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name")    if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")         if proj_row else "—"
        due_date    = safe_get(proj_row, proj_col, "Balance_Due_Date") if proj_row else "—"
        text = (
            f"💳 *Confirm Balance Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name} | Lead: `{target_id}`\n\n"
            f"💰 Amount: *${balance}*\n"
            f"📅 Due: {due_date}\n\n"
            f"⚠️ Confirm you have received the full balance payment in Zoho?"
        )
        buttons = [
            [
                {"text": "✅ Yes — Mark Paid", "callback_data": f"balance_paid_confirm|{target_id}"},
                {"text": "❌ Cancel",           "callback_data": f"view_project|{target_id}"}
            ]
        ]
        smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)

    # ── Balance Paid — execute write + show retention CTA ──
    elif action == "balance_paid_confirm":
        today_str   = ph_now().strftime("%Y-%m-%d")
        proj_rows, proj_col = read_sheet_with_headers("Projects!A1:Z200")
        proj_row    = next((r for r in proj_rows if safe_get(r, proj_col, "Lead_ID") == target_id), None)
        client_name = safe_get(proj_row, proj_col, "Client_Name") if proj_row else "—"
        project_id  = safe_get(proj_row, proj_col, "Project_ID")  if proj_row else "—"
        balance     = safe_get(proj_row, proj_col, "Balance")      if proj_row else "—"

        _write_back("Projects", "Projects!A1:Z200", "Lead_ID", target_id, {
            "Balance_Paid": "TRUE"
        })
        _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", target_id, {
            "Last_Action":      "Balance Received",
            "Next_Action":      "Run Retention Sequence",
            "Next_Action_Date": today_str
        })

        answer_callback(cb["id"], "💰 Balance marked as paid!", use_pipeline)

        text = (
            f"💰 *Balance Confirmed Paid*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {client_name}\n"
            f"🆔 Lead: `{target_id}` | Project: `{project_id}`\n"
            f"💵 Amount: ${balance}\n"
            f"📅 Received: {today_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ Balance marked as paid in the system.\n"
            f"Ready to send the retention sequence to the client?"
        )
        buttons = [[{"text": "⭐ Run Retention", "callback_data": f"trigger_retention_confirm|{target_id}"}]]
        smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)

    # ── Retention — confirmation screen before firing ──
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
            f"• Update client LTV and tier\n\n"
            f"⚠️ Confirm?"
        )
        buttons = [
            [
                {"text": "✅ Yes — Send It", "callback_data": f"trigger_retention|{target_id}"},
                {"text": "❌ Cancel",         "callback_data": f"view_project|{target_id}"}
            ]
        ]
        smart_send(chat_id, text, {"inline_keyboard": buttons}, msg_id=msg_id, use_pipeline=use_pipeline)

    # ── Retention execute — calls shared helper ──
    elif action == "trigger_retention":
        result = _execute_retention(target_id)
        answer_callback(cb["id"], "⭐ Retention sequence initiated.", use_pipeline)

        # The smart_send for the full confirmation is now handled by /retention_notify
        # This block can be simplified or removed if _execute_retention handles the initial message
        # and /retention_notify handles the update.
        # For now, we'll keep a minimal message here if the webhook failed.
        if not result["fired"]:
            smart_send(chat_id, "⚠️ Webhook failed — check RETENTION_WEBHOOK in Railway.", msg_id=msg_id, use_pipeline=use_pipeline)



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
        elif text.startswith("/client"):
            parts = text.split()
            client_id = parts[1] if len(parts) > 1 else None
            handle_client_command(chat_id, client_id)
        elif text.startswith("/search"):
            parts = text.split()
            query = parts[1] if len(parts) > 1 else None
            handle_search_command(chat_id, query)
        elif text.startswith("/updateemail"):
            parts = text.split()
            if len(parts) == 3:
                handle_update_email_command(chat_id, parts[1], parts[2])
            else:
                send_msg(chat_id, "Usage: `/updateemail <Lead_ID> <new_email>`")
        elif text == "/resetleadcounter":
            handle_reset_lead_counter_command(chat_id)
        elif text.startswith("/retention"):
            parts = text.split()
            lead_id = parts[1] if len(parts) > 1 else None
            if lead_id:
                # Directly trigger the confirmation UI
                _confirm_retention(chat_id, None, lead_id, use_pipeline=True)
            else:
                send_pipeline_msg(chat_id, "Usage: `/retention <Lead_ID>`")
        elif text.startswith("/setbudget"):
            parts = text.split()
            if len(parts) == 3:
                handle_set_budget_command(chat_id, parts[1], parts[2])
            else:
                send_msg(chat_id, "Usage: `/setbudget <Lead_ID> <amount>`")
        elif text == "/briefing":
            handle_briefing_command(chat_id)
        elif text.startswith("/setbriefingtime"):
            parts = text.split()
            if len(parts) == 2:
                handle_set_briefing_time_command(chat_id, parts[1])
            else:
                send_msg(chat_id, "Usage: `/setbriefingtime HH:MM`")

    elif "callback_query" in data:
        handle_callbacks(data, use_pipeline=False)

    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# MAIN WEBHOOK — PIPELINE BOT
# ─────────────────────────────────────────────
@app.route("/pipeline", methods=["POST"])
def pipeline():
    data = request.json
    if "callback_query" in data:
        handle_callbacks(data, use_pipeline=True)
    # The pipeline bot doesn't handle text commands, only button callbacks.
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# ZAPIER NOTIFY ROUTES — Lead Ingestion
# ─────────────────────────────────────────────
@app.route("/new_lead_notify", methods=["POST"])
def new_lead_notify():
    data = request.json
    lead_id = data.get("lead_id", "N/A")
    name = data.get("name", "N/A")
    email = data.get("email", "N/A")
    phone = data.get("phone", "N/A")
    event_type = data.get("event_type", "N/A")
    event_date = data.get("event_date", "N/A")
    budget = data.get("budget", "N/A")
    source = data.get("source", "N/A")
    ai_summary = data.get("ai_summary", "N/A")
    recommended_action = data.get("recommended_action", "N/A")

    text = (
        f"✨ *New Lead Ingested*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Lead: `{lead_id}`\n"
        f"👤 Name: *{name}*\n"
        f"✉️ Email: {email}\n"
        f"📞 Phone: {phone}\n"
        f"🎉 Event: {event_type} on {event_date}\n"
        f"💰 Budget: {budget}\n"
        f"🌐 Source: {source}\n"
        f"🤖 AI Summary: _{ai_summary}_\n"
        f"💡 Recommended Action: *{recommended_action}*\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    buttons = [[{"text": "View Lead Card", "callback_data": f"view_lead|{lead_id}"}]]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# ZAPIER NOTIFY ROUTES — System 3A Contract + Deposit
# ─────────────────────────────────────────────
@app.route("/contract_sent_notify", methods=["POST"])
def contract_sent_notify():
    data = request.json
    lead_id = data.get("lead_id", "—")
    lead_name = data.get("lead_name", "—")
    project_id = data.get("project_id", "—")
    contract_status = data.get("contract_status", "—")
    proposal_name = data.get("proposal_name", "—")

    text = (
        f"✍️ *Contract Sent for E-Signature*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"📄 Proposal: {proposal_name}\n"
        f"⏳ Status: {contract_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"System will notify you when the client signs and pays the deposit."
    )
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "Markdown"
    })
    return jsonify({"status": "ok"})

@app.route("/deposit_paid_notify", methods=["POST"])
def deposit_paid_notify():
    data = request.json
    lead_id = data.get("lead_id", "—")
    lead_name = data.get("lead_name", "—")
    project_id = data.get("project_id", "—")
    deposit_amt = data.get("deposit_amt", "—")
    payment_date = data.get("payment_date", "—")

    text = (
        f"💰 *Deposit Payment Received!*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lead_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"💰 Amount: ${deposit_amt}\n"
        f"📅 Received: {payment_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Deposit payment confirmed in Zoho.\n"
        f"Project is now *Active* and ready for pre-production."
    )
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "Markdown"
    })
    return jsonify({"status": "ok"})

@app.route("/gallery_notify", methods=["POST"])
def gallery_notify():
    data         = request.json
    lead_id      = data.get("lead_id",      "—")
    client_name  = data.get("lead_name",    "—")
    project_id   = data.get("project_id",   "—")
    gallery_link = data.get("gallery_url",   "—")
    delivery_date = data.get("delivery_date", "—")

    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Current_Stage": "Delivered",
        "Gallery_Link": gallery_link,
        "Delivery_Date": delivery_date
    })
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Current_Stage": "Delivered",
        "Last_Action": "Gallery Delivered",
        "Next_Action": "Await Balance Payment / Run Retention",
        "Next_Action_Date": ph_now().strftime("%Y-%m-%d")
    })

    text = (
        f"📸 *Gallery Delivered*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {client_name}\n"
        f"🆔 Lead: `{lead_id}` | Project: `{project_id}`\n"
        f"🔗 [View Gallery]({gallery_link})\n"
        f"📅 Delivered: {delivery_date}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Gallery delivered to client.\n"
        f"Project moved to *Delivered* stage.\n"
        f"Remember to check for balance payment and run retention sequence."
    )
    buttons = [[{"text": "💳 Mark Balance Paid", "callback_data": f"balance_paid|{lead_id}"}]]
    requests.post(f"{PIPELINE_API}/sendMessage", json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": buttons}
    })
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────
# ZAPIER NOTIFY ROUTES — Everly & Co. — System 5 Review + Rebooking Sequence (Review Request)
#
# CRITICAL: Does NOT set Current_Stage = "Completed".
# Projects must stay "Delivered" so APScheduler can find the row
# after 7 days and auto-complete via check_retention_completions().
# ─────────────────────────────────────────────
@app.route("/retention_notify", methods=["POST"])
def retention_notify():
    data        = request.json
    lead_id     = data.get("lead_id",     "—")
    message_id  = data.get("message_id") # Retrieve message_id from Zapier payload
    client_name = data.get("client_name", "—")
    project_id  = data.get("project_id",  "—")
    event_type  = data.get("event_type",  "—")
    new_ltv     = data.get("new_ltv",     "—")
    new_tier    = data.get("new_tier",    "—")
    bookings    = data.get("bookings",    "—")

    tier_emoji = {"VIP": "⭐", "Premium": "💎", "Standard": "🔹"}.get(str(new_tier), "👤")
    today_str  = ph_now().strftime("%Y-%m-%d")

    # ── Write review flags only — DO NOT set Current_Stage = "Completed" ──
    # Projects must stay "Delivered" so APScheduler check_retention_completions()
    # can find this row and auto-complete it after the 7-day window.
    _write_back("Projects", "Projects!A1:Z200", "Lead_ID", lead_id, {
        "Review_Sent":      "TRUE",
        "Review_Sent_Date": today_str
    })

    # Pipeline: confirm in-flight Retention stage, update action notes
    # Current_Stage is already "Retention" (set by trigger_retention callback)
    _write_back("Pipeline Tracker", "Pipeline Tracker!A1:L200", "Lead_ID", lead_id, {
        "Last_Action":      "Review Request Email Sent",
        "Next_Action":      "Await 7-day Window — Rebooking Email Queued",
        "Next_Action_Date": today_str
    })

    # Reorder the message to have the completion title at the bottom
    text = (
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
        f"⭐ *Everly & Co. — System 5 Review + Rebooking Sequence (Review Request) Complete*\n"
    )

    # Edit the existing message using smart_send, removing the inline keyboard
    smart_send(CHAT_ID, text, markup=None, msg_id=message_id, use_pipeline=True)

    return jsonify({"status": "ok"})


# ─────────────────────────────────────────────
# ZAPIER NOTIFY ROUTES — Everly & Co. — System 5 Review + Rebooking Sequence (Rebooking Upsell)
# ─────────────────────────────────────────────
@app.route("/retention_rebooking_notify", methods=["POST"])
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
    # Pipeline → Closed Won (parallel to APScheduler — both are safe/idempotent)
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
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
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
