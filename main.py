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
ZAPIER_WEBHOOK_URL    = os.environ.get("ZAPIER_WEBHOOK_URL")
DASHBOARD_BOT_TOKEN   = os.environ.get("DASHBOARD_BOT_TOKEN")
SPREADSHEET_ID        = os.environ.get("SPREADSHEET_ID")

TELEGRAM_API          = f"https://api.telegram.org/bot{BOT_TOKEN}"
DASHBOARD_API         = f"https://api.telegram.org/bot{DASHBOARD_BOT_TOKEN}"

PENDING_CALLS_FILE    = "pending_calls.json"
SCOPES                = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# ─────────────────────────────────────────────
# PENDING CALLS HELPERS
# ─────────────────────────────────────────────

def load_pending_calls():
    if os.path.exists(PENDING_CALLS_FILE):
        with open(PENDING_CALLS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_pending_calls(data):
    with open(PENDING_CALLS_FILE, "w") as f:
        json.dump(data, f)


def is_high_value_budget(budget):
    if not budget:
        return False
    budget_str = str(budget).strip()
    return "$10,000+" in budget_str or "TBD" in budget_str.upper() or budget_str.upper() == "TBD"


# ─────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────

def get_sheets_service():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def read_sheet(range_name):
    service = get_sheets_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name
    ).execute()
    return result.get("values", [])


# ─────────────────────────────────────────────
# DASHBOARD BOT — SEND MESSAGE HELPER
# ─────────────────────────────────────────────

def send_dashboard_message(chat_id, text, parse_mode="Markdown"):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    }
    requests.post(f"{DASHBOARD_API}/sendMessage", json=payload)


# ─────────────────────────────────────────────
# DASHBOARD BOT — COMMAND HANDLERS
# ─────────────────────────────────────────────

def handle_leads_command(chat_id):
    """
    /leads — shows latest 15 leads with status summary
    Leads columns:
      0  Lead_ID         1  Timestamp       2  Full_Name
      3  Email           4  Phone           5  Event_Type
      6  Event_Date      7  Venue           8  Guest_Count
      9  Budget          10 Source          11 Message
      12 Lead_Status     13 Urgency_Score   14 AI_Summary
      15 Recommended_Action  16 Primary_Package  17 Upsell
      18 Respondent's_Email
    """
    try:
        rows = read_sheet("Leads!A2:S200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Leads sheet:\n`{str(e)}`")
        return

    if not rows:
        send_dashboard_message(chat_id, "📭 No leads found.")
        return

    hot, warm, cold = 0, 0, 0
    lines = ["📋 *LEADS OVERVIEW*\n"]

    for i, row in enumerate(rows[:15], 1):
        name    = row[2]  if len(row) > 2  else "Unknown"
        event   = row[5]  if len(row) > 5  else "?"
        status  = row[12] if len(row) > 12 else "?"
        urgency = row[13] if len(row) > 13 else "?"
        package = row[16] if len(row) > 16 else "?"

        emoji = {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}.get(
            status.upper(), "⚪"
        )
        lines.append(
            f"{i}. {emoji} *{name}* — {event}\n"
            f"   └ Status: {status} | Urgency: {urgency} | Pkg: {package}"
        )

        s = status.upper()
        if s == "HOT":    hot  += 1
        elif s == "WARM": warm += 1
        elif s == "COLD": cold += 1

    total = len(rows)
    lines.append(
        f"\n📊 *Total: {total}*\n"
        f"🔴 HOT: {hot} | 🟡 WARM: {warm} | 🔵 COLD: {cold}"
    )

    send_dashboard_message(chat_id, "\n".join(lines))


def handle_pipeline_command(chat_id):
    """
    /pipeline — shows current stage of all active projects
    Pipeline Tracker columns:
      0  Lead_ID         1  Project_ID      2  Client_Name
      3  Current_Stage   4  Last_Action     5  Next_Action
      6  Next_Action_Date  7  Call_Status   8  Proposal_Doc_URL
      9  Proposal_Sent_Date  10 Proposal_Status
    """
    try:
        rows = read_sheet("Pipeline Tracker!A2:K200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Pipeline sheet:\n`{str(e)}`")
        return

    if not rows:
        send_dashboard_message(chat_id, "📭 Pipeline is empty.")
        return

    lines = ["📊 *PIPELINE SNAPSHOT*\n"]

    for i, row in enumerate(rows[:15], 1):
        client      = row[2] if len(row) > 2 else "Unknown"
        stage       = row[3] if len(row) > 3 else "?"
        next_action = row[5] if len(row) > 5 else "?"
        next_date   = row[6] if len(row) > 6 else "?"
        project_id  = row[1] if len(row) > 1 else "?"

        lines.append(
            f"{i}. *{client}* — `{project_id}`\n"
            f"   └ Stage: {stage}\n"
            f"   └ Next: {next_action} ({next_date})"
        )

    send_dashboard_message(chat_id, "\n".join(lines))


def handle_project_command(chat_id, project_id):
    """
    /project PRJ-001 — full project record from Projects sheet
    Projects columns:
      0  Lead_ID         1  Project_ID      2  Client_Name
      3  Event_Date      4  Package         5  Zoho_Contact_ID
      6  Total_Price     7  Deposit         8  Balance
      9  Deposit_Paid    10 Contract_Sent   11 Contract_Date
      12 Current_Stage   13 Calendar_Blocked  14 Drive_Folder_Created
      15 Gallery_Folder_URL  16 Invoice_Sent  17 Invoice_Date
      18 Delivery_Date   19 Shoot_Complete  20 Review
    """
    try:
        rows = read_sheet("Projects!A2:U200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Projects sheet:\n`{str(e)}`")
        return

    match = next(
        (r for r in rows if len(r) > 1 and r[1].strip() == project_id.strip()),
        None
    )

    if not match:
        send_dashboard_message(chat_id, f"❌ Project `{project_id}` not found.")
        return

    def g(row, i):
        return row[i] if len(row) > i and row[i] else "—"

    send_dashboard_message(chat_id,
        f"📁 *Project: {g(match, 1)}*\n\n"
        f"👤 Client: {g(match, 2)}\n"
        f"📅 Event Date: {g(match, 3)}\n"
        f"📦 Package: {g(match, 4)}\n\n"
        f"💰 Total: ${g(match, 6)}\n"
        f"💳 Deposit: ${g(match, 7)} | Paid: {g(match, 9)}\n"
        f"💵 Balance: ${g(match, 8)}\n\n"
        f"📝 Contract Sent: {g(match, 10)}\n"
        f"📅 Contract Date: {g(match, 11)}\n"
        f"🎯 Stage: {g(match, 12)}\n"
        f"📅 Calendar Blocked: {g(match, 13)}\n\n"
        f"📸 Shoot Complete: {g(match, 19)}\n"
        f"📦 Delivery Date: {g(match, 18)}\n"
        f"⭐ Review: {g(match, 20)}"
    )


def handle_search_command(chat_id, query):
    """
    /search Sarah — searches Full_Name column in Leads sheet
    """
    try:
        rows = read_sheet("Leads!A2:S200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Leads sheet:\n`{str(e)}`")
        return

    q = query.lower()
    matches = [r for r in rows if len(r) > 2 and q in r[2].lower()]

    if not matches:
        send_dashboard_message(chat_id, f"🔍 No results for *{query}*")
        return

    lines = [f"🔍 *Search: {query}*\n"]
    for row in matches[:5]:
        lead_id = row[0]  if len(row) > 0  else "?"
        name    = row[2]  if len(row) > 2  else "?"
        event   = row[5]  if len(row) > 5  else "?"
        status  = row[12] if len(row) > 12 else "?"
        package = row[16] if len(row) > 16 else "?"

        lines.append(
            f"• *{name}* | {event}\n"
            f"  └ {status} | {package} | ID: `{lead_id}`"
        )

    send_dashboard_message(chat_id, "\n".join(lines))


def handle_hot_command(chat_id):
    """
    /hot — shows only HOT leads
    """
    try:
        rows = read_sheet("Leads!A2:S200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Leads sheet:\n`{str(e)}`")
        return

    hot_leads = [r for r in rows if len(r) > 12 and r[12].upper() == "HOT"]

    if not hot_leads:
        send_dashboard_message(chat_id, "🔴 No HOT leads right now.")
        return

    lines = [f"🔴 *HOT LEADS ({len(hot_leads)})*\n"]
    for i, row in enumerate(hot_leads, 1):
        name    = row[2]  if len(row) > 2  else "Unknown"
        event   = row[5]  if len(row) > 5  else "?"
        urgency = row[13] if len(row) > 13 else "?"
        package = row[16] if len(row) > 16 else "?"
        action  = row[15] if len(row) > 15 else "?"

        lines.append(
            f"{i}. *{name}* — {event}\n"
            f"   └ Urgency: {urgency} | Pkg: {package}\n"
            f"   └ Action: {action}"
        )

    send_dashboard_message(chat_id, "\n".join(lines))


def handle_today_command(chat_id):
    """
    /today — shows pipeline items where Next_Action_Date is today
    """
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")

    try:
        rows = read_sheet("Pipeline Tracker!A2:K200")
    except Exception as e:
        send_dashboard_message(chat_id, f"❌ Error reading Pipeline sheet:\n`{str(e)}`")
        return

    due_today = [r for r in rows if len(r) > 6 and today in r[6]]

    if not due_today:
        send_dashboard_message(chat_id, f"📭 Nothing due today ({today}).")
        return

    lines = [f"📅 *DUE TODAY — {today}*\n"]
    for i, row in enumerate(due_today, 1):
        client      = row[2] if len(row) > 2 else "Unknown"
        stage       = row[3] if len(row) > 3 else "?"
        next_action = row[5] if len(row) > 5 else "?"
        project_id  = row[1] if len(row) > 1 else "?"

        lines.append(
            f"{i}. *{client}* — `{project_id}`\n"
            f"   └ Stage: {stage}\n"
            f"   └ Action: {next_action}"
        )

    send_dashboard_message(chat_id, "\n".join(lines))


def handle_help_command(chat_id):
    send_dashboard_message(chat_id,
        "🤖 *Everly CRM Dashboard*\n\n"
        "Available commands:\n\n"
        "/leads — All leads overview\n"
        "/hot — HOT leads only\n"
        "/pipeline — Full pipeline snapshot\n"
        "/today — Items due today\n"
        "/search `[name]` — Search by client name\n"
        "/project `[id]` — Full project details\n\n"
        "Examples:\n"
        "`/search Sarah`\n"
        "`/project PRJ-001`"
    )


# ─────────────────────────────────────────────
# DASHBOARD BOT — WEBHOOK ROUTE
# ─────────────────────────────────────────────

@app.route("/dashboard", methods=["POST"])
def dashboard():
    data = request.json

    if "message" not in data:
        return jsonify({"status": "ignored"})

    message   = data["message"]
    chat_id   = message["chat"]["id"]
    text      = message.get("text", "").strip()

    if text == "/start" or text == "/help":
        handle_help_command(chat_id)

    elif text == "/leads":
        handle_leads_command(chat_id)

    elif text == "/pipeline":
        handle_pipeline_command(chat_id)

    elif text == "/hot":
        handle_hot_command(chat_id)

    elif text == "/today":
        handle_today_command(chat_id)

    elif text.startswith("/project "):
        project_id = text[9:].strip()
        handle_project_command(chat_id, project_id)

    elif text.startswith("/search "):
        query = text[8:].strip()
        handle_search_command(chat_id, query)

    elif text == "/project" or text == "/search":
        send_dashboard_message(chat_id,
            "⚠️ Please include a value.\n\n"
            "Usage:\n"
            "`/search Sarah`\n"
            "`/project PRJ-001`"
        )

    else:
        send_dashboard_message(chat_id,
            "❓ Unknown command. Type /help to see available commands."
        )

    return jsonify({"status": "ok"})


# ─────────────────────────────────────────────
# EXISTING CRM BOT — NOTIFY ENDPOINT
# ─────────────────────────────────────────────

@app.route("/notify", methods=["POST"])
def notify():
    data = request.json

    lead_id   = data.get("lead_id")
    lead_name = data.get("lead_name")
    event_type = data.get("event_type")
    call_date = data.get("call_date")
    call_time = data.get("call_time")
    timezone  = data.get("timezone")
    venue     = data.get("venue")
    package   = data.get("package")
    meet_link = data.get("meet_link")

    pending_calls = load_pending_calls()
    pending_calls[lead_id] = {
        "lead_id":    lead_id,
        "lead_name":  lead_name,
        "event_type": event_type,
        "call_date":  call_date,
        "call_time":  call_time
    }
    save_pending_calls(pending_calls)

    meet_line = f"\n🔗 Meet Link: {meet_link}" if meet_link else ""

    message = (
        f"📅 Discovery Call Booked\n\n"
        f"👤 Client: {lead_name}\n"
        f"🎉 Event: {event_type}\n"
        f"📍 Venue: {venue}"
        f"{meet_line}\n"
        f"📦 Package Interest: {package}\n"
        f"🕐 Call: {call_date} at {call_time}\n"
        f"🌎 Timezone: {timezone}\n"
        f"🆔 Lead ID: {lead_id}\n\n"
        f"After the call, update the outcome below:"
    )

    keyboard = {
        "inline_keyboard": [
            [{"text": "✅ Completed - Continue",     "callback_data": f"completed_continue|{lead_id}"}],
            [{"text": "❌ Completed - Not Continue", "callback_data": f"completed_stop|{lead_id}"}],
            [{"text": "👻 No Show",                  "callback_data": f"no_show|{lead_id}"}],
            [{"text": "📅 Reschedule",               "callback_data": f"reschedule|{lead_id}"}]
        ]
    }

    response = requests.post(f"{TELEGRAM_API}/sendMessage", json={
        "chat_id":                  CHAT_ID,
        "text":                     message,
        "disable_web_page_preview": True,
        "reply_markup":             keyboard
    })

    return jsonify({"status": "sent", "telegram_response": response.json()})


# ─────────────────────────────────────────────
# EXISTING CRM BOT — PROPOSAL CONFIRMED
# ─────────────────────────────────────────────

@app.route("/proposal_confirmed", methods=["POST"])
def proposal_confirmed():
    data       = request.json
    lead_id    = data.get("lead_id")
    lead_name  = data.get("lead_name")
    project_id = data.get("project_id")
    budget     = data.get("budget", "")

    pending_calls = load_pending_calls()
    if lead_id not in pending_calls:
        pending_calls[lead_id] = {
            "lead_id":    lead_id,
            "lead_name":  lead_name,
            "project_id": project_id,
            "budget":     budget
        }
    else:
        pending_calls[lead_id]["project_id"] = project_id
        pending_calls[lead_id]["budget"]     = budget
    save_pending_calls(pending_calls)

    requests.post(f"{TELEGRAM_API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": (
            f"📩 Did the client confirm the proposal?\n\n"
            f"👤 Client: {lead_name}\n"
            f"🆔 Project ID: {project_id}\n"
            f"🆔 Lead ID: {lead_id}\n\n"
            f"Once they reply yes, send the contract:"
        ),
        "reply_markup": {
            "inline_keyboard": [
                [{"text": "📝 Yes — Send Contract", "callback_data": f"send_contract|{lead_id}"}],
                [{"text": "❌ No — Close Lead",     "callback_data": f"close_lead|{lead_id}"}]
            ]
        }
    })

    return jsonify({"status": "confirmation_sent"})


# ─────────────────────────────────────────────
# EXISTING CRM BOT — INVOICE SENT
# ─────────────────────────────────────────────

@app.route("/invoice_sent", methods=["POST"])
def invoice_sent():
    data = request.json

    lead_id           = data.get("lead_id")
    lead_name         = data.get("lead_name")
    project_id        = data.get("project_id")
    package           = data.get("package")
    deposit           = data.get("deposit")
    invoice_date      = data.get("invoice_date")
    due_date          = data.get("due_date")
    gallery_folder_url = data.get("gallery_folder_url", "")

    pending_calls = load_pending_calls()
    if lead_id not in pending_calls:
        pending_calls[lead_id] = {}
    pending_calls[lead_id].update({
        "lead_id":           lead_id,
        "lead_name":         lead_name,
        "project_id":        project_id,
        "package":           package,
        "deposit":           deposit,
        "gallery_folder_url": gallery_folder_url
    })
    save_pending_calls(pending_calls)

    requests.post(f"{TELEGRAM_API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": (
            f"🖊️ Contract Signed & Invoice Sent\n\n"
            f"👤 Client: {lead_name}\n"
            f"📁 Project ID: {project_id}\n"
            f"📦 Package: {package}\n"
            f"💰 Deposit Due: ${deposit}\n"
            f"📅 Invoice Date: {invoice_date}\n"
            f"⏳ Due Date: {due_date}\n\n"
            f"Deposit invoice has been sent to the client automatically.\n"
            f"Awaiting deposit payment before blocking the calendar.\n\n"
            f"Once the client pays, tap Mark Deposit Paid below:"
        ),
        "reply_markup": {
            "inline_keyboard": [
                [{"text": "✅ Mark Deposit Paid",    "callback_data": f"deposit_paid|{lead_id}"}],
                [{"text": "📸 Mark Shoot Complete",  "callback_data": f"deliver_gallery|{lead_id}"}]
            ]
        }
    })

    return jsonify({"status": "invoice_notification_sent"})


# ─────────────────────────────────────────────
# EXISTING CRM BOT — MAIN WEBHOOK
# ─────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

    # ── Free-text messages ──
    if "message" in data:
        message_text = data["message"].get("text", "").strip()

        if message_text == "/start":
            requests.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": (
                    "👋 Everly Photography CRM Bot is active!\n\n"
                    "I'll notify you here after each Discovery Call is booked.\n"
                    "Tap the outcome buttons after each call to update the pipeline.\n\n"
                    "✅ Ready and listening."
                )
            })
            return jsonify({"status": "ok"})

        # ── Handle price input for $10,000+ leads ──
        pending_calls    = load_pending_calls()
        awaiting_lead_id = None

        for lid, info in pending_calls.items():
            if info.get("awaiting_price"):
                awaiting_lead_id = lid
                break

        if awaiting_lead_id:
            lead_info   = pending_calls[awaiting_lead_id]
            clean_input = message_text.replace(",", "").replace("$", "").strip()

            if not clean_input.isdigit():
                requests.post(f"{TELEGRAM_API}/sendMessage", json={
                    "chat_id": CHAT_ID,
                    "text": (
                        f"⚠️ Invalid input. Please type numbers only.\n"
                        f"Example: 12000\n\n"
                        f"What is the agreed total price for:\n"
                        f"👤 {lead_info.get('lead_name')} | {lead_info.get('project_id')}"
                    )
                })
                return jsonify({"status": "invalid_price"})

            total_price = int(clean_input)
            deposit     = round(total_price * 0.30)
            balance     = total_price - deposit

            pending_calls[awaiting_lead_id]["pending_total_price"] = total_price
            pending_calls[awaiting_lead_id]["pending_deposit"]     = deposit
            pending_calls[awaiting_lead_id]["pending_balance"]     = balance
            save_pending_calls(pending_calls)

            requests.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": (
                    f"⚠️ Please confirm the total price:\n\n"
                    f"👤 Client: {lead_info.get('lead_name')}\n"
                    f"📁 Project: {lead_info.get('project_id')}\n"
                    f"💰 Total Price: ${total_price:,}\n"
                    f"💳 Deposit (30%): ${deposit:,}\n"
                    f"💵 Balance (70%): ${balance:,}\n\n"
                    f"Is this correct?"
                ),
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "✅ Confirm & Send Contract", "callback_data": f"confirm_contract|{awaiting_lead_id}"}],
                        [{"text": "❌ Re-enter Price",          "callback_data": f"reenter_price|{awaiting_lead_id}"}]
                    ]
                }
            })
            return jsonify({"status": "price_confirmation_sent"})

        return jsonify({"status": "ok"})

    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    callback      = data["callback_query"]
    callback_id   = callback["id"]
    callback_data = callback["data"]
    message_id    = callback["message"]["message_id"]

    parts   = callback_data.split("|")
    action  = parts[0]
    lead_id = parts[1] if len(parts) > 1 else "unknown"

    pending_calls = load_pending_calls()
    lead_info     = pending_calls.get(lead_id, {})

    # ── Send Proposal ──
    if action == "send_proposal":
        PROPOSAL_ZAPIER_WEBHOOK = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
        if PROPOSAL_ZAPIER_WEBHOOK:
            requests.post(PROPOSAL_ZAPIER_WEBHOOK, json={
                "lead_id":   lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger":   "send_proposal"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📋 Proposal flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"📋 Proposal Flow Triggered\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"✅ Proposal is being generated and sent."
            )
        })
        return jsonify({"status": "proposal_triggered"})

    # ── Hold Proposal ──
    if action == "hold_proposal":
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "⏸ Held — send proposal when ready."
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"⏸ Proposal On Hold\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"🆔 Lead ID: {lead_id}\n\n"
                f"Proposal not sent yet. Trigger manually when ready."
            ),
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "📋 Send Proposal Now", "callback_data": f"send_proposal|{lead_id}"}]
                ]
            }
        })
        return jsonify({"status": "proposal_held"})

    # ── Send Contract ──
    if action == "send_contract":
        budget = lead_info.get("budget", "")

        if is_high_value_budget(budget):
            pending_calls[lead_id]["awaiting_price"] = True
            save_pending_calls(pending_calls)

            requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                "callback_query_id": callback_id,
                "text": "💰 Price input required"
            })
            requests.post(f"{TELEGRAM_API}/editMessageText", json={
                "chat_id":    CHAT_ID,
                "message_id": message_id,
                "text": (
                    f"📝 Contract — Price Required\n\n"
                    f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                    f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n"
                    f"💰 Budget: {budget}\n\n"
                    f"This lead requires manual price entry.\n"
                    f"Please type the agreed total price below:\n"
                    f"(numbers only, e.g. 12000)"
                )
            })
            return jsonify({"status": "awaiting_price"})

        CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
        if CONTRACT_ZAPIER_WEBHOOK:
            requests.post(CONTRACT_ZAPIER_WEBHOOK, json={
                "lead_id":   lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger":   "send_contract"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📝 Contract flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"📝 Contract Flow Triggered\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"✅ Contract is being generated and sent for signature."
            )
        })
        return jsonify({"status": "contract_triggered"})

    # ── Confirm Contract (after price entry) ──
    if action == "confirm_contract":
        total_price = lead_info.get("pending_total_price")
        deposit     = lead_info.get("pending_deposit")
        balance     = lead_info.get("pending_balance")

        pending_calls[lead_id]["awaiting_price"]    = False
        pending_calls[lead_id].pop("pending_total_price", None)
        pending_calls[lead_id].pop("pending_deposit",     None)
        pending_calls[lead_id].pop("pending_balance",     None)
        pending_calls[lead_id]["confirmed_total_price"] = total_price
        pending_calls[lead_id]["confirmed_deposit"]     = deposit
        pending_calls[lead_id]["confirmed_balance"]     = balance
        save_pending_calls(pending_calls)

        CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
        if CONTRACT_ZAPIER_WEBHOOK:
            requests.post(CONTRACT_ZAPIER_WEBHOOK, json={
                "lead_id":     lead_id,
                "lead_name":   lead_info.get("lead_name"),
                "trigger":     "send_contract",
                "total_price": total_price,
                "deposit":     deposit,
                "balance":     balance
            })

        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📝 Contract flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"📝 Contract Flow Triggered\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n"
                f"💰 Total: ${total_price:,}\n"
                f"💳 Deposit (30%): ${deposit:,}\n"
                f"💵 Balance (70%): ${balance:,}\n\n"
                f"✅ Contract is being generated and sent for signature."
            )
        })
        return jsonify({"status": "contract_triggered"})

    # ── Re-enter Price ──
    if action == "reenter_price":
        pending_calls[lead_id].pop("pending_total_price", None)
        pending_calls[lead_id].pop("pending_deposit",     None)
        pending_calls[lead_id].pop("pending_balance",     None)
        save_pending_calls(pending_calls)

        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "🔄 Please re-enter the price"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"🔄 Re-enter Total Price\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n\n"
                f"Please type the correct total price:\n"
                f"(numbers only, e.g. 12000)"
            )
        })
        return jsonify({"status": "reenter_price"})

    # ── Close Lead ──
    if action == "close_lead":
        CLOSE_LEAD_WEBHOOK = os.environ.get("CLOSE_LEAD_WEBHOOK")
        if CLOSE_LEAD_WEBHOOK:
            requests.post(CLOSE_LEAD_WEBHOOK, json={
                "lead_id":   lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger":   "close_lead"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "❌ Lead closed."
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "text": (
                f"❌ Lead Closed\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"Pipeline updated. Lead marked as closed."
            )
        })
        return jsonify({"status": "lead_closed"})

    # ─────────────────────────────────────────────
    # S3C — DEPOSIT PAID
    # ─────────────────────────────────────────────

    if action == "deposit_paid":
        DEPOSIT_PAID_WEBHOOK = os.environ.get("DEPOSIT_PAID_WEBHOOK")
        if DEPOSIT_PAID_WEBHOOK:
            requests.post(DEPOSIT_PAID_WEBHOOK, json={
                "lead_id":    lead_id,
                "project_id": lead_info.get("project_id"),
                "lead_name":  lead_info.get("lead_name"),
                "trigger":    "deposit_paid"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "✅ Deposit marked as paid"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "parse_mode": "Markdown",
            "text": (
                f"✅ *Deposit Paid — {lead_info.get('lead_name', 'Unknown')}*\n\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n"
                f"📦 Package: {lead_info.get('package', 'Unknown')}\n"
                f"💰 Deposit: ${lead_info.get('deposit', '0')}\n\n"
                f"📅 Calendar is being blocked for the shoot date.\n\n"
                f"After the shoot is done and photos are ready,\n"
                f"tap below to deliver the gallery:"
            ),
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "📸 Mark Shoot Complete", "callback_data": f"deliver_gallery|{lead_id}"}]
                ]
            }
        })
        return jsonify({"status": "deposit_paid"})

    # ─────────────────────────────────────────────
    # S4 — GALLERY DELIVERY
    # ─────────────────────────────────────────────

    if action == "deliver_gallery":
        gallery_url = lead_info.get("gallery_folder_url", "")
        drive_line  = f"\n📁 [Review Photos in Drive]({gallery_url})" if gallery_url else ""

        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "Review and confirm below 👇"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "parse_mode": "Markdown",
            "text": (
                f"⚠️ *Confirm Gallery Delivery?*\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n"
                f"📦 Package: {lead_info.get('package', 'Unknown')}"
                f"{drive_line}\n\n"
                f"This will share the folder and send the delivery email.\n"
                f"Are you sure?"
            ),
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "✅ Yes, Send Gallery", "callback_data": f"confirm_delivery|{lead_id}"}],
                    [{"text": "❌ Cancel",            "callback_data": f"cancel_delivery|{lead_id}"}]
                ]
            }
        })
        return jsonify({"status": "confirmation_shown"})

    if action == "confirm_delivery":
        DELIVER_GALLERY_WEBHOOK = os.environ.get("DELIVER_GALLERY_WEBHOOK")
        if DELIVER_GALLERY_WEBHOOK:
            requests.post(DELIVER_GALLERY_WEBHOOK, json={
                "lead_id":    lead_id,
                "lead_name":  lead_info.get("lead_name"),
                "project_id": lead_info.get("project_id"),
                "package":    lead_info.get("package"),
                "trigger":    "deliver_gallery"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📸 Gallery delivery triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "parse_mode": "Markdown",
            "text": (
                f"📸 *Gallery Delivery Triggered*\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n"
                f"📦 Package: {lead_info.get('package', 'Unknown')}\n\n"
                f"✅ Folder is being shared and delivery email is on its way."
            )
        })
        return jsonify({"status": "gallery_delivery_triggered"})

    if action == "cancel_delivery":
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "Cancelled — no action taken."
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id":    CHAT_ID,
            "message_id": message_id,
            "parse_mode": "Markdown",
            "text": (
                f"❌ *Delivery Cancelled*\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"📁 Project: {lead_info.get('project_id', 'Unknown')}\n\n"
                f"No action was taken. Tap below when ready to retry."
            ),
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "📸 Mark Shoot Complete", "callback_data": f"deliver_gallery|{lead_id}"}]
                ]
            }
        })
        return jsonify({"status": "delivery_cancelled"})

    # ─────────────────────────────────────────────
    # DISCOVERY CALL OUTCOME BUTTONS
    # ─────────────────────────────────────────────

    status_map = {
        "completed_continue": "Completed - Continue",
        "completed_stop":     "Completed - Not Continue",
        "no_show":            "No Show",
        "reschedule":         "Reschedule"
    }

    stage_map = {
        "completed_continue": "Discovery Call - Completed",
        "completed_stop":     "Discovery Call - Closed",
        "no_show":            "Discovery Call - No Show",
        "reschedule":         "Discovery Call - Rescheduled"
    }

    status        = status_map.get(action, "Unknown")
    current_stage = stage_map.get(action, "Discovery Call - Unknown")

    requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
        "callback_query_id": callback_id,
        "text": f"Logged: {status}"
    })

    requests.post(f"{TELEGRAM_API}/editMessageText", json={
        "chat_id":    CHAT_ID,
        "message_id": message_id,
        "text": (
            f"📋 Call Outcome Logged\n\n"
            f"🆔 Lead ID: {lead_id}\n"
            f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
            f"📊 Status: {status}\n\n"
            f"✅ Pipeline updated automatically."
        )
    })

    if ZAPIER_WEBHOOK_URL:
        requests.post(ZAPIER_WEBHOOK_URL, json={
            "lead_id":       lead_id,
            "lead_name":     lead_info.get("lead_name"),
            "call_status":   status,
            "action":        action,
            "current_stage": current_stage
        })

    if action == "completed_continue":
        requests.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": CHAT_ID,
            "text": (
                f"📋 Ready to send proposal?\n\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
                f"🆔 Lead ID: {lead_id}\n\n"
                f"The call went well! Send the proposal now?"
            ),
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "📋 Send Proposal", "callback_data": f"send_proposal|{lead_id}"}],
                    [{"text": "⏸ Hold for Now",  "callback_data": f"hold_proposal|{lead_id}"}]
                ]
            }
        })

    return jsonify({"status": "processed"})


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Everly CRM Bot is running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
