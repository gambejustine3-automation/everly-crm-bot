from flask import Flask, request, jsonify
import requests
import os
import json

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
ZAPIER_WEBHOOK_URL = os.environ.get("ZAPIER_WEBHOOK_URL")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
PENDING_CALLS_FILE = "pending_calls.json"


def load_pending_calls():
    if os.path.exists(PENDING_CALLS_FILE):
        with open(PENDING_CALLS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_pending_calls(data):
    with open(PENDING_CALLS_FILE, "w") as f:
        json.dump(data, f)


def is_high_value_budget(budget):
    """Check if budget requires manual price entry"""
    if not budget:
        return False
    budget_str = str(budget).strip()
    return "$10,000+" in budget_str or "TBD" in budget_str.upper() or budget_str.upper() == "TBD"


@app.route("/notify", methods=["POST"])
def notify():
    """Zapier calls this endpoint when a discovery call is booked"""
    data = request.json

    lead_id = data.get("lead_id")
    lead_name = data.get("lead_name")
    event_type = data.get("event_type")
    call_date = data.get("call_date")
    call_time = data.get("call_time")
    timezone = data.get("timezone")
    venue = data.get("venue")
    package = data.get("package")
    meet_link = data.get("meet_link")

    pending_calls = load_pending_calls()
    pending_calls[lead_id] = {
        "lead_id": lead_id,
        "lead_name": lead_name,
        "event_type": event_type,
        "call_date": call_date,
        "call_time": call_time
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
            [{"text": "✅ Completed - Continue", "callback_data": f"completed_continue|{lead_id}"}],
            [{"text": "❌ Completed - Not Continue", "callback_data": f"completed_stop|{lead_id}"}],
            [{"text": "👻 No Show", "callback_data": f"no_show|{lead_id}"}],
            [{"text": "📅 Reschedule", "callback_data": f"reschedule|{lead_id}"}]
        ]
    }

    response = requests.post(f"{TELEGRAM_API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
        "reply_markup": keyboard
    })

    return jsonify({"status": "sent", "telegram_response": response.json()})


@app.route("/proposal_confirmed", methods=["POST"])
def proposal_confirmed():
    """
    Called after proposal is sent — asks Victoria if client confirmed.
    POST body: lead_id, lead_name, project_id, budget
    """
    data = request.json
    lead_id = data.get("lead_id")
    lead_name = data.get("lead_name")
    project_id = data.get("project_id")
    budget = data.get("budget", "")

    pending_calls = load_pending_calls()
    if lead_id not in pending_calls:
        pending_calls[lead_id] = {
            "lead_id": lead_id,
            "lead_name": lead_name,
            "project_id": project_id,
            "budget": budget
        }
    else:
        pending_calls[lead_id]["project_id"] = project_id
        pending_calls[lead_id]["budget"] = budget
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
                [{"text": "❌ No — Close Lead", "callback_data": f"close_lead|{lead_id}"}]
            ]
        }
    })

    return jsonify({"status": "confirmation_sent"})


@app.route("/webhook", methods=["POST"])
def webhook():
    """Telegram calls this when Victoria taps a button or sends a message"""
    data = request.json

    # --- Handle regular messages (free text input) ---
    if "message" in data:
        message_text = data["message"].get("text", "").strip()
        chat_id = data["message"]["chat"]["id"]

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

        # --- Handle price input for $10,000+ leads ---
        pending_calls = load_pending_calls()
        awaiting_lead_id = None

        for lid, info in pending_calls.items():
            if info.get("awaiting_price"):
                awaiting_lead_id = lid
                break

        if awaiting_lead_id:
            lead_info = pending_calls[awaiting_lead_id]

            # Validate input is a number
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
            deposit = round(total_price * 0.30)
            balance = total_price - deposit

            # Store pending price for confirmation
            pending_calls[awaiting_lead_id]["pending_total_price"] = total_price
            pending_calls[awaiting_lead_id]["pending_deposit"] = deposit
            pending_calls[awaiting_lead_id]["pending_balance"] = balance
            save_pending_calls(pending_calls)

            # Send confirmation message
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
                        [{"text": "❌ Re-enter Price", "callback_data": f"reenter_price|{awaiting_lead_id}"}]
                    ]
                }
            })
            return jsonify({"status": "price_confirmation_sent"})

        return jsonify({"status": "ok"})

    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    callback = data["callback_query"]
    callback_id = callback["id"]
    callback_data = callback["data"]
    message_id = callback["message"]["message_id"]

    parts = callback_data.split("|")
    action = parts[0]
    lead_id = parts[1] if len(parts) > 1 else "unknown"

    pending_calls = load_pending_calls()
    lead_info = pending_calls.get(lead_id, {})

    # --- Send Proposal ---
    if action == "send_proposal":
        PROPOSAL_ZAPIER_WEBHOOK = os.environ.get("PROPOSAL_ZAPIER_WEBHOOK")
        if PROPOSAL_ZAPIER_WEBHOOK:
            requests.post(PROPOSAL_ZAPIER_WEBHOOK, json={
                "lead_id": lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger": "send_proposal"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📋 Proposal flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": (
                f"📋 Proposal Flow Triggered\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"✅ Proposal is being generated and sent."
            )
        })
        return jsonify({"status": "proposal_triggered"})

    # --- Hold Proposal ---
    if action == "hold_proposal":
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "⏸ Held — send proposal when ready."
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
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

    # --- Send Contract ---
    if action == "send_contract":
        budget = lead_info.get("budget", "")

        # High value lead — ask Victoria to type the price first
        if is_high_value_budget(budget):
            pending_calls[lead_id]["awaiting_price"] = True
            save_pending_calls(pending_calls)

            requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
                "callback_query_id": callback_id,
                "text": "💰 Price input required"
            })
            requests.post(f"{TELEGRAM_API}/editMessageText", json={
                "chat_id": CHAT_ID,
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

        # Normal lead — fire contract webhook immediately
        CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
        if CONTRACT_ZAPIER_WEBHOOK:
            requests.post(CONTRACT_ZAPIER_WEBHOOK, json={
                "lead_id": lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger": "send_contract"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📝 Contract flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": (
                f"📝 Contract Flow Triggered\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"✅ Contract is being generated and sent for signature."
            )
        })
        return jsonify({"status": "contract_triggered"})

    # --- Confirm Contract (after price entry) ---
    if action == "confirm_contract":
        total_price = lead_info.get("pending_total_price")
        deposit = lead_info.get("pending_deposit")
        balance = lead_info.get("pending_balance")

        # Clear awaiting state and pending values
        pending_calls[lead_id]["awaiting_price"] = False
        pending_calls[lead_id].pop("pending_total_price", None)
        pending_calls[lead_id].pop("pending_deposit", None)
        pending_calls[lead_id].pop("pending_balance", None)
        pending_calls[lead_id]["confirmed_total_price"] = total_price
        pending_calls[lead_id]["confirmed_deposit"] = deposit
        pending_calls[lead_id]["confirmed_balance"] = balance
        save_pending_calls(pending_calls)

        CONTRACT_ZAPIER_WEBHOOK = os.environ.get("CONTRACT_ZAPIER_WEBHOOK")
        if CONTRACT_ZAPIER_WEBHOOK:
            requests.post(CONTRACT_ZAPIER_WEBHOOK, json={
                "lead_id": lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger": "send_contract",
                "total_price": total_price,
                "deposit": deposit,
                "balance": balance
            })

        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "📝 Contract flow triggered ✅"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
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

    # --- Re-enter Price ---
    if action == "reenter_price":
        pending_calls[lead_id].pop("pending_total_price", None)
        pending_calls[lead_id].pop("pending_deposit", None)
        pending_calls[lead_id].pop("pending_balance", None)
        save_pending_calls(pending_calls)

        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "🔄 Please re-enter the price"
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
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

    # --- Close Lead ---
    if action == "close_lead":
        CLOSE_LEAD_WEBHOOK = os.environ.get("CLOSE_LEAD_WEBHOOK")
        if CLOSE_LEAD_WEBHOOK:
            requests.post(CLOSE_LEAD_WEBHOOK, json={
                "lead_id": lead_id,
                "lead_name": lead_info.get("lead_name"),
                "trigger": "close_lead"
            })
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": "❌ Lead closed."
        })
        requests.post(f"{TELEGRAM_API}/editMessageText", json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": (
                f"❌ Lead Closed\n\n"
                f"🆔 Lead ID: {lead_id}\n"
                f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n\n"
                f"Pipeline updated. Lead marked as closed."
            )
        })
        return jsonify({"status": "lead_closed"})

    # --- Call outcome buttons ---
    status_map = {
        "completed_continue": "Completed - Continue",
        "completed_stop": "Completed - Not Continue",
        "no_show": "No Show",
        "reschedule": "Reschedule"
    }

    stage_map = {
        "completed_continue": "Discovery Call - Completed",
        "completed_stop": "Discovery Call - Closed",
        "no_show": "Discovery Call - No Show",
        "reschedule": "Discovery Call - Rescheduled"
    }

    status = status_map.get(action, "Unknown")
    current_stage = stage_map.get(action, "Discovery Call - Unknown")

    requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
        "callback_query_id": callback_id,
        "text": f"Logged: {status}"
    })

    requests.post(f"{TELEGRAM_API}/editMessageText", json={
        "chat_id": CHAT_ID,
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
            "lead_id": lead_id,
            "lead_name": lead_info.get("lead_name"),
            "call_status": status,
            "action": action,
            "current_stage": current_stage
        })

    # After Completed Continue: ask about proposal
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
                    [{"text": "⏸ Hold for Now", "callback_data": f"hold_proposal|{lead_id}"}]
                ]
            }
        })

    return jsonify({"status": "processed"})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Everly CRM Bot is running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
