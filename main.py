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
    """Load pending calls from file"""
    if os.path.exists(PENDING_CALLS_FILE):
        with open(PENDING_CALLS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_pending_calls(data):
    """Save pending calls to file"""
    with open(PENDING_CALLS_FILE, "w") as f:
        json.dump(data, f)


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

    # Load, update, and save pending calls
    pending_calls = load_pending_calls()
    pending_calls[lead_id] = {
        "lead_id": lead_id,
        "lead_name": lead_name,
        "event_type": event_type,
        "call_date": call_date,
        "call_time": call_time
    }
    save_pending_calls(pending_calls)

    # Build meet link line conditionally
    meet_line = f"\n🔗 *Meet Link:* {meet_link}" if meet_link else ""

    # Build the message
    message = (
        f"📅 *Discovery Call Booked*\n\n"
        f"👤 *Client:* {lead_name}\n"
        f"🎉 *Event:* {event_type}\n"
        f"📍 *Venue:* {venue}"
        f"{meet_line}\n"
        f"📦 *Package Interest:* {package}\n"
        f"🕐 *Call:* {call_date} at {call_time}\n"
        f"🌎 *Timezone:* {timezone}\n"
        f"🆔 *Lead ID:* {lead_id}\n\n"
        f"After the call, update the outcome below:"
    )

    # Build inline keyboard buttons
    keyboard = {
        "inline_keyboard": [
            [{"text": "✅ Completed - Continue", "callback_data": f"completed_continue|{lead_id}"}],
            [{"text": "❌ Completed - Not Continue", "callback_data": f"completed_stop|{lead_id}"}],
            [{"text": "👻 No Show", "callback_data": f"no_show|{lead_id}"}],
            [{"text": "📅 Reschedule", "callback_data": f"reschedule|{lead_id}"}]
        ]
    }

    # Send to Telegram
    response = requests.post(f"{TELEGRAM_API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
        "reply_markup": keyboard
    })

    return jsonify({"status": "sent", "telegram_response": response.json()})


@app.route("/webhook", methods=["POST"])
def webhook():
    """Telegram calls this when Victoria sends a message or taps a button"""
    data = request.json

    # Handle incoming text messages (e.g. /start)
    if "message" in data:
        message_text = data["message"].get("text", "")
        if message_text == "/start":
            requests.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": (
                    "👋 *Everly Photography CRM Bot is active!*\n\n"
                    "I'll notify you here after each Discovery Call is booked.\n"
                    "Tap the outcome buttons after each call to update the pipeline automatically.\n\n"
                    "✅ Ready and listening."
                ),
                "parse_mode": "Markdown"
            })
        return jsonify({"status": "ok"})

    # Ignore anything that isn't a callback query
    if "callback_query" not in data:
        return jsonify({"status": "ignored"})

    callback = data["callback_query"]
    callback_id = callback["id"]
    callback_data = callback["data"]
    message_id = callback["message"]["message_id"]

    # Parse the callback
    parts = callback_data.split("|")
    action = parts[0]
    lead_id = parts[1] if len(parts) > 1 else "unknown"

    # Map action to status
    status_map = {
        "completed_continue": "Completed - Continue",
        "completed_stop": "Completed - Not Continue",
        "no_show": "No Show",
        "reschedule": "Reschedule"
    }

    # Map action to clean stage name
    stage_map = {
        "completed_continue": "Discovery Call - Completed",
        "completed_stop": "Discovery Call - Closed",
        "no_show": "Discovery Call - No Show",
        "reschedule": "Discovery Call - Rescheduled"
    }

    status = status_map.get(action, "Unknown")
    current_stage = stage_map.get(action, "Discovery Call - Unknown")

    # Load pending calls from file
    pending_calls = load_pending_calls()
    lead_info = pending_calls.get(lead_id, {})

    # Answer the callback (removes loading spinner)
    requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={
        "callback_query_id": callback_id,
        "text": f"Logged: {status}"
    })

    # Edit the original message to show the outcome
    requests.post(f"{TELEGRAM_API}/editMessageText", json={
        "chat_id": CHAT_ID,
        "message_id": message_id,
        "text": (
            f"📋 *Call Outcome Logged*\n\n"
            f"🆔 Lead ID: {lead_id}\n"
            f"👤 Client: {lead_info.get('lead_name', 'Unknown')}\n"
            f"📊 Status: *{status}*\n\n"
            f"✅ Pipeline updated automatically."
        ),
        "parse_mode": "Markdown"
    })

    # Send outcome to Zapier webhook
    if ZAPIER_WEBHOOK_URL:
        requests.post(ZAPIER_WEBHOOK_URL, json={
            "lead_id": lead_id,
            "lead_name": lead_info.get("lead_name"),
            "call_status": status,
            "action": action,
            "current_stage": current_stage
        })

    return jsonify({"status": "processed"})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Everly CRM Bot is running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
