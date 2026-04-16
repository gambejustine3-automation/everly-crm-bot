from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
ZAPIER_WEBHOOK_URL = os.environ.get("ZAPIER_WEBHOOK_URL")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Store pending calls temporarily
pending_calls = {}

@app.route("/notify", methods=["POST"])
def notify():
    """Zapier calls this endpoint when a discovery call is booked"""
    data = request.json

    lead_id = data.get("lead_id")
    lead_name = data.get("lead_name")
    event_type = data.get("event_type")
    call_date = data.get("call_date")
    call_time = data.get("call_time")
    venue = data.get("venue")
    package = data.get("package")

    # Store lead info for when Victoria taps a button
    pending_calls[lead_id] = {
        "lead_id": lead_id,
        "lead_name": lead_name,
        "event_type": event_type,
        "call_date": call_date,
        "call_time": call_time
    }

    # Build the message
    message = (
        f"📅 *Discovery Call Booked*\n\n"
        f"👤 *Client:* {lead_name}\n"
        f"🎉 *Event:* {event_type}\n"
        f"📍 *Venue:* {venue}\n"
        f"📦 *Package Interest:* {package}\n"
        f"🕐 *Call:* {call_date} at {call_time}\n"
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
        "reply_markup": keyboard
    })

    return jsonify({"status": "sent", "telegram_response": response.json()})


@app.route("/webhook", methods=["POST"])
def webhook():
    """Telegram calls this when Victoria taps a button"""
    data = request.json

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

    status = status_map.get(action, "Unknown")
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
            "action": action
        })

    return jsonify({"status": "processed"})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Everly CRM Bot is running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
