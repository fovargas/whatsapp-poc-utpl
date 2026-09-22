import os
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "UTPL WhatsApp POC"
    }


@app.get("/webhook")
async def verify_webhook(request: Request):
    params = request.query_params

    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return PlainTextResponse(challenge)

    return PlainTextResponse("Forbidden", status_code=403)


async def send_message(to: str, text: str):

    url = (
        f"https://graph.facebook.com/v23.0/"
        f"{PHONE_NUMBER_ID}/messages"
    )

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {
            "body": text
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            url,
            headers=headers,
            json=payload
        )

        print(
            "Respuesta de WhatsApp:",
            response.status_code,
            response.text
        )


@app.post("/webhook")
async def receive_webhook(request: Request):

    body = await request.json()

    print("Evento recibido desde Meta:")
    print(body)

    try:
        value = (
            body["entry"][0]
            ["changes"][0]
            ["value"]
        )

        messages = value.get("messages", [])

        if not messages:
            return {"status": "ok"}

        message = messages[0]

        from_number = message["from"]

        if message["type"] == "text":

            text = (
                message["text"]["body"]
                .strip()
                .lower()
            )

            print(
                f"Mensaje recibido de {from_number}: {text}"
            )

            if text == "hola":

                await send_message(
                    from_number,
                    "Hola 👋 Bienvenido al servicio de información UTPL.\n\n"
                    "Puedes consultar nuestros servicios en:\n"
                    "https://info.utpl.edu.ec/"
                )

    except Exception as e:
        print("Error procesando webhook:", str(e))

    return {"status": "ok"}