import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse


# ============================================================
# CONFIGURACION
# ============================================================

app = FastAPI(
    title="UTPL WhatsApp Event Gateway",
    version="1.0.0"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("utpl-whatsapp-gateway")


# ============================================================
# CLOUDFLARE QUEUES
# ============================================================

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_QUEUE_ID = os.getenv("CF_QUEUE_ID")
CF_QUEUES_TOKEN = os.getenv("CF_QUEUES_TOKEN")


# ============================================================
# WHATSAPP / META
# ============================================================

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")

# Estas dos variables NO son necesarias para recibir mensajes.
# Las usaremos después si Render también necesita enviar mensajes.
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")


# ============================================================
# HELPERS
# ============================================================

def get_queue_url() -> str:

    if not CF_ACCOUNT_ID:
        raise RuntimeError("CF_ACCOUNT_ID no configurado")

    if not CF_QUEUE_ID:
        raise RuntimeError("CF_QUEUE_ID no configurado")

    return (
        "https://api.cloudflare.com/client/v4/"
        f"accounts/{CF_ACCOUNT_ID}/queues/{CF_QUEUE_ID}/messages"
    )


async def publish_event(event: dict[str, Any]) -> dict:
    """
    Publica un evento en Cloudflare Queue.
    """

    if not CF_QUEUES_TOKEN:
        raise RuntimeError(
            "CF_QUEUES_TOKEN no configurado"
        )

    headers = {
        "Authorization": f"Bearer {CF_QUEUES_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "body": event,
        "content_type": "json"
    }

    async with httpx.AsyncClient(
        timeout=15.0
    ) as client:

        response = await client.post(
            get_queue_url(),
            headers=headers,
            json=payload
        )

    logger.info(
        "Cloudflare status=%s body=%s",
        response.status_code,
        response.text
    )

    if not response.is_success:

        raise HTTPException(
            status_code=502,
            detail={
                "message":
                    "Error publicando en Cloudflare Queue",

                "status":
                    response.status_code,

                "response":
                    response.text
            }
        )

    return response.json()


# ============================================================
# HEALTH
# ============================================================

@app.get("/")
async def root():

    return {
        "status": "ok",
        "service": "UTPL WhatsApp Event Gateway",
        "queue_configured": bool(
            CF_ACCOUNT_ID
            and CF_QUEUE_ID
            and CF_QUEUES_TOKEN
        ),
        "whatsapp_configured":
            bool(VERIFY_TOKEN)
    }


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat()
    }


# ============================================================
# POC POSTMAN
# ============================================================

@app.post("/poc/message")
async def poc_message(
    request: Request
):

    body = await request.json()

    text = body.get("text")

    if not text:

        raise HTTPException(
            status_code=400,
            detail="El campo text es obligatorio"
        )

    event = {
        "event_id":
            str(uuid.uuid4()),

        "event_type":
            "poc.message.received",

        "event_version":
            "1.0",

        "source":
            "postman",

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "payload": {
            "from":
                body.get(
                    "from",
                    "postman-user"
                ),

            "text":
                text,

            "metadata":
                body.get(
                    "metadata",
                    {}
                )
        }
    }

    response = await publish_event(event)

    return {
        "status":
            "accepted",

        "event_id":
            event["event_id"],

        "queue_response":
            response
    }


# ============================================================
# META WEBHOOK VERIFICATION
# ============================================================

@app.get("/webhook")
async def verify_webhook(
    request: Request
):

    params = request.query_params

    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    logger.info(
        "Webhook verification mode=%s",
        mode
    )

    if (
        mode == "subscribe"
        and token == VERIFY_TOKEN
    ):

        return PlainTextResponse(
            challenge or ""
        )

    return PlainTextResponse(
        "Forbidden",
        status_code=403
    )


# ============================================================
# WHATSAPP WEBHOOK
# ============================================================

@app.post("/webhook")
async def receive_whatsapp_webhook(
    request: Request
):

    body = await request.json()

    logger.info(
        "Evento recibido de Meta: %s",
        body
    )

    try:

        entries = body.get(
            "entry",
            []
        )

        # Meta puede enviar varios entries.
        for entry in entries:

            changes = entry.get(
                "changes",
                []
            )

            for change in changes:

                value = change.get(
                    "value",
                    {}
                )

                messages = value.get(
                    "messages",
                    []
                )

                # Los eventos de delivered/read/etc.
                # normalmente no tienen messages.
                if not messages:
                    continue

                for message in messages:

                    message_type = message.get(
                        "type"
                    )

                    from_number = message.get(
                        "from"
                    )

                    whatsapp_message_id = (
                        message.get("id")
                    )

                    # --------------------------------
                    # POR AHORA SOLO TEXTO
                    # --------------------------------

                    if message_type != "text":

                        logger.info(
                            "Ignorando mensaje tipo %s",
                            message_type
                        )

                        continue

                    text = (
                        message
                        .get("text", {})
                        .get("body", "")
                        .strip()
                    )

                    if not text:
                        continue

                    event = {

                        "event_id":
                            str(
                                uuid.uuid4()
                            ),

                        "event_type":
                            "whatsapp.message.received",

                        "event_version":
                            "1.0",

                        "source":
                            "meta-whatsapp",

                        "timestamp":
                            datetime.now(
                                timezone.utc
                            ).isoformat(),

                        "payload": {

                            "from":
                                from_number,

                            "text":
                                text,

                            "whatsapp_message_id":
                                whatsapp_message_id,

                            "phone_number_id":
                                value
                                .get(
                                    "metadata",
                                    {}
                                )
                                .get(
                                    "phone_number_id"
                                ),

                            "display_phone_number":
                                value
                                .get(
                                    "metadata",
                                    {}
                                )
                                .get(
                                    "display_phone_number"
                                )
                        }
                    }

                    await publish_event(
                        event
                    )

    except Exception as exc:

        logger.exception(
            "Error procesando webhook"
        )

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )

    # Meta espera respuesta rápida.
    return {
        "status": "accepted"
    }