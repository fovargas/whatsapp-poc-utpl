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
    title="UTPL Event Gateway POC",
    version="1.0.0"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("utpl-event-gateway")


# ------------------------------------------------------------
# CLOUDFLARE QUEUES
# ------------------------------------------------------------

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_QUEUE_ID = os.getenv("CF_QUEUE_ID")
CF_QUEUES_TOKEN = os.getenv("CF_QUEUES_TOKEN")


def get_queue_url() -> str:
    if not CF_ACCOUNT_ID:
        raise RuntimeError("CF_ACCOUNT_ID no esta configurado")

    if not CF_QUEUE_ID:
        raise RuntimeError("CF_QUEUE_ID no esta configurado")

    return (
        "https://api.cloudflare.com/client/v4/"
        f"accounts/{CF_ACCOUNT_ID}/queues/{CF_QUEUE_ID}/messages"
    )


# ------------------------------------------------------------
# WHATSAPP
#
# POR AHORA NO SE USA.
# Lo dejamos preparado para la segunda etapa.
# ------------------------------------------------------------

# VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
# ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
# PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")


# ============================================================
# UTILIDADES
# ============================================================

async def publish_event(event: dict[str, Any]) -> dict:
    """
    Publica un evento JSON en Cloudflare Queue.
    """

    if not CF_QUEUES_TOKEN:
        raise RuntimeError("CF_QUEUES_TOKEN no esta configurado")

    url = get_queue_url()

    headers = {
        "Authorization": f"Bearer {CF_QUEUES_TOKEN}",
        "Content-Type": "application/json"
    }

    # Cloudflare espera:
    #
    # {
    #   "body": { ... }
    # }
    #
    payload = {
        "body": event
    }

    logger.info(
        "Publicando evento %s en Cloudflare Queue",
        event.get("event_id")
    )

    async with httpx.AsyncClient(timeout=15.0) as client:

        response = await client.post(
            url,
            headers=headers,
            json=payload
        )

    logger.info(
        "Cloudflare status=%s response=%s",
        response.status_code,
        response.text
    )

    if not response.is_success:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "No fue posible publicar el evento en Cloudflare Queue",
                "cloudflare_status": response.status_code,
                "cloudflare_response": response.text
            }
        )

    try:
        return response.json()

    except Exception:
        return {
            "success": True,
            "raw_response": response.text
        }


# ============================================================
# ENDPOINTS GENERALES
# ============================================================

@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "UTPL Event Gateway POC",
        "queue_enabled": bool(
            CF_ACCOUNT_ID
            and CF_QUEUE_ID
            and CF_QUEUES_TOKEN
        )
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ============================================================
# POC
#
# ESTE ES EL ENDPOINT QUE USAREMOS DESDE POSTMAN
# ============================================================

@app.post("/poc/message")
async def poc_message(request: Request):

    try:
        body = await request.json()

    except Exception:
        raise HTTPException(
            status_code=400,
            detail="El body debe ser JSON valido"
        )

    # --------------------------------------------------------
    # Simulamos que este mensaje eventualmente vendra
    # desde WhatsApp.
    # --------------------------------------------------------

    text = body.get("text")

    if not text:
        raise HTTPException(
            status_code=400,
            detail="El campo 'text' es obligatorio"
        )

    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "event_type": "poc.message.received",
        "event_version": "1.0",
        "source": "postman",
        "timestamp": datetime.now(timezone.utc).isoformat(),

        "payload": {
            "from": body.get(
                "from",
                "postman-user"
            ),
            "text": text,
            "metadata": body.get(
                "metadata",
                {}
            )
        }
    }

    cloudflare_response = await publish_event(event)

    return {
        "status": "accepted",
        "message": "Evento enviado a Cloudflare Queue",
        "event_id": event_id,
        "event": event,
        "queue_response": cloudflare_response
    }


# ============================================================
# WHATSAPP - SEGUNDA ETAPA
# ============================================================
#
# Cuando terminemos la POC con Postman,
# habilitaremos estos endpoints.
#
#
# @app.get("/webhook")
# async def verify_webhook(request: Request):
#
#     params = request.query_params
#
#     mode = params.get("hub.mode")
#     token = params.get("hub.verify_token")
#     challenge = params.get("hub.challenge")
#
#     if (
#         mode == "subscribe"
#         and token == VERIFY_TOKEN
#     ):
#         return PlainTextResponse(challenge)
#
#     return PlainTextResponse(
#         "Forbidden",
#         status_code=403
#     )
#
#
# @app.post("/webhook")
# async def receive_whatsapp_webhook(request: Request):
#
#     body = await request.json()
#
#     logger.info(
#         "Evento recibido desde WhatsApp: %s",
#         body
#     )
#
#     try:
#
#         value = (
#             body["entry"][0]
#             ["changes"][0]
#             ["value"]
#         )
#
#         messages = value.get(
#             "messages",
#             []
#         )
#
#         # Meta tambien envia eventos de:
#         #
#         # delivered
#         # read
#         # sent
#         #
#         # que no contienen messages.
#
#         if not messages:
#             return {
#                 "status": "ok"
#             }
#
#         message = messages[0]
#
#         # En esta primera version
#         # procesaremos solamente texto.
#
#         if message.get("type") != "text":
#             return {
#                 "status": "ignored",
#                 "reason": "unsupported_message_type"
#             }
#
#         from_number = message["from"]
#
#         text = (
#             message["text"]["body"]
#             .strip()
#         )
#
#         event = {
#             "event_id": str(uuid.uuid4()),
#
#             "event_type":
#                 "whatsapp.message.received",
#
#             "event_version": "1.0",
#
#             "source":
#                 "meta-whatsapp",
#
#             "timestamp":
#                 datetime.now(
#                     timezone.utc
#                 ).isoformat(),
#
#             "payload": {
#                 "from": from_number,
#                 "text": text,
#                 "whatsapp_message_id":
#                     message.get("id")
#             }
#         }
#
#         await publish_event(event)
#
#     except Exception as exc:
#
#         logger.exception(
#             "Error procesando webhook de WhatsApp"
#         )
#
#         # Importante:
#         #
#         # luego revisaremos que respuesta
#         # queremos devolver a Meta en caso
#         # de error.
#
#         raise HTTPException(
#             status_code=500,
#             detail=str(exc)
#         )
#
#     return {
#         "status": "accepted"
#     }


# ============================================================
# ENVIO WHATSAPP - SEGUNDA ETAPA
# ============================================================
#
# Lo activaremos desde n8n posteriormente.
#
#
# async def send_whatsapp_message(
#     to: str,
#     text: str
# ):
#
#     url = (
#         "https://graph.facebook.com/v23.0/"
#         f"{PHONE_NUMBER_ID}/messages"
#     )
#
#     headers = {
#         "Authorization":
#             f"Bearer {ACCESS_TOKEN}",
#
#         "Content-Type":
#             "application/json"
#     }
#
#     payload = {
#         "messaging_product":
#             "whatsapp",
#
#         "to":
#             to,
#
#         "type":
#             "text",
#
#         "text": {
#             "body":
#                 text
#         }
#     }
#
#     async with httpx.AsyncClient() as client:
#
#         response = await client.post(
#             url,
#             headers=headers,
#             json=payload
#         )
#
#     logger.info(
#         "WhatsApp response %s %s",
#         response.status_code,
#         response.text
#     )
#
#     return response