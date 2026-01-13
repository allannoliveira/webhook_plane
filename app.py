import hmac
import hashlib
import requests
from flask import Flask, request, jsonify
from config import (
    PLANE_SECRET,
    GOOGLE_CHAT_WEBHOOK,
    PLANE_PROJECT_ID
)

app = Flask(__name__)

# =====================================================
# Utils
# =====================================================

def validar_assinatura(secret: str, body: bytes, assinatura_recebida: str) -> bool:
    assinatura_calculada = hmac.new(
        secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(assinatura_calculada, assinatura_recebida)


def normalizar(valor, padrao="N/A"):
    if valor in [None, "", [], {}, "none"]:
        return padrao
    return valor


def lista_para_texto(lista, campo="name", padrao="Nenhuma"):
    if not lista:
        return padrao
    valores = [item.get(campo) for item in lista if item.get(campo)]
    return ", ".join(valores) if valores else padrao


# =====================================================
# Webhook Plane
# =====================================================

@app.route("/webhooks/plane", methods=["POST"])
def plane_webhook():
    # -------------------------------------------------
    # Validação da assinatura
    # -------------------------------------------------
    raw_body = request.get_data()
    signature = request.headers.get("X-Plane-Signature")

    if not signature:
        return jsonify({"error": "missing signature"}), 401

    if not validar_assinatura(PLANE_SECRET, raw_body, signature):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.json or {}

    event = payload.get("event")
    action = payload.get("action")

    # Trabalhamos apenas com issues
    if event != "issue":
        return jsonify({"status": "ignored_event"}), 200

    data = payload.get("data", {})
    activity = payload.get("activity", {})

    # -------------------------------------------------
    # 🔒 FILTRO POR PROJETO (SUPORTE TI - HELP DESK)
    # -------------------------------------------------
    project_id = data.get("project")

    if project_id != PLANE_PROJECT_ID:
        print(f"Ignorado projeto {project_id}")
        return jsonify({"status": "ignored_project"}), 200

    # =================================================
    # Dados da Issue
    # =================================================

    numero = normalizar(data.get("sequence_id"))
    titulo = normalizar(data.get("name"), "Sem título")

    status = normalizar(data.get("state", {}).get("name"))
    prioridade = normalizar(data.get("priority"))
    pontos = normalizar(data.get("point"))

    responsaveis = lista_para_texto(
        data.get("assignees", []),
        campo="display_name",
        padrao="Não atribuído"
    )

    labels = lista_para_texto(
        data.get("labels", []),
        campo="name",
        padrao="Nenhuma"
    )

    inicio = normalizar(data.get("start_date"), "—")
    prazo = normalizar(data.get("target_date"), "—")

    autor = activity.get("actor", {}).get("display_name", "Usuário")

    # =================================================
    # Título do Card
    # =================================================

    if action == "created":
        header_title = "🆕 Nova Issue criada"
    elif action == "updated":
        header_title = "✏️ Issue atualizada"
    elif action == "deleted":
        header_title = "🗑️ Issue removida"
    else:
        header_title = "📌 Evento de Issue"

    # =================================================
    # Card Google Chat (Cards V2 – compatível)
    # =================================================

    mensagem = {
        "cardsV2": [
            {
                "cardId": "plane-issue",
                "card": {
                    "header": {
                        "title": header_title,
                        "subtitle": f"#{numero} • {titulo}",
                        "imageUrl": "https://www.gstatic.com/images/icons/material/system/1x/assignment_googblue_48dp.png"
                    },
                    "sections": [
                        {
                            "widgets": [
                                {"decoratedText": {"topLabel": "Status", "text": status}},
                                {"decoratedText": {"topLabel": "Prioridade", "text": prioridade}},
                                {"decoratedText": {"topLabel": "Responsável", "text": responsaveis}},
                                {"decoratedText": {"topLabel": "Labels", "text": labels}},
                            ]
                        },
                        {
                            "widgets": [
                                {"decoratedText": {"topLabel": "Pontos", "text": str(pontos)}},
                                {"decoratedText": {"topLabel": "Início", "text": inicio}},
                                {"decoratedText": {"topLabel": "Prazo", "text": prazo}},
                            ]
                        },
                        {
                            "widgets": [
                                {
                                    "textParagraph": {
                                        "text": f"<i>Atualizado por {autor}</i>"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        ]
    }

    # =================================================
    # Envio ao Google Chat
    # =================================================

    resp = requests.post(
        GOOGLE_CHAT_WEBHOOK,
        json=mensagem,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        timeout=5
    )

    print("Google Chat status:", resp.status_code)

    return jsonify({"status": "ok"}), 200


# =====================================================
# Main
# =====================================================

if __name__ == "__main__":
    app.run(port=5000, debug=True)
