import hmac
import hashlib
import json
import threading
import time
from datetime import datetime, timezone

import gspread
import requests
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from config import (
    PLANE_SECRET,
    GOOGLE_CHAT_WEBHOOK,
    PLANE_PROJECT_ID
)

app = Flask(__name__)

# =====================================================
# Cache de deduplicacao (em memoria)
# =====================================================

_ultimo_evento: dict = {}
_dedup_lock = threading.Lock()
DEDUP_JANELA_SEGUNDOS = 5

# Set de issues atualmente sendo processadas pelo worker (evita duplo disparo)
_issues_processando: set = set()
_processando_lock = threading.Lock()


def ja_processado_recentemente(issue_id: str) -> bool:
    agora = time.time()
    with _dedup_lock:
        ultimo = _ultimo_evento.get(issue_id)
        if ultimo and (agora - ultimo) < DEDUP_JANELA_SEGUNDOS:
            return True
        _ultimo_evento[issue_id] = agora
        return False


# =====================================================
# Google Sheets -- configuracao
# =====================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1QZbabFKLsvcnGJn5VsXbT99vupJvAV-U22_3dC4CDzk"
SHEET_NAME = "Sheet1"
DEBOUNCE_SEGUNDOS = 30

_sheets_lock = threading.Lock()


def get_sheet():
    creds = Credentials.from_service_account_file(
        "google_service_account.json",
        scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def garantir_cabecalho(sheet):
    primeira = sheet.row_values(1)
    if not primeira:
        sheet.append_row(
            ["issue_id", "ultimo_evento", "dados_json", "status_enviado"],
            value_input_option="RAW"
        )


def buscar_linha(sheet, issue_id: str):
    registros = sheet.get_all_values()
    for i, row in enumerate(registros[1:], start=2):
        if row and row[0] == issue_id:
            return i, row
    return None, None


def salvar_pendente(issue_id: str, dados: dict):
    with _sheets_lock:
        sheet = get_sheet()
        garantir_cabecalho(sheet)
        agora = datetime.now(timezone.utc).isoformat()
        row_idx, _ = buscar_linha(sheet, issue_id)
        if row_idx:
            sheet.update(
                values=[[issue_id, agora, json.dumps(dados, ensure_ascii=False), "pendente"]],
                range_name=f"A{row_idx}:D{row_idx}"
            )
        else:
            sheet.append_row(
                [issue_id, agora, json.dumps(dados, ensure_ascii=False), "pendente"],
                value_input_option="RAW"
            )


def marcar_status_sheet(issue_id: str, status: str):
    with _sheets_lock:
        sheet = get_sheet()
        row_idx, _ = buscar_linha(sheet, issue_id)
        if row_idx:
            sheet.update_cell(row_idx, 4, status)


# =====================================================
# Mapas de traducao
# =====================================================

STATUS_MAP = {
    "backlog":     "Backlog",
    "todo":        "A Fazer",
    "to do":       "A Fazer",
    "in_progress": "Em andamento",
    "in progress": "Em andamento",
    "doing":       "Em andamento",
    "done":        "Finalizado",
    "completed":   "Finalizado"
}

PRIORITY_MAP = {
    "none":   "Sem prioridade",
    "low":    "Baixa",
    "medium": "Media",
    "high":   "Alta",
    "urgent": "Urgente"
}

PRIORITY_EMOJI = {
    "Sem prioridade": "",
    "Baixa":          "🔽",
    "Media":          "🔶",
    "Alta":           "🔴",
    "Urgente":        "🚨"
}

STATUS_EMOJI = {
    "Backlog":       "📋",
    "A Fazer":       "📝",
    "Em andamento":  "⚙️",
    "Finalizado":    "✅"
}

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


def traduzir(valor: str, mapa: dict, padrao="N/A"):
    if not valor:
        return padrao
    return mapa.get(valor.lower(), valor)


def lista_para_texto(lista, campo="name", padrao="Nenhuma"):
    if not lista:
        return padrao
    valores = [item.get(campo) for item in lista if item.get(campo)]
    return ", ".join(valores) if valores else padrao



# =====================================================
# Background worker -- verifica debounce a cada 10s
# =====================================================

def worker_debounce():
    while True:
        time.sleep(10)
        try:
            with _sheets_lock:
                sheet = get_sheet()
                registros = sheet.get_all_values()

            agora = datetime.now(timezone.utc)

            for i, row in enumerate(registros[1:], start=2):
                if len(row) < 4:
                    continue

                issue_id, ultimo_evento_str, dados_json, status_enviado = row[:4]

                # 🔥 só processa pendente
                if status_enviado != "pendente":
                    continue

                try:
                    ultimo_evento = datetime.fromisoformat(ultimo_evento_str)
                except ValueError:
                    continue

                if ultimo_evento.tzinfo is None:
                    ultimo_evento = ultimo_evento.replace(tzinfo=timezone.utc)

                # ⏱️ debounce
                if (agora - ultimo_evento).total_seconds() < DEBOUNCE_SEGUNDOS:
                    continue

                # 🔒 evita concorrência
                with _processando_lock:
                    if issue_id in _issues_processando:
                        continue
                    _issues_processando.add(issue_id)

                marcar_status_sheet(issue_id, "processando")

                try:
                    # 🔥 NÃO envia mais nada
                    # apenas valida JSON
                    json.loads(dados_json)

                    # ✅ pronto pro Apps Script
                    marcar_status_sheet(issue_id, "processado")

                    print(f"[debounce] Issue pronta para Apps Script {issue_id}")

                except Exception as e:
                    marcar_status_sheet(issue_id, "pendente")
                    print(f"[debounce] Erro issue {issue_id}: {e}")

                finally:
                    with _processando_lock:
                        _issues_processando.discard(issue_id)

        except Exception as e:
            print(f"[worker_debounce] Erro geral: {e}")
            
threading.Thread(target=worker_debounce, daemon=True).start()


# =====================================================
# Webhook Plane
# =====================================================

@app.route("/webhooks/plane", methods=["POST"])
def plane_webhook():
    raw_body  = request.get_data()
    signature = request.headers.get("X-Plane-Signature")

    if not signature:
        return jsonify({"error": "missing signature"}), 401

    if not validar_assinatura(PLANE_SECRET, raw_body, signature):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.json or {}
    event   = payload.get("event")
    action  = payload.get("action")

    if event != "issue":
        return jsonify({"status": "ignored_event"}), 200

    data = payload.get("data", {})

    # Filtro por projeto
    project_id = data.get("project")
    if project_id != PLANE_PROJECT_ID:
        print(f"Ignorado projeto {project_id}")
        return jsonify({"status": "ignored_project"}), 200

    issue_id = data.get("id")
    if not issue_id:
        return jsonify({"error": "missing issue id"}), 400

    # Deduplicacao -- ignora se ja recebemos um evento dessa issue nos ultimos 5s
    if ja_processado_recentemente(issue_id):
        print(f"[dedup] Evento duplicado ignorado para issue {issue_id}")
        return jsonify({"status": "ignored_duplicate"}), 200

    # Envio imediato apenas para delecao
    if action == "deleted":
        print(f"[imediato] Tarefa deletada {issue_id}")
        enviar_google_chat(payload)
        marcar_status_sheet(issue_id, "enviado")
        return jsonify({"status": "ok_imediato"}), 200

    # Debounce: criacao, edicoes e mudancas de status aguardam 30s
    salvar_pendente(issue_id, payload)
    print(f"[debounce] Issue {issue_id} salva/atualizada na fila")
    return jsonify({"status": "ok_debounce"}), 200


# =====================================================
# Main
# =====================================================

if __name__ == "__main__":
    app.run(port=5500, debug=True)