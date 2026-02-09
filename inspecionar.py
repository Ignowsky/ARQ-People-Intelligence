import requests
import json
import time

# --- CONFIGURAÇÃO ---
# Insira seu Token de produção
API_TOKEN = "a5658a69da14d2ba5b849844abf4fc9988a63ea9f4d2daafb0d1"
BASE_URL = "https://app.solides.com/pt-BR/api/v1"
COLABORADOR_ID = 2117091  # ID do colaborador alvo


def generate_strict_debug():
    print(f"--- INICIANDO DEBUG ESTRITO (Apenas endpoint /colaboradores/{COLABORADOR_ID}) ---")

    headers = {
        "Authorization": f"Token token={API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    url = f"{BASE_URL}/colaboradores/{COLABORADOR_ID}"
    print(f"GET {url}")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Garante que vamos capturar erros 404/401/500

        data = response.json()

        # Salva exatamente o que a API devolveu
        filename = f"debug_solides_strict_{COLABORADOR_ID}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"\n[SUCESSO] Arquivo gerado: {filename}")
        print("Este arquivo contém apenas os dados nativos do endpoint de colaboradores.")

    except requests.exceptions.RequestException as e:
        print(f"\n[ERRO] Falha na requisição:")
        print(e)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta da API: {e.response.text}")


if __name__ == "__main__":
    generate_strict_debug()