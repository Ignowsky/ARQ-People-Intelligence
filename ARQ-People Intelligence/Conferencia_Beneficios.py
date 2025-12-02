# INSTALAÇÃO DAS BIBLIOTECAS
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Configuração
load_dotenv()
API_TOKEN = os.getenv('SOLIDES_API_TOKEN')
BASE_URL = "https://app.solides.com/pt-BR/api/v1"
HEADERS = {"Authorization": f"Token token={API_TOKEN}", "Accept": "application/json"}

print("--- Diagnóstico de Benefícios ---")

# 1. Pega uma lista pequena de colaboradores
url = f"{BASE_URL}/colaboradores?page=1&page_size=20&status=todos"
resp = requests.get(url, headers=HEADERS)

if resp.status_code != 200:
    print(f"Erro na API: {resp.status_code}")
else:
    lista = resp.json()
    encontrou_algum = False
    
    print(f"Verificando {len(lista)} colaboradores...")
    
    for colab in lista:
        # Para cada um, busca o detalhe (onde ficam os benefícios)
        id_colab = colab.get('id')
        resp_detalhe = requests.get(f"{BASE_URL}/colaboradores/{id_colab}", headers=HEADERS)
        
        if resp_detalhe.status_code == 200:
            dados = resp_detalhe.json()
            
            # Verifica os campos
            total = dados.get('totalBenefits', 'N/A')
            lista_beneficios = dados.get('benefits', [])
            
            # Se tiver qualquer coisa diferente de zero ou vazio, avisa
            if (total != "0.0" and total != 0) or (len(lista_beneficios) > 0):
                print(f"[ACHEI!] ID {id_colab} ({dados.get('name')})")
                print(f"   - Total: {total}")
                print(f"   - Lista: {lista_beneficios}")
                encontrou_algum = True
                # Se quiser parar no primeiro que achar, descomente abaixo:
            #break 
    
    if not encontrou_algum:
        print("\n[RESULTADO] Nenhum benefício encontrado nos primeiros 20 colaboradores.")
        print("Possíveis causas: Dados realmente não cadastrados na Sólides ou falta de permissão no Token.")