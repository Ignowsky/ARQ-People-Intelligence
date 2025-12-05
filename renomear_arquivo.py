import sys
import os

# --- CORREÇÃO DE CAMINHO ---
sys.path.append(os.getcwd())

import shutil
import pdfplumber
import re
from src.extract import extrair_info_base 

# --- CONFIGURAÇÃO ---
PASTA_ALVO = "C:\Projetos\ARQ-People Intelligence - Pessoal\input"

def definir_nome_padronizado(texto_pdf, info_base):
    # 1. Definir a DATA
    competencia_str = info_base.get('competencia')
    
    if not competencia_str:
        # Tenta um último fallback bruto se a função extract falhou
        match_pagto = re.search(r'(?:pagamento|data)[:\s]+\d{2}/(\d{2}/\d{4})', texto_pdf, re.IGNORECASE)
        if match_pagto:
            competencia_str = match_pagto.group(1)
            print(f"   (DEBUG) Data encontrada via fallback local: {competencia_str}")
            
    if not competencia_str:
        return None, None

    try:
        mes, ano = competencia_str.split('/')
        prefixo_data = f"{ano}-{mes}"
    except:
        return None, None

    # 2. Definir o TIPO
    tipo_bruto = str(info_base.get('tipo_calculo') or "").lower()
    texto_lower = texto_pdf.lower()

    if "13" in tipo_bruto or "décimo" in tipo_bruto or "13º" in texto_lower or "13o" in texto_lower:
        tipo_final = "13_Salario"
    else:
        tipo_final = "Folha_Mensal"

    return prefixo_data, tipo_final

def executar_renomeacao():
    print(f"\n=======================================================")
    print(f"   ORGANIZADOR DE ARQUIVOS (MODO DEBUG)")
    print(f"=======================================================\n")
    
    if not os.path.exists(PASTA_ALVO):
        print(f"[ERRO] A pasta '{PASTA_ALVO}' não existe.")
        return

    arquivos = [f for f in os.listdir(PASTA_ALVO) if f.lower().endswith('.pdf')]
    
    if not arquivos:
        print(f"[AVISO] Nenhum PDF encontrado em '{PASTA_ALVO}'.")
        return

    count_ok = 0
    count_ignorado = 0

    for arquivo in arquivos:
        if re.match(r'^\d{4}-\d{2}_', arquivo):
            continue

        print(f"\n> Analisando: {arquivo}")
        caminho_full = os.path.join(PASTA_ALVO, arquivo)
        
        try:
            with pdfplumber.open(caminho_full) as pdf:
                if not pdf.pages: 
                    print(f"   [!] Arquivo vazio.")
                    continue
                
                texto_pagina = pdf.pages[0].extract_text() or ""
                
                # DEBUG: Mostra o início do texto para conferirmos o padrão
                print(f"   [TEXTO EXTRAÍDO]: {texto_pagina[:100].replace(chr(10), ' ')}...") 
                
                dados_extraidos = extrair_info_base(texto_pagina)
                print(f"   [DADOS SRC]: {dados_extraidos}")

            prefixo_data, tipo_folha = definir_nome_padronizado(texto_pagina, dados_extraidos)

            if prefixo_data and tipo_folha:
                nome_limpo = os.path.splitext(arquivo)[0].replace(' ', '_')
                novo_nome = f"{prefixo_data}_{tipo_folha}_{nome_limpo}.pdf"
                novo_caminho = os.path.join(PASTA_ALVO, novo_nome)

                # Verifica se o arquivo de destino já existe para não sobrepor
                if os.path.exists(novo_caminho):
                    print(f"   [!] Arquivo de destino já existe. Pulando.")
                    count_ignorado += 1
                    continue

                shutil.move(caminho_full, novo_caminho)
                print(f"   ✅ RENOMEADO PARA: {novo_nome}")
                count_ok += 1
            else:
                print(f"   ⚠️  NÃO FOI POSSÍVEL IDENTIFICAR A DATA.")
                count_ignorado += 1

        except Exception as e:
            print(f"   ❌ Erro crítico: {e}")
            count_ignorado += 1

    print(f"\n-------------------------------------------------------")
    print(f"RESUMO: {count_ok} arquivos renomeados | {count_ignorado} ignorados.")

if __name__ == "__main__":
    executar_renomeacao()