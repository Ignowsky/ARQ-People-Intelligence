import os
import re
import requests
import pdfplumber
import pandas as pd
from .constants import MAPEAMENTO_CODIGOS
from .utils import limpar_valor_moeda

# -----------------------------------------------------------------------------
# 1. FUNÇÕES AUXILIARES DE EXTRAÇÃO (PDF)
# -----------------------------------------------------------------------------

def mapear_rubrica_codigo(codigo, descricao):
    """
    Busca no mapa global (constants) a descrição e o tipo.
    """
    codigo_limpo = str(codigo).strip()

    if codigo_limpo in MAPEAMENTO_CODIGOS:
        nome_mapeado_completo = MAPEAMENTO_CODIGOS[codigo_limpo] 
        partes = nome_mapeado_completo.split('_', 2)
        tipo_rubrica = 'Provento' if partes[0] == 'P' else 'Desconto'
        nome_limpo = partes[2]
        return codigo_limpo, nome_limpo, tipo_rubrica

    # Fallback
    descricao_limpa = re.sub(r'[\d\s/]+$', '', descricao).strip()
    descricao_limpa = re.sub(r'\s+', '_', descricao_limpa).upper()
    nome_fallback = f"NAO_MAPEADO_{descricao_limpa}"
    return codigo_limpo, nome_fallback, None

def extrair_info_base(texto_pagina):
    """
    Extrai a competência e o tipo de cálculo do documento.
    """
    # Busca Competência ou Referência (ex: 10/2023)
    competencia_match = re.search(
        r'(?:Competência|Competencia|Referência|Referencia|Ref\.?)\s*[:.]?\s*(\d{2}/\d{4})', 
        texto_pagina, 
        re.IGNORECASE
    )
    
    calculo_match = re.search(r'Cálculo\s*:\s*(.+)', texto_pagina, re.IGNORECASE)

    # Fallbacks (Se não achou no cabeçalho padrão)
    if not competencia_match:
         # Tenta Período de Gozo (Férias)
         gozo_match = re.search(
             r'(?:Período de Gozo|Gozo).*?\d{2}/\d{2}/\d{4}\s+a\s+\d{2}/(\d{2}/\d{4})', 
             texto_pagina, 
             re.IGNORECASE | re.DOTALL
         )
         if gozo_match:
             competencia_match = gozo_match
         else:
             # Tenta Data de Pagamento
             pagto_match = re.search(
                 r'(?:Data de Pagamento|Pagamento|Data)[:\s]+\d{2}/(\d{2}/\d{4})', 
                 texto_pagina, 
                 re.IGNORECASE
             )
             if pagto_match:
                 competencia_match = pagto_match

    return {
        'competencia': competencia_match.group(1).strip() if competencia_match else None,
        'tipo_calculo': calculo_match.group(1).strip() if calculo_match else None
    }

# -----------------------------------------------------------------------------
# 2. PROCESSAMENTO DE PDF (Lógica Original Restaurada)
# -----------------------------------------------------------------------------

def processar_pdfs(pasta_path):
    """
    Varre a pasta e retorna DOIS DataFrames: (df_consolidado, df_detalhado).
    """
    if not os.path.exists(pasta_path):
        print(f"Pasta não encontrada: {pasta_path}")
        return pd.DataFrame(), pd.DataFrame()

    arquivos_pdf = [f for f in os.listdir(pasta_path) if f.lower().endswith('.pdf')]
    if not arquivos_pdf:
        print(f"Nenhum arquivo PDF encontrado em: {pasta_path}")
        return pd.DataFrame(), pd.DataFrame()

    lista_geral_rubricas_detalhadas = []
    lista_geral_consolidados = []

    print(f"Processando {len(arquivos_pdf)} PDFs...")

    for nome_arquivo in arquivos_pdf:
        print(f" -> Lendo: {nome_arquivo}")
        try:
            with pdfplumber.open(os.path.join(pasta_path, nome_arquivo)) as pdf:
                texto_completo_pdf = "".join([(page.extract_text(x_tolerance=1, y_tolerance=1) or "") + "\n" for page in pdf.pages])
                
                info_base = extrair_info_base(texto_completo_pdf)
                
                depto_map = {match.start(): match.group(1).strip() for match in re.finditer(r'Departamento:\s*(.+)', texto_completo_pdf)}
                depto_indices = sorted(depto_map.keys())

                blocos_texto = re.split(r'(?=(?:Empr|Contr)\.?\s*:\s*\d+|Matrícula:\s*\d+)', texto_completo_pdf, flags=re.IGNORECASE)

                for bloco in blocos_texto:
                    if len(bloco) < 50: continue
                    if "CPF:" not in bloco and "Matrícula:" not in bloco: continue

                    posicao_bloco = texto_completo_pdf.find(bloco)
                    departamento_atual = next((depto_map[idx] for idx in reversed(depto_indices) if idx < posicao_bloco), None)

                    dados_funcionario = {'departamento': departamento_atual, **info_base}

                    # --- VINCULO ---
                    vinculo_match = re.search(r'(Empr|Contr)\.?', bloco)
                    dados_funcionario['vinculo'] = 'Empregado' if vinculo_match and 'Empr' in vinculo_match.group(0) else 'Contribuinte' if vinculo_match else None

                    # --- SITUAÇÃO ---
                    situacao_match = re.search(r'Situação:\s*([^\n\r]+)', bloco)
                    if situacao_match:
                        situacao_str = re.split(r'\s+(?:CPF:|Adm:|PIS/PASEP:|Matrícula:)', situacao_match.group(1), maxsplit=1)[0].strip()
                        dados_funcionario['situacao'] = situacao_str
                    else:
                        header_chunk_match = re.search(r'(?:Empr|Contr)\.?\s*:\s*\d+.*?(?=\n|CPF:)', bloco, re.DOTALL)
                        if header_chunk_match:
                            header_chunk = header_chunk_match.group(0)
                            unlabeled_status_match = re.search(r'\s(Trabalhando|Afastado|Férias|Demitido)\s*$', header_chunk, re.IGNORECASE)
                            dados_funcionario['situacao'] = unlabeled_status_match.group(1) if unlabeled_status_match else None
                        else:
                            dados_funcionario['situacao'] = None

                    # --- DEMISSÃO ---
                    demissao_motivo_match = re.search(r'DEMITIDO EM\s+(\d{2}/\d{2}/\d{4})\s*-\s*(.*?)(?=\n|$)', bloco, re.IGNORECASE | re.DOTALL)
                    if demissao_motivo_match:
                        dados_funcionario['data_demissao'] = demissao_motivo_match.group(1).strip()
                        dados_funcionario['motivo_demissao'] = demissao_motivo_match.group(2).strip()
                    else:
                        demissao_match_antigo = re.search(r'(?:Data Demissão|Demissão):\s*(\d{2}/\d{2}/\d{4})', bloco, re.IGNORECASE)
                        dados_funcionario['data_demissao'] = demissao_match_antigo.group(1).strip() if demissao_match_antigo else None
                        dados_funcionario['motivo_demissao'] = None

                    # --- NOME ---
                    regex_nome = r'(?:Empr|Contr)\.?\s*:\s*\d+\s+(.*?)' + r'(?=\s*Situação:|\s*CPF:|\s*Adm:|\n)'
                    nome_match = re.search(regex_nome, bloco, re.DOTALL | re.IGNORECASE)

                    if not nome_match:
                        regex_nome_ferias = r'Nome do Funcionário\s+(.*?)' + r'(?=\s*Situação:|\s*PIS/PASEP:|\s*Matrícula:|\n)'
                        nome_match = re.search(regex_nome_ferias, bloco, re.DOTALL | re.IGNORECASE)

                    if nome_match:
                        nome_capturado = nome_match.group(1).replace('\n', ' ').strip()
                        status_encontrado = dados_funcionario.get('situacao', None)
                        nome_limpo = nome_capturado
                        if status_encontrado != None and nome_limpo.lower().endswith(status_encontrado.lower()):
                            tamanho_status = len(status_encontrado)
                            nome_limpo = nome_limpo[:-tamanho_status].strip()
                        nome_limpo = re.sub(r'[^\s]+:\s*$', '', nome_limpo).strip()
                        dados_funcionario['nome_funcionario'] = nome_limpo
                    else:
                        dados_funcionario['nome_funcionario'] = None

                    # --- CPF / ADMISSAO ---
                    cpf_match = re.search(r'CPF:\s*([\d\.\-]+)', bloco)
                    dados_funcionario['cpf'] = cpf_match.group(1).strip() if cpf_match else None
                    
                    admissao_match = re.search(r'Adm?:\s*(\d{2}/\d{2}/\d{4})', bloco)
                    dados_funcionario['data_admissao'] = admissao_match.group(1).strip() if admissao_match else None

                    # --- CARGO ---
                    cargo_match = re.search(r'Cargo:\s*\d+\s+(.*?)(?=\s+Salário:|\s+C\.|С\.)', bloco, re.DOTALL)
                    if not cargo_match:
                         cargo_match = re.search(r'Cargo:\s+(.*?)(?=\s+Data de Pagamento:|\n)', bloco, re.DOTALL)
                    dados_funcionario['cargo'] = cargo_match.group(1).replace('\n', ' ').strip() if cargo_match else None

                    # --- SALARIO ---
                    salario_match = re.search(r'Salário:\s*([\d\.,]+)', bloco)
                    dados_funcionario['salario_contratual'] = limpar_valor_moeda(salario_match.group(1)) if salario_match else None

                    # --- [RODAPÉ (TOTAIS)] ---
                    dados_funcionario.update({
                        'total_proventos': None, 'total_descontos': None, 'valor_liquido': None,
                        'base_inss': None, 'base_fgts': None, 'valor_fgts': None, 'base_irrf': None
                    })

                    match_proventos = re.search(r'Proventos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_proventos: match_proventos = re.search(r'Total de Proventos\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_descontos = re.search(r'Descontos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_descontos: match_descontos = re.search(r'Total de Descontos\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_liquido = re.search(r'L[íi]quido:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_liquido: match_liquido = re.search(r'L[íi]quido de F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_inss = re.search(r'Base INSS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_inss: match_inss = re.search(r'Base INSS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_fgts = re.search(r'Base FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_fgts: match_fgts = re.search(r'Base FGTS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_vlr_fgts = re.search(r'Valor FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_vlr_fgts: match_vlr_fgts = re.search(r'Valor FGTS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    match_irrf = re.search(r'Base IRRF:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_irrf: match_irrf = re.search(r'Base IRRF F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    dados_funcionario['total_proventos'] = limpar_valor_moeda(match_proventos.group(1) if match_proventos else None)
                    dados_funcionario['total_descontos'] = limpar_valor_moeda(match_descontos.group(1) if match_descontos else None)
                    dados_funcionario['valor_liquido'] =   limpar_valor_moeda(match_liquido.group(1) if match_liquido else None)
                    dados_funcionario['base_inss'] =       limpar_valor_moeda(match_inss.group(1) if match_inss else None)
                    dados_funcionario['base_fgts'] =       limpar_valor_moeda(match_fgts.group(1) if match_fgts else None)
                    dados_funcionario['valor_fgts'] =      limpar_valor_moeda(match_vlr_fgts.group(1) if match_vlr_fgts else None)
                    dados_funcionario['base_irrf'] =       limpar_valor_moeda(match_irrf.group(1) if match_irrf else None)

                    lista_geral_consolidados.append(dados_funcionario.copy())

                    # --- RUBRICAS (DETALHE) ---
                    chaves_rubrica = {
                        'competencia': dados_funcionario.get('competencia'),
                        'tipo_calculo': dados_funcionario.get('tipo_calculo'),
                        'departamento': dados_funcionario.get('departamento'),
                        'vinculo': dados_funcionario.get('vinculo'),
                        'nome_funcionario': dados_funcionario.get('nome_funcionario'),
                        'cpf': dados_funcionario.get('cpf'),
                        # --- CORREÇÃO APLICADA AQUI ---
                        'situacao': dados_funcionario.get('situacao')
                    }

                    inicio_tabela = bloco.find("CPF:")
                    if inicio_tabela == -1: inicio_tabela = bloco.find("Matrícula:")

                    fim_tabela_padrao = bloco.find("\nND:")
                    fim_tabela_ferias = bloco.find("Total de Proventos")
                    if fim_tabela_ferias == -1: fim_tabela_ferias = bloco.find("Base INSS Férias")

                    fim_tabela = fim_tabela_padrao if fim_tabela_padrao != -1 else fim_tabela_ferias

                    rubricas_neste_func = []
                    if inicio_tabela != -1 and fim_tabela != -1:
                        tabela_str = bloco[inicio_tabela:fim_tabela].split('\n')[1:]
                        for linha in tabela_str:
                            if not re.search(r'\d', linha): continue

                            padrao_holerite = r'(\d+)\s+(.*?)\s+([\d\.,]+)\s+([PD])(?=\s+\d{2,}|$)'
                            padrao_ferias = r'(\d+)\s+(.*?)\s+[\d\.,/%]+\s+([\d\.,]+)\s+([PD])(?=\s+\d{2,}|$)'

                            matches_ferias = list(re.finditer(padrao_ferias, linha))
                            matches_holerite = list(re.finditer(padrao_holerite, linha))
                            
                            # Lógica original de prioridade
                            matches = matches_ferias if len(matches_ferias) > len(matches_holerite) else matches_holerite

                            for match in matches:
                                valor_limpo = limpar_valor_moeda(match.group(3))
                                if not valor_limpo: continue

                                cod_l, nome_l, tipo_l_map = mapear_rubrica_codigo(match.group(1), match.group(2))
                                tipo_detectado = match.group(4)
                                tipo_final = tipo_l_map if tipo_l_map else ('Provento' if tipo_detectado == 'P' else 'Desconto')

                                if tipo_l_map and tipo_l_map[0] != tipo_detectado:
                                     tipo_final = 'Provento' if tipo_detectado == 'P' else 'Desconto'

                                rubricas_neste_func.append({
                                    **chaves_rubrica,
                                    'codigo_rubrica': cod_l,
                                    'nome_rubrica': nome_l,
                                    'tipo_rubrica': tipo_final,
                                    'valor_rubrica': valor_limpo
                                })
                    
                    if rubricas_neste_func:
                        lista_geral_rubricas_detalhadas.extend(rubricas_neste_func)
                    else:
                        vazia = chaves_rubrica.copy()
                        vazia.update({'codigo_rubrica': None, 'nome_rubrica': None, 'tipo_rubrica': None, 'valor_rubrica': 0.0})
                        lista_geral_rubricas_detalhadas.append(vazia)

        except Exception as e:
            print(f"Erro ao ler PDF {nome_arquivo}: {e}")

    return pd.DataFrame(lista_geral_consolidados), pd.DataFrame(lista_geral_rubricas_detalhadas)


# -----------------------------------------------------------------------------
# 3. EXTRAÇÃO API SOLIDES
# -----------------------------------------------------------------------------

def extrair_api_solides(token):
    base_url = "https://app.solides.com/pt-BR/api/v1"
    headers = {"Authorization": f"Token token={token}", "Accept": "application/json"}
    colabs_lista = []
    page = 1
    
    print("--- API Solides: Buscando lista de IDs... ---")
    while True:
        try:
            r = requests.get(f"{base_url}/colaboradores", headers=headers, params={'page': page, 'page_size': 100, 'status': 'todos'})
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            colabs_lista.extend(data)
            print(f"Página {page} carregada...")
            page += 1
        except Exception: break

    print(f"--- API Solides: Buscando detalhes de {len(colabs_lista)} colaboradores... ---")
    detalhes_finais = []
    for i, item in enumerate(colabs_lista):
        cid = item.get('id')
        if not cid: continue
        try:
            r_det = requests.get(f"{base_url}/colaboradores/{cid}", headers=headers)
            if r_det.status_code == 200: detalhes_finais.append(r_det.json())
            else: detalhes_finais.append(item)
        except: detalhes_finais.append(item)
            
    return detalhes_finais