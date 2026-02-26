import os
import ftplib
import time
import ssl
import re
import concurrent.futures
import pdfplumber
import requests
import pandas as pd
from .constants import MAPEAMENTO_CODIGOS
from .utils import limpar_valor_moeda
from .logger import setup_logger

logger = setup_logger(__name__)

# Configurações de Concorrência
MAX_WORKERS_API = 1
MAX_WORKERS_FTP = 25
MAX_WORKERS_PDF = os.cpu_count() or 4


# -----------------------------------------------------------------------------
# 0. CLASSE FTP CUSTOMIZADA (COM DEBUG SSL E CIPHERS)
# -----------------------------------------------------------------------------
class FTPS_Session(ftplib.FTP_TLS):
    def __init__(self, host='', user='', passwd='', acct='', keyfile=None, certfile=None, timeout=60):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        try:
            context.set_ciphers('DEFAULT:@SECLEVEL=1')
        except:
            context.set_ciphers('DEFAULT')

        super().__init__(host=host, context=context, timeout=timeout)

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)
        return conn, size


# -----------------------------------------------------------------------------
# 1. FUNÇÕES DE INGESTÃO FTP (COM SUPORTE A PLAIN FTP)
# -----------------------------------------------------------------------------

def _worker_ftp_baixar_deletar(file_name, host, user, passwd, remote_dir, local_dir, use_tls=True):
    ftp = None
    try:
        # DECISÃO: TLS ou PLAIN?
        if use_tls:
            ftp = FTPS_Session(host)
            ftp.auth()  # Explicit TLS
        else:
            ftp = ftplib.FTP(host)  # Plain FTP (Porta 21 sem SSL)

        # Login
        ftp.login(user.strip(), passwd.strip())

        # Proteção de dados só existe no modo TLS
        if use_tls:
            ftp.prot_p()

        ftp.cwd(remote_dir)
        local_path = os.path.join(local_dir, file_name)

        with open(local_path, 'wb') as f:
            ftp.retrbinary('RETR ' + file_name, f.write)

        ftp.delete(file_name)
        ftp.quit()
        return True, file_name
    except Exception as e:
        if ftp:
            try:
                ftp.close()
            except:
                pass
        return False, f"{file_name}: {str(e)}"


def ingestar_ftp_paralelo(host, user, passwd, remote_dir, local_dir):
    # LÊ A CONFIG DO ENV (Padrão é True/Seguro)
    use_tls_env = os.getenv("FTP_USE_TLS", "True").lower() == "true"
    modo_str = "SEGURO (FTPS)" if use_tls_env else "LEGADO (PLAIN FTP)"

    logger.info(f"--- [FTP] Conectando em {host} | Modo: {modo_str} ---")

    user_clean = str(user).strip()
    pass_clean = str(passwd).strip()

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    arquivos_pdf = []

    # 1. LISTAGEM INICIAL
    try:
        if use_tls_env:
            ftp_main = FTPS_Session(host)
            ftp_main.auth()
        else:
            ftp_main = ftplib.FTP(host)

        ftp_main.login(user_clean, pass_clean)

        if use_tls_env:
            ftp_main.prot_p()

        ftp_main.cwd(remote_dir)
        lista_arquivos = ftp_main.nlst()
        arquivos_pdf = [f for f in lista_arquivos if f.lower().endswith('.pdf')]

        ftp_main.quit()
        logger.info(f"Listagem OK. Encontrados {len(arquivos_pdf)} arquivos.")

    except Exception as e:
        logger.error(f"Erro CRÍTICO no FTP ({modo_str}): {e}")
        return

    total_arquivos = len(arquivos_pdf)
    if total_arquivos == 0:
        return

    # 2. DOWNLOAD PARALELO
    logger.info(f"Iniciando download paralelo...")
    sucessos = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_FTP) as executor:
        # Passamos a flag use_tls para os workers também
        futures = {
            executor.submit(_worker_ftp_baixar_deletar, arquivo, host, user_clean, pass_clean, remote_dir, local_dir,
                            use_tls_env): arquivo
            for arquivo in arquivos_pdf
        }

        for future in concurrent.futures.as_completed(futures):
            status, msg = future.result()
            if status:
                sucessos += 1
            else:
                logger.error(f"Falha worker: {msg}")

    logger.info(f"--- [FTP] Concluído: {sucessos}/{total_arquivos} ---")
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

    # Fallbacks
    if not competencia_match:
        gozo_match = re.search(
            r'(?:Período de Gozo|Gozo).*?\d{2}/\d{2}/\d{4}\s+a\s+\d{2}/(\d{2}/\d{4})',
            texto_pagina,
            re.IGNORECASE | re.DOTALL
        )
        if gozo_match:
            competencia_match = gozo_match
        else:
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
# 2. PROCESSAMENTO DE PDF (Processo Único - Worker)
# -----------------------------------------------------------------------------

def processar_arquivo_pdf(caminho_completo_pdf):
    """
    Processa UM arquivo PDF.
    Esta função será executada em paralelo por múltiplos processos.
    """
    nome_arquivo = os.path.basename(caminho_completo_pdf)
    # logger.info(f" -> Processando PDF: {nome_arquivo}")
    # (Evitar excesso de logs em multiprocessamento para não embaralhar o console)

    lista_consol = []
    lista_detalhe = []

    try:
        with pdfplumber.open(caminho_completo_pdf) as pdf:
            texto_completo_pdf = "".join(
                [(page.extract_text(x_tolerance=1, y_tolerance=1) or "") + "\n" for page in pdf.pages])

            info_base = extrair_info_base(texto_completo_pdf)

            depto_map = {match.start(): match.group(1).strip() for match in
                         re.finditer(r'Departamento:\s*(.+)', texto_completo_pdf)}
            depto_indices = sorted(depto_map.keys())

            blocos_texto = re.split(r'(?=(?:Empr|Contr)\.?\s*:\s*\d+|Matrícula:\s*\d+)', texto_completo_pdf,
                                    flags=re.IGNORECASE)

            for bloco in blocos_texto:
                if len(bloco) < 50: continue
                if "CPF:" not in bloco and "Matrícula:" not in bloco: continue

                posicao_bloco = texto_completo_pdf.find(bloco)
                departamento_atual = next((depto_map[idx] for idx in reversed(depto_indices) if idx < posicao_bloco),
                                          None)

                dados_funcionario = {'departamento': departamento_atual, **info_base}

                # --- Regex e Parsing (Lógica Original) ---
                vinculo_match = re.search(r'(Empr|Contr)\.?', bloco)
                dados_funcionario['vinculo'] = 'Empregado' if vinculo_match and 'Empr' in vinculo_match.group(
                    0) else 'Contribuinte' if vinculo_match else None

                situacao_match = re.search(r'Situação:\s*([^\n\r]+)', bloco)
                if situacao_match:
                    situacao_str = \
                    re.split(r'\s+(?:CPF:|Adm:|PIS/PASEP:|Matrícula:)', situacao_match.group(1), maxsplit=1)[0].strip()
                    dados_funcionario['situacao'] = situacao_str
                else:
                    header_chunk_match = re.search(r'(?:Empr|Contr)\.?\s*:\s*\d+.*?(?=\n|CPF:)', bloco, re.DOTALL)
                    if header_chunk_match:
                        header_chunk = header_chunk_match.group(0)
                        unlabeled_status_match = re.search(r'\s(Trabalhando|Afastado|Férias|Demitido)\s*$',
                                                           header_chunk, re.IGNORECASE)
                        dados_funcionario['situacao'] = unlabeled_status_match.group(
                            1) if unlabeled_status_match else None
                    else:
                        dados_funcionario['situacao'] = None

                demissao_motivo_match = re.search(r'DEMITIDO EM\s+(\d{2}/\d{2}/\d{4})\s*-\s*(.*?)(?=\n|$)', bloco,
                                                  re.IGNORECASE | re.DOTALL)
                if demissao_motivo_match:
                    dados_funcionario['data_demissao'] = demissao_motivo_match.group(1).strip()
                    dados_funcionario['motivo_demissao'] = demissao_motivo_match.group(2).strip()
                else:
                    demissao_match_antigo = re.search(r'(?:Data Demissão|Demissão):\s*(\d{2}/\d{2}/\d{4})', bloco,
                                                      re.IGNORECASE)
                    dados_funcionario['data_demissao'] = demissao_match_antigo.group(
                        1).strip() if demissao_match_antigo else None
                    dados_funcionario['motivo_demissao'] = None

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

                cpf_match = re.search(r'CPF:\s*([\d\.\-]+)', bloco)
                dados_funcionario['cpf'] = cpf_match.group(1).strip() if cpf_match else None

                admissao_match = re.search(r'Adm?:\s*(\d{2}/\d{2}/\d{4})', bloco)
                dados_funcionario['data_admissao'] = admissao_match.group(1).strip() if admissao_match else None

                cargo_match = re.search(r'Cargo:\s*\d+\s+(.*?)(?=\s+Salário:|\s+C\.|С\.)', bloco, re.DOTALL)
                if not cargo_match:
                    cargo_match = re.search(r'Cargo:\s+(.*?)(?=\s+Data de Pagamento:|\n)', bloco, re.DOTALL)
                dados_funcionario['cargo'] = cargo_match.group(1).replace('\n', ' ').strip() if cargo_match else None

                salario_match = re.search(r'Salário:\s*([\d\.,]+)', bloco)
                dados_funcionario['salario_contratual'] = limpar_valor_moeda(
                    salario_match.group(1)) if salario_match else None

                # Rodapé
                dados_funcionario.update({
                    'total_proventos': None, 'total_descontos': None, 'valor_liquido': None,
                    'base_inss': None, 'base_fgts': None, 'valor_fgts': None, 'base_irrf': None
                })

                match_proventos = re.search(r'Proventos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_proventos: match_proventos = re.search(r'Total de Proventos\s+([\d\.,]+)', bloco,
                                                                    re.IGNORECASE | re.DOTALL)
                match_descontos = re.search(r'Descontos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_descontos: match_descontos = re.search(r'Total de Descontos\s+([\d\.,]+)', bloco,
                                                                    re.IGNORECASE | re.DOTALL)
                match_liquido = re.search(r'L[íi]quido:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_liquido: match_liquido = re.search(r'L[íi]quido de F[ée]rias\s+([\d\.,]+)', bloco,
                                                                re.IGNORECASE | re.DOTALL)
                match_inss = re.search(r'Base INSS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_inss: match_inss = re.search(r'Base INSS F[ée]rias\s+([\d\.,]+)', bloco,
                                                          re.IGNORECASE | re.DOTALL)
                match_fgts = re.search(r'Base FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_fgts: match_fgts = re.search(r'Base FGTS F[ée]rias\s+([\d\.,]+)', bloco,
                                                          re.IGNORECASE | re.DOTALL)
                match_vlr_fgts = re.search(r'Valor FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_vlr_fgts: match_vlr_fgts = re.search(r'Valor FGTS F[ée]rias\s+([\d\.,]+)', bloco,
                                                                  re.IGNORECASE | re.DOTALL)
                match_irrf = re.search(r'Base IRRF:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                if not match_irrf: match_irrf = re.search(r'Base IRRF F[ée]rias\s+([\d\.,]+)', bloco,
                                                          re.IGNORECASE | re.DOTALL)

                dados_funcionario['total_proventos'] = limpar_valor_moeda(
                    match_proventos.group(1) if match_proventos else None)
                dados_funcionario['total_descontos'] = limpar_valor_moeda(
                    match_descontos.group(1) if match_descontos else None)
                dados_funcionario['valor_liquido'] = limpar_valor_moeda(
                    match_liquido.group(1) if match_liquido else None)
                dados_funcionario['base_inss'] = limpar_valor_moeda(match_inss.group(1) if match_inss else None)
                dados_funcionario['base_fgts'] = limpar_valor_moeda(match_fgts.group(1) if match_fgts else None)
                dados_funcionario['valor_fgts'] = limpar_valor_moeda(
                    match_vlr_fgts.group(1) if match_vlr_fgts else None)
                dados_funcionario['base_irrf'] = limpar_valor_moeda(match_irrf.group(1) if match_irrf else None)

                lista_consol.append(dados_funcionario.copy())

                # Rubricas
                chaves_rubrica = {
                    'competencia': dados_funcionario.get('competencia'),
                    'tipo_calculo': dados_funcionario.get('tipo_calculo'),
                    'departamento': dados_funcionario.get('departamento'),
                    'vinculo': dados_funcionario.get('vinculo'),
                    'nome_funcionario': dados_funcionario.get('nome_funcionario'),
                    'cpf': dados_funcionario.get('cpf'),
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
                        matches = matches_ferias if len(matches_ferias) > len(matches_holerite) else matches_holerite

                        for match in matches:
                            valor_limpo = limpar_valor_moeda(match.group(3))
                            if not valor_limpo: continue

                            cod_l, nome_l, tipo_l_map = mapear_rubrica_codigo(match.group(1), match.group(2))
                            tipo_detectado = match.group(4)
                            tipo_final = tipo_l_map if tipo_l_map else (
                                'Provento' if tipo_detectado == 'P' else 'Desconto')

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
                    lista_detalhe.extend(rubricas_neste_func)
                else:
                    vazia = chaves_rubrica.copy()
                    vazia.update(
                        {'codigo_rubrica': None, 'nome_rubrica': None, 'tipo_rubrica': None, 'valor_rubrica': 0.0})
                    lista_detalhe.append(vazia)

    except Exception as e:
        logger.error(f"Erro no worker PDF {nome_arquivo}: {e}")

    # Retorna tupla (consol, detalhe) para ser combinada depois
    return (lista_consol, lista_detalhe)


def processar_pdfs(pasta_path):
    """
    Varre a pasta e distribui o processamento dos PDFs para TODOS os núcleos da CPU.
    Usa ProcessPoolExecutor para contornar o GIL do Python.
    """
    if not os.path.exists(pasta_path):
        logger.error(f"Pasta não encontrada: {pasta_path}")
        return pd.DataFrame(), pd.DataFrame()

    arquivos_pdf = [f for f in os.listdir(pasta_path) if f.lower().endswith('.pdf')]
    if not arquivos_pdf:
        logger.warning(f"Nenhum arquivo PDF encontrado em: {pasta_path}")
        return pd.DataFrame(), pd.DataFrame()

    # Cria caminhos completos para os workers
    caminhos_completos = [os.path.join(pasta_path, f) for f in arquivos_pdf]

    logger.info(f"Iniciando processamento PARALELO de {len(arquivos_pdf)} PDFs usando {MAX_WORKERS_PDF} núcleos CPU...")

    lista_geral_consol = []
    lista_geral_detalhe = []

    # Aqui está a mágica do MULTIPROCESSAMENTO
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS_PDF) as executor:
        # Mapeia a função para cada arquivo. O executor gerencia os processos.
        # list() força a execução imediata e aguarda resultados.
        resultados = list(executor.map(processar_arquivo_pdf, caminhos_completos))

        # Agrega os resultados de cada processo
        for res_consol, res_detalhe in resultados:
            if res_consol: lista_geral_consol.extend(res_consol)
            if res_detalhe: lista_geral_detalhe.extend(res_detalhe)

    logger.info("Processamento PDF finalizado. Consolidando DataFrames...")
    return pd.DataFrame(lista_geral_consol), pd.DataFrame(lista_geral_detalhe)


# -----------------------------------------------------------------------------
# 3. EXTRAÇÃO API SOLIDES (Otimizada com Concorrência I/O)
# -----------------------------------------------------------------------------

def _worker_fetch_details(session, base_url, headers, item_basico):
    cid = item_basico.get('id')
    if not cid: return item_basico

    url = f"{base_url}/colaboradores/{cid}"

    # Loop de tolerância a falhas (Resiliência de Rede)
    for tentativa in range(3):
        try:
            r_det = session.get(url, headers=headers, timeout=30)

            if r_det.status_code == 200:
                return r_det.json()
            elif r_det.status_code == 429:
                # Interceptou o block por excesso de requisições. O worker aguarda e retenta.
                time.sleep(15)
                continue
            else:
                # Outros erros (404, etc), interrompe o loop
                break
        except Exception as e:
            time.sleep(15)
            continue

    # Se exaurir as 3 tentativas, aciona o log de advertência
    logger.warning(f"Extracao comprometida: Detalhes do ID {cid} inacessiveis. Inserindo payload rudimentar.")
    return item_basico


def extrair_api_solides(token):
    base_url = "https://app.solides.com/pt-BR/api/v1"
    headers = {"Authorization": f"Token token={token}", "Accept": "application/json"}
    session = requests.Session()
    colabs_lista = []
    page = 1

    logger.info("--- API Solides: Buscando lista de IDs... ---")
    while True:
        try:
            r = session.get(f"{base_url}/colaboradores", headers=headers,
                            params={'page': page, 'page_size': 100, 'status': 'todos'})
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            colabs_lista.extend(data)
            logger.info(f"Página {page} carregada...")
            page += 1
        except Exception as e:
            logger.error(f"Erro na paginação API: {e}", exc_info=True)
            break

    total = len(colabs_lista)
    logger.info(f"--- API Solides: Iniciando extração PARALELA de {total} colaboradores... ---")

    detalhes_finais = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_API) as executor:
        futures_map = {executor.submit(_worker_fetch_details, session, base_url, headers, item): item for item in
                       colabs_lista}
        completed_count = 0
        for future in concurrent.futures.as_completed(futures_map):
            detalhes_finais.append(future.result())
            completed_count += 1
            if completed_count % 100 == 0:
                logger.info(f"Progresso API: {completed_count}/{total} concluídos...")

    return detalhes_finais


def _profile_fetch_details(session, base_url, headers, item_basico):
    """
    Função Worker: Busca o detalhe do Profiler para um ID específico.
    Se der erro ou não tiver perfil, retorna None (para não sujar a lista final).
    """
    cid = item_basico.get('id')
    if not cid:
        return None

    try:
        # Endpoint específico do Profiler
        url = f"{base_url}/profiler/{cid}"
        r = session.get(url, headers=headers, timeout=30)

        if r.status_code == 200:
            dados = r.json()
            # Validação extra: Se o JSON vier vazio ou sem a chave 'perfil'
            if dados and 'perfil' in dados:
                return dados

        # Se for 404 ou não tiver perfil, retornamos None
        return None

    except Exception as e:
        # Em caso de timeout ou erro de rede, apenas logamos (opcional) e seguimos
        return None


def extrair_profiler_solides(token, lista_colaboradores):
    """
    Busca o Profiler APENAS para os colaboradores listados (Ativos + Desligados).
    Realiza uma busca cirúrgica (Lookup) por ID, evitando paginação desnecessária.
    """
    if not lista_colaboradores:
        logger.warning("Lista de colaboradores vazia. Pulando extração de Profiler.")
        return []

    base_url = "https://app.solides.com/pt-BR/api/v1"
    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/json"
    }
    session = requests.Session()

    # 1. Filtra apenas quem tem ID válido na lista recebida
    alvos = [c for c in lista_colaboradores if c.get('id')]
    total = len(alvos)

    logger.info(f"--- API Solides: Buscando Profiler CIRÚRGICO para {total} colaboradores... ---")

    detalhes_finais = []

    # 2. Execução Paralela (Multithreading)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_API) as executor:
        # Mapeia cada colaborador para uma execução da função worker
        futures_map = {
            executor.submit(_profile_fetch_details, session, base_url, headers, item): item
            for item in alvos
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(futures_map):
            resultado = future.result()

            # Só adicionamos na lista se trouxe um perfil válido
            if resultado:
                detalhes_finais.append(resultado)

            completed_count += 1
            if completed_count % 50 == 0:
                logger.info(f"Progresso Profiler: {completed_count}/{total}...")

    logger.info(f"Extração Profiler concluída. {len(detalhes_finais)} perfis encontrados.")
    return detalhes_finais