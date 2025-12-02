import pdfplumber
import pandas as pd
import os
import re
import time
from decimal import Decimal, InvalidOperation # Importa o Decimal

# --- MAPEAMENTO DE RUBRICAS (GLOBAL) ---
# (Seu mapa de rubricas original - sem alterações)
MAPEAMENTO_ORIGINAL = {
    '12': 'P_12_13_Salario_Integral', '13': 'P_13_13_Salario_Adiantamento', '19': 'P_19_Retroativo_Salarial',
    '22': 'P_22_Aviso_Previo', '28': 'P_28_Ferias_Vencidas', '29': 'P_29_Ferias_Proporcionais',
    '49': 'P_49_Aviso_Previo_Nao_Trabalhado', '50': 'P_50_Adiantamento_13_Salario', '64': 'P_64_1_3_Ferias_Rescisao',
    '150': 'P_150_Horas_Extras_50', '200': 'P_200_Horas_Extras_100', '242': 'P_242_Honorarios',
    '246': 'P_246_Diferenca_Salarial', '250': 'P_250_Reflexo_Extra_DSR', '258': 'P_258_Anuenio_Sindpd_PA',
    '263': 'P_263_Pag_Banco_Horas', '276': 'P_276_Trienio_Sindpd', '283': 'P_283_VT_Mes_Seguinte',
    '295': 'P_295_Hora_Extra_50', '314': 'P_314_Dev_Desc_Indevido', '316': 'D_316_Devolucao_Desc_Plano_Odonto', # Corrected code
    '317': 'D_317_Dev_Desc_Plano_Odonto', # Corrected code
    '340': 'P_340_Adicional_Noturno', '399': 'P_399_Banco_Horas_Pago',
    '461': 'P_461_Gratificacao_Funcao', '572': 'D_572_Dev_Desc_Plano_Odonto', # Corrected code
    '574': 'P_574_Gratificacao',
    '623': 'P_623_Gratificacao_Funcao', '695': 'P_695_Bolsa_Auxilio_Bonificacao', '700': 'P_700_Dev_Desc_INSS_Maior',
    '725': 'P_725_Dif_Plano_Medico_Dep', '763': 'P_763_Reembolso_Conselho', '766': 'P_766_Dif_Trienio',
    '800': 'P_800_Media_Horas_13', '801': 'P_801_Media_Valor_13', '802': 'P_802_Media_Fixa_13',
    '803': 'P_803_13_1_12_Indenizado', '805': 'P_805_Media_Valor_Ferias', '806': 'P_806_Media_Horas_Ferias',
    '807': 'P_807_Media_Fixa_Ferias', '808': 'P_808_Media_Valor_Abono', '809': 'P_809_Media_Horas_Abono',
    '810': 'P_810_Media_Fixa_Abono', '811': 'P_811_Ferias_1_12_Indenizado', '817': 'P_817_Media_Fer_Proporcionais',
    '820': 'P_820_Media_Ferias_Vencidas', '833': 'P_833_Media_Horas_13_Adiantado', '834': 'P_834_Media_Valor_13_Adiantado',
    '835': 'P_835_Adiocional_Fixo_13_Adiantado', '836': 'P_836_Ajuste_Inss', '846': 'P_846_Dif_Abono_Ferias',
    '854': 'P_854_Reflexo_Adicional_Noturno_DSR', '919': 'P_919_Trienio_Sinpd', '931': 'P_931_1_3_Ferias',
    '932': 'P_932_1_3_Abono_Ferias', '940': 'P_940_Diferenca_Ferias', '995': 'P_995_Salario_Familia',
    '1015': 'P_1015_Anuenio_Sindpd_PA', '8104': 'P_8104_13_Salario_Maternidade', '8112': 'P_8112_Dif_13_Ferias',
    '8126': 'P_8126_1_3_Ferias_Indenizada_Resc', '8130': 'P_8130_Estouro_Rescisao', '8158': 'P_8158_Media_Ferias_1_12_Indenizado',
    '8169': 'P_8169_1_3_Ferias_Proporcionais_Resc', '8181': 'P_8181_Dif_Media_Hora_13', '8182': 'P_8182_Dif_Media_Valor_13',
    '8184': 'P_8184_Dif_Adicional_13', '8189': 'P_8189_Dif_Media_Horas_Ferias', '8190': 'P_8190_Dif_Media_Valor_Ferias',
    '8192': 'P_8192_Dif_Media_Valor_Ferias', '8197': 'P_8197_Dif_Media_Horas_Abono_Ferias', '8200': 'P_8200_Dif_Adicional_Abono_Ferias',
    '8392': 'P_8392_13_Salario_Adiantado_Ferias', '8393': 'P_8393_Media_Horas_13_Adiantado_Ferias', '8394': 'P_8394_Media_Valor_13_Adiantado_Ferias', '8156': 'P_8156_Media_Ferias_1_12_Indenizada_Resc', '8157': 'P_8157_Media_Horas_Ferias_1_12_Indenizada_Resc', '815': 'P_815_Media_Horas_Fer_Proporcional', '816': 'P_816_Media_Valor_Fer_Proporcional',
    '8219': 'P_8219_Vantagem_13_Licenca_Maternidade', '8551': 'P_8551_Media_Horas_13_Rescisao', '8552': 'P_8552_Media_Valor_13_Rescisao',
    '9596': 'P_9596_Media_Valor_Aviso_Previo', '9597': 'P_9597_Media_Horas_Aviso_Previo', '9600': 'P_9600_Media_Valor_1_12_Indenizado',
    '9601': 'P_9601_Media_Horas_13_1_12_Indenizado','8396': 'P_8396_Vantagem_13_Adiantado', '8417': 'P_8417_Dif_1_3_Abono_Ferias', '8490': 'P_8490_Bolsa_Auxilio_Ferias_Proporcionais','8550': 'P_8550_13_Salario_Integral_Rescisao', '8553': 'P_8553_Media_13_Rescisao', '8781': 'P_8781_Salario_Empregado','8783': 'P_8783_Dias_Ferias', '8784': 'P_8784_Salario_Maternidade_Dias', '8791': 'P_8791_Dias_Afast_Dir_Integrais',
    '8797': 'P_8797_Dias_Bolsa_Estagio', '8800': 'P_8800_Dias_Abono(Ferias)', '8832': 'P_8832_Dias_Licença_Maternidade',
    '8870': 'P_8870_Dias_Afast_Doenca_Dir_Integrais', '9180': 'P_9180_Saldo_Salario_Dias', '9380': 'P_9380_Pro_Labore_Dias',
    '9591': 'P_9591_Aviso_Previo', '9592': 'P_9592_13_1_12_Indenizado', '9598': 'P_9598_Vantagem_Aviso_Indenizado',
    '9602': 'P_9602_Vantagem_13_1_12_Indenizado', '638': 'P_Dif._VT_Meses_Anteriores',
    '48': 'D_48_Vale_Transporte', '51': 'D_51_Liquido_Rescisao', '241': 'D_241_Desc_Vale_Transporte',
    '286': 'D_286_Desc_Plano_Medico_Dep', '291': 'D_291_Desc_Banco_Horas', '296': 'D_296_VT_Nao_Utilizado',
    '297': 'D_297_VA_Nao_Utilizado', '311': 'D_311_Desc_2_Via_Cartao', '325': 'D_325_Desc_Plano_Odonto',
    '331': 'D_331_Desc_Banco_Horas', '362': 'D_362_Desconto_VA_VR', '375': 'D_375_Desconto_Plano_Saude_Dep_F',
    '379': 'D_379_Desconto_Plano_Odonto_F', '394': 'D_394_Desconto_Diversos', '447': 'D_447_Desc_Plano_Odonto_Alfa_Dep',
    '449': 'D_449_Desc_Plano_Odonto_Beta', '451': 'D_451_Desc_Plano_Odonto_Alfa_Dep_F', '453': 'D_453_Desc_Plano_Odonto_Beta_F',
    '637': 'D_637_Taxa_Campanha_Sindical', '639': 'D_639_Desconto_Valor_Pago', '777': 'D_777_VT_VA_Nao_Utilizado',
    '804': 'D_804_IRRF_13', '812': 'D_812_INSS_Ferias', '821': 'D_821_Dif_Inss_Ferias',
    '825': 'D_825_Inss_13_Salario', '826': 'D_826_Inss_Sobre_Rescisao', '827': 'D_827_IRRF_13_Salario_Rescisao',
    '828': 'D_828_Irrf_Rescisao', '842': 'D_842_Multa_Estabilidade_Art_482', '843': 'D_843_Inss_Empregador',
    '856': 'D_856_Irrf_Empregador', '858': 'D_858_INSS_Autonomo', '869': 'D_869_ISS',
    '937': 'D_937_Adiantamento_Ferias', '942': 'D_942_Irrf_Ferias', '963': 'D_963_Desc_Odonto_Mais_Orto',
    '964': 'D_964_Desc_Odonto_Mais_Clarear', '965': 'D_963_Desc_Odonto_Mais_Doc', '989': 'D_989_Inss_13_Sal_Rescisao',
    '998': 'D_998_INSS', '999': 'D_999_IRRF', '1069': 'D_1069_Desc_Emprestimo_Consignado',
    '8069': 'D_8069_Faltas_Horas_Parciais', '8111': 'D_8111_Desc_Plano_Saude_Dep', '8128': 'D_8128_IRRF_Dif_Ferias',
    '8918': 'D_8918_Adiantamento_13_Media_Valor', '8919': 'D_8919_Adiantamento_13_Media_Horas', '8921': 'D_8921_Adiantamento_13_Media_Fixa',
    '9750': 'D_9750_Desc_Emprestimo_Consignado', '8214': 'D_8214_INSS_Dif_13_Salario', '8215': 'D_8215_IRRF_Dif_13_Salario',
    '8517': 'D_8517_Liquido_Rescisao_Estagiario', '8566': 'D_8566_Adiantamento_13_Salario_Rescisao', '1043': 'D_Desconto_Vale_Transporte',
    '474': 'P_474_Trienio_SINDPD', '831': 'P_831_Multa_Estabilidade_Art._479/CLT', '386': 'D_386_Faltas_Atraso_Valor', 
    '364': 'D_364_Horas_Faltas_Parcial', '402': 'P_402_Pag_Saldo_Banco_Horas', '8154': 'P_8154_Media_13_1/12_Indenizado', '8146': 'P_8146_Media_Fixa_Aviso/Previo',
    '990': 'P_990_Insuf_Saldo_Credor', '8794': 'D_8794_Faltas_Dias_DSR', '8792': 'D_8792_Faltas_Dias', '818': 'P_818_Media_HR_Ferias_Vencidas', '819': 'P_819_Media_VL_Ferias_Vencidas',
    '8144': 'P_8144_Media_Valor_Aviso_Previo', '8145': 'P_8145_Media_Horas_Aviso_Previo', '991': 'D_991_Insuficiencia_Saldo', '686': 'P_686_Bonus', '8932': 'P_8932_Dias_Ausencias_Justificada',
    '643': 'P_643_VA_Retroativo_CCT', '730': 'P_730_Abono_CCT', '8869': 'P_8869_Dias_Afast_P/Acid_Trabalho_C/D', '294': 'P_294_Auxilio_Educacao', '293': 'P_293_Dev_Desconto_VT'
    
}
proventos_map = {k: v for k, v in MAPEAMENTO_ORIGINAL.items() if v.startswith('P_')}
descontos_map = {k: v for k, v in MAPEAMENTO_ORIGINAL.items() if v.startswith('D_')}
sorted_proventos = dict(sorted(proventos_map.items(), key=lambda item: int(item[0])))
sorted_descontos = dict(sorted(descontos_map.items(), key=lambda item: int(item[0])))
MAPEAMENTO_CODIGOS = {**sorted_proventos, **sorted_descontos}
# --- FIM DO MAPEAMENTO GLOBAL ---

# --- [FUNÇÃO DE LIMPEZA (CORRETA - SEM ALTERAÇÃO)] ---

def limpar_valor(valor_str):
    """
    Converte uma string monetária (formato brasileiro 1.234,56) para float.
    Retorna None se o valor for nulo ou inválido.
    """
    if valor_str is None:
        return None
    if isinstance(valor_str, (int, float)):
        return float(valor_str)
    if isinstance(valor_str, str):
        try:
            # Lógica padrão para formato brasileiro:
            # 1. Remove espaços em branco
            valor_limpo = valor_str.strip()
            # 2. Remove pontos de milhar (ex: '1.234,56' -> '1234,56')
            valor_limpo = valor_limpo.replace('.', '')
            # 3. Troca vírgula decimal por ponto (ex: '1234,56' -> '1234.56')
            valor_limpo = valor_limpo.replace(',', '.')

            # Trata caso de string vazia ou que só tinha '.'
            if not valor_limpo:
                return None

            return float(valor_limpo)
        except (ValueError, TypeError):
            # Retorna None se a string não for um número válido
            return None
    # Retorna None para outros tipos (ex: objetos inesperados)
    return None

def mapear_rubrica(codigo, descricao):
    """
    Busca no mapa global a descrição e o tipo (Provento/Desconto) da rubrica.
    Retorna (codigo_limpo, nome_limpo, tipo_rubrica).
    """
    codigo_limpo = str(codigo).strip()

    if codigo_limpo in MAPEAMENTO_CODIGOS:
        nome_mapeado_completo = MAPEAMENTO_CODIGOS[codigo_limpo] # ex: 'P_12_13_Salario_Integral'
        partes = nome_mapeado_completo.split('_', 2)
        tipo_rubrica = 'Provento' if partes[0] == 'P' else 'Desconto'
        nome_limpo = partes[2]
        return codigo_limpo, nome_limpo, tipo_rubrica

    # Fallback se não estiver no mapa
    descricao_limpa = re.sub(r'[\d\s/]+$', '', descricao).strip()
    descricao_limpa = re.sub(r'\s+', '_', descricao_limpa).upper()
    nome_fallback = f"NAO_MAPEADO_{descricao_limpa}"
    return codigo_limpo, nome_fallback, None


def extrair_info_base(texto_pagina):
    """Extrai a competência e o tipo de cálculo do documento."""
    competencia_match = re.search(r'Competência:\s*(\d{2}/\d{4})', texto_pagina)
    calculo_match = re.search(r'Cálculo\s*:\s*(.+)', texto_pagina)

    # Adiciona fallback para "Recibo de Férias" que não tem competência
    if not competencia_match:
         # Tenta extrair do período de gozo
         gozo_match = re.search(r'Período de Gozo:\s*\d{2}/\d{2}/\d{4}\s+a\s+\d{2}/(\d{2}/\d{4})', texto_pagina)
         if gozo_match:
             competencia_match = gozo_match
         else:
             # Tenta extrair da data de pagamento
             pagto_match = re.search(r'Data de Pagamento:\s*\d{2}/(\d{2}/\d{4})', texto_pagina)
             if pagto_match:
                 competencia_match = pagto_match


    return {
        'competencia': competencia_match.group(1).strip() if competencia_match else None,
        'tipo_calculo': calculo_match.group(1).strip() if calculo_match else None
    }

# --- [FUNÇÃO DE TRATAMENTO DE TIPOS (SEM ALTERAÇÃO)] ---
# Esta função já converte de float para Decimal (correto para Postgres).

def converter_para_decimal(valor):
    """Função auxiliar para converter valores (float ou str) para Decimal."""
    if valor is None or pd.isna(valor):
        return None
    try:
        return Decimal(str(valor))
    except (InvalidOperation, ValueError, TypeError):
        return None

def tratar_tipos_para_postgres(df):
    """
    Converte as colunas de um DataFrame para os tipos corretos (datetime, Decimal, string)
    ANTES de enviar ao Postgres.
    """
    if df is None or df.empty:
        return df

    print(f"\nIniciando conversão de tipos para {df.shape[1]} colunas...")

    # --- COLUNAS DE DATA (Formato DD/MM/YYYY) ---
    colunas_data = ['data_admissao', 'data_demissao']
    for col in colunas_data:
        if col in df.columns:
            print(f"Convertendo coluna de data: {col}")
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce', dayfirst=True)

    # --- COLUNA DE COMPETÊNCIA (Formato especial MM/YYYY) ---
    if 'competencia' in df.columns:
        print("Convertendo coluna de data: competencia (MM/YYYY)")
        df['competencia'] = pd.to_datetime('01/' + df['competencia'].astype(str), format='%d/%m/%Y', errors='coerce')


    # --- COLUNAS MONETÁRIAS (Para NUMERIC/Decimal) ---
    colunas_monetarias = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf', 'valor_rubrica'
    ]
    for col in colunas_monetarias:
        if col in df.columns:
            print(f"Convertendo coluna monetária para Decimal: {col}")
            df[col] = df[col].apply(converter_para_decimal)

    # --- COLUNAS DE TEXTO (String/Object) ---
    colunas_texto_candidatas = [col for col in df.columns if col not in colunas_data + colunas_monetarias + ['cpf', 'codigo_rubrica']]
    for col in colunas_texto_candidatas:
        if col in df.columns:
            # print(f"Limpando coluna de texto candidata: {col}") # Reduzindo o print
            try:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('None', None)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('', None)
            except Exception as e:
                print(f"Warning: Could not process column '{col}' as string. Error: {e}")
                pass

    # Tratamento especial para CPF (remover formatação)
    if 'cpf' in df.columns:
         print("Limpando e padronizando coluna: cpf")
         df['cpf'] = df['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True).replace('', None)
         df['cpf'] = df['cpf'].replace('None', None)

    # Garante que codigo_rubrica seja texto
    if 'codigo_rubrica' in df.columns:
         print("Padronizando coluna: codigo_rubrica")
         df['codigo_rubrica'] = df['codigo_rubrica'].astype(str).replace('None', None)

    print("Conversão de tipos finalizada.")
    return df

# --- [FUNÇÃO PRINCIPAL (COM A CORREÇÃO CRÍTICA)] ---

def processar_pdfs_na_pasta(pasta_path):
    """
    Função principal que varre uma pasta, processa todos os PDFs e
    retorna DOIS DataFrames: um consolidado (totais) e um detalhado (rubricas).
    """
    arquivos_pdf = [f for f in os.listdir(pasta_path) if f.lower().endswith('.pdf')]
    if not arquivos_pdf:
        print(f"Nenhum arquivo PDF encontrado na pasta: {pasta_path}")
        return None, None

    lista_geral_rubricas_detalhadas = []
    lista_geral_consolidados = []

    print(f"Encontrados {len(arquivos_pdf)} PDFs para processar...")

    for nome_arquivo in arquivos_pdf:
        print(f"\n---> Processando arquivo: {nome_arquivo}")
        try:
            with pdfplumber.open(os.path.join(pasta_path, nome_arquivo)) as pdf:
                texto_completo_pdf = "".join([(page.extract_text(x_tolerance=1, y_tolerance=1) or "") + "\n" for page in pdf.pages])
                info_base = extrair_info_base(texto_completo_pdf)
                depto_map = {match.start(): match.group(1).strip() for match in re.finditer(r'Departamento:\s*(.+)', texto_completo_pdf)}
                depto_indices = sorted(depto_map.keys())

                blocos_texto = re.split(r'(?=(?:Empr|Contr)\.?\s*:\s*\d+|Matrícula:\s*\d+)', texto_completo_pdf, flags=re.IGNORECASE)

                funcionarios_processados_no_arquivo = 0
                for bloco in blocos_texto:

                    if len(bloco) < 50: continue
                    if "CPF:" not in bloco and "Matrícula:" not in bloco: continue

                    posicao_bloco = texto_completo_pdf.find(bloco)
                    departamento_atual = next((depto_map[idx] for idx in reversed(depto_indices) if idx < posicao_bloco), None)

                    dados_funcionario = {'departamento': departamento_atual, **info_base}

                    vinculo_match = re.search(r'(Empr|Contr)\.?', bloco)
                    dados_funcionario['vinculo'] = 'Empregado' if vinculo_match and 'Empr' in vinculo_match.group(0) else 'Contribuinte' if vinculo_match else None

                    # --- [LÓGICA SITUAÇÃO (Sem alteração)] ---
                    situacao_match = re.search(r'Situação:\s*([^\n\r]+)', bloco)
                    if situacao_match:
                        situacao_str = re.split(r'\s+(?:CPF:|Adm:|PIS/PASEP:|Matrícula:)', situacao_match.group(1), maxsplit=1)[0].strip()
                        dados_funcionario['situacao'] = situacao_str
                    else:
                        header_chunk_match = re.search(r'(?:Empr|Contr)\.?\s*:\s*\d+.*?(?=\n|CPF:)', bloco, re.DOTALL)
                        if header_chunk_match:
                            header_chunk = header_chunk_match.group(0)
                            unlabeled_status_match = re.search(r'\s(Trabalhando|Afastado|Férias|Demitido)\s*$', header_chunk, re.IGNORECASE)
                            if unlabeled_status_match:
                                dados_funcionario['situacao'] = unlabeled_status_match.group(1)
                            else:
                                dados_funcionario['situacao'] = None
                        else:
                            dados_funcionario['situacao'] = None
                    # --- [FIM SITUAÇÃO] ---

                    demissao_motivo_match = re.search(r'DEMITIDO EM\s+(\d{2}/\d{2}/\d{4})\s*-\s*(.*?)(?=\n|$)', bloco, re.IGNORECASE | re.DOTALL)
                    if demissao_motivo_match:
                        dados_funcionario['data_demissao'] = demissao_motivo_match.group(1).strip()
                        dados_funcionario['motivo_demissao'] = demissao_motivo_match.group(2).strip()
                    else:
                        demissao_match_antigo = re.search(r'(?:Data Demissão|Demissão):\s*(\d{2}/\d{2}/\d{4})', bloco, re.IGNORECASE)
                        dados_funcionario['data_demissao'] = demissao_match_antigo.group(1).strip() if demissao_match_antigo else None
                        dados_funcionario['motivo_demissao'] = None

                    # --- [LÓGICA NOME (Sem alteração)] ---
                    delimitadores_nome = r'(?=\s*Situação:|\s*CPF:|\s*Adm:|\n)'
                    regex_nome = r'(?:Empr|Contr)\.?\s*:\s*\d+\s+(.*?)' + delimitadores_nome
                    nome_match = re.search(regex_nome, bloco, re.DOTALL | re.IGNORECASE)

                    if not nome_match:
                        delimitadores_nome_ferias = r'(?=\s*Situação:|\s*PIS/PASEP:|\s*Matrícula:|\n)'
                        regex_nome_ferias = r'Nome do Funcionário\s+(.*?)' + delimitadores_nome_ferias
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
                    # --- [FIM NOME] ---


                    cpf_match = re.search(r'CPF:\s*([\d\.\-]+)', bloco)
                    dados_funcionario['cpf'] = cpf_match.group(1).strip() if cpf_match else None
                    admissao_match = re.search(r'Adm?:\s*(\d{2}/\d{2}/\d{4})', bloco)
                    dados_funcionario['data_admissao'] = admissao_match.group(1).strip() if admissao_match else None

                    # --- [LÓGICA CARGO (Sem alteração)] ---
                    cargo_match = re.search(r'Cargo:\s*\d+\s+(.*?)(?=\s+Salário:|\s+C\.|С\.)', bloco, re.DOTALL)
                    if not cargo_match:
                         cargo_match = re.search(r'Cargo:\s+(.*?)(?=\s+Data de Pagamento:|\n)', bloco, re.DOTALL)
                    dados_funcionario['cargo'] = cargo_match.group(1).replace('\n', ' ').strip() if cargo_match else None
                    # --- [FIM CARGO] ---

                    salario_match = re.search(r'Salário:\s*([\d\.,]+)', bloco)
                    dados_funcionario['salario_contratual'] = limpar_valor(salario_match.group(1)) if salario_match else None


                    # --- [INÍCIO DA CORREÇÃO CRÍTICA DO RODAPÉ] ---
                    #
                    # A lógica anterior de if/elif/else (padrão VS férias) era "tudo ou nada".
                    # Se uma regex falhasse, todos os campos ficavam nulos.
                    #
                    # A nova lógica abaixo é "campo a campo". Procuramos cada valor individualmente.
                    # Isso é MUITO mais robusto contra pequenas variações de layout.
                    #
                    
                    # 1. Inicializa todos os dados do rodapé como None
                    dados_funcionario.update({
                        'total_proventos': None, 'total_descontos': None, 'valor_liquido': None,
                        'base_inss': None, 'base_fgts': None, 'valor_fgts': None, 'base_irrf': None
                    })

                    # 2. Define os padrões de regex para CADA campo (Holerite e Férias)
                    # Usamos re.IGNORECASE para 'Líquido' vs 'líquido' etc.
                    # Procuramos no 'bloco' inteiro.
                    
                    # --- Proventos ---
                    match_proventos = re.search(r'Proventos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_proventos: # Se não achou padrão holerite, tenta padrão férias
                        match_proventos = re.search(r'Total de Proventos\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Descontos ---
                    match_descontos = re.search(r'Descontos:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_descontos: # Fallback para férias
                        match_descontos = re.search(r'Total de Descontos\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Líquido ---
                    match_liquido = re.search(r'L[íi]quido:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_liquido: # Fallback para férias
                        match_liquido = re.search(r'L[íi]quido de F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Base INSS ---
                    match_inss = re.search(r'Base INSS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_inss: # Fallback para férias
                        match_inss = re.search(r'Base INSS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Base FGTS ---
                    match_fgts = re.search(r'Base FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_fgts: # Fallback para férias
                        match_fgts = re.search(r'Base FGTS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Valor FGTS ---
                    match_vlr_fgts = re.search(r'Valor FGTS:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_vlr_fgts: # Fallback para férias (geralmente não tem, mas buscamos)
                        match_vlr_fgts = re.search(r'Valor FGTS F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)

                    # --- Base IRRF ---
                    match_irrf = re.search(r'Base IRRF:\s*([\d\.,]+)', bloco, re.IGNORECASE)
                    if not match_irrf: # Fallback para férias
                        match_irrf = re.search(r'Base IRRF F[ée]rias\s+([\d\.,]+)', bloco, re.IGNORECASE | re.DOTALL)


                    # 3. Atualiza o dicionário com os valores que foram encontrados
                    #    Se um 'match' for None, a função limpar_valor(None) retorna None,
                    #    mantendo o valor nulo como deveria.
                    dados_funcionario['total_proventos'] = limpar_valor(match_proventos.group(1) if match_proventos else None)
                    dados_funcionario['total_descontos'] = limpar_valor(match_descontos.group(1) if match_descontos else None)
                    dados_funcionario['valor_liquido'] =   limpar_valor(match_liquido.group(1) if match_liquido else None)
                    dados_funcionario['base_inss'] =       limpar_valor(match_inss.group(1) if match_inss else None)
                    dados_funcionario['base_fgts'] =       limpar_valor(match_fgts.group(1) if match_fgts else None)
                    dados_funcionario['valor_fgts'] =      limpar_valor(match_vlr_fgts.group(1) if match_vlr_fgts else None)
                    dados_funcionario['base_irrf'] =       limpar_valor(match_irrf.group(1) if match_irrf else None)

                    # --- [FIM DA CORREÇÃO CRÍTICA DO RODAPÉ] ---


                    # 1. Salva a linha CONSOLIDADA (agora com os valores corretos)
                    lista_geral_consolidados.append(dados_funcionario.copy())

                    # 2. Define as CHAVES para a tabela detalhada (Sem alteração)
                    chaves_funcionario_para_rubricas = {
                        'competencia': dados_funcionario.get('competencia'),
                        'tipo_calculo': dados_funcionario.get('tipo_calculo'),
                        'departamento': dados_funcionario.get('departamento'),
                        'vinculo': dados_funcionario.get('vinculo'),
                        'nome_funcionario': dados_funcionario.get('nome_funcionario'),
                        'cpf': dados_funcionario.get('cpf')
                    }

                    # 3. Processa as RUBRICAS (Sem alteração)
                    rubricas_extraidas = []

                    # --- [LÓGICA TABELA RUBRICAS (Sem alteração)] ---
                    inicio_tabela = bloco.find("CPF:")
                    if inicio_tabela == -1:
                         inicio_tabela = bloco.find("Matrícula:")

                    fim_tabela_padrao = bloco.find("\nND:")
                    fim_tabela_ferias = bloco.find("Total de Proventos") # Fim em Recibo de Férias
                    if fim_tabela_ferias == -1: # Fallback se "Total de Proventos" falhar
                        fim_tabela_ferias = bloco.find("Base INSS Férias")

                    fim_tabela = -1
                    if fim_tabela_padrao != -1:
                        fim_tabela = fim_tabela_padrao
                    elif fim_tabela_ferias != -1:
                        fim_tabela = fim_tabela_ferias
                    # --- [FIM TABELA RUBRICAS] ---


                    if inicio_tabela != -1 and fim_tabela != -1:
                        tabela_str = bloco[inicio_tabela:fim_tabela].split('\n')[1:]
                        for linha in tabela_str:
                            if not re.search(r'\d', linha): continue

                            # --- [LÓGICA REGEX RUBRICAS (Sem alteração)] ---
                            padrao_holerite = r'(\d+)\s+(.*?)\s+([\d\.,]+)\s+([PD])(?=\s+\d{2,}|$)'
                            padrao_ferias = r'(\d+)\s+(.*?)\s+[\d\.,/%]+\s+([\d\.,]+)\s+([PD])(?=\s+\d{2,}|$)'

                            matches_ferias = list(re.finditer(padrao_ferias, linha))
                            matches_holerite = list(re.finditer(padrao_holerite, linha))

                            matches = matches_ferias if len(matches_ferias) > len(matches_holerite) else matches_holerite
                            # --- [FIM REGEX RUBRICAS] ---

                            for match in matches:
                                valor_limpo = limpar_valor(match.group(3)) # Usa a função 'limpar_valor'
                                if valor_limpo == 0 or valor_limpo is None: continue
                                codigo_bruto = match.group(1)
                                descricao_bruta = match.group(2).strip()
                                descricao_bruta = re.sub(r'\s[\d\.,%]+$', '', descricao_bruta).strip()
                                (codigo_limpo, nome_mapeado, tipo_rubrica) = mapear_rubrica(codigo_bruto, descricao_bruta)

                                tipo_detectado = match.group(4)
                                tipo_mapeado = tipo_rubrica[0] if tipo_rubrica else None

                                if tipo_mapeado and tipo_detectado != tipo_mapeado:
                                    tipo_rubrica = 'Provento' if tipo_detectado == 'P' else 'Desconto'

                                rubricas_extraidas.append({
                                    'codigo_rubrica': codigo_limpo,
                                    'nome_rubrica': nome_mapeado,
                                    'tipo_rubrica': tipo_rubrica,
                                    'valor_rubrica': valor_limpo
                                })

                    # 4. Adiciona as rubricas à lista DETALHADA (Sem alteração)
                    if rubricas_extraidas:
                        for rubrica in rubricas_extraidas:
                            nova_linha_longa = chaves_funcionario_para_rubricas.copy()
                            nova_linha_longa.update(rubrica)
                            lista_geral_rubricas_detalhadas.append(nova_linha_longa)
                    else:
                        linha_sem_rubrica = chaves_funcionario_para_rubricas.copy()
                        linha_sem_rubrica.update({
                            'codigo_rubrica': None, 'nome_rubrica': None,
                            'tipo_rubrica': None, 'valor_rubrica': 0.0
                        })
                        lista_geral_rubricas_detalhadas.append(linha_sem_rubrica)

                    funcionarios_processados_no_arquivo += 1

            print(f"       - Sucesso! Foram processados {funcionarios_processados_no_arquivo} funcionários neste arquivo.")

        except Exception as e:
            print(f"       ERRO CRÍTICO ao processar o arquivo {nome_arquivo}: {e}")
            import traceback
            traceback.print_exc()

    if not lista_geral_consolidados and not lista_geral_rubricas_detalhadas:
        print("\nProcesso concluído, mas nenhum dado pôde ser extraído.")
        return None, None

    # --- [CRIAÇÃO E TRATAMENTO DOS DATAFRAMES FINAIS (Sem alteração)] ---

    # 1. DataFrame CONSOLIDADO (Totais)
    df_consolidado = pd.DataFrame(lista_geral_consolidados)
    colunas_info_pessoal = [
        'competencia', 'tipo_calculo', 'departamento', 'vinculo', 'nome_funcionario',
        'situacao', 'data_demissao', 'motivo_demissao', 'cargo', 'data_admissao', 'cpf',
        'salario_contratual', 'total_proventos', 'total_descontos', 'valor_liquido', 'base_inss', 'base_fgts',
        'valor_fgts', 'base_irrf'
    ]
    colunas_presentes_consol = [col for col in colunas_info_pessoal if col in df_consolidado.columns]
    if not df_consolidado.empty:
        df_consolidado = df_consolidado[colunas_presentes_consol]

    # 2. DataFrame DETALHADO (Rubricas)
    df_detalhado = pd.DataFrame(lista_geral_rubricas_detalhadas)
    colunas_chaves = [
         'competencia', 'tipo_calculo', 'departamento', 'vinculo', 'nome_funcionario', 'cpf'
    ]
    colunas_rubrica_longa = [
        'codigo_rubrica', 'nome_rubrica', 'tipo_rubrica', 'valor_rubrica'
    ]
    colunas_presentes_chaves = [col for col in colunas_chaves if col in df_detalhado.columns]
    colunas_presentes_rubricas = [col for col in colunas_rubrica_longa if col in df_detalhado.columns]
    ordem_final_detalhada = colunas_presentes_chaves + colunas_presentes_rubricas
    if not df_detalhado.empty:
        df_detalhado = df_detalhado[ordem_final_detalhada]

    # --- [ETAPA FINAL DE CONVERSÃO DE TIPOS (Sem alteração)] ---
    df_consolidado_tratado = tratar_tipos_para_postgres(df_consolidado)
    df_detalhado_tratado = tratar_tipos_para_postgres(df_detalhado)

    return df_detalhado_tratado, df_consolidado_tratado

# --- PONTO DE EXECUÇÃO (Sem alteração) ---
if __name__ == "__main__":
    inicio_tempo = time.perf_counter()
    # ATENÇÃO: Coloque o caminho correto para sua pasta de PDFs
    caminho_da_pasta = r'C:\Users\JoãoPedrodosSantosSa\Desktop\ARQ-People Intelligence\FOPAG' # Exemplo
    # Em ambiente local, seria algo como:
    # caminho_da_pasta = r'C:\Meus Documentos\Folha Pagamento\PDFs'

    # Os dataframes retornados agora estão com os tipos corretos
    df_detalhado_final, df_consolidado_final = processar_pdfs_na_pasta(caminho_da_pasta)

    if df_consolidado_final is not None and not df_consolidado_final.empty:
        # Mostra os tipos de dados (dtypes) do DataFrame final
        print("\n--- Tipos de Dados Finais (Consolidado) ---")
        df_consolidado_final.info()

        nome_arquivo_saida_consol = 'BASE_FOPAG_CONSOLIDADA_TOTAIS.csv'
        # Salva em CSV (para manter seu processo original)
        df_consolidado_final.to_csv(nome_arquivo_saida_consol, index=False, sep=';', decimal=',', encoding='utf-8-sig')
        print("\n\n--- Processo Finalizado com Sucesso! ---")
        print(f"Sua base CONSOLIDADA (Totais) foi salva em: {os.path.abspath(nome_arquivo_saida_consol)}")
    else:
        print("\nNenhum dado CONSOLIDADO foi gerado.")

    if df_detalhado_final is not None and not df_detalhado_final.empty:
        # Mostra os tipos de dados (dtypes) do DataFrame final
        print("\n--- Tipos de Dados Finais (Detalhado) ---")
        df_detalhado_final.info()

        nome_arquivo_saida_detal = 'BASE_FOPAG_DETALHADA_RUBRICAS.csv'
        # Salva em CSV
        df_detalhado_final.to_csv(nome_arquivo_saida_detal, index=False, sep=';', decimal=',', encoding='utf-8-sig')
        print(f"Sua base DETALHADA (Rubricas) foi salva em: {os.path.abspath(nome_arquivo_saida_detal)}")
    else:
        print("Nenhum dado DETALHADO de rubricas foi gerado.")

    if df_consolidado_final is None and df_detalhado_final is None:
         print("\nNenhum dado foi gerado. Verifique se os PDFs estão na pasta correta e não estão corrompidos.")

    fim_tempo = time.perf_counter()
    duracao = (fim_tempo - inicio_tempo)
    print(f"\nTempo total de execução: {duracao:.2f} segundos")