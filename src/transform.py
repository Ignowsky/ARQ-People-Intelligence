import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .utils import clean_text_series, limpar_valor_moeda

# --- NOVO: Import do Logger ---
from .logger import setup_logger

logger = setup_logger(__name__)


def converter_para_decimal(val):
    """
    Converte valor para Decimal, tratando nulos e strings vazias.
    """
    if val is None or pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_date_seguro(val):
    """
    Tenta converter qualquer coisa para data (Date Object).
    Se falhar, retornar None.
    Isso impede que strings vazias cheguem no banco e causem erro.
    """
    if pd.isna(val) or val is None or str(val).strip() == '' or str(val).lower() == 'nan':
        return None

    val_str = str(val).strip()

    # Lista de formatos possíveis
    formatos = [
        '%d/%m/%Y',  # 01/01/2023
        '%Y-%m-%d',  # 2023-01-01
        '%m/%Y',  # 01/2023
        '%m-%Y',  # 01-2023
        '%d-%m-%Y',  # 01-01-2023
        '%Y/%m/%d'  # 2023/01/01
    ]

    for fmt in formatos:
        try:
            dt = datetime.strptime(val_str, fmt)
            # Se for formato só de mês/ano, assumimos dia 1
            if fmt == '%m/%Y' or fmt == '%m-%Y':
                dt = dt.replace(day=1)
            return dt.date()
        except ValueError:
            continue

    return None


def transformar_dados_pdf(df_consol, df_detalhe):
    """
    Aplica tipagem forte (Date, Decimal) nos dados extraídos do PDF.
    """
    # --- LOG ---
    logger.info(
        f"Iniciando transformação PDF. Consolidado: {len(df_consol)} linhas, Detalhado: {len(df_detalhe)} linhas.")

    colunas_monetarias_consol = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf'
    ]
    colunas_monetarias_detalhe = ['valor_rubrica']

    # --- 1. CONSOLIDADO ---
    if not df_consol.empty:
        # Tratamento de Datas (Usa o parser seguro)
        for col in ['data_admissao', 'data_demissao', 'competencia']:
            if col in df_consol.columns:
                df_consol[col] = df_consol[col].apply(parse_date_seguro)

        # Tratamento Monetário
        for col in colunas_monetarias_consol:
            if col in df_consol.columns:
                df_consol[col] = df_consol[col].apply(converter_para_decimal)

        # CPF
        if 'cpf' in df_consol.columns:
            df_consol['cpf'] = df_consol['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df_consol['cpf'] = df_consol['cpf'].replace(['', 'None', 'nan', 'NaT'], None)

        # Texto Geral
        cols_ignoradas = ['competencia', 'data_admissao', 'data_demissao', 'cpf'] + colunas_monetarias_consol
        cols_texto = [c for c in df_consol.columns if c not in cols_ignoradas]
        for col in cols_texto:
            df_consol[col] = clean_text_series(df_consol[col])

    # --- 2. DETALHADO ---
    if not df_detalhe.empty:
        # Competência
        if 'competencia' in df_detalhe.columns:
            df_detalhe['competencia'] = df_detalhe['competencia'].apply(parse_date_seguro)

        # Monetário
        for col in colunas_monetarias_detalhe:
            if col in df_detalhe.columns:
                df_detalhe[col] = df_detalhe[col].apply(converter_para_decimal)

        # CPF
        if 'cpf' in df_detalhe.columns:
            df_detalhe['cpf'] = df_detalhe['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df_detalhe['cpf'] = df_detalhe['cpf'].replace(['', 'None', 'nan'], None)

        # Texto
        cols_ignoradas_det = ['competencia', 'cpf'] + colunas_monetarias_detalhe
        cols_texto_det = [c for c in df_detalhe.columns if c not in cols_ignoradas_det]
        for col in cols_texto_det:
            df_detalhe[col] = clean_text_series(df_detalhe[col])

    # --- LOG ---
    logger.info("Transformação PDF concluída.")
    return df_consol, df_detalhe


def transformar_dados_api(lista_dicts_api):
    """
    Transforma a lista de dicionários brutos da API Solides em um DataFrame Pandas.
    Aplica achatamento (flatten) completo para pegar campos aninhados.
    """
    if not lista_dicts_api:
        # --- LOG ---
        logger.warning("Lista da API está vazia. Retornando DataFrame vazio.")
        return pd.DataFrame()

    # --- LOG ---
    logger.info(f"Transformando {len(lista_dicts_api)} colaboradores da API...")

    # O json_normalize faz o trabalho pesado de 'achatar' objetos aninhados
    df = pd.json_normalize(lista_dicts_api)

    # Função auxiliar para limpar CPF apenas números
    def clean_digits(val):
        if pd.isna(val) or val is None: return None
        return ''.join(filter(str.isdigit, str(val)))

    # Mapeamento Completo (JSON -> Staging)
    rename_map = {
        'id': 'colaborador_id_solides',
        'name': 'nome_completo',
        'email': 'email_corporativo',
        'registration': 'matricula',
        'birthDate': 'data_nascimento',
        'gender': 'genero',
        'maritalStatus': 'estado_civil',
        'salutation': 'saudacao',
        'nationality': 'nacionalidade',
        'typeOfSpecialNeed': 'tipo_necessidade_especial',
        'birthplace': 'naturalidade',
        'fatherName': 'nome_pai',
        'motherName': 'nome_mae',
        'disabledPerson': 'pcd',
        'dateAdmission': 'data_admissao',
        'dateDismissal': 'data_demissao',
        'reasonDismissal': 'motivo_demissao',  # Adicionado
        'salary': 'salario_api',
        'workShift': 'turno_trabalho',
        'typeContract': 'tipo_contrato',
        'dateContract': 'data_contrato',
        'education': 'escolaridade',
        'course': 'curso_formacao',
        'hierarchicalLevel': 'nivel_hierarquico',
        'durationContract': 'duracao_contrato',
        'contractExpirationDate': 'data_expiracao_contrato',
        'experiencePeriod': 'periodo_experiencia_dias',
        'formDismissal': 'forma_demissao',
        'decisionDismissal': 'decisao_demissao',
        'terminationAmount': 'valor_rescisao',
        'totalBenefits': 'total_beneficios_api',
        'active': 'ativo',
        'ethnicity': 'etnia',
        'updated_at': 'data_ultima_atualizacao_api',

        # Objetos Aninhados
        'senior.name': 'nome_lider_imediato',
        'senior.id': 'lider_id_solides',
        'unity.name': 'unidade_nome',
        'unity.id': 'unidade_id_solides',
        'position.name': 'cargo_nome_api',
        'position.briefDescription': 'descricao_cargo',  # Adicionado
        'position.activities': 'atividades_cargo',  # Adicionado
        'position.id': 'cargo_id_solides',
        'departament.name': 'departamento_nome_api',
        'departament.id': 'departamento_id_solides',

        # Endereço
        'address.zipCode': 'cep',
        'address.streetName': 'logradouro',
        'address.number': 'numero_endereco',
        'address.additionalInformation': 'complemento_endereco',
        'address.neighborhood': 'bairro',
        'address.city.name': 'cidade',
        'address.city.state.initials': 'estado',

        # Contato
        'contact.cellPhone': 'celular',
        'contact.personalEmail': 'email_pessoal',
        'contact.emergencyPhoneNumber': 'telefone_emergencia',

        # Documentos
        'documents.idNumber': 'cpf',
        'documents.rg': 'rg',
        'documents.dispatchDate': 'data_emissao_rg',
        'documents.issuingBody': 'orgao_emissor_rg',
        'documents.voterRegistration': 'titulo_eleitor',
        'documents.electoralZone': 'zona_eleitoral',
        'documents.electoralSection': 'secao_eleitoral',
        'documents.ctpsNum': 'ctps_numero',
        'documents.ctpsSerie': 'ctps_serie',
        'documents.pis': 'pis',
        'documents.bank': 'banco_nome',
        'documents.agency': 'banco_agencia',
        'documents.checkingsAccount': 'banco_conta'
    }

    df = df.rename(columns=rename_map)

    # --- Limpeza de Tipos ---
    # Nomes
    cols_texto = [
        'nome_completo', 'nome_pai', 'nome_mae', 'curso_formacao', 'escolaridade',
        'nome_lider_imediato', 'unidade_nome', 'cargo_nome_api'
    ]

    for col in cols_texto:
        if col in df.columns:
            # remove acentos e converte para maisculo/minusculo padrao
            df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    # CPF
    if 'cpf' in df.columns:
        df['cpf'] = df['cpf'].apply(clean_digits)
    else:
        df['cpf'] = None

    # (Adicione aqui todos os documentos para limpar de uma vez)
    cols_numericas = ['rg', 'pis', 'cep', 'ctps_numero']

    for col in cols_numericas:
        if col in df.columns:
            # Aplica a limpeza (remove pontos, traços, letras)
            df[col] = df[col].apply(clean_digits)

        # Moeda
    for col in ['salario_api', 'valor_rescisao', 'total_beneficios_api']:
        if col in df.columns:
            df[col] = df[col].apply(limpar_valor_moeda)

    # Datas (Agora usando o parse seguro)
    date_cols = [
        'data_nascimento', 'data_admissao', 'data_demissao', 'data_contrato',
        'data_expiracao_contrato', 'data_emissao_rg', 'data_ultima_atualizacao_api'
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_date_seguro)

    # Booleanos
    if 'pcd' in df.columns: df['pcd'] = df['pcd'].astype('boolean')
    if 'ativo' in df.columns: df['ativo'] = df['ativo'].astype('boolean')

    # --- Schema Final ---
    colunas_finais = [
        'colaborador_id_solides', 'cpf', 'nome_completo', 'matricula', 'email_corporativo',
        'data_nascimento', 'genero', 'estado_civil', 'saudacao', 'nacionalidade',
        'tipo_necessidade_especial', 'naturalidade', 'nome_pai', 'nome_mae', 'pcd',
        'data_admissao', 'data_demissao', 'motivo_demissao', 'salario_api', 'turno_trabalho', 'tipo_contrato',
        'data_contrato', 'escolaridade', 'curso_formacao', 'nivel_hierarquico',
        'duracao_contrato', 'data_expiracao_contrato', 'periodo_experiencia_dias',
        'forma_demissao', 'decisao_demissao', 'valor_rescisao', 'total_beneficios_api',
        'ativo', 'etnia', 'data_ultima_atualizacao_api',
        'nome_lider_imediato', 'lider_id_solides', 'unidade_nome', 'unidade_id_solides',
        'cargo_nome_api', 'descricao_cargo', 'atividades_cargo', 'cargo_id_solides', 'departamento_nome_api',
        'departamento_id_solides',
        'cep', 'logradouro', 'numero_endereco', 'complemento_endereco', 'bairro', 'cidade', 'estado',
        'celular', 'email_pessoal', 'telefone_emergencia',
        'rg', 'data_emissao_rg', 'orgao_emissor_rg', 'titulo_eleitor', 'zona_eleitoral', 'secao_eleitoral',
        'ctps_numero', 'ctps_serie', 'pis', 'banco_nome', 'banco_agencia', 'banco_conta'
    ]

    for col in colunas_finais:
        if col not in df.columns:
            df[col] = None

    # --- LOG ---
    logger.info(f"Transformação API concluída. {len(df)} colaboradores processados.")

    return df[colunas_finais].copy()


def transformar_beneficios_api(lista_dicts_api):
    if not lista_dicts_api:
        logger.warning("Lista API vazia, nenhum benefício a processar.")
        return pd.DataFrame()

    lista_beneficios = []
    for colab in lista_dicts_api:
        colab_id = colab.get('id')
        benefits_data = colab.get('benefits', [])

        if isinstance(benefits_data, list):
            for ben in benefits_data:
                # Tenta buscar as variações mais comuns de erro de digitação na API
                nome_ben = ben.get('benefitName') or ben.get('BenefitName') or ben.get('name')

                # O ESCUDO: Se não achar nome nenhum, printa o BO e pula a linha
                if not nome_ben:
                    print(f"⚠️ [ALERTA DE ANOMALIA] ID {colab_id} com benefício zoado. O que a API mandou: {ben}")
                    continue

                lista_beneficios.append({
                    'colaborador_id_solides': colab_id,
                    'nome_beneficio': nome_ben  ,
                    'tipo_beneficio': ben.get('typeBenefit'),
                    'valor_bruto': ben.get('value'),
                    'valor_desconto_bruto': ben.get('valueDiscount'),
                    'periodicidade': ben.get('dates'),
                    'opcao_desconto': ben.get('discountOption'),
                    'aplicado_como': ben.get('benefitAppliedAs')
                })

    df = pd.DataFrame(lista_beneficios)

    if df.empty:
        logger.info("Nenhum benefício útil encontrado nos dados da API.")
        return pd.DataFrame(columns=[
            'colaborador_id_solides', 'nome_beneficio', 'tipo_beneficio',
            'valor_beneficio', 'valor_desconto', 'periodicidade',
            'opcao_desconto', 'aplicado_como'
        ])

    # Conversão blindada direto pra número (evita o bug dos valores milionários)
    df['valor_beneficio'] = pd.to_numeric(
        df['valor_bruto'].astype(str)
        .str.replace('R$', '', regex=False)
        .str.replace(' ', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0.00)

    df['valor_desconto'] = pd.to_numeric(
        df['valor_desconto_bruto'].astype(str)
        .str.replace('R$', '', regex=False)
        .str.replace(' ', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0.00)

    df.drop(columns=['valor_bruto', 'valor_desconto_bruto'], inplace=True)

    logger.info(f"Transformação de Benefícios concluída. {len(df)} registros limpos.")
    return df

def transformar_dependentes_api(dados_brutos):
    """
    Extrai a lista de dependentes e prepara a carga via staging.

    return: pd.DataFrame
    """
    logger.info("Transformando dados de Dependentes e prepara a carga via staging...")

    def clean_digits(val):
        if pd.isna(val) or val is None: return None
        return ''.join(filter(str.isdigit, str(val)))

    if not dados_brutos:
        return pd.DataFrame()

    lista_dependentes = []

    for colab in dados_brutos:
        colab_id = colab.get('id')

        # Tenta pegar 'dependents' ou 'dependentes' (caso venha em PT-BR)
        dependentes = colab.get('dependents',[])

        if isinstance(dependentes, list):
            for dep in dependentes:
                lista_dependentes.append({
                    'colaborador_id_solides': colab_id,
                    'nome_dependente': dep.get('name'),
                    'cpf_dependente': dep.get('IdNumber'),
                    'rg_dependente': dep.get('rg'),
                    'data_nascimento': dep.get('birthDate'),
                    'parentesco': dep.get('relationship')
                })

    df = pd.DataFrame(lista_dependentes)

    if df.empty:
        return pd.DataFrame(columns=['cpf_titular', 'nome_dependente', 'cpf_dependente', 'rg_dependente', 'data_nascimento', 'parentesco'])

    # Tratamento dos dados
    # (Adicione aqui todos os documentos para limpar de uma vez)
    cols_numericas = ['cpf_titular', 'cpf_dependente', 'rg_dependente']
    # Aplica a limpeza (remove pontos, traços, letras)
    for col in cols_numericas:
        if col in df.columns:
            df[col] = df[col].apply(clean_digits)

    if 'data_nascimento' in df.columns:
        df['data_nascimento'] = df['data_nascimento'].apply(parse_date_seguro)

    return df


def transformar_profiler_api(dados_brutos):
    """
    Transforma os dados de Profiler (Perfil Comportamental).
    Campos alvo: id (colaborador), perfil, testDate.
    """
    logger.info("Transformando dados de Profiler...")

    if not dados_brutos:
        return pd.DataFrame()

    lista_profiler = []

    for item in dados_brutos:
        # Segurança: Se a extração falhou e retornou o item básico sem perfil, pulamos
        # Ou se o colaborador não tem perfil mapeado na Sólides
        if 'perfil' not in item:
            continue

        lista_profiler.append({
            'colaborador_id_solides': item.get('id'),
            'perfil_comportamental': item.get('perfil'),
            'data_teste': item.get('testDate')
        })

    df = pd.DataFrame(lista_profiler)

    if df.empty:
        # Retorna DF vazio com as colunas certas para não quebrar o load
        return pd.DataFrame(columns=['colaborador_id_solides', 'perfil_comportamental', 'data_teste'])

    # --- Tratamentos de Dados ---

    # 1. Datas (usando sua função utilitária parse_date_seguro)
    if 'data_teste' in df.columns:
        df['data_teste'] = df['data_teste'].apply(parse_date_seguro)

    # 2. Garantir IDs numéricos
    if 'colaborador_id_solides' in df.columns:
        df['colaborador_id_solides'] = df['colaborador_id_solides'].astype('Int64')

    # 3. Limpeza de Texto (opcional, mas recomendada)
    if 'perfil_comportamental' in df.columns:
        df['perfil_comportamental'] = clean_text_series(df['perfil_comportamental'])

    return df