# src/transform.py
import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
from .utils import clean_text_series, limpar_valor_moeda

def converter_para_decimal(val):
    if val is None or pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None

def transformar_dados_pdf(df_consol, df_detalhe):
    colunas_monetarias_consol = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf'
    ]
    colunas_monetarias_detalhe = ['valor_rubrica']

    # --- 1. Tratamento do CONSOLIDADO ---
    if not df_consol.empty:
        # Tratamento de Datas (Blindagem contra string vazia)
        for col in ['data_admissao', 'data_demissao']:
            if col in df_consol.columns:
                df_consol[col] = pd.to_datetime(df_consol[col], format='%d/%m/%Y', errors='coerce').dt.date
                # Força substituição de NaT/NaN por None (Python null)
                df_consol[col] = df_consol[col].astype(object).where(df_consol[col].notnull(), None)

        # Tratamento da Competência
        if 'competencia' in df_consol.columns:
            df_consol['competencia'] = df_consol['competencia'].astype(str).str.strip()
            mask_valid = (df_consol['competencia'].notna()) & (df_consol['competencia'] != '') & (df_consol['competencia'] != 'None')
            df_consol.loc[mask_valid, 'competencia'] = '01/' + df_consol.loc[mask_valid, 'competencia']
            
            df_consol['competencia'] = pd.to_datetime(df_consol['competencia'], format='%d/%m/%Y', errors='coerce').dt.date
            df_consol['competencia'] = df_consol['competencia'].astype(object).where(df_consol['competencia'].notnull(), None)

        # Tratamento Monetário
        for col in colunas_monetarias_consol:
            if col in df_consol.columns:
                df_consol[col] = df_consol[col].apply(converter_para_decimal)

        # CPF
        if 'cpf' in df_consol.columns:
            df_consol['cpf'] = df_consol['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df_consol['cpf'] = df_consol['cpf'].replace(['', 'None', 'nan'], None)

        # Texto Geral
        cols_ignoradas = ['competencia', 'data_admissao', 'data_demissao', 'cpf'] + colunas_monetarias_consol
        cols_texto = [c for c in df_consol.columns if c not in cols_ignoradas]
        for col in cols_texto:
            df_consol[col] = clean_text_series(df_consol[col])

    # --- 2. Tratamento do DETALHADO ---
    if not df_detalhe.empty:
        if 'competencia' in df_detalhe.columns:
            df_detalhe['competencia'] = df_detalhe['competencia'].astype(str).str.strip()
            mask_valid = (df_detalhe['competencia'].notna()) & (df_detalhe['competencia'] != '') & (df_detalhe['competencia'] != 'None')
            df_detalhe.loc[mask_valid, 'competencia'] = '01/' + df_detalhe.loc[mask_valid, 'competencia']
            
            df_detalhe['competencia'] = pd.to_datetime(df_detalhe['competencia'], format='%d/%m/%Y', errors='coerce').dt.date
            df_detalhe['competencia'] = df_detalhe['competencia'].astype(object).where(df_detalhe['competencia'].notnull(), None)

        for col in colunas_monetarias_detalhe:
            if col in df_detalhe.columns:
                df_detalhe[col] = df_detalhe[col].apply(converter_para_decimal)
        
        if 'cpf' in df_detalhe.columns:
            df_detalhe['cpf'] = df_detalhe['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df_detalhe['cpf'] = df_detalhe['cpf'].replace(['', 'None', 'nan'], None)

        cols_ignoradas_det = ['competencia', 'cpf'] + colunas_monetarias_detalhe
        cols_texto_det = [c for c in df_detalhe.columns if c not in cols_ignoradas_det]
        for col in cols_texto_det:
            df_detalhe[col] = clean_text_series(df_detalhe[col])

    return df_consol, df_detalhe

# ... (Mantenha as funções transformar_dados_api e transformar_beneficios_api iguais) ...
# Para garantir que não quebre a API, vou colar elas aqui também:

def transformar_dados_api(lista_dicts_api):
    if not lista_dicts_api: return pd.DataFrame()
    df = pd.json_normalize(lista_dicts_api)

    df['dept_name_temp'] = None
    if 'departament.name' in df.columns: df['dept_name_temp'] = df['departament.name']
    elif 'department.name' in df.columns: df['dept_name_temp'] = df['department.name']
    
    df['cargo_name_temp'] = None
    if 'position.name' in df.columns: df['cargo_name_temp'] = df['position.name']
    elif 'cargo.name' in df.columns: df['cargo_name_temp'] = df['cargo.name']
    
    df['education_level_temp'] = None
    cols_educ = ['education', 'educationLevel', 'scholarship', 'schooling', 'escolaridade']
    for c in cols_educ:
        if c in df.columns and df['education_level_temp'].isnull().all(): df['education_level_temp'] = df[c]

    df['cpf_temp'] = None 
    cols_cpf = ['documents.idNumber', 'documents.cpf', 'idNumber', 'cpf', 'document']
    for c in cols_cpf:
        if c in df.columns and df['cpf_temp'].isnull().all(): df['cpf_temp'] = df[c]
    
    if 'cpf_temp' in df.columns:
         df['cpf_temp'] = df['cpf_temp'].astype(str).str.replace(r'\D', '', regex=True)
         df['cpf_temp'] = df['cpf_temp'].replace(r'^\s*$', np.nan, regex=True).replace('None', np.nan).replace('nan', np.nan)
    else: df['cpf_temp'] = None

    if 'salary' in df.columns: df['salario_api_temp'] = df['salary'].apply(limpar_valor_moeda)
    else: df['salario_api_temp'] = np.nan
        
    df = df.rename(columns={
        'id': 'colaborador_id_solides', 'name': 'nome_completo', 'cpf_temp': 'cpf', 
        'birthDate': 'data_nascimento', 'gender': 'genero', 'dateAdmission': 'data_admissao',
        'dateDismissal': 'data_demissao', 'active': 'ativo', 'dept_name_temp': 'departamento_nome_api', 
        'cargo_name_temp': 'cargo_nome_api', 'email': 'email', 'contact.phone': 'telefone_pessoal', 
        'contact.cellPhone': 'celular', 'nationality': 'nacionalidade', 'education_level_temp': 'nivel_educacional', 
        'motherName': 'nome_mae', 'fatherName': 'nome_pai', 'address.streetName': 'logradouro', 
        'address.number': 'numero_endereco', 'address.additionalInformation': 'complemento_endereco', 
        'address.neighborhood': 'bairro', 'address.city.name': 'cidade', 'address.state.initials': 'estado', 
        'address.zipCode': 'cep', 'registration': 'matricula', 'maritalStatus': 'estado_civil',
        'salario_api_temp': 'salario_api', 'workShift': 'turno_trabalho', 'typeContract': 'tipo_contrato',
        'course': 'curso_formacao', 'hierarchicalLevel': 'nivel_hierarquico', 'senior.name': 'nome_lider_imediato',
        'ethnicity': 'etnia', 'unity.name': 'unidade_nome', 'salutation': 'saudacao',
        'typeOfSpecialNeed': 'tipo_necessidade_especial', 'birthplace': 'local_nascimento', 'disabledPerson': 'pcd',
        'reasonDismissal': 'motivo_demissao_api', 'dateContract': 'data_contrato', 'durationContract': 'duracao_contrato',
        'contractExpirationDate': 'data_expiracao_contrato', 'experiencePeriod': 'periodo_experiencia_dias',
        'formDismissal': 'forma_demissao', 'decisionDismissal': 'decisao_demissao', 'terminationAmount': 'valor_rescisao',
        'totalBenefits': 'total_beneficios_api', 'updated_at': 'data_ultima_atualizacao_api',
        'contact.emergencyPhoneNumber': 'telefone_emergencia', 'contact.personalEmail': 'email_pessoal',
        'contact.corporateEmail': 'email_corporativo_sec', 'position.id': 'cargo_id_solides',
        'departament.id': 'departamento_id_solides', 'documents.rg': 'rg', 'documents.dispatchDate': 'data_emissao_rg',
        'documents.issuingBody': 'orgao_emissor_rg', 'documents.voterRegistration': 'titulo_eleitor',
        'documents.electoralZone': 'zona_eleitoral', 'documents.electoralSection': 'secao_eleitoral',
        'documents.ctpsNum': 'ctps_numero', 'documents.ctpsSerie': 'ctps_serie', 'documents.pis': 'pis'
    })
    
    date_cols = ['data_nascimento', 'data_admissao', 'data_demissao', 'data_contrato', 
                 'data_expiracao_contrato', 'data_emissao_rg', 'data_ultima_atualizacao_api']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date 
            df[col] = df[col].astype(object).where(df[col].notnull(), None)

    numeric_cols = ['valor_rescisao', 'total_beneficios_api', 'periodo_experiencia_dias', 'cargo_id_solides', 'departamento_id_solides']
    for col in numeric_cols:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'pcd' in df.columns: df['pcd'] = df['pcd'].astype('boolean')
            
    colunas_staging = [
        'colaborador_id_solides', 'cpf', 'nome_completo', 'data_nascimento', 'genero',
        'data_admissao', 'data_demissao', 'ativo', 'departamento_nome_api', 'cargo_nome_api',
        'email', 'telefone_pessoal', 'celular', 'nacionalidade', 'nivel_educacional',
        'nome_mae', 'nome_pai', 'logradouro', 'numero_endereco', 'complemento_endereco', 'bairro', 'cidade', 'estado', 'cep',
        'matricula', 'estado_civil', 'salario_api', 'turno_trabalho', 'tipo_contrato',
        'curso_formacao', 'nivel_hierarquico', 'nome_lider_imediato', 'etnia', 'unidade_nome',
        'saudacao', 'tipo_necessidade_especial', 'local_nascimento', 'pcd', 
        'motivo_demissao_api', 'data_contrato', 'duracao_contrato', 
        'data_expiracao_contrato', 'periodo_experiencia_dias', 'forma_demissao',
        'decisao_demissao', 'valor_rescisao', 'total_beneficios_api', 
        'data_ultima_atualizacao_api', 'telefone_emergencia', 'email_pessoal',
        'email_corporativo_sec', 'cargo_id_solides', 'departamento_id_solides',
        'rg', 'data_emissao_rg', 'orgao_emissor_rg', 'titulo_eleitor', 
        'zona_eleitoral', 'secao_eleitoral', 'ctps_numero', 'ctps_serie', 'pis'
    ]
    for col in colunas_staging:
        if col not in df.columns: df[col] = None 
            
    return df[colunas_staging].copy()

def transformar_beneficios_api(lista_dicts_api):
    if not lista_dicts_api: return pd.DataFrame()
    lista_beneficios = []
    for colab in lista_dicts_api:
        colab_id = colab.get('id')
        benefits_data = colab.get('benefits', [])
        if isinstance(benefits_data, list):
            for ben in benefits_data:
                lista_beneficios.append({
                    'colaborador_id_solides': colab_id, 'nome_beneficio': ben.get('benefitName'),
                    'tipo_beneficio': ben.get('typeBenefit'), 'valor_bruto': ben.get('value'),
                    'valor_desconto_bruto': ben.get('valueDiscount'), 'periodicidade': ben.get('dates'),
                    'opcao_desconto': ben.get('discountOption'), 'aplicado_como': ben.get('benefitAppliedAs')
                })
    df = pd.DataFrame(lista_beneficios)
    if df.empty:
        return pd.DataFrame(columns=['colaborador_id_solides', 'nome_beneficio', 'tipo_beneficio', 
                                     'valor_beneficio', 'valor_desconto', 'periodicidade', 'opcao_desconto', 'aplicado_como'])
    df['valor_beneficio'] = df['valor_bruto'].apply(limpar_valor_moeda)
    df['valor_desconto'] = df['valor_desconto_bruto'].apply(limpar_valor_moeda)
    df.drop(columns=['valor_bruto', 'valor_desconto_bruto'], inplace=True)
    return df