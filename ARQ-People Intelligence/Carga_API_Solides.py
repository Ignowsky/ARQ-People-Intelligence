# INSTALAÇÃO DAS BIBLIOTECAS
# !pip install pdfplumber sqlalchemy psycopg2-binary pandas dotenv requests

# # Fase 1: Pipelines das dimensões (Dados da API)
# ## Passo 1.A: Pipelie da ```dim_departamentos```
# IMPORTAÇÃO DAS BIBILIOTECAS
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, exc as sqlalchemy_exc
import sys
import numpy as np 
import json 
from decimal import Decimal, InvalidOperation
from sqlalchemy.types import String, Date, Numeric # Usado apenas para a API, mas mantido
# --- [FIM DAS IMPORTAÇÕES CORRIGIDAS] ---


# 1. CARREGAR VARIÁVEIS DE AMBIENTE
# -----------------------------------
print("Iniciando ETL...")
load_dotenv()

# Carrega o Token da API
API_TOKEN = os.getenv('SOLIDES_API_TOKEN')

# Carrega os componentes do Banco
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_SCHEMA = os.getenv('DB_SCHEMA')

# Verifica se tudo foi carregado
if not all([API_TOKEN, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, DB_SCHEMA]):
    print("ERRO: Faltando uma ou mais variáveis no arquivo .env")
    print(f"API_TOKEN Carregado: {'Sim' if API_TOKEN else 'NÃO'}")
    print(f"DB_SCHEMA Carregado: {DB_SCHEMA}")
    sys.exit() # Encerra o script se faltar configuração

# 2. CONFIGURAÇÕES GLOBAIS
# -----------------------------------
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BASE_URL = "https://app.solides.com/pt-BR/api/v1"
HEADERS = {
    "Authorization": f"Token token={API_TOKEN}",
    "Accept": "application/json"
}

# 3. CRIA A CONEXÃO E GARANTE O SCHEMA (COM ASPAS)
# ----------------------------------------------------
try:
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        
        # 1. Garante que o schema ("FOPAG") existe PRIMEIRO.
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS \"{DB_SCHEMA}\"'))
        
        # 2. Instala a extensão explicitamente DENTRO do seu schema.
        conn.execute(text(f'CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA \"{DB_SCHEMA}\";'))

    # Recria a engine, definindo o search_path
    engine = create_engine(
        DB_URL,
        connect_args={'options': f'-csearch_path=\"{DB_SCHEMA}\"'}
    )

    print(f"Conexão com PostgreSQL estabelecida e schema '\"{DB_SCHEMA}\"' garantido.")

except Exception as e:
    print(f"Erro ao conectar ao PostgreSQL ou criar schema: {e}")
    sys.exit()


# --- FUNÇÕES HELPER ---

def limpar_salario_api(salario_str):
    """Limpa a string de salário vinda da API (ex: "R$ 8.200,00") para float."""
    if salario_str is None or pd.isna(salario_str):
        return np.nan
    try:
        # Remove 'R$', espaços, e usa '.' como separador de milhar
        salario_limpo = str(salario_str).replace('R$', '').replace(' ', '').replace('.', '')
        # Troca ',' por '.' para ser decimal
        salario_limpo = salario_limpo.replace(',', '.')
        return pd.to_numeric(salario_limpo, errors='coerce')
    except Exception:
        return np.nan

# --- [INÍCIO DA ATUALIZAÇÃO] ---
# A 'dim_colaboradores_base' agora é a dimensão MESTRE
def atualizar_dim_colaboradores_base(engine, df_colaboradores, schema_name):
    """
    Cria a tabela dim_colaboradores_base (se não existir) e
    faz o UPSERT (INSERT ... ON CONFLICT) dos dados de colaboradores.
    AGORA, esta tabela contém os dados mestres vindos do CSV.
    """
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING_TEMP = "stg_colab_temp_upsert" 

    if df_colaboradores is None or df_colaboradores.empty:
        print("Nenhum dado de colaborador fornecido para o UPSERT.")
        return

    print(f"\n--- Iniciando UPSERT para '{NOME_TABELA_BASE}' (Tabela Mestre) ---")

    # SQL para criar a tabela base (AGORA ENRIQUECIDA)
    sql_create_base = text(f"""
        CREATE TABLE IF NOT EXISTS \"{schema_name}\".\"{NOME_TABELA_BASE}\" (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL,
            
            -- Novos campos mestres (do CSV)
            data_admissao_csv DATE,
            data_demissao_csv DATE,
            situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255),
            cargo_csv VARCHAR(255)
        );
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A')
        ON CONFLICT (colaborador_sk) DO NOTHING;
    """)

    # SQL de UPSERT (AGORA ENRIQUECIDO)
    sql_upsert = text(f"""
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (
            nome_colaborador, cpf, 
            data_admissao_csv, data_demissao_csv, situacao_csv, 
            departamento_csv, cargo_csv
        )
        SELECT
            DISTINCT ON (src.cpf)
            src.nome_colaborador,
            src.cpf,
            src.data_admissao_csv,
            src.data_demissao_csv,
            src.situacao_csv,
            src.departamento_csv,
            src.cargo_csv
        FROM
            \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\" AS src
        WHERE
            src.cpf IS NOT NULL AND src.cpf != 'N/A'
        ORDER BY
            src.cpf, src.nome_colaborador DESC
        ON CONFLICT (cpf) DO UPDATE SET
            nome_colaborador = EXCLUDED.nome_colaborador,
            data_admissao_csv = EXCLUDED.data_admissao_csv,
            data_demissao_csv = EXCLUDED.data_demissao_csv,
            situacao_csv = EXCLUDED.situacao_csv,
            departamento_csv = EXCLUDED.departamento_csv,
            cargo_csv = EXCLUDED.cargo_csv;
    """)
    
    try:
        with engine.begin() as conn:
            # 1. Cria a tabela base (se não existir) com a NOVA ESTRUTURA
            conn.execute(sql_create_base)

            # 2. Carga dos dados do DataFrame para a tabela temporária de staging
            # O DataFrame já deve vir com os nomes de colunas corretos 
            # (ex: 'data_admissao_csv')
            df_colaboradores.to_sql(
                NOME_TABELA_STAGING_TEMP,
                con=conn,
                schema=schema_name,
                if_exists='replace',
                index=False
            )

            # 3. Executa o UPSERT (agora enriquecido)
            conn.execute(sql_upsert)

            # 4. (Opcional) Limpa a tabela temporária
            conn.execute(text(f"DROP TABLE \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\""))

        print(f"SUCESSO! '{NOME_TABELA_BASE}' (Mestre) foi atualizada com os dados do DataFrame.")
        # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 1] ---
        return True # <-- Retorna Sucesso
        # --- [FIM DA CORREÇÃO 3/3 - Parte 1] ---

    except Exception as e:
        print(f"ERRO ao fazer UPSERT na '{NOME_TABELA_BASE}': {e}")
        # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 2] ---
        return False # <-- Retorna Falha
        # --- [FIM DA CORREÇÃO 3/3 - Parte 2] ---
# --- [FIM DA ATUALIZAÇÃO] ---


# --- FASE 1: PIPELINES DAS DIMENSÕES (API) ---

def pipeline_dim_colaboradores():
    """
    PUXA dados de Colaboradores da API (paginado) e carrega na dim_colaboradores.
    Popula a dim_colaboradores_base com API (será sobrescrito/enriquecido pelo CSV).
    """
    print("\n--- Iniciando Pipeline: dim_colaboradores (API Sólides) ---")

    # 1. Extração (E)
    # (Lógica de extração idêntica)
    all_colaboradores_lista = []
    page = 1
    page_size = 100
    ENDPOINT_LISTA = "/colaboradores" 
    print("Iniciando extração (Passo 1/2): Buscando lista de IDs de colaboradores...")
    while True:
        params = {'page': page, 'page_size': page_size, 'status': 'todos'} 
        try:
            response = requests.get(f"{BASE_URL}{ENDPOINT_LISTA}", headers=HEADERS, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    print(f"Extração da lista concluída. Total de {len(all_colaboradores_lista)} colaboradores encontrados.")
                    break
                all_colaboradores_lista.extend(data) 
                print(f"Página {page} da lista carregada...")
                page += 1
            else:
                print(f"Erro na API (Página {page}): {response.status_code} {response.text}")
                return False
        except Exception as e:
            print(f"Erro na extração de colaboradores (lista): {e}")
            return False
    if not all_colaboradores_lista:
        print("Nenhum colaborador encontrado.")
        return True
    print(f"Passo 1/2 concluído. {len(all_colaboradores_lista)} colaboradores encontrados.")
    all_colaboradores_detalhado = []
    total_colabs = len(all_colaboradores_lista)
    print(f"Iniciando extração (Passo 2/2): Buscando detalhes completos...")
    for i, colab_info in enumerate(all_colaboradores_lista):
        colab_id = colab_info.get('id')
        if not colab_id:
            continue
        print(f"   Buscando colaborador {i+1} de {total_colabs} (ID: {colab_id})...")
        ENDPOINT_DETALHE = f"/colaboradores/{colab_id}"
        try:
            response_detalhe = requests.get(f"{BASE_URL}{ENDPOINT_DETALHE}", headers=HEADERS)
            if response_detalhe.status_code == 200:
                data_detalhe = response_detalhe.json()
                all_colaboradores_detalhado.append(data_detalhe)
            else:
                print(f"    ERRO ao buscar detalhes do ID {colab_id}: {response_detalhe.status_code}. Usando dados básicos da lista.")
                all_colaboradores_detalhado.append(colab_info) 
        except Exception as e:
            print(f"    EXCEÇÃO ao buscar detalhes do ID {colab_id}: {e}. Usando dados básicos da lista.")
            all_colaboradores_detalhado.append(colab_info) 
    print("Passo 2/2 concluído. Detalhes de todos os colaboradores buscados.")

    # 2. Transformação (T)
    df = pd.json_normalize(all_colaboradores_detalhado)

    # --- (Lógica de transformação da API Sólides - IDÊNTICA A ANTES) ---
    df['dept_name_temp'] = None
    if 'departament.name' in df.columns:
        print("Info: Departamento encontrado na chave 'departament.name'.")
        df['dept_name_temp'] = df['departament.name']
    elif 'department.name' in df.columns: 
        print("Info: Departamento encontrado na chave 'department.name'.")
        df['dept_name_temp'] = df['department.name']
    else:
        print("Aviso: Nenhuma chave de Departamento ('departament.name', 'department.name') foi encontrada.")
    df['cargo_name_temp'] = None
    if 'position.name' in df.columns:
        print("Info: Cargo encontrado na chave 'position.name'.")
        df['cargo_name_temp'] = df['position.name']
    elif 'cargo.name' in df.columns: 
        print("Info: Cargo encontrado na chave 'cargo.name'.")
        df['cargo_name_temp'] = df['cargo.name']
    else:
        print("Aviso: Nenhuma chave de Cargo ('position.name', 'cargo.name') foi encontrada.")
    df['education_level_temp'] = None
    if 'education' in df.columns: 
        print("Info: Nível Educacional encontrado na chave 'education'.")
        df['education_level_temp'] = df['education']
    elif 'educationLevel' in df.columns: 
        print("Info: Nível Educacional encontrado na chave 'educationLevel'.")
        df['education_level_temp'] = df['educationLevel']
    elif 'scholarship' in df.columns: 
        print("Info: Nível Educacional encontrado na chave 'scholarship'.")
        df['education_level_temp'] = df['scholarship']
    elif 'schooling' in df.columns:
        print("Info: Nível Educacional encontrado na chave 'schooling'.")
        df['education_level_temp'] = df['schooling']
    elif 'escolaridade' in df.columns: 
        print("Info: Nível Educacional encontrado na chave 'escolaridade'.")
        df['education_level_temp'] = df['escolaridade']
    else:
        print("Aviso: Nenhuma chave de Nível Educacional ('education', 'educationLevel', 'scholarship', 'schooling', 'escolaridade') foi encontrada.")
    df['cpf_temp'] = None 
    if 'documents.idNumber' in df.columns: 
        print("Info: CPF encontrado na chave 'documents.idNumber'.")
        df['cpf_temp'] = df['documents.idNumber']
    elif 'documents.cpf' in df.columns:
        print("Info: CPF encontrado na chave 'documents.cpf'.")
        df['cpf_temp'] = df['documents.cpf']
    elif 'idNumber' in df.columns:
        print("Info: CPF encontrado na chave 'idNumber' (raiz).")
        df['cpf_temp'] = df['idNumber']
    elif 'cpf' in df.columns:
        print("Info: CPF encontrado na chave 'cpf' (raiz).")
        df['cpf_temp'] = df['cpf']
    elif 'document' in df.columns:
        print("Info: CPF encontrado na chave 'document' (raiz).")
        df['cpf_temp'] = df['document']
    else:
        print("Aviso: Nenhuma chave de CPF ('documents.idNumber', 'idNumber', 'cpf', 'document') foi encontrada.")
    if 'cpf_temp' in df.columns:
         df['cpf_temp'] = df['cpf_temp'].astype(str).str.replace(r'\D', '', regex=True)
         df['cpf_temp'] = df['cpf_temp'].replace(r'^\s*$', np.nan, regex=True).replace('None', np.nan).replace('nan', np.nan)
    else:
         df['cpf_temp'] = None
    if 'salary' in df.columns:
        df['salario_api_temp'] = df['salary'].apply(limpar_salario_api)
    else:
        df['salario_api_temp'] = np.nan
        
    # --- (Dicionário de Renomeação - IDÊNTICO) ---
    df = df.rename(columns={
        'id': 'colaborador_id_solides',
        'name': 'nome_completo',
        'cpf_temp': 'cpf', 
        'birthDate': 'data_nascimento',
        'gender': 'genero',
        'dateAdmission': 'data_admissao',
        'dateDismissal': 'data_demissao',
        'active': 'ativo',
        'dept_name_temp': 'departamento_nome_api', 
        'cargo_name_temp': 'cargo_nome_api',           
        'email': 'email', # Email principal (geralmente corporativo)
        'contact.phone': 'telefone_pessoal', 
        'contact.cellPhone': 'celular', 
        'nationality': 'nacionalidade',
        'education_level_temp': 'nivel_educacional', 
        'motherName': 'nome_mae',
        'fatherName': 'nome_pai',
        'address.streetName': 'logradouro', 
        'address.number': 'numero_endereco',
        'address.additionalInformation': 'complemento_endereco', 
        'address.neighborhood': 'bairro',
        'address.city.name': 'cidade', 
        'address.state.initials': 'estado', 
        'address.zipCode': 'cep',
        'registration': 'matricula',
        'maritalStatus': 'estado_civil',
        'salario_api_temp': 'salario_api',
        'workShift': 'turno_trabalho',
        'typeContract': 'tipo_contrato',
        'course': 'curso_formacao',
        'hierarchicalLevel': 'nivel_hierarquico',
        'senior.name': 'nome_lider_imediato',
        'ethnicity': 'etnia',
        'unity.name': 'unidade_nome',
        'salutation': 'saudacao',
        'typeOfSpecialNeed': 'tipo_necessidade_especial',
        'birthplace': 'local_nascimento',
        'disabledPerson': 'pcd',
        'reasonDismissal': 'motivo_demissao_api',
        'dateContract': 'data_contrato',
        'durationContract': 'duracao_contrato',
        'contractExpirationDate': 'data_expiracao_contrato',
        'experiencePeriod': 'periodo_experiencia_dias',
        'formDismissal': 'forma_demissao',
        'decisionDismissal': 'decisao_demissao',
        'terminationAmount': 'valor_rescisao',
        'totalBenefits': 'total_beneficios_api',
        'updated_at': 'data_ultima_atualizacao_api',
        'contact.emergencyPhoneNumber': 'telefone_emergencia',
        'contact.personalEmail': 'email_pessoal',
        'contact.corporateEmail': 'email_corporativo_sec', # 'email' raiz já foi pego
        'position.id': 'cargo_id_solides',
        'departament.id': 'departamento_id_solides',
        'documents.rg': 'rg',
        'documents.dispatchDate': 'data_emissao_rg',
        'documents.issuingBody': 'orgao_emissor_rg',
        'documents.voterRegistration': 'titulo_eleitor',
        'documents.electoralZone': 'zona_eleitoral',
        'documents.electoralSection': 'secao_eleitoral',
        'documents.ctpsNum': 'ctps_numero',
        'documents.ctpsSerie': 'ctps_serie',
        'documents.pis': 'pis'
    })
    
    # --- (Tratamento de Tipos - IDÊNTICO) ---
    date_cols = [
        'data_nascimento', 'data_admissao', 'data_demissao',
        'data_contrato', 'data_expiracao_contrato', 'data_emissao_rg', 
        'data_ultima_atualizacao_api'
    ]
    for col in date_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='raise')
            except ValueError:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
        else:
             df[col] = pd.NaT
    numeric_cols = [
        'valor_rescisao', 'total_beneficios_api', 'periodo_experiencia_dias',
        'cargo_id_solides', 'departamento_id_solides'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    boolean_cols = ['pcd'] # 'disabledPerson'
    for col in boolean_cols:
        if col in df.columns:
            df[col] = df[col].astype('boolean')
            
    # --- (Lista de Staging - IDÊNTICA) ---
    colunas_staging = [
        'colaborador_id_solides', 'cpf', 'nome_completo', 'data_nascimento', 'genero',
        'data_admissao', 'data_demissao', 'ativo',
        'departamento_nome_api', 'cargo_nome_api',
        'email', 'telefone_pessoal', 'celular', 'nacionalidade', 'nivel_educacional',
        'nome_mae', 'nome_pai',
        'logradouro', 'numero_endereco', 'complemento_endereco', 'bairro', 'cidade', 'estado', 'cep',
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
        if col not in df.columns:
            df[col] = None 
    df_staging = df[colunas_staging].copy()
    print("Transformação de colaboradores concluída.")

    # 3. Carga (L)
    NOME_TABELA_RICA = "dim_colaboradores" # Tabela rica da API
    NOME_TABELA_BASE = "dim_colaboradores_base" # Tabela conformada (SK, CPF, Nome)
    NOME_TABELA_STAGING = "staging_colaboradores"

    try:
        df_staging.to_sql(NOME_TABELA_STAGING, engine, if_exists='replace', index=False, schema=DB_SCHEMA)
        print("Carga na staging de colaboradores concluída.")

        # --- [INÍCIO DA CORREÇÃO 1/3] ---
        # Expansão do SQL de Carga (Adicionando ALTER TABLE)
        sql = f"""
        -- PASSO 1: Cria a Tabela Base (Conformada) conforme a NOVA ESTRUTURA MESTRE
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_BASE} (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL,
            
            -- Campos mestres (do CSV)
            data_admissao_csv DATE,
            data_demissao_csv DATE,
            situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255),
            cargo_csv VARCHAR(255)
        );
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_BASE} (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A')
        ON CONFLICT (colaborador_sk) DO NOTHING;

        -- *** CORREÇÃO PARA ERRO 1 ***
        -- Garante que as colunas mestre existem, mesmo se a tabela foi criada por um script antigo
        ALTER TABLE "{DB_SCHEMA}".{NOME_TABELA_BASE}
            ADD COLUMN IF NOT EXISTS data_admissao_csv DATE,
            ADD COLUMN IF NOT EXISTS data_demissao_csv DATE,
            ADD COLUMN IF NOT EXISTS situacao_csv VARCHAR(100),
            ADD COLUMN IF NOT EXISTS departamento_csv VARCHAR(255),
            ADD COLUMN IF NOT EXISTS cargo_csv VARCHAR(255);
        -- *** FIM DA CORREÇÃO 1/3 ***

        -- PASSO 2: Faz o UPSERT da API (staging) para a Tabela Base
        -- Nota: Isso só popula Nome e CPF. O CSV irá enriquecer o restante.
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_BASE} (nome_colaborador, cpf)
        SELECT
            DISTINCT ON (stg.cpf)
            stg.nome_completo,
            stg.cpf
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        WHERE
            stg.cpf IS NOT NULL AND stg.cpf != 'N/A'
        ORDER BY
            stg.cpf, stg.colaborador_id_solides DESC 
        ON CONFLICT (cpf) DO UPDATE SET
            nome_colaborador = EXCLUDED.nome_colaborador;

        -- PASSO 3: Cria a Tabela Rica (dim_colaboradores)
        -- (SQL IDÊNTICO AO ANTERIOR)
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_RICA} (
            colaborador_sk INTEGER PRIMARY KEY, 
            colaborador_id_solides INTEGER UNIQUE NOT NULL, 
            cpf VARCHAR(11), 
            nome_completo VARCHAR(255),
            data_nascimento DATE,
            genero VARCHAR(50),
            data_admissao DATE,
            data_demissao DATE,
            ativo BOOLEAN,
            departamento_nome_api VARCHAR(255),
            cargo_nome_api VARCHAR(255),
            email VARCHAR(255),
            telefone_pessoal VARCHAR(50),
            celular VARCHAR(50),
            nacionalidade VARCHAR(100),
            nivel_educacional VARCHAR(100),
            nome_mae VARCHAR(255),
            nome_pai VARCHAR(255),
            logradouro VARCHAR(255),
            numero_endereco VARCHAR(50),
            complemento_endereco VARCHAR(100),
            bairro VARCHAR(100),
            cidade VARCHAR(100),
            estado VARCHAR(50),
            cep VARCHAR(20),
            matricula VARCHAR(50),
            estado_civil VARCHAR(50),
            salario_api NUMERIC(12, 2),
            turno_trabalho VARCHAR(100),
            tipo_contrato VARCHAR(100),
            curso_formacao VARCHAR(255),
            nivel_hierarquico VARCHAR(100),
            nome_lider_imediato VARCHAR(255),
            etnia VARCHAR(50),
            unidade_nome VARCHAR(255),
            data_ultima_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE}(colaborador_sk)
        );

        -- (SQL IDÊNTICO AO ANTERIOR)
        ALTER TABLE "{DB_SCHEMA}".{NOME_TABELA_RICA}
            ADD COLUMN IF NOT EXISTS matricula VARCHAR(50),
            ADD COLUMN IF NOT EXISTS estado_civil VARCHAR(50),
            ADD COLUMN IF NOT EXISTS salario_api NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS turno_trabalho VARCHAR(100),
            ADD COLUMN IF NOT EXISTS tipo_contrato VARCHAR(100),
            ADD COLUMN IF NOT EXISTS curso_formacao VARCHAR(255),
            ADD COLUMN IF NOT EXISTS nivel_hierarquico VARCHAR(100),
            ADD COLUMN IF NOT EXISTS nome_lider_imediato VARCHAR(255),
            ADD COLUMN IF NOT EXISTS etnia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS unidade_nome VARCHAR(255),
            ADD COLUMN IF NOT EXISTS saudacao VARCHAR(50),
            ADD COLUMN IF NOT EXISTS tipo_necessidade_especial VARCHAR(100),
            ADD COLUMN IF NOT EXISTS local_nascimento VARCHAR(100),
            ADD COLUMN IF NOT EXISTS pcd BOOLEAN,
            ADD COLUMN IF NOT EXISTS motivo_demissao_api VARCHAR(255),
            ADD COLUMN IF NOT EXISTS data_contrato DATE,
            ADD COLUMN IF NOT EXISTS duracao_contrato VARCHAR(100),
            ADD COLUMN IF NOT EXISTS data_expiracao_contrato DATE,
            ADD COLUMN IF NOT EXISTS periodo_experiencia_dias INTEGER,
            ADD COLUMN IF NOT EXISTS forma_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS decisao_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS valor_rescisao NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS total_beneficios_api NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS data_ultima_atualizacao_api DATE,
            ADD COLUMN IF NOT EXISTS telefone_emergencia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS email_pessoal VARCHAR(255),
            ADD COLUMN IF NOT EXISTS email_corporativo_sec VARCHAR(255),
            ADD COLUMN IF NOT EXISTS cargo_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS departamento_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS data_emissao_rg DATE,
            ADD COLUMN IF NOT EXISTS orgao_emissor_rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS titulo_eleitor VARCHAR(50),
            ADD COLUMN IF NOT EXISTS zona_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS secao_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_numero VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_serie VARCHAR(50),
            ADD COLUMN IF NOT EXISTS pis VARCHAR(50);
        
        -- (SQL IDÊNTICO AO ANTERIOR)
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_RICA} (colaborador_sk, colaborador_id_solides)
        VALUES (0, -1)
        ON CONFLICT (colaborador_sk) DO NOTHING;

        -- PASSO 4: Faz o UPSERT na Tabela Rica (com todas as colunas)
        -- (SQL IDÊNTICO AO ANTERIOR)
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_RICA} (
            colaborador_sk, 
            colaborador_id_solides, 
            cpf, 
            nome_completo, data_nascimento, genero,
            nacionalidade, nivel_educacional, nome_mae, nome_pai,
            estado_civil, etnia,
            data_admissao, data_demissao, ativo,
            departamento_nome_api, cargo_nome_api,
            matricula, salario_api, turno_trabalho, tipo_contrato,
            curso_formacao, nivel_hierarquico, nome_lider_imediato,
            unidade_nome,
            email, telefone_pessoal, celular, 
            logradouro, numero_endereco, complemento_endereco, bairro, 
            cidade, estado, cep,
            saudacao, tipo_necessidade_especial, local_nascimento, pcd,
            telefone_emergencia, email_pessoal, email_corporativo_sec,
            rg, data_emissao_rg, orgao_emissor_rg, titulo_eleitor,
            zona_eleitoral, secao_eleitoral, ctps_numero, ctps_serie, pis,
            motivo_demissao_api, data_contrato, duracao_contrato,
            data_expiracao_contrato, periodo_experiencia_dias, forma_demissao,
            decisao_demissao, valor_rescisao, total_beneficios_api,
            cargo_id_solides, departamento_id_solides,
            data_ultima_atualizacao_api,
            data_ultima_atualizacao
        )
        SELECT
            base.colaborador_sk, 
            stg.colaborador_id_solides, 
            stg.cpf,
            stg.nome_completo, stg.data_nascimento, stg.genero,
            stg.nacionalidade, stg.nivel_educacional, stg.nome_mae, stg.nome_pai,
            stg.estado_civil, stg.etnia,
            stg.data_admissao, stg.data_demissao, stg.ativo,
            stg.departamento_nome_api,
            stg.cargo_nome_api,
            stg.matricula, stg.salario_api, stg.turno_trabalho, stg.tipo_contrato,
            stg.curso_formacao, stg.nivel_hierarquico, stg.nome_lider_imediato,
            stg.unidade_nome,
            stg.email, stg.telefone_pessoal, stg.celular,
            stg.logradouro, stg.numero_endereco, stg.complemento_endereco, stg.bairro, 
            stg.cidade, stg.estado, stg.cep,
            stg.saudacao, stg.tipo_necessidade_especial, stg.local_nascimento, stg.pcd,
            stg.telefone_emergencia, stg.email_pessoal, stg.email_corporativo_sec,
            stg.rg, stg.data_emissao_rg, stg.orgao_emissor_rg, stg.titulo_eleitor,
            stg.zona_eleitoral, stg.secao_eleitoral, stg.ctps_numero, stg.ctps_serie, stg.pis,
            stg.motivo_demissao_api, stg.data_contrato, stg.duracao_contrato,
            stg.data_expiracao_contrato, stg.periodo_experiencia_dias, stg.forma_demissao,
            stg.decisao_demissao, stg.valor_rescisao, stg.total_beneficios_api,
            stg.cargo_id_solides, stg.departamento_id_solides,
            stg.data_ultima_atualizacao_api,
            current_timestamp
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        JOIN
            "{DB_SCHEMA}".{NOME_TABELA_BASE} AS base ON stg.cpf = base.cpf
        WHERE
            stg.colaborador_id_solides IS NOT NULL

        ON CONFLICT (colaborador_id_solides) DO UPDATE SET
            cpf = EXCLUDED.cpf, 
            nome_completo = EXCLUDED.nome_completo,
            data_nascimento = EXCLUDED.data_nascimento,
            genero = EXCLUDED.genero,
            nacionalidade = EXCLUDED.nacionalidade,
            nivel_educacional = EXCLUDED.nivel_educacional,
            nome_mae = EXCLUDED.nome_mae,
            nome_pai = EXCLUDED.nome_pai,
            estado_civil = EXCLUDED.estado_civil,
            etnia = EXCLUDED.etnia,
            data_admissao = EXCLUDED.data_admissao,
            data_demissao = EXCLUDED.data_demissao,
            ativo = EXCLUDED.ativo,
            departamento_nome_api = EXCLUDED.departamento_nome_api,
            cargo_nome_api = EXCLUDED.cargo_nome_api,
            matricula = EXCLUDED.matricula,
            salario_api = EXCLUDED.salario_api,
            turno_trabalho = EXCLUDED.turno_trabalho,
            tipo_contrato = EXCLUDED.tipo_contrato,
            curso_formacao = EXCLUDED.curso_formacao,
            nivel_hierarquico = EXCLUDED.nivel_hierarquico,
            nome_lider_imediato = EXCLUDED.nome_lider_imediato,
            unidade_nome = EXCLUDED.unidade_nome,
            email = EXCLUDED.email,
            telefone_pessoal = EXCLUDED.telefone_pessoal,
            celular = EXCLUDED.celular,
            logradouro = EXCLUDED.logradouro,
            numero_endereco = EXCLUDED.numero_endereco,
            complemento_endereco = EXCLUDED.complemento_endereco,
            bairro = EXCLUDED.bairro,
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            cep = EXCLUDED.cep,
            saudacao = EXCLUDED.saudacao,
            tipo_necessidade_especial = EXCLUDED.tipo_necessidade_especial,
            local_nascimento = EXCLUDED.local_nascimento,
            pcd = EXCLUDED.pcd,
            telefone_emergencia = EXCLUDED.telefone_emergencia,
            email_pessoal = EXCLUDED.email_pessoal,
            email_corporativo_sec = EXCLUDED.email_corporativo_sec,
            rg = EXCLUDED.rg,
            data_emissao_rg = EXCLUDED.data_emissao_rg,
            orgao_emissor_rg = EXCLUDED.orgao_emissor_rg,
            titulo_eleitor = EXCLUDED.titulo_eleitor,
            zona_eleitoral = EXCLUDED.zona_eleitoral,
            secao_eleitoral = EXCLUDED.secao_eleitoral,
            ctps_numero = EXCLUDED.ctps_numero,
            ctps_serie = EXCLUDED.ctps_serie,
            pis = EXCLUDED.pis,
            motivo_demissao_api = EXCLUDED.motivo_demissao_api,
            data_contrato = EXCLUDED.data_contrato,
            duracao_contrato = EXCLUDED.duracao_contrato,
            data_expiracao_contrato = EXCLUDED.data_expiracao_contrato,
            periodo_experiencia_dias = EXCLUDED.periodo_experiencia_dias,
            forma_demissao = EXCLUDED.forma_demissao,
            decisao_demissao = EXCLUDED.decisao_demissao,
            valor_rescisao = EXCLUDED.valor_rescisao,
            total_beneficios_api = EXCLUDED.total_beneficios_api,
            cargo_id_solides = EXCLUDED.cargo_id_solides,
            departamento_id_solides = EXCLUDED.departamento_id_solides,
            data_ultima_atualizacao_api = EXCLUDED.data_ultima_atualizacao_api,
            data_ultima_atualizacao = current_timestamp;
        """
        # --- [FIM DA ATUALIZAÇÃO] ---

        with engine.begin() as conn:
            conn.execute(text(sql))

        print(f"Carga na {NOME_TABELA_BASE} e {NOME_TABELA_RICA} concluída com sucesso!")
        return True

    except Exception as e:
        print(f"Erro na carga de {NOME_TABELA_RICA}: {e}")
        print(f"Detalhe do erro: {e}")
        return False


# --- NOVA DIMENSÃO: CALENDÁRIO ---
def pipeline_dim_calendario():
    """Gera ou atualiza a dimensão de calendário (dim_calendario)."""
    print("\n--- Iniciando Pipeline: dim_calendario ---")
    
    NOME_TABELA_FINAL = "dim_calendario"
    
    # (Lógica idêntica, sem alterações)
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
        data DATE PRIMARY KEY,
        ano INTEGER,
        mes INTEGER,
        dia INTEGER,
        trimestre INTEGER,
        semestre INTEGER,
        dia_da_semana INTEGER, 
        nome_dia_da_semana VARCHAR(20),
        nome_mes VARCHAR(20),
        nome_mes_abrev CHAR(3),
        ano_mes VARCHAR(7), 
        dia_do_ano INTEGER,
        semana_do_ano INTEGER
    );
    DO $$
    DECLARE
        data_inicio DATE := '2023-01-01'; 
        data_fim DATE := '2030-12-31';
    BEGIN
        BEGIN
            SET LOCAL lc_time = 'pt_BR.UTF-8';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                SET LOCAL lc_time = 'pt_BR';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Não foi possível definir o locale pt_BR. Nomes de mês/dia podem ficar em inglês.';
            END;
        END;
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            data,
            ano, mes, dia, trimestre, semestre,
            dia_da_semana, nome_dia_da_semana, nome_mes, nome_mes_abrev,
            ano_mes, dia_do_ano, semana_do_ano
        )
        SELECT
            d AS data,
            EXTRACT(YEAR FROM d) AS ano,
            EXTRACT(MONTH FROM d) AS mes,
            EXTRACT(DAY FROM d) AS dia,
            EXTRACT(QUARTER FROM d) AS trimestre,
            CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END AS semestre,
            EXTRACT(DOW FROM d) AS dia_da_semana, 
            to_char(d, 'TMDay') AS nome_dia_da_semana,
            to_char(d, 'TMMonth') AS nome_mes,
            to_char(d, 'TMMon') AS nome_mes_abrev,
            to_char(d, 'YYYY-MM') AS ano_mes,
            EXTRACT(DOY FROM d) AS dia_do_ano,
            EXTRACT(WEEK FROM d) AS semana_do_ano
        FROM generate_series(data_inicio, data_fim, '1 day'::interval) d
        ON CONFLICT (data) DO NOTHING; 
    END $$;
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True
    except Exception as e:
        print(f"Erro na carga de {NOME_TABELA_FINAL}: {e}")
        print(f"Detalhe do erro: {e}")
        return False


# --- FASE 2: PIPELINES DAS FATOS (CSV) ---

# --- (FUNÇÕES HELPER DE CSV - IDÊNTICAS) ---

def clean_text(series):
    """Limpa uma série de texto (object) de forma segura."""
    if series.dtype == 'object':
        series = series.str.strip()
        series = series.str.replace(u'\xa0', '', regex=False)
        series = series.replace(['N/A', '', 'nan', 'None', 'NULL'], None) # Adicionado 'NULL'
    return series

def para_float(valor_str):
    """Converte uma string (já limpa) para float."""
    if valor_str is None or pd.isna(valor_str):
        return np.nan # Use numpy's NaN para floats
    try:
        # CSV do Notebook 1 salva com PONTO decimal (ex: "1234.56")
        return float(valor_str) 
    except (ValueError, TypeError):
        return np.nan

def tratar_tipos_dataframe_csv(df, nome_arquivo):
    """
    Função de tratamento de tipos para os CSVs da FOPAG.
    *** VERSÃO CORRIGIDA PARA DATA E FLOAT ***
    """
    print(f"Iniciando tratamento de tipos para {nome_arquivo}...")

    # --- [CORREÇÃO DATAS] ---
    colunas_data = ['competencia', 'data_admissao', 'data_demissao']
    for col in colunas_data:
        if col in df.columns:
            print(f"Tratando tipo de data: {col}")
            df[col] = clean_text(df[col])
            # O CSV já está em formato ISO (YYYY-MM-DD), o pandas lê automaticamente
            df[col] = pd.to_datetime(df[col], errors='coerce') 
            df[col] = df[col].dt.date # Converte para objeto date (YYYY-MM-DD)

    # --- [CORREÇÃO NUMÉRICOS PARA FLOAT] ---
    colunas_monetarias = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf', 'valor_rubrica'
    ]
    for col in colunas_monetarias:
        if col in df.columns:
            print(f"Tratando tipo: {col} (String -> Float)")
            df[col] = clean_text(df[col]) 
            # Usa a nova função para_float
            df[col] = df[col].apply(para_float)
            # Garante que a coluna inteira seja do tipo float no Pandas
            df[col] = pd.to_numeric(df[col], errors='coerce') 

    # --- [CPF E TEXTO] ---
    if 'cpf' in df.columns:
        print("Tratando tipo: cpf (String -> String Limpa)")
        df['cpf'] = clean_text(df['cpf'])
        df['cpf'] = df['cpf'].str.replace(r'[^\d]', '', regex=True)

    # Adiciona os novos campos de FATO à limpeza de texto
    colunas_texto = [
        'departamento', 'vinculo', 'nome_funcionario',
        'motivo_demissao', 'cargo', 'codigo_rubrica',
        'nome_rubrica', 'tipo_rubrica',
        
        # NOVOS CAMPOS PARA A FATO
        'situacao', 
        'tipo_calculo' 
    ]
    
    for col in colunas_texto:
        if col in df.columns:
            df[col] = clean_text(df[col])

    print("Tratamento de tipos finalizado.")
    return df
# --- [FIM DAS FUNÇÕES HELPER DE CSV CORRIGIDAS] ---


def pipeline_fato_folha_consolidada():
    print("\n--- Iniciando Pipeline: fato_folha_consolidada ---")

    CSV_FILE = 'BASE_FOPAG_CONSOLIDADA_TOTAIS.csv'
    NOME_TABELA_STAGING = 'staging_folha_consolidada'
    NOME_TABELA_FINAL = 'fato_folha_consolidada'
    NOME_TABELA_BASE_COLAB = 'dim_colaboradores_base' 

    try:
        # 1. Extract
        try:
            df_csv = pd.read_csv(CSV_FILE, sep=';', dtype=str) # Lê tudo como string
        except FileNotFoundError:
             print(f"ERRO: Arquivo '{CSV_FILE}' não encontrado.")
             print("Por favor, execute o Notebook 1 (Automação_FOPAG.ipynb) para gerar os CSVs primeiro.")
             return False
        except Exception as read_err:
            print(f"Erro ao ler CSV {CSV_FILE}: {read_err}.")
            return False

        # 2. Transformação (T)
        df_tratado = tratar_tipos_dataframe_csv(df_csv.copy(), CSV_FILE)

        # --- [INÍCIO DA ATUALIZAÇÃO] ---
        # Popula a dim_colaboradores_base com os dados MESTRE do CSV
        print(f"Extraindo dados mestre do '{CSV_FILE}' para popular a dim_colaboradores_base...")
        if 'cpf' in df_tratado.columns:
            # Ordena para pegar os dados mais recentes de cada CPF (baseado na competência)
            df_recentes = df_tratado.sort_values(by='competencia', ascending=False).drop_duplicates(subset=['cpf'])
            
            # Seleciona as colunas mestre
            colunas_mestre = [
                'cpf', 
                'nome_funcionario', 
                'data_admissao', 
                'data_demissao', 
                'situacao', 
                'departamento', # 'departamento' do CSV
                'cargo'         # 'cargo' do CSV
            ]
            
            # Garante que todas as colunas existem no DF
            colunas_presentes = [col for col in colunas_mestre if col in df_recentes.columns]
            df_colabs_unicos = df_recentes[colunas_presentes].copy()
            
            # Renomeia para o padrão da função helper
            df_colabs_unicos = df_colabs_unicos.rename(columns={
                'nome_funcionario': 'nome_colaborador',
                'data_admissao': 'data_admissao_csv',
                'data_demissao': 'data_demissao_csv',
                'situacao': 'situacao_csv',
                'departamento': 'departamento_csv',
                'cargo': 'cargo_csv'
            })
            
            # Garante que a função helper receba colunas mesmo se não existirem no CSV
            colunas_helper_esperadas = [
                'nome_colaborador', 'cpf', 'data_admissao_csv', 'data_demissao_csv',
                'situacao_csv', 'departamento_csv', 'cargo_csv'
            ]
            for col in colunas_helper_esperadas:
                if col not in df_colabs_unicos.columns:
                    df_colabs_unicos[col] = None

            # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 3] ---
            # Captura o retorno da função helper
            sucesso_upsert = atualizar_dim_colaboradores_base(engine, df_colabs_unicos, DB_SCHEMA)
            
            if not sucesso_upsert:
                print("Falha ao atualizar a dim_colaboradores_base. Abortando pipeline da Fato Consolidada.")
                return False # Para a execução
            # --- [FIM DA CORREÇÃO 3/3 - Parte 3] ---

        else:
            print("AVISO: Coluna 'cpf' não encontrada no CSV. Não foi possível popular a dim_colaboradores_base.")
        # --- [FIM DA ATUALIZAÇÃO] ---


        # 3. Carga (L)
        
        # Agora o df_tratado tem colunas float, o to_sql vai criar a staging table
        # com os tipos numéricos corretos, sem precisar do dtype_map.
        print(f"Carregando CSV para {NOME_TABELA_STAGING}...")
        df_tratado.to_sql(
            NOME_TABELA_STAGING, 
            engine, 
            if_exists='replace', 
            index=False, 
            schema=DB_SCHEMA
        )
        print(f"CSV carregado para {NOME_TABELA_STAGING}.")
        
        
        competencias_no_df = df_tratado['competencia'].dropna().unique()
        if len(competencias_no_df) == 0:
            print("ERRO CRÍTICO: Nenhuma competência válida encontrada no CSV após o tratamento.")
            return False 
        
        print(f"Competências a serem carregadas na Fato: {len(competencias_no_df)} meses/períodos.")

        # --- (SQL para popular a Fato - IDÊNTICO) ---
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            fato_folha_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER, 
            competencia DATE,
            nome_funcionario_csv VARCHAR(255), 
            centro_de_custo VARCHAR(255), 
            cargo_nome_csv VARCHAR(255),  
            cpf_csv VARCHAR(11),
            
            -- Novos campos de FATO
            situacao_csv VARCHAR(100),
            tipo_calculo_csv VARCHAR(100),
            
            -- Métricas
            salario_contratual NUMERIC(12, 2),
            total_proventos NUMERIC(12, 2),
            total_descontos NUMERIC(12, 2),
            valor_liquido NUMERIC(12, 2),
            base_inss NUMERIC(12, 2),
            base_fgts NUMERIC(12, 2),
            valor_fgts NUMERIC(12, 2),
            base_irrf NUMERIC(12, 2),
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB}(colaborador_sk)
        );

        DELETE FROM "{DB_SCHEMA}".{NOME_TABELA_FINAL}
        WHERE competencia IN :competencias_list;

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            colaborador_sk, 
            competencia,
            nome_funcionario_csv, centro_de_custo, cargo_nome_csv, cpf_csv,
            situacao_csv, tipo_calculo_csv, -- NOVOS
            salario_contratual, total_proventos, total_descontos, valor_liquido,
            base_inss, base_fgts, valor_fgts, base_irrf
        )
        SELECT
            COALESCE(base.colaborador_sk, 0) AS colaborador_sk,
            stg.competencia,
            stg.nome_funcionario AS nome_funcionario_csv, 
            stg.departamento AS centro_de_custo, 
            stg.cargo AS cargo_nome_csv,        
            stg.cpf AS cpf_csv,               

            stg.situacao AS situacao_csv,       -- NOVO
            stg.tipo_calculo AS tipo_calculo_csv, -- NOVO

            stg.salario_contratual, stg.total_proventos, stg.total_descontos, stg.valor_liquido,
            stg.base_inss, stg.base_fgts, stg.valor_fgts, stg.base_irrf
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        LEFT JOIN
            "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB} AS base ON stg.cpf = base.cpf
        ;
        """
        
        with engine.begin() as conn:
            conn.execute(text(sql), {"competencias_list": tuple(competencias_no_df)})
            
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True

    except sqlalchemy_exc.SQLAlchemyError as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (SQLAlchemyError): {e}")
        if hasattr(e, 'orig') and e.orig:
             print(f"  Erro original (psycopg2): {e.orig}")
        return False
    except pd.errors.ParserError as e: 
       print(f"Falha ao ler o CSV {CSV_FILE}: {e}")
       return False
    except Exception as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (Erro genérico): {e}")
        return False


def pipeline_fato_folha_detalhada():
    print("\n--- Iniciando Pipeline: fato_folha_detalhada ---")

    CSV_FILE = 'BASE_FOPAG_DETALHADA_RUBRICAS.csv'
    NOME_TABELA_STAGING = 'staging_folha_detalhada'
    NOME_TABELA_FINAL = 'fato_folha_detalhada'
    NOME_TABELA_BASE_COLAB = 'dim_colaboradores_base' 

    try:
        # 1. Extract
        try:
            df_csv = pd.read_csv(CSV_FILE, sep=';', dtype=str) # Lê tudo como string
        except FileNotFoundError:
             print(f"ERRO: Arquivo '{CSV_FILE}' não encontrado.")
             print("Por favor, execute o Notebook 1 (Automação_FOPAG.ipynb) para gerar os CSVs primeiro.")
             return False
        except Exception as read_err:
            print(f"Erro ao ler CSV {CSV_FILE}: {read_err}.")
            return False

        # 2. Transformação (T)
        df_tratado = tratar_tipos_dataframe_csv(df_csv.copy(), CSV_FILE)
        
        # --- [INÍCIO DA CORREÇÃO 2/3] ---
        # Garante que as colunas de FATO esperadas existam no DataFrame
        # antes de carregar para a staging. Isso corrige o Erro 2.
        colunas_fato_esperadas = ['situacao', 'tipo_calculo']
        for col in colunas_fato_esperadas:
            if col not in df_tratado.columns:
                print(f"Aviso: Coluna '{col}' não encontrada no CSV {CSV_FILE}. Será preenchida com Nulo.")
                df_tratado[col] = None
        # --- [FIM DA CORREÇÃO 2/3] ---
        
        
        # 3. Carga (L)
        
        # Converte para float para o to_sql criar a staging table com tipo numérico
        print(f"Carregando CSV para {NOME_TABELA_STAGING}...")
        df_tratado.to_sql(
            NOME_TABELA_STAGING, 
            engine, 
            if_exists='replace', 
            index=False, 
            schema=DB_SCHEMA
        )
        print(f"CSV carregado para {NOME_TABELA_STAGING}.")
        
        
        competencias_no_df = df_tratado['competencia'].dropna().unique()
        if len(competencias_no_df) == 0:
            print("ERRO CRÍTICO: Nenhuma competência válida encontrada no CSV após o tratamento.")
            return False 
        
        print(f"Competências a serem carregadas na Fato: {len(competencias_no_df)} meses/períodos.")

        # --- (SQL para popular a Fato - IDÊNTICO) ---
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            fato_rubrica_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER, 
            competencia DATE,
            nome_funcionario_csv VARCHAR(255), 
            centro_de_custo VARCHAR(255), 
            cpf_csv VARCHAR(11),
            
            -- Novos campos de FATO
            situacao_csv VARCHAR(100),
            tipo_calculo_csv VARCHAR(100),
            
            -- Detalhes da Rubrica
            codigo_rubrica VARCHAR(100),
            nome_rubrica VARCHAR(255),
            tipo_rubrica VARCHAR(100),
            valor_rubrica NUMERIC(12, 2),
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB}(colaborador_sk)
        );

        DELETE FROM "{DB_SCHEMA}".{NOME_TABELA_FINAL}
        WHERE competencia IN :competencias_list;

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            colaborador_sk, 
            competencia,
            nome_funcionario_csv, centro_de_custo, cpf_csv,
            situacao_csv, tipo_calculo_csv, -- NOVOS
            codigo_rubrica, nome_rubrica, tipo_rubrica, valor_rubrica
        )
        SELECT
            COALESCE(base.colaborador_sk, 0) AS colaborador_sk,
            stg.competencia,
            stg.nome_funcionario AS nome_funcionario_csv, 
            stg.departamento AS centro_de_custo, 
            stg.cpf AS cpf_csv,               
            
            stg.situacao AS situacao_csv,       -- NOVO
            stg.tipo_calculo AS tipo_calculo_csv, -- NOVO
            
            stg.codigo_rubrica, stg.nome_rubrica, stg.tipo_rubrica, stg.valor_rubrica
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        LEFT JOIN
            "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB} AS base ON stg.cpf = base.cpf
        ;
        """

        with engine.begin() as conn:
            conn.execute(text(sql), {"competencias_list": tuple(competencias_no_df)})
            
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True

    except sqlalchemy_exc.SQLAlchemyError as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (SQLAlchemyError): {e}")
        if hasattr(e, 'orig') and e.orig:
             print(f"  Erro original (psycopg2): {e.orig}")
        return False
    except pd.errors.ParserError as e: 
       print(f"Falha ao ler o CSV {CSV_FILE}: {e}")
       return False
    except Exception as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (Erro genérico): {e}")
        return False
    

def processar_status_transferidos():
    """
    Identifica colaboradores que 'sumiram' da carga (API e CSV) sem data de demissão
    e atualiza o status para 'Transferido'.
    """
    print("\n--- Iniciando Pós-Processamento: Identificação de Transferidos ---")
    
    # Nomes das tabelas
    TB_BASE = f'"{DB_SCHEMA}".dim_colaboradores_base'
    TB_RICA = f'"{DB_SCHEMA}".dim_colaboradores'
    TB_STG_API = f'"{DB_SCHEMA}".staging_colaboradores'
    TB_STG_CSV = f'"{DB_SCHEMA}".staging_folha_consolidada'

    # SQL Lógico:
    # 1. Pegar quem está na BASE e NÃO tem data de demissão.
    # 2. Verificar se esse CPF NÃO está na Staging da API (carga de hoje).
    # 3. Verificar se esse CPF NÃO está na Staging do CSV (carga do mês).
    # 4. Se atender a tudo, marca como Transferido.

    sql_update = text(f"""
        UPDATE {TB_BASE}
        SET 
            situacao_csv = 'Transferido'
        WHERE cpf IN (
            -- Seleciona CPFs candidatos a Transferência
            SELECT base.cpf
            FROM {TB_BASE} base
            -- Que NÃO estão na API hoje
            LEFT JOIN {TB_STG_API} api ON base.cpf = api.cpf
            -- Que NÃO estão no CSV hoje
            LEFT JOIN {TB_STG_CSV} csv ON base.cpf = csv.cpf
            WHERE 
                api.cpf IS NULL              -- Sumiu da API
                AND csv.cpf IS NULL          -- Sumiu do CSV
                AND base.data_demissao_csv IS NULL -- Não foi demitido oficialmente
                AND base.situacao_csv != 'Transferido' -- Já não é transferido
                AND base.situacao_csv != 'Desligado'   -- Já não é desligado
        );
    """)
    
    # Atualiza também a Dimensão Rica para refletir a mudança
    sql_sync_rica = text(f"""
        UPDATE {TB_RICA}
        SET 
            ativo = False, -- Transferido não conta como ativo na unidade atual
            data_ultima_atualizacao = current_timestamp
        FROM {TB_BASE} base
        WHERE {TB_RICA}.colaborador_sk = base.colaborador_sk
        AND base.situacao_csv = 'Transferido'
        AND {TB_RICA}.ativo = True; -- Só atualiza se ainda constava como ativo
    """)

    try:
        with engine.begin() as conn:
            # Executa a marcação na Base
            result = conn.execute(sql_update)
            afetados = result.rowcount
            print(f"Detectados e marcados como 'Transferido' na Base: {afetados} colaboradores.")
            
            # Sincroniza a Rica
            if afetados > 0:
                result_rica = conn.execute(sql_sync_rica)
                print(f"Status 'Ativo' atualizado para False na Dimensão Rica: {result_rica.rowcount} registros.")
            
        return True
    except Exception as e:
        print(f"Erro ao processar transferidos: {e}")
        return False


# --- PONTO DE EXECUÇÃO PRINCIPAL --
if __name__ == "__main__":

    # Ordem de execution é crucial

    # 1. Dimensões independentes
    sucesso_colab = pipeline_dim_colaboradores() # Popula a Base com dados da API
    sucesso_calendario = pipeline_dim_calendario() 

    # 2. Fatos (dependentes)
    # Agora, o pipeline da FATO irá *primeiro* popular a Base com dados do CSV
    # e *depois* carregar a Fato.
    
    if sucesso_colab and sucesso_calendario:
        # A Consolidada AGORA atualiza a dim_colaboradores_base
        sucesso_fato_cons = pipeline_fato_folha_consolidada()
        
        # A Detalhada apenas lê da dim_colaboradores_base
        # Ela só executa se a consolidada (que atualiza a base) funcionar
        if sucesso_fato_cons:
            sucesso_fato_det = pipeline_fato_folha_detalhada()
            # --- NOVO PASSO: RODAR APÓS AS CARGAS ---
            processar_status_transferidos()
            # ----------------------------------------
        else:
            sucesso_fato_det = False # Pula a execução
        
        if not sucesso_fato_cons or not sucesso_fato_det:
             print("\n!!! Atenção: Pelo menos um pipeline de FATO falhou. Verifique os logs acima. !!!")
        else:
             print("\n--- Pipeline ETL Concluído com Sucesso! ---")

    else:
        if not sucesso_colab:
             print("\nFalha ao carregar dim_colaboradores (API). Abortando pipelines de Fatos.")
        if not sucesso_calendario:
             print("\nFalha ao carregar dim_calendario. Abortando pipelines de Fatos.")
        sys.exit() # Encerra se as dimensões falharem


    # Fecha a conexão com o banco
    engine.dispose()

# Debug JSON
# 
# Nessa célula entendo os pontos do JSON (Benefícios)
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, exc as sqlalchemy_exc
# --- [INÍCIO DAS IMPORTAÇÕES CORRIGIDAS] ---
import sys
import numpy as np 
import json 
from decimal import Decimal, InvalidOperation
from sqlalchemy.types import String, Date, Numeric # Usado apenas para a API, mas mantido
# --- [FIM DAS IMPORTAÇÕES CORRIGIDAS] ---


# 1. CARREGAR VARIÁVEIS DE AMBIENTE
# -----------------------------------
print("Iniciando ETL...")
load_dotenv()

# Carrega o Token da API
API_TOKEN = os.getenv('SOLIDES_API_TOKEN')

# Carrega os componentes do Banco
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_SCHEMA = os.getenv('DB_SCHEMA')

# Verifica se tudo foi carregado
if not all([API_TOKEN, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, DB_SCHEMA]):
    print("ERRO: Faltando uma ou mais variáveis no arquivo .env")
    print(f"API_TOKEN Carregado: {'Sim' if API_TOKEN else 'NÃO'}")
    print(f"DB_SCHEMA Carregado: {DB_SCHEMA}")
    sys.exit() # Encerra o script se faltar configuração

# 2. CONFIGURAÇÕES GLOBAIS
# -----------------------------------
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BASE_URL = "https://app.solides.com/pt-BR/api/v1"
HEADERS = {
    "Authorization": f"Token token={API_TOKEN}",
    "Accept": "application/json"
}

# 3. CRIA A CONEXÃO E GARANTE O SCHEMA (COM ASPAS)
# ----------------------------------------------------
try:
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        
        # 1. Garante que o schema ("FOPAG") existe PRIMEIRO.
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS \"{DB_SCHEMA}\"'))
        
        # 2. Instala a extensão explicitamente DENTRO do seu schema.
        conn.execute(text(f'CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA \"{DB_SCHEMA}\";'))

    # Recria a engine, definindo o search_path
    engine = create_engine(
        DB_URL,
        connect_args={'options': f'-csearch_path=\"{DB_SCHEMA}\"'}
    )

    print(f"Conexão com PostgreSQL estabelecida e schema '\"{DB_SCHEMA}\"' garantido.")

except Exception as e:
    print(f"Erro ao conectar ao PostgreSQL ou criar schema: {e}")
    sys.exit()


# --- FUNÇÕES HELPER ---

def limpar_salario_api(salario_str):
    """Limpa a string de salário vinda da API (ex: "R$ 8.200,00") para float."""
    if salario_str is None or pd.isna(salario_str):
        return np.nan
    try:
        # Remove 'R$', espaços, e usa '.' como separador de milhar
        salario_limpo = str(salario_str).replace('R$', '').replace(' ', '').replace('.', '')
        # Troca ',' por '.' para ser decimal
        salario_limpo = salario_limpo.replace(',', '.')
        return pd.to_numeric(salario_limpo, errors='coerce')
    except Exception:
        return np.nan

# --- [INÍCIO DA ATUALIZAÇÃO] ---
# A 'dim_colaboradores_base' agora é a dimensão MESTRE
def atualizar_dim_colaboradores_base(engine, df_colaboradores, schema_name):
    """
    Cria a tabela dim_colaboradores_base (se não existir) e
    faz o UPSERT (INSERT ... ON CONFLICT) dos dados de colaboradores.
    AGORA, esta tabela contém os dados mestres vindos do CSV.
    """
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING_TEMP = "stg_colab_temp_upsert" 

    if df_colaboradores is None or df_colaboradores.empty:
        print("Nenhum dado de colaborador fornecido para o UPSERT.")
        return

    print(f"\n--- Iniciando UPSERT para '{NOME_TABELA_BASE}' (Tabela Mestre) ---")

    # SQL para criar a tabela base (AGORA ENRIQUECIDA)
    sql_create_base = text(f"""
        CREATE TABLE IF NOT EXISTS \"{schema_name}\".\"{NOME_TABELA_BASE}\" (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL,
            
            -- Novos campos mestres (do CSV)
            data_admissao_csv DATE,
            data_demissao_csv DATE,
            situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255),
            cargo_csv VARCHAR(255)
        );
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A')
        ON CONFLICT (colaborador_sk) DO NOTHING;
    """)

    # SQL de UPSERT (AGORA ENRIQUECIDO)
    sql_upsert = text(f"""
        INSERT INTO \"{schema_name}\".\"{NOME_TABELA_BASE}\" (
            nome_colaborador, cpf, 
            data_admissao_csv, data_demissao_csv, situacao_csv, 
            departamento_csv, cargo_csv
        )
        SELECT
            DISTINCT ON (src.cpf)
            src.nome_colaborador,
            src.cpf,
            src.data_admissao_csv,
            src.data_demissao_csv,
            src.situacao_csv,
            src.departamento_csv,
            src.cargo_csv
        FROM
            \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\" AS src
        WHERE
            src.cpf IS NOT NULL AND src.cpf != 'N/A'
        ORDER BY
            src.cpf, src.nome_colaborador DESC
        ON CONFLICT (cpf) DO UPDATE SET
            nome_colaborador = EXCLUDED.nome_colaborador,
            data_admissao_csv = EXCLUDED.data_admissao_csv,
            data_demissao_csv = EXCLUDED.data_demissao_csv,
            situacao_csv = EXCLUDED.situacao_csv,
            departamento_csv = EXCLUDED.departamento_csv,
            cargo_csv = EXCLUDED.cargo_csv;
    """)
    
    try:
        with engine.begin() as conn:
            # 1. Cria a tabela base (se não existir) com a NOVA ESTRUTURA
            conn.execute(sql_create_base)

            # 2. Carga dos dados do DataFrame para a tabela temporária de staging
            # O DataFrame já deve vir com os nomes de colunas corretos 
            # (ex: 'data_admissao_csv')
            df_colaboradores.to_sql(
                NOME_TABELA_STAGING_TEMP,
                con=conn,
                schema=schema_name,
                if_exists='replace',
                index=False
            )

            # 3. Executa o UPSERT (agora enriquecido)
            conn.execute(sql_upsert)

            # 4. (Opcional) Limpa a tabela temporária
            conn.execute(text(f"DROP TABLE \"{schema_name}\".\"{NOME_TABELA_STAGING_TEMP}\""))

        print(f"SUCESSO! '{NOME_TABELA_BASE}' (Mestre) foi atualizada com os dados do DataFrame.")
        # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 1] ---
        return True # <-- Retorna Sucesso
        # --- [FIM DA CORREÇÃO 3/3 - Parte 1] ---

    except Exception as e:
        print(f"ERRO ao fazer UPSERT na '{NOME_TABELA_BASE}': {e}")
        # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 2] ---
        return False # <-- Retorna Falha
        # --- [FIM DA CORREÇÃO 3/3 - Parte 2] ---
# --- [FIM DA ATUALIZAÇÃO] ---


# --- FASE 1: PIPELINES DAS DIMENSÕES (API) ---
def pipeline_dim_colaboradores():
    """
    PUXA dados de Colaboradores da API (paginado) e carrega na dim_colaboradores.
    TAMBÉM extrai a lista detalhada de benefícios para a tabela 'fato_beneficios_api'.
    """
    print("\n--- Iniciando Pipeline: dim_colaboradores & fatos_beneficios ---")

    # 1. Extração (E)
    all_colaboradores_lista = []
    page = 1
    page_size = 100
    ENDPOINT_LISTA = "/colaboradores" 
    print("Iniciando extração (Passo 1/2): Buscando lista de IDs de colaboradores...")
    while True:
        params = {'page': page, 'page_size': page_size, 'status': 'todos'} 
        try:
            response = requests.get(f"{BASE_URL}{ENDPOINT_LISTA}", headers=HEADERS, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    print(f"Extração da lista concluída. Total de {len(all_colaboradores_lista)} colaboradores encontrados.")
                    break
                all_colaboradores_lista.extend(data) 
                print(f"Página {page} da lista carregada...")
                page += 1
            else:
                print(f"Erro na API (Página {page}): {response.status_code} {response.text}")
                return False
        except Exception as e:
            print(f"Erro na extração de colaboradores (lista): {e}")
            return False
            
    if not all_colaboradores_lista:
        print("Nenhum colaborador encontrado.")
        return True

    print(f"Passo 1/2 concluído. {len(all_colaboradores_lista)} colaboradores encontrados.")
    all_colaboradores_detalhado = []
    total_colabs = len(all_colaboradores_lista)
    print(f"Iniciando extração (Passo 2/2): Buscando detalhes completos...")
    
    for i, colab_info in enumerate(all_colaboradores_lista):
        colab_id = colab_info.get('id')
        if not colab_id: continue
        
        # Log a cada 50 para não poluir
        if (i+1) % 50 == 0:
            print(f"   Buscando colaborador {i+1} de {total_colabs}...")

        ENDPOINT_DETALHE = f"/colaboradores/{colab_id}"
        try:
            response_detalhe = requests.get(f"{BASE_URL}{ENDPOINT_DETALHE}", headers=HEADERS)
            if response_detalhe.status_code == 200:
                data_detalhe = response_detalhe.json()
                all_colaboradores_detalhado.append(data_detalhe)
            else:
                print(f"    ERRO ID {colab_id}: {response_detalhe.status_code}. Usando dados básicos.")
                all_colaboradores_detalhado.append(colab_info) 
        except Exception as e:
            print(f"    EXCEÇÃO ID {colab_id}: {e}. Usando dados básicos.")
            all_colaboradores_detalhado.append(colab_info) 
    print("Passo 2/2 concluído.")

    # =========================================================================
    # 2. Transformação (T)
    # =========================================================================
    
    # --- 2.1 Processamento dos Benefícios ---
    print("Processando lista de benefícios...")
    lista_beneficios = []

    for colab in all_colaboradores_detalhado:
        colab_id = colab.get('id')
        # A chave 'benefits' contém a lista
        benefits_data = colab.get('benefits', [])
        
        if isinstance(benefits_data, list):
            for ben in benefits_data:
                lista_beneficios.append({
                    'colaborador_id_solides': colab_id,
                    'nome_beneficio': ben.get('benefitName'),
                    'tipo_beneficio': ben.get('typeBenefit'), 
                    'valor_bruto': ben.get('value'),
                    'valor_desconto_bruto': ben.get('valueDiscount'),
                    'periodicidade': ben.get('dates'),
                    'opcao_desconto': ben.get('discountOption'),
                    'aplicado_como': ben.get('benefitAppliedAs')
                })

    df_beneficios = pd.DataFrame(lista_beneficios)
    
    if not df_beneficios.empty:
        df_beneficios['valor_beneficio'] = df_beneficios['valor_bruto'].apply(limpar_salario_api)
        df_beneficios['valor_desconto'] = df_beneficios['valor_desconto_bruto'].apply(limpar_salario_api)
        df_beneficios.drop(columns=['valor_bruto', 'valor_desconto_bruto'], inplace=True)
    else:
        df_beneficios = pd.DataFrame(columns=[
            'colaborador_id_solides', 'nome_beneficio', 'tipo_beneficio', 
            'valor_beneficio', 'valor_desconto', 'periodicidade', 
            'opcao_desconto', 'aplicado_como'
        ])

    print(f"Benefícios extraídos: {len(df_beneficios)} registros.")

    # --- 2.2 Transformação dos Colaboradores ---
    df = pd.json_normalize(all_colaboradores_detalhado)

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
    else:
         df['cpf_temp'] = None

    if 'salary' in df.columns: df['salario_api_temp'] = df['salary'].apply(limpar_salario_api)
    else: df['salario_api_temp'] = np.nan
        
    df = df.rename(columns={
        'id': 'colaborador_id_solides',
        'name': 'nome_completo',
        'cpf_temp': 'cpf', 
        'birthDate': 'data_nascimento',
        'gender': 'genero',
        'dateAdmission': 'data_admissao',
        'dateDismissal': 'data_demissao',
        'active': 'ativo',
        'dept_name_temp': 'departamento_nome_api', 
        'cargo_name_temp': 'cargo_nome_api',           
        'email': 'email',
        'contact.phone': 'telefone_pessoal', 
        'contact.cellPhone': 'celular', 
        'nationality': 'nacionalidade',
        'education_level_temp': 'nivel_educacional', 
        'motherName': 'nome_mae',
        'fatherName': 'nome_pai',
        'address.streetName': 'logradouro', 
        'address.number': 'numero_endereco',
        'address.additionalInformation': 'complemento_endereco', 
        'address.neighborhood': 'bairro',
        'address.city.name': 'cidade', 
        'address.state.initials': 'estado', 
        'address.zipCode': 'cep',
        'registration': 'matricula',
        'maritalStatus': 'estado_civil',
        'salario_api_temp': 'salario_api',
        'workShift': 'turno_trabalho',
        'typeContract': 'tipo_contrato',
        'course': 'curso_formacao',
        'hierarchicalLevel': 'nivel_hierarquico',
        'senior.name': 'nome_lider_imediato',
        'ethnicity': 'etnia',
        'unity.name': 'unidade_nome',
        'salutation': 'saudacao',
        'typeOfSpecialNeed': 'tipo_necessidade_especial',
        'birthplace': 'local_nascimento',
        'disabledPerson': 'pcd',
        'reasonDismissal': 'motivo_demissao_api',
        'dateContract': 'data_contrato',
        'durationContract': 'duracao_contrato',
        'contractExpirationDate': 'data_expiracao_contrato',
        'experiencePeriod': 'periodo_experiencia_dias',
        'formDismissal': 'forma_demissao',
        'decisionDismissal': 'decisao_demissao',
        'terminationAmount': 'valor_rescisao',
        'totalBenefits': 'total_beneficios_api',
        'updated_at': 'data_ultima_atualizacao_api',
        'contact.emergencyPhoneNumber': 'telefone_emergencia',
        'contact.personalEmail': 'email_pessoal',
        'contact.corporateEmail': 'email_corporativo_sec',
        'position.id': 'cargo_id_solides',
        'departament.id': 'departamento_id_solides',
        'documents.rg': 'rg',
        'documents.dispatchDate': 'data_emissao_rg',
        'documents.issuingBody': 'orgao_emissor_rg',
        'documents.voterRegistration': 'titulo_eleitor',
        'documents.electoralZone': 'zona_eleitoral',
        'documents.electoralSection': 'secao_eleitoral',
        'documents.ctpsNum': 'ctps_numero',
        'documents.ctpsSerie': 'ctps_serie',
        'documents.pis': 'pis'
    })
    
    # Tratamento de Tipos (CORRIGIDO dayfirst=True)
    date_cols = ['data_nascimento', 'data_admissao', 'data_demissao', 'data_contrato', 'data_expiracao_contrato', 'data_emissao_rg', 'data_ultima_atualizacao_api']
    for col in date_cols:
        if col in df.columns:
            # dayfirst=True corrige o aviso de parsing para datas formato BR (DD/MM/AAAA)
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date 
            
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
    df_staging = df[colunas_staging].copy()
    print("Transformação de colaboradores concluída.")

    # =========================================================================
    # 3. Carga (L)
    # =========================================================================
    NOME_TABELA_RICA = "dim_colaboradores"
    NOME_TABELA_BASE = "dim_colaboradores_base"
    NOME_TABELA_STAGING = "staging_colaboradores"
    NOME_STAGING_BEN = "staging_beneficios_api"
    NOME_FATO_BEN = "fato_beneficios_api"

    try:
        df_staging.to_sql(NOME_TABELA_STAGING, engine, if_exists='replace', index=False, schema=DB_SCHEMA)
        
        print(f"Carregando {NOME_STAGING_BEN}...")
        df_beneficios.to_sql(NOME_STAGING_BEN, engine, if_exists='replace', index=False, schema=DB_SCHEMA)

        # SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_BASE} (
            colaborador_sk SERIAL PRIMARY KEY,
            nome_colaborador VARCHAR(255) NOT NULL,
            cpf VARCHAR(20) UNIQUE NOT NULL,
            data_admissao_csv DATE,
            data_demissao_csv DATE,
            situacao_csv VARCHAR(100),
            departamento_csv VARCHAR(255),
            cargo_csv VARCHAR(255)
        );
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_BASE} (colaborador_sk, nome_colaborador, cpf)
        VALUES (0, 'Desconhecido', 'N/A')
        ON CONFLICT (colaborador_sk) DO NOTHING;

        ALTER TABLE "{DB_SCHEMA}".{NOME_TABELA_BASE}
            ADD COLUMN IF NOT EXISTS data_admissao_csv DATE,
            ADD COLUMN IF NOT EXISTS data_demissao_csv DATE,
            ADD COLUMN IF NOT EXISTS situacao_csv VARCHAR(100),
            ADD COLUMN IF NOT EXISTS departamento_csv VARCHAR(255),
            ADD COLUMN IF NOT EXISTS cargo_csv VARCHAR(255);

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_BASE} (nome_colaborador, cpf)
        SELECT DISTINCT ON (stg.cpf) stg.nome_completo, stg.cpf
        FROM "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        WHERE stg.cpf IS NOT NULL AND stg.cpf != 'N/A'
        ORDER BY stg.cpf, stg.colaborador_id_solides DESC 
        ON CONFLICT (cpf) DO UPDATE SET nome_colaborador = EXCLUDED.nome_colaborador;

        -- (Dimensão Rica)
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_RICA} (
            colaborador_sk INTEGER PRIMARY KEY, 
            colaborador_id_solides INTEGER UNIQUE NOT NULL, 
            cpf VARCHAR(11), 
            nome_completo VARCHAR(255),
            data_nascimento DATE,
            genero VARCHAR(50),
            data_admissao DATE,
            data_demissao DATE,
            ativo BOOLEAN,
            departamento_nome_api VARCHAR(255),
            cargo_nome_api VARCHAR(255),
            email VARCHAR(255),
            telefone_pessoal VARCHAR(50),
            celular VARCHAR(50),
            nacionalidade VARCHAR(100),
            nivel_educacional VARCHAR(100),
            nome_mae VARCHAR(255),
            nome_pai VARCHAR(255),
            logradouro VARCHAR(255),
            numero_endereco VARCHAR(50),
            complemento_endereco VARCHAR(100),
            bairro VARCHAR(100),
            cidade VARCHAR(100),
            estado VARCHAR(50),
            cep VARCHAR(20),
            matricula VARCHAR(50),
            estado_civil VARCHAR(50),
            salario_api NUMERIC(12, 2),
            turno_trabalho VARCHAR(100),
            tipo_contrato VARCHAR(100),
            curso_formacao VARCHAR(255),
            nivel_hierarquico VARCHAR(100),
            nome_lider_imediato VARCHAR(255),
            etnia VARCHAR(50),
            unidade_nome VARCHAR(255),
            data_ultima_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE}(colaborador_sk)
        );

        -- *** CORREÇÃO: ADICIONANDO AS COLUNAS NOVAS CASO NÃO EXISTAM ***
        ALTER TABLE "{DB_SCHEMA}".{NOME_TABELA_RICA}
            ADD COLUMN IF NOT EXISTS total_benefits_api NUMERIC(12,2),
            ADD COLUMN IF NOT EXISTS total_beneficios_api NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS data_ultima_atualizacao_api DATE,
            ADD COLUMN IF NOT EXISTS cargo_id_solides INTEGER,
            ADD COLUMN IF NOT EXISTS departamento_id_solides INTEGER,
            -- Colunas novas que causaram o erro:
            ADD COLUMN IF NOT EXISTS saudacao VARCHAR(50),
            ADD COLUMN IF NOT EXISTS tipo_necessidade_especial VARCHAR(100),
            ADD COLUMN IF NOT EXISTS local_nascimento VARCHAR(100),
            ADD COLUMN IF NOT EXISTS pcd BOOLEAN,
            ADD COLUMN IF NOT EXISTS telefone_emergencia VARCHAR(50),
            ADD COLUMN IF NOT EXISTS email_pessoal VARCHAR(255),
            ADD COLUMN IF NOT EXISTS email_corporativo_sec VARCHAR(255),
            ADD COLUMN IF NOT EXISTS rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS data_emissao_rg DATE,
            ADD COLUMN IF NOT EXISTS orgao_emissor_rg VARCHAR(50),
            ADD COLUMN IF NOT EXISTS titulo_eleitor VARCHAR(50),
            ADD COLUMN IF NOT EXISTS zona_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS secao_eleitoral VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_numero VARCHAR(50),
            ADD COLUMN IF NOT EXISTS ctps_serie VARCHAR(50),
            ADD COLUMN IF NOT EXISTS pis VARCHAR(50),
            ADD COLUMN IF NOT EXISTS motivo_demissao_api VARCHAR(255),
            ADD COLUMN IF NOT EXISTS data_contrato DATE,
            ADD COLUMN IF NOT EXISTS duracao_contrato VARCHAR(100),
            ADD COLUMN IF NOT EXISTS data_expiracao_contrato DATE,
            ADD COLUMN IF NOT EXISTS periodo_experiencia_dias INTEGER,
            ADD COLUMN IF NOT EXISTS forma_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS decisao_demissao VARCHAR(100),
            ADD COLUMN IF NOT EXISTS valor_rescisao NUMERIC(12, 2);

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_RICA} (colaborador_sk, colaborador_id_solides)
        VALUES (0, -1) ON CONFLICT (colaborador_sk) DO NOTHING;

        -- UPSERT DIMENSÃO RICA
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_RICA} (
            colaborador_sk, colaborador_id_solides, cpf, nome_completo, data_nascimento, genero,
            nacionalidade, nivel_educacional, nome_mae, nome_pai, estado_civil, etnia,
            data_admissao, data_demissao, ativo, departamento_nome_api, cargo_nome_api,
            matricula, salario_api, turno_trabalho, tipo_contrato, curso_formacao,
            nivel_hierarquico, nome_lider_imediato, unidade_nome, email, telefone_pessoal,
            celular, logradouro, numero_endereco, complemento_endereco, bairro, cidade,
            estado, cep, saudacao, tipo_necessidade_especial, local_nascimento, pcd,
            telefone_emergencia, email_pessoal, email_corporativo_sec, rg, data_emissao_rg,
            orgao_emissor_rg, titulo_eleitor, zona_eleitoral, secao_eleitoral, ctps_numero,
            ctps_serie, pis, motivo_demissao_api, data_contrato, duracao_contrato,
            data_expiracao_contrato, periodo_experiencia_dias, forma_demissao, decisao_demissao,
            valor_rescisao, total_beneficios_api, cargo_id_solides, departamento_id_solides,
            data_ultima_atualizacao_api, data_ultima_atualizacao
        )
        SELECT
            base.colaborador_sk, stg.colaborador_id_solides, stg.cpf, stg.nome_completo, stg.data_nascimento, stg.genero,
            stg.nacionalidade, stg.nivel_educacional, stg.nome_mae, stg.nome_pai, stg.estado_civil, stg.etnia,
            stg.data_admissao, stg.data_demissao, stg.ativo, stg.departamento_nome_api, stg.cargo_nome_api,
            stg.matricula, stg.salario_api, stg.turno_trabalho, stg.tipo_contrato, stg.curso_formacao,
            stg.nivel_hierarquico, stg.nome_lider_imediato, stg.unidade_nome, stg.email, stg.telefone_pessoal,
            stg.celular, stg.logradouro, stg.numero_endereco, stg.complemento_endereco, stg.bairro, stg.cidade,
            stg.estado, stg.cep, stg.saudacao, stg.tipo_necessidade_especial, stg.local_nascimento, stg.pcd,
            stg.telefone_emergencia, stg.email_pessoal, stg.email_corporativo_sec, stg.rg, stg.data_emissao_rg,
            stg.orgao_emissor_rg, stg.titulo_eleitor, stg.zona_eleitoral, stg.secao_eleitoral, stg.ctps_numero,
            stg.ctps_serie, stg.pis, stg.motivo_demissao_api, stg.data_contrato, stg.duracao_contrato,
            stg.data_expiracao_contrato, stg.periodo_experiencia_dias, stg.forma_demissao, stg.decisao_demissao,
            stg.valor_rescisao, stg.total_beneficios_api, stg.cargo_id_solides, stg.departamento_id_solides,
            stg.data_ultima_atualizacao_api, current_timestamp
        FROM "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        JOIN "{DB_SCHEMA}".{NOME_TABELA_BASE} AS base ON stg.cpf = base.cpf
        WHERE stg.colaborador_id_solides IS NOT NULL
        ON CONFLICT (colaborador_id_solides) DO UPDATE SET
            cpf = EXCLUDED.cpf, nome_completo = EXCLUDED.nome_completo, data_nascimento = EXCLUDED.data_nascimento,
            genero = EXCLUDED.genero, nacionalidade = EXCLUDED.nacionalidade, nivel_educacional = EXCLUDED.nivel_educacional,
            nome_mae = EXCLUDED.nome_mae, nome_pai = EXCLUDED.nome_pai, estado_civil = EXCLUDED.estado_civil,
            etnia = EXCLUDED.etnia, data_admissao = EXCLUDED.data_admissao, data_demissao = EXCLUDED.data_demissao,
            ativo = EXCLUDED.ativo, departamento_nome_api = EXCLUDED.departamento_nome_api, cargo_nome_api = EXCLUDED.cargo_nome_api,
            matricula = EXCLUDED.matricula, salario_api = EXCLUDED.salario_api, turno_trabalho = EXCLUDED.turno_trabalho,
            tipo_contrato = EXCLUDED.tipo_contrato, curso_formacao = EXCLUDED.curso_formacao, nivel_hierarquico = EXCLUDED.nivel_hierarquico,
            nome_lider_imediato = EXCLUDED.nome_lider_imediato, unidade_nome = EXCLUDED.unidade_nome, email = EXCLUDED.email,
            telefone_pessoal = EXCLUDED.telefone_pessoal, celular = EXCLUDED.celular, logradouro = EXCLUDED.logradouro,
            numero_endereco = EXCLUDED.numero_endereco, complemento_endereco = EXCLUDED.complemento_endereco, bairro = EXCLUDED.bairro,
            cidade = EXCLUDED.cidade, estado = EXCLUDED.estado, cep = EXCLUDED.cep, saudacao = EXCLUDED.saudacao,
            tipo_necessidade_especial = EXCLUDED.tipo_necessidade_especial, local_nascimento = EXCLUDED.local_nascimento,
            pcd = EXCLUDED.pcd, telefone_emergencia = EXCLUDED.telefone_emergencia, email_pessoal = EXCLUDED.email_pessoal,
            email_corporativo_sec = EXCLUDED.email_corporativo_sec, rg = EXCLUDED.rg, data_emissao_rg = EXCLUDED.data_emissao_rg,
            orgao_emissor_rg = EXCLUDED.orgao_emissor_rg, titulo_eleitor = EXCLUDED.titulo_eleitor, zona_eleitoral = EXCLUDED.zona_eleitoral,
            secao_eleitoral = EXCLUDED.secao_eleitoral, ctps_numero = EXCLUDED.ctps_numero, ctps_serie = EXCLUDED.ctps_serie,
            pis = EXCLUDED.pis, motivo_demissao_api = EXCLUDED.motivo_demissao_api, data_contrato = EXCLUDED.data_contrato,
            duracao_contrato = EXCLUDED.duracao_contrato, data_expiracao_contrato = EXCLUDED.data_expiracao_contrato,
            periodo_experiencia_dias = EXCLUDED.periodo_experiencia_dias, forma_demissao = EXCLUDED.forma_demissao,
            decisao_demissao = EXCLUDED.decisao_demissao, valor_rescisao = EXCLUDED.valor_rescisao,
            total_beneficios_api = EXCLUDED.total_beneficios_api, cargo_id_solides = EXCLUDED.cargo_id_solides,
            departamento_id_solides = EXCLUDED.departamento_id_solides, data_ultima_atualizacao_api = EXCLUDED.data_ultima_atualizacao_api,
            data_ultima_atualizacao = current_timestamp;

        -- 3.4 FATO BENEFICIOS
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_FATO_BEN} (
            beneficio_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER,
            tipo_beneficio VARCHAR(100),
            nome_beneficio VARCHAR(255),
            valor_beneficio NUMERIC(12,2),
            valor_desconto NUMERIC(12,2),
            periodicidade VARCHAR(50),
            opcao_desconto VARCHAR(50),
            aplicado_como VARCHAR(50),
            data_atualizacao TIMESTAMP DEFAULT current_timestamp,
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE}(colaborador_sk)
        );
        
        TRUNCATE TABLE "{DB_SCHEMA}".{NOME_FATO_BEN};
        
        INSERT INTO "{DB_SCHEMA}".{NOME_FATO_BEN} (
            colaborador_sk, tipo_beneficio, nome_beneficio, 
            valor_beneficio, valor_desconto, periodicidade, 
            opcao_desconto, aplicado_como
        )
        SELECT 
            base.colaborador_sk,
            stg.tipo_beneficio,
            stg.nome_beneficio,
            stg.valor_beneficio,
            stg.valor_desconto,
            stg.periodicidade,
            stg.opcao_desconto,
            stg.aplicado_como
        FROM "{DB_SCHEMA}".{NOME_STAGING_BEN} stg
        JOIN "{DB_SCHEMA}".{NOME_TABELA_STAGING} stg_colab ON stg.colaborador_id_solides = stg_colab.colaborador_id_solides
        JOIN "{DB_SCHEMA}".{NOME_TABELA_BASE} base ON stg_colab.cpf = base.cpf;
        """
        
        with engine.begin() as conn:
            conn.execute(text(sql))

        print(f"Carga na {NOME_TABELA_BASE}, {NOME_TABELA_RICA} e {NOME_FATO_BEN} concluída com sucesso!")
        return True

    except Exception as e:
        print(f"Erro na carga de {NOME_TABELA_RICA} ou Beneficios: {e}")
        return False

# --- NOVA DIMENSÃO: CALENDÁRIO ---
def pipeline_dim_calendario():
    """Gera ou atualiza a dimensão de calendário (dim_calendario)."""
    print("\n--- Iniciando Pipeline: dim_calendario ---")
    
    NOME_TABELA_FINAL = "dim_calendario"
    
    # (Lógica idêntica, sem alterações)
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
        data DATE PRIMARY KEY,
        ano INTEGER,
        mes INTEGER,
        dia INTEGER,
        trimestre INTEGER,
        semestre INTEGER,
        dia_da_semana INTEGER, 
        nome_dia_da_semana VARCHAR(20),
        nome_mes VARCHAR(20),
        nome_mes_abrev CHAR(3),
        ano_mes VARCHAR(7), 
        dia_do_ano INTEGER,
        semana_do_ano INTEGER
    );
    DO $$
    DECLARE
        data_inicio DATE := '2023-01-01'; 
        data_fim DATE := '2030-12-31';
    BEGIN
        BEGIN
            SET LOCAL lc_time = 'pt_BR.UTF-8';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                SET LOCAL lc_time = 'pt_BR';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Não foi possível definir o locale pt_BR. Nomes de mês/dia podem ficar em inglês.';
            END;
        END;
        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            data,
            ano, mes, dia, trimestre, semestre,
            dia_da_semana, nome_dia_da_semana, nome_mes, nome_mes_abrev,
            ano_mes, dia_do_ano, semana_do_ano
        )
        SELECT
            d AS data,
            EXTRACT(YEAR FROM d) AS ano,
            EXTRACT(MONTH FROM d) AS mes,
            EXTRACT(DAY FROM d) AS dia,
            EXTRACT(QUARTER FROM d) AS trimestre,
            CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END AS semestre,
            EXTRACT(DOW FROM d) AS dia_da_semana, 
            to_char(d, 'TMDay') AS nome_dia_da_semana,
            to_char(d, 'TMMonth') AS nome_mes,
            to_char(d, 'TMMon') AS nome_mes_abrev,
            to_char(d, 'YYYY-MM') AS ano_mes,
            EXTRACT(DOY FROM d) AS dia_do_ano,
            EXTRACT(WEEK FROM d) AS semana_do_ano
        FROM generate_series(data_inicio, data_fim, '1 day'::interval) d
        ON CONFLICT (data) DO NOTHING; 
    END $$;
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True
    except Exception as e:
        print(f"Erro na carga de {NOME_TABELA_FINAL}: {e}")
        print(f"Detalhe do erro: {e}")
        return False


# --- FASE 2: PIPELINES DAS FATOS (CSV) ---

# --- (FUNÇÕES HELPER DE CSV - IDÊNTICAS) ---

def clean_text(series):
    """Limpa uma série de texto (object) de forma segura."""
    if series.dtype == 'object':
        series = series.str.strip()
        series = series.str.replace(u'\xa0', '', regex=False)
        series = series.replace(['N/A', '', 'nan', 'None', 'NULL'], None) # Adicionado 'NULL'
    return series

def para_float(valor_str):
    """Converte uma string (já limpa) para float."""
    if valor_str is None or pd.isna(valor_str):
        return np.nan # Use numpy's NaN para floats
    try:
        # CSV do Notebook 1 salva com PONTO decimal (ex: "1234.56")
        return float(valor_str) 
    except (ValueError, TypeError):
        return np.nan

def tratar_tipos_dataframe_csv(df, nome_arquivo):
    """
    Função de tratamento de tipos para os CSVs da FOPAG.
    *** VERSÃO CORRIGIDA PARA DATA E FLOAT ***
    """
    print(f"Iniciando tratamento de tipos para {nome_arquivo}...")

    # --- [CORREÇÃO DATAS] ---
    colunas_data = ['competencia', 'data_admissao', 'data_demissao']
    for col in colunas_data:
        if col in df.columns:
            print(f"Tratando tipo de data: {col}")
            df[col] = clean_text(df[col])
            # O CSV já está em formato ISO (YYYY-MM-DD), o pandas lê automaticamente
            df[col] = pd.to_datetime(df[col], errors='coerce') 
            df[col] = df[col].dt.date # Converte para objeto date (YYYY-MM-DD)

    # --- [CORREÇÃO NUMÉRICOS PARA FLOAT] ---
    colunas_monetarias = [
        'salario_contratual', 'total_proventos', 'total_descontos',
        'valor_liquido', 'base_inss', 'base_fgts', 'valor_fgts',
        'base_irrf', 'valor_rubrica'
    ]
    for col in colunas_monetarias:
        if col in df.columns:
            print(f"Tratando tipo: {col} (String -> Float)")
            df[col] = clean_text(df[col]) 
            # Usa a nova função para_float
            df[col] = df[col].apply(para_float)
            # Garante que a coluna inteira seja do tipo float no Pandas
            df[col] = pd.to_numeric(df[col], errors='coerce') 

    # --- [CPF E TEXTO] ---
    if 'cpf' in df.columns:
        print("Tratando tipo: cpf (String -> String Limpa)")
        df['cpf'] = clean_text(df['cpf'])
        df['cpf'] = df['cpf'].str.replace(r'[^\d]', '', regex=True)

    # Adiciona os novos campos de FATO à limpeza de texto
    colunas_texto = [
        'departamento', 'vinculo', 'nome_funcionario',
        'motivo_demissao', 'cargo', 'codigo_rubrica',
        'nome_rubrica', 'tipo_rubrica',
        
        # NOVOS CAMPOS PARA A FATO
        'situacao', 
        'tipo_calculo' 
    ]
    
    for col in colunas_texto:
        if col in df.columns:
            df[col] = clean_text(df[col])

    print("Tratamento de tipos finalizado.")
    return df
# --- [FIM DAS FUNÇÕES HELPER DE CSV CORRIGIDAS] ---


def pipeline_fato_folha_consolidada():
    print("\n--- Iniciando Pipeline: fato_folha_consolidada ---")

    CSV_FILE = 'BASE_FOPAG_CONSOLIDADA_TOTAIS.csv'
    NOME_TABELA_STAGING = 'staging_folha_consolidada'
    NOME_TABELA_FINAL = 'fato_folha_consolidada'
    NOME_TABELA_BASE_COLAB = 'dim_colaboradores_base' 

    try:
        # 1. Extract
        try:
            df_csv = pd.read_csv(CSV_FILE, sep=';', dtype=str) # Lê tudo como string
        except FileNotFoundError:
             print(f"ERRO: Arquivo '{CSV_FILE}' não encontrado.")
             print("Por favor, execute o Notebook 1 (Automação_FOPAG.ipynb) para gerar os CSVs primeiro.")
             return False
        except Exception as read_err:
            print(f"Erro ao ler CSV {CSV_FILE}: {read_err}.")
            return False

        # 2. Transformação (T)
        df_tratado = tratar_tipos_dataframe_csv(df_csv.copy(), CSV_FILE)

        # --- [INÍCIO DA ATUALIZAÇÃO] ---
        # Popula a dim_colaboradores_base com os dados MESTRE do CSV
        print(f"Extraindo dados mestre do '{CSV_FILE}' para popular a dim_colaboradores_base...")
        if 'cpf' in df_tratado.columns:
            # Ordena para pegar os dados mais recentes de cada CPF (baseado na competência)
            df_recentes = df_tratado.sort_values(by='competencia', ascending=False).drop_duplicates(subset=['cpf'])
            
            # Seleciona as colunas mestre
            colunas_mestre = [
                'cpf', 
                'nome_funcionario', 
                'data_admissao', 
                'data_demissao', 
                'situacao', 
                'departamento', # 'departamento' do CSV
                'cargo'         # 'cargo' do CSV
            ]
            
            # Garante que todas as colunas existem no DF
            colunas_presentes = [col for col in colunas_mestre if col in df_recentes.columns]
            df_colabs_unicos = df_recentes[colunas_presentes].copy()
            
            # Renomeia para o padrão da função helper
            df_colabs_unicos = df_colabs_unicos.rename(columns={
                'nome_funcionario': 'nome_colaborador',
                'data_admissao': 'data_admissao_csv',
                'data_demissao': 'data_demissao_csv',
                'situacao': 'situacao_csv',
                'departamento': 'departamento_csv',
                'cargo': 'cargo_csv'
            })
            
            # Garante que a função helper receba colunas mesmo se não existirem no CSV
            colunas_helper_esperadas = [
                'nome_colaborador', 'cpf', 'data_admissao_csv', 'data_demissao_csv',
                'situacao_csv', 'departamento_csv', 'cargo_csv'
            ]
            for col in colunas_helper_esperadas:
                if col not in df_colabs_unicos.columns:
                    df_colabs_unicos[col] = None

            # --- [INÍCIO DA CORREÇÃO 3/3 - Parte 3] ---
            # Captura o retorno da função helper
            sucesso_upsert = atualizar_dim_colaboradores_base(engine, df_colabs_unicos, DB_SCHEMA)
            
            if not sucesso_upsert:
                print("Falha ao atualizar a dim_colaboradores_base. Abortando pipeline da Fato Consolidada.")
                return False # Para a execução
            # --- [FIM DA CORREÇÃO 3/3 - Parte 3] ---

        else:
            print("AVISO: Coluna 'cpf' não encontrada no CSV. Não foi possível popular a dim_colaboradores_base.")
        # --- [FIM DA ATUALIZAÇÃO] ---


        # 3. Carga (L)
        
        # Agora o df_tratado tem colunas float, o to_sql vai criar a staging table
        # com os tipos numéricos corretos, sem precisar do dtype_map.
        print(f"Carregando CSV para {NOME_TABELA_STAGING}...")
        df_tratado.to_sql(
            NOME_TABELA_STAGING, 
            engine, 
            if_exists='replace', 
            index=False, 
            schema=DB_SCHEMA
        )
        print(f"CSV carregado para {NOME_TABELA_STAGING}.")
        
        
        competencias_no_df = df_tratado['competencia'].dropna().unique()
        if len(competencias_no_df) == 0:
            print("ERRO CRÍTICO: Nenhuma competência válida encontrada no CSV após o tratamento.")
            return False 
        
        print(f"Competências a serem carregadas na Fato: {len(competencias_no_df)} meses/períodos.")

        # --- (SQL para popular a Fato - IDÊNTICO) ---
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            fato_folha_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER, 
            competencia DATE,
            nome_funcionario_csv VARCHAR(255), 
            centro_de_custo VARCHAR(255), 
            cargo_nome_csv VARCHAR(255),  
            cpf_csv VARCHAR(11),
            
            -- Novos campos de FATO
            situacao_csv VARCHAR(100),
            tipo_calculo_csv VARCHAR(100),
            
            -- Métricas
            salario_contratual NUMERIC(12, 2),
            total_proventos NUMERIC(12, 2),
            total_descontos NUMERIC(12, 2),
            valor_liquido NUMERIC(12, 2),
            base_inss NUMERIC(12, 2),
            base_fgts NUMERIC(12, 2),
            valor_fgts NUMERIC(12, 2),
            base_irrf NUMERIC(12, 2),
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB}(colaborador_sk)
        );

        DELETE FROM "{DB_SCHEMA}".{NOME_TABELA_FINAL}
        WHERE competencia IN :competencias_list;

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            colaborador_sk, 
            competencia,
            nome_funcionario_csv, centro_de_custo, cargo_nome_csv, cpf_csv,
            situacao_csv, tipo_calculo_csv, -- NOVOS
            salario_contratual, total_proventos, total_descontos, valor_liquido,
            base_inss, base_fgts, valor_fgts, base_irrf
        )
        SELECT
            COALESCE(base.colaborador_sk, 0) AS colaborador_sk,
            stg.competencia,
            stg.nome_funcionario AS nome_funcionario_csv, 
            stg.departamento AS centro_de_custo, 
            stg.cargo AS cargo_nome_csv,        
            stg.cpf AS cpf_csv,               

            stg.situacao AS situacao_csv,       -- NOVO
            stg.tipo_calculo AS tipo_calculo_csv, -- NOVO

            stg.salario_contratual, stg.total_proventos, stg.total_descontos, stg.valor_liquido,
            stg.base_inss, stg.base_fgts, stg.valor_fgts, stg.base_irrf
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        LEFT JOIN
            "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB} AS base ON stg.cpf = base.cpf
        ;
        """
        
        with engine.begin() as conn:
            conn.execute(text(sql), {"competencias_list": tuple(competencias_no_df)})
            
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True

    except sqlalchemy_exc.SQLAlchemyError as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (SQLAlchemyError): {e}")
        if hasattr(e, 'orig') and e.orig:
             print(f"  Erro original (psycopg2): {e.orig}")
        return False
    except pd.errors.ParserError as e: 
       print(f"Falha ao ler o CSV {CSV_FILE}: {e}")
       return False
    except Exception as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (Erro genérico): {e}")
        return False


def pipeline_fato_folha_detalhada():
    print("\n--- Iniciando Pipeline: fato_folha_detalhada ---")

    CSV_FILE = 'BASE_FOPAG_DETALHADA_RUBRICAS.csv'
    NOME_TABELA_STAGING = 'staging_folha_detalhada'
    NOME_TABELA_FINAL = 'fato_folha_detalhada'
    NOME_TABELA_BASE_COLAB = 'dim_colaboradores_base' 

    try:
        # 1. Extract
        try:
            df_csv = pd.read_csv(CSV_FILE, sep=';', dtype=str) # Lê tudo como string
        except FileNotFoundError:
             print(f"ERRO: Arquivo '{CSV_FILE}' não encontrado.")
             print("Por favor, execute o Notebook 1 (Automação_FOPAG.ipynb) para gerar os CSVs primeiro.")
             return False
        except Exception as read_err:
            print(f"Erro ao ler CSV {CSV_FILE}: {read_err}.")
            return False

        # 2. Transformação (T)
        df_tratado = tratar_tipos_dataframe_csv(df_csv.copy(), CSV_FILE)
        
        # --- [INÍCIO DA CORREÇÃO 2/3] ---
        # Garante que as colunas de FATO esperadas existam no DataFrame
        # antes de carregar para a staging. Isso corrige o Erro 2.
        colunas_fato_esperadas = ['situacao', 'tipo_calculo']
        for col in colunas_fato_esperadas:
            if col not in df_tratado.columns:
                print(f"Aviso: Coluna '{col}' não encontrada no CSV {CSV_FILE}. Será preenchida com Nulo.")
                df_tratado[col] = None
        # --- [FIM DA CORREÇÃO 2/3] ---
        
        
        # 3. Carga (L)
        
        # Converte para float para o to_sql criar a staging table com tipo numérico
        print(f"Carregando CSV para {NOME_TABELA_STAGING}...")
        df_tratado.to_sql(
            NOME_TABELA_STAGING, 
            engine, 
            if_exists='replace', 
            index=False, 
            schema=DB_SCHEMA
        )
        print(f"CSV carregado para {NOME_TABELA_STAGING}.")
        
        
        competencias_no_df = df_tratado['competencia'].dropna().unique()
        if len(competencias_no_df) == 0:
            print("ERRO CRÍTICO: Nenhuma competência válida encontrada no CSV após o tratamento.")
            return False 
        
        print(f"Competências a serem carregadas na Fato: {len(competencias_no_df)} meses/períodos.")

        # --- (SQL para popular a Fato - IDÊNTICO) ---
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            fato_rubrica_id SERIAL PRIMARY KEY,
            colaborador_sk INTEGER, 
            competencia DATE,
            nome_funcionario_csv VARCHAR(255), 
            centro_de_custo VARCHAR(255), 
            cpf_csv VARCHAR(11),
            
            -- Novos campos de FATO
            situacao_csv VARCHAR(100),
            tipo_calculo_csv VARCHAR(100),
            
            -- Detalhes da Rubrica
            codigo_rubrica VARCHAR(100),
            nome_rubrica VARCHAR(255),
            tipo_rubrica VARCHAR(100),
            valor_rubrica NUMERIC(12, 2),
            FOREIGN KEY (colaborador_sk) REFERENCES "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB}(colaborador_sk)
        );

        DELETE FROM "{DB_SCHEMA}".{NOME_TABELA_FINAL}
        WHERE competencia IN :competencias_list;

        INSERT INTO "{DB_SCHEMA}".{NOME_TABELA_FINAL} (
            colaborador_sk, 
            competencia,
            nome_funcionario_csv, centro_de_custo, cpf_csv,
            situacao_csv, tipo_calculo_csv, -- NOVOS
            codigo_rubrica, nome_rubrica, tipo_rubrica, valor_rubrica
        )
        SELECT
            COALESCE(base.colaborador_sk, 0) AS colaborador_sk,
            stg.competencia,
            stg.nome_funcionario AS nome_funcionario_csv, 
            stg.departamento AS centro_de_custo, 
            stg.cpf AS cpf_csv,               
            
            stg.situacao AS situacao_csv,       -- NOVO
            stg.tipo_calculo AS tipo_calculo_csv, -- NOVO
            
            stg.codigo_rubrica, stg.nome_rubrica, stg.tipo_rubrica, stg.valor_rubrica
        FROM
            "{DB_SCHEMA}".{NOME_TABELA_STAGING} AS stg
        LEFT JOIN
            "{DB_SCHEMA}".{NOME_TABELA_BASE_COLAB} AS base ON stg.cpf = base.cpf
        ;
        """

        with engine.begin() as conn:
            conn.execute(text(sql), {"competencias_list": tuple(competencias_no_df)})
            
        print(f"Carga na {NOME_TABELA_FINAL} concluída com sucesso!")
        return True

    except sqlalchemy_exc.SQLAlchemyError as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (SQLAlchemyError): {e}")
        if hasattr(e, 'orig') and e.orig:
             print(f"  Erro original (psycopg2): {e.orig}")
        return False
    except pd.errors.ParserError as e: 
       print(f"Falha ao ler o CSV {CSV_FILE}: {e}")
       return False
    except Exception as e: 
        print(f"Falha no pipeline {NOME_TABELA_FINAL} (Erro genérico): {e}")
        return False
    

def processar_status_transferidos():
    """
    Identifica colaboradores que 'sumiram' da carga (API e CSV) sem data de demissão
    e atualiza o status para 'Transferido'.
    """
    print("\n--- Iniciando Pós-Processamento: Identificação de Transferidos ---")
    
    # Nomes das tabelas
    TB_BASE = f'"{DB_SCHEMA}".dim_colaboradores_base'
    TB_RICA = f'"{DB_SCHEMA}".dim_colaboradores'
    TB_STG_API = f'"{DB_SCHEMA}".staging_colaboradores'
    TB_STG_CSV = f'"{DB_SCHEMA}".staging_folha_consolidada'

    # SQL Lógico:
    # 1. Pegar quem está na BASE e NÃO tem data de demissão.
    # 2. Verificar se esse CPF NÃO está na Staging da API (carga de hoje).
    # 3. Verificar se esse CPF NÃO está na Staging do CSV (carga do mês).
    # 4. Se atender a tudo, marca como Transferido.

    sql_update = text(f"""
        UPDATE {TB_BASE}
        SET 
            situacao_csv = 'Transferido'
        WHERE cpf IN (
            -- Seleciona CPFs candidatos a Transferência
            SELECT base.cpf
            FROM {TB_BASE} base
            -- Que NÃO estão na API hoje
            LEFT JOIN {TB_STG_API} api ON base.cpf = api.cpf
            -- Que NÃO estão no CSV hoje
            LEFT JOIN {TB_STG_CSV} csv ON base.cpf = csv.cpf
            WHERE 
                api.cpf IS NULL              -- Sumiu da API
                AND csv.cpf IS NULL          -- Sumiu do CSV
                AND base.data_demissao_csv IS NULL -- Não foi demitido oficialmente
                AND base.situacao_csv != 'Transferido' -- Já não é transferido
                AND base.situacao_csv != 'Desligado'   -- Já não é desligado
        );
    """)
    
    # Atualiza também a Dimensão Rica para refletir a mudança
    sql_sync_rica = text(f"""
        UPDATE {TB_RICA}
        SET 
            ativo = False, -- Transferido não conta como ativo na unidade atual
            data_ultima_atualizacao = current_timestamp
        FROM {TB_BASE} base
        WHERE {TB_RICA}.colaborador_sk = base.colaborador_sk
        AND base.situacao_csv = 'Transferido'
        AND {TB_RICA}.ativo = True; -- Só atualiza se ainda constava como ativo
    """)

    try:
        with engine.begin() as conn:
            # Executa a marcação na Base
            result = conn.execute(sql_update)
            afetados = result.rowcount
            print(f"Detectados e marcados como 'Transferido' na Base: {afetados} colaboradores.")
            
            # Sincroniza a Rica
            if afetados > 0:
                result_rica = conn.execute(sql_sync_rica)
                print(f"Status 'Ativo' atualizado para False na Dimensão Rica: {result_rica.rowcount} registros.")
            
        return True
    except Exception as e:
        print(f"Erro ao processar transferidos: {e}")
        return False


# --- PONTO DE EXECUÇÃO PRINCIPAL --
if __name__ == "__main__":

    # Ordem de execution é crucial

    # 1. Dimensões independentes
    sucesso_colab = pipeline_dim_colaboradores() # Popula a Base com dados da API
    sucesso_calendario = pipeline_dim_calendario() 

    # 2. Fatos (dependentes)
    # Agora, o pipeline da FATO irá *primeiro* popular a Base com dados do CSV
    # e *depois* carregar a Fato.
    
    if sucesso_colab and sucesso_calendario:
        # A Consolidada AGORA atualiza a dim_colaboradores_base
        sucesso_fato_cons = pipeline_fato_folha_consolidada()
        
        # A Detalhada apenas lê da dim_colaboradores_base
        # Ela só executa se a consolidada (que atualiza a base) funcionar
        if sucesso_fato_cons:
            sucesso_fato_det = pipeline_fato_folha_detalhada()
            # --- NOVO PASSO: RODAR APÓS AS CARGAS ---
            processar_status_transferidos()
            # ----------------------------------------
        else:
            sucesso_fato_det = False # Pula a execução
        
        if not sucesso_fato_cons or not sucesso_fato_det:
             print("\n!!! Atenção: Pelo menos um pipeline de FATO falhou. Verifique os logs acima. !!!")
        else:
             print("\n--- Pipeline ETL Concluído com Sucesso! ---")

    else:
        if not sucesso_colab:
             print("\nFalha ao carregar dim_colaboradores (API). Abortando pipelines de Fatos.")
        if not sucesso_calendario:
             print("\nFalha ao carregar dim_calendario. Abortando pipelines de Fatos.")
        sys.exit() # Encerra se as dimensões falharem


    # Fecha a conexão com o banco
    engine.dispose()
# DEPURAÇÂO 



