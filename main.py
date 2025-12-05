# main.py
import os
import sys
from dotenv import load_dotenv
from src.database import get_db_engine
from src.extract import processar_pdfs, extrair_api_solides
from src.transform import (
    transformar_dados_pdf, 
    transformar_dados_api, 
    transformar_beneficios_api
)
from src.load import (
    garantir_schema_banco, 
    carregar_dim_calendario,
    carregar_dados_api,
    carregar_fatos_folha,
    processar_status_transferidos
)

def run_pipeline():
    print("\n=======================================================")
    print("   INICIANDO PIPELINE DE DADOS - ARQ PEOPLE INTEL")
    print("=======================================================\n")

    load_dotenv()
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PATH_INPUT = os.path.join(BASE_DIR, 'input')
    PATH_OUTPUT = os.path.join(BASE_DIR, 'output')

    if not os.path.exists(PATH_OUTPUT):
        os.makedirs(PATH_OUTPUT)

    try:
        engine, schema = get_db_engine()
        garantir_schema_banco(engine, schema)
        print(f"[OK] Conexão com banco estabelecida. Schema: {schema}")
    except Exception as e:
        print(f"[ERRO FATAL] Não foi possível conectar ao banco: {e}")
        sys.exit(1)

    # 1. DIMENSÃO CALENDÁRIO (Independente)
    print("\n--- [ETAPA 1] Dimensão Calendário ---")
    carregar_dim_calendario(engine, schema)

    # 2. PIPELINE FOLHA DE PAGAMENTO (PDFs) - PRIMEIRO A RODAR
    if os.path.exists(PATH_INPUT):
        print(f"\n--- [ETAPA 2] Pipeline Folha de Pagamento (PDFs) ---")
        
        df_raw_consol, df_raw_detalhe = processar_pdfs(PATH_INPUT)
        
        if not df_raw_consol.empty:
            print("Transformando dados da Folha...")
            df_final_consol, df_final_detalhe = transformar_dados_pdf(df_raw_consol, df_raw_detalhe)
            
            # Exportação CSV
            path_csv_consol = os.path.join(PATH_OUTPUT, 'FOPAG_Consolidada_Tratada.csv')
            path_csv_detalhe = os.path.join(PATH_OUTPUT, 'FOPAG_Detalhada_Tratada.csv')
            df_final_consol.to_csv(path_csv_consol, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            if not df_final_detalhe.empty:
                df_final_detalhe.to_csv(path_csv_detalhe, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            print(f"[OK] CSVs gerados em output.")

            # Load Banco
            print("Carregando Fatos de Folha no Banco...")
            carregar_fatos_folha(df_final_consol, df_final_detalhe, engine, schema)
        else:
            print("[AVISO] Nenhum dado extraído dos PDFs.")
    else:
        print(f"\n[ERRO] Pasta de input não encontrada: {PATH_INPUT}")

    # 3. PIPELINE API SOLIDES (DIMENSÕES RICAS) - SEGUNDO A RODAR
    token_api = os.getenv("SOLIDES_API_TOKEN")
    if token_api:
        print("\n--- [ETAPA 3] Pipeline API Solides ---")
        dados_brutos_api = extrair_api_solides(token_api)
        
        print("Transformando dados da API...")
        df_colaboradores = transformar_dados_api(dados_brutos_api)
        df_beneficios = transformar_beneficios_api(dados_brutos_api)
        
        print("Carregando dados da API no Banco...")
        carregar_dados_api(df_colaboradores, df_beneficios, engine, schema)
    else:
        print("\n[AVISO] Token da API não encontrado. Pulando etapa API.")

    # 4. PÓS PROCESSAMENTO
    print("\n--- [ETAPA 4] Pós-Processamento ---")
    processar_status_transferidos(engine, schema)

    print("\n=======================================================")
    print("   PIPELINE FINALIZADO")
    print("=======================================================\n")

if __name__ == "__main__":
    run_pipeline()