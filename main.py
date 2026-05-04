# main.py
import os
import sys
import time
from dotenv import load_dotenv

# Imports dos módulos
from src.database import get_db_engine
from src.extract import processar_pdfs, extrair_api_solides, ingestar_ftp_paralelo, extrair_profiler_solides # <--- NOVO IMPORT
from src.transform import (
    transformar_dados_pdf,
    transformar_dados_api,
    transformar_beneficios_api,
    transformar_dependentes_api,
    transformar_profiler_api
)
from src.load import (
    garantir_schema_banco,
    carregar_dim_calendario,
    carregar_dados_api,
    carregar_fatos_folha,
    processar_status_transferidos,
    limpar_tabelas_staging # <---- Adicionado
)
from src.utils import limpar_diretorio_local  # <--- NOVO IMPORT
from src.logger import setup_logger


# Inicializa o logger principal
logger = setup_logger("Main_Pipeline")


def run_pipeline():
    start_time = time.time()

    logger.info("=======================================================")
    logger.info("   INICIANDO PIPELINE DE DADOS - ARQ PEOPLE INTELIGENCE")
    logger.info("=======================================================")

    load_dotenv()
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PATH_INPUT = os.path.join(BASE_DIR, 'input')
    PATH_OUTPUT = os.path.join(BASE_DIR, 'output')

    if not os.path.exists(PATH_OUTPUT):
        os.makedirs(PATH_OUTPUT)

    # =========================================================================
    # 0. INGESTÃO FTP (NOVA ETAPA ADICIONADA)
    # =========================================================================
    ftp_host = os.getenv("FTP_HOST")
    ftp_user = os.getenv("FTP_USER")
    ftp_pass = os.getenv("FTP_PASS")
    ftp_dir = os.getenv("FTP_DIR")

    if ftp_host and ftp_user:
        # Baixa os arquivos e remove do servidor
        ingestar_ftp_paralelo(ftp_host, ftp_user, ftp_pass, ftp_dir, PATH_INPUT)
    else:
        logger.warning("[AVISO] Credenciais FTP não configuradas. Usando arquivos locais existentes.")

    # =========================================================================
    # 1. CONEXÃO COM BANCO (SEU CÓDIGO ORIGINAL)
    # =========================================================================
    try:
        engine, schema = get_db_engine()
        garantir_schema_banco(engine, schema)
        logger.info(f"[OK] Conexão com banco estabelecida. Schema: {schema}")
    except Exception as e:
        logger.critical(f"[ERRO FATAL] Não foi possível conectar ao banco: {e}", exc_info=True)
        sys.exit(1)

    # =========================================================================
    # 2. DIMENSÃO CALENDÁRIO (SEU CÓDIGO ORIGINAL)
    # =========================================================================
    logger.info("--- [ETAPA 1] Dimensão Calendário ---")
    carregar_dim_calendario(engine, schema)

    # =========================================================================
    # 3. PIPELINE FOLHA DE PAGAMENTO (PDFs) (SEU CÓDIGO ORIGINAL)
    # =========================================================================
    if os.path.exists(PATH_INPUT):
        logger.info("--- [ETAPA 2] Pipeline Folha de Pagamento (PDFs) ---")

        # O processar_pdfs já tem logs internos
        df_raw_consol, df_raw_detalhe = processar_pdfs(PATH_INPUT)

        if not df_raw_consol.empty:
            logger.info("Transformando dados da Folha...")
            df_final_consol, df_final_detalhe = transformar_dados_pdf(df_raw_consol, df_raw_detalhe)

            # Exportação CSV para Auditoria (MANTIDA)
            path_csv_consol = os.path.join(PATH_OUTPUT, 'FOPAG_Consolidada_Tratada.csv')
            path_csv_detalhe = os.path.join(PATH_OUTPUT, 'FOPAG_Detalhada_Tratada.csv')

            try:
                df_final_consol.to_csv(path_csv_consol, index=False, sep=';', decimal=',', encoding='utf-8-sig')
                if not df_final_detalhe.empty:
                    df_final_detalhe.to_csv(path_csv_detalhe, index=False, sep=';', decimal=',', encoding='utf-8-sig')
                logger.info(f"[OK] CSVs de auditoria gerados em: {PATH_OUTPUT}")
            except Exception as e:
                logger.error(f"Erro ao gerar CSVs: {e}")

            # Load Banco (MANTIDO)
            logger.info("Carregando Fatos de Folha no Banco...")
            carregar_fatos_folha(df_final_consol, df_final_detalhe, engine, schema)
        else:
            logger.warning("[AVISO] Nenhum dado foi extraído dos PDFs. Verifique a pasta input ou o layout.")
    else:
        logger.error(f"[ERRO] Pasta de input não encontrada: {PATH_INPUT}")

    # =========================================================================
    # 4. PIPELINE API SOLIDES (SEU CÓDIGO ORIGINAL)
    # =========================================================================
    # Token da API sólides, de ambas as empresas para carregar os dados da ARQDIGITAL descomentar a linha onde o .env
    # Puxa a váriavel SOLIDES_API_TOKEN, para puxar da ARQCONAM manter a váriavel SOLIDES_API_TOKEN_CONAM

    token_api = os.getenv("SOLIDES_API_TOKEN") # Quando for atualizar o DW da arqdigital descomentar essa linha
    #token_api = os.getenv("SOLIDES_API_TOKEN_CONAM") # Quando for atualizar o DW da arqconam descomentar essa linha

    if token_api:
        logger.info("--- [ETAPA 3] Pipeline API Solides ---")

        dados_brutos_api = extrair_api_solides(token_api)


        if dados_brutos_api:

            dados_profiler = extrair_profiler_solides(token_api,dados_brutos_api)
            logger.info("Transformando dados da API...")
            # [NOVO] - Transformações dos Dataframes
            df_colaboradores = transformar_dados_api(dados_brutos_api)
            df_beneficios = transformar_beneficios_api(dados_brutos_api)
            df_dependentes = transformar_dependentes_api(dados_brutos_api)
            df_profiler = transformar_profiler_api(dados_profiler)

            # [NOVO] - Carga unificanda de todos os Dataframes
            logger.info("Carregando dados da API no Banco...")
            carregar_dados_api(df_colaboradores,
                               df_beneficios,
                               df_dependentes,
                               df_profiler, # <---- Novo dataframe adicionado para termos as informações de profiler
                               engine,
                               schema)
        else:
            logger.warning("A API retornou uma lista vazia de dados brutos.")
    else:
        logger.warning("[AVISO] Token da API não encontrado no .env. Pulando etapa API.")

    # =========================================================================
    # 5. PÓS PROCESSAMENTO (SEU CÓDIGO ORIGINAL)
    # =========================================================================
    logger.info("--- [ETAPA 4] Pós-Processamento ---")
    processar_status_transferidos(engine, schema)

    # =========================================================================
    # 6. LIMPEZA FINAL (NOVA ETAPA ADICIONADA)
    # =========================================================================
    logger.info("--- [ETAPA FINAL] Limpeza de Arquivos Locais ---")
    qtd_removida = limpar_diretorio_local(PATH_INPUT, extensao='.pdf')
    limpar_tabelas_staging(engine, schema)
    logger.info(f"Limpeza concluída: {qtd_removida} arquivos removidos da pasta input.")

    # Cálculo do tempo total
    end_time = time.time()
    tempo_total = end_time - start_time
    mins, secs = divmod(tempo_total, 60)


    logger.info("=======================================================")
    logger.info(f"   PIPELINE FINALIZADO EM {int(mins)}m {int(secs)}s")
    logger.info("=======================================================")


if __name__ == "__main__":
    run_pipeline()