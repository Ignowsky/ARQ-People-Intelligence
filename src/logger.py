import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(nome_modulo=None):
    """
    Configura e retorna um logger padronizado.
    Salva logs em arquivo (pasta logs/) e mostra no terminal.
    """
    # 1. Cria a pasta de logs se não existir
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # 2. Configura o formatador (Data - Nível - Modulo - Mensagem)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(name)s - %(message)s')

    # 3. Handler de Arquivo (Rotativo: max 5MB, guarda 3 arquivos antigos)
    file_handler = RotatingFileHandler('logs/arq_people.log', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # 4. Handler de Console (Terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # 5. Obtém o logger
    # Se nome_modulo for passado, cria logger específico. Senão, pega o root.
    logger = logging.getLogger(nome_modulo if nome_modulo else 'ARQ_People')

    # Evita duplicar logs se a função for chamada múltiplas vezes
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger