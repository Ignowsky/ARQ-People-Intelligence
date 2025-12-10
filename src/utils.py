# src/utils.py
import pandas as pd
import numpy as np


def clean_text_series(series):
    """
    Limpa uma série de texto (object) do pandas de forma segura.
    """
    if series.dtype == 'object':
        series = series.str.strip()
        series = series.str.replace(u'\xa0', '', regex=False)
        series = series.replace(['N/A', '', 'nan', 'None', 'NULL'], None)
    return series


def limpar_valor_moeda(valor_str):
    """
    Converte string (ex: 'R$ 1.200,50') ou float para float python.
    RESTAURADA LOGICA DO MONOLITO (Remove R$).
    """
    if valor_str is None or pd.isna(valor_str):
        return None

    if isinstance(valor_str, (int, float)):
        return float(valor_str)

    if isinstance(valor_str, str):
        try:
            # Lógica agressiva do script original:
            # 1. Remove R$
            valor_limpo = valor_str.replace('R$', '')
            # 2. Remove espaços em branco (invisíveis ou não)
            valor_limpo = valor_limpo.strip()
            # 3. Remove pontos de milhar (1.000 -> 1000)
            valor_limpo = valor_limpo.replace('.', '')
            # 4. Troca vírgula decimal por ponto (1000,50 -> 1000.50)
            valor_limpo = valor_limpo.replace(',', '.')

            if not valor_limpo:
                return None

            return float(valor_limpo)
        except (ValueError, TypeError):
            return None
    return None