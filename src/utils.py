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
    Converte string para float. 
    Lógica RESTAURADA do Automação_FOPAG.py (sem replace de R$).
    """
    if valor_str is None:
        return None
    if isinstance(valor_str, (int, float)):
        return float(valor_str)
    if isinstance(valor_str, str):
        try:
            # 1. Remove espaços
            valor_limpo = valor_str.strip()
            # 2. Remove pontos de milhar
            valor_limpo = valor_limpo.replace('.', '')
            # 3. Troca vírgula por ponto
            valor_limpo = valor_limpo.replace(',', '.')

            if not valor_limpo:
                return None

            return float(valor_limpo)
        except (ValueError, TypeError):
            return None
    return None