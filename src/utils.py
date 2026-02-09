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
        Converte string ou float para float python.
        Híbrido: Aceita '2.500,00' (BR) E '2500.00' (US/API).
        """
        if valor_str is None or pd.isna(valor_str) or str(valor_str).strip() == '':
            return None

        if isinstance(valor_str, (int, float)):
            return float(valor_str)

        valor_limpo = str(valor_str).strip()
        valor_limpo = valor_limpo.replace('R$', '').strip()

        try:
            # LÓGICA HÍBRIDA (A CORREÇÃO):
            # Cenário 1: Formato Brasileiro (tem vírgula decimal) -> Ex: "2.619,76"
            if ',' in valor_limpo:
                valor_limpo = valor_limpo.replace('.', '')  # Remove ponto de milhar
                valor_limpo = valor_limpo.replace(',', '.')  # Troca vírgula por ponto

            # Cenário 2: Formato Americano/API (NÃO tem vírgula) -> Ex: "2619.76"
            # O código antigo removia o ponto aqui, transformando 2619.76 em 261976.
            # Agora, se não tiver vírgula, nós NÃO tocamos no ponto.

            return float(valor_limpo)
        except (ValueError, TypeError):
            return None