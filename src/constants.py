# --- MAPEAMENTO DE RUBRICAS (GLOBAL) ---
# (Seu mapa de rubricas original - sem alterações)
# Mapeamento de códigos para nomes de rubricas
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
    '643': 'P_643_VA_Retroativo_CCT', '730': 'P_730_Abono_CCT', '8869': 'P_8869_Dias_Afast_P/Acid_Trabalho_C/D', '294': 'P_294_Auxilio_Educacao', '293': 'P_293_Dev_Desconto_VT',
    '1076': 'D_1076_Desc_Emprestimo_Consignado', '1078': 'D_1078_Desc_Emprestimo_Consignado', '9754': 'P_9754_Estorno_Desc_Prov_Emprestimo_Consignado',
    '9751': 'D_9751_Desc_Emprestimo_Consginado', '9752': 'D_9752_Provisao_Desc._Emprestimo_Consignado', '557': 'P_557_VT_Mes_Atual'
    
}
proventos_map = {k: v for k, v in MAPEAMENTO_ORIGINAL.items() if v.startswith('P_')}
descontos_map = {k: v for k, v in MAPEAMENTO_ORIGINAL.items() if v.startswith('D_')}
sorted_proventos = dict(sorted(proventos_map.items(), key=lambda item: int(item[0])))
sorted_descontos = dict(sorted(descontos_map.items(), key=lambda item: int(item[0])))
MAPEAMENTO_CODIGOS = {**sorted_proventos, **sorted_descontos}


# --- SCHEMAS PARA VALIDAÇÃO DE DADOS COM SQLALCHEMY ---
from sqlalchemy.types import String, Date, Numeric

SCHEMA_TOTAIS = {
    'competencia': Date(),
    'tipo_calculo': String(),
    'departamento': String(),
    'vinculo': String(),
    'nome_funcionario': String(),
    'situacao': String(),
    'data_demissao': Date(),
    'motivo_demissao': String(),
    'cargo': String(),
    'data_admissao': Date(),
    'cpf': String(11),
    'salario_contratual': Numeric(10, 2),
    'total_proventos': Numeric(10, 2),
    'total_descontos': Numeric(10, 2),
    'valor_liquido': Numeric(10, 2),
    'base_inss': Numeric(10, 2),
    'base_fgts': Numeric(10, 2),
    'valor_fgts': Numeric(10, 2),
    'base_irrf': Numeric(10, 2)
}

SCHEMA_RUBRICAS = {
    'competencia': Date(),
    'tipo_calculo': String(),
    'departamento': String(),
    'vinculo': String(),
    'nome_funcionario': String(),
    'cpf': String(11),
    'codigo_rubrica': String(),
    'nome_rubrica': String(),
    'tipo_rubrica': String(),
    'valor_rubrica': Numeric(10, 2)
}