import requests
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, to_date, dayofmonth, trunc, date_format

data_inicio = '01/01/2022'
data_final = '31/12/2024'

### DEMANDA POR CRÉDITO RURAL 
# Contratos Crédito Rural - Quantidade
datas_qntcredito = spark.table('workspace.default.tb_contratos_credito')
df_qntcredito = datas_qntcredito
df_qntcredito = (
    df_qntcredito
    .withColumnRenamed('Data', 'Date_event')
    .withColumnRenamed('QtdInvestimento', 'Qty_credit')
    .withColumnRenamed('VlInvestimento', 'Value_credit')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
)

# Taxa Crédito Rural - Taxa médias mercado (mensal)
api_txcredito = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20758/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_final}'
response_txcredito = requests.get(api_txcredito)
datas_txcredito = response_txcredito.json()
df_txcredito = spark.createDataFrame(datas_txcredito)
df_txcredito = (
    df_txcredito
    .withColumnRenamed('data', 'Date_event')
    .withColumnRenamed('valor', 'Rate_credit')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
    .withColumn('Rate_credit', col('Rate_credit').cast('double'))
)

# CRA - Quantidade de Certificados e Valor Somado
df_cra = spark.table('workspace.default.tb_cra_cvm')
df_cra = (
    df_cra
    .withColumnRenamed('data', 'Date_event')
    .withColumnRenamed('Quantidade_Certificados', 'Qty_cra')
    .withColumnRenamed('Valor_Certificados', 'Price_cra')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
)

df_demanda_credito = (df_qntcredito
                      .join(df_txcredito, on='Date_event', how='left')
                      .join(df_cra, on='Date_event', how='left'))

# Conversões de tipo e ordenação
df_demanda_credito = (
    df_demanda_credito
    .withColumn('Qty_credit', col('Qty_credit').cast('double'))
    .withColumn('Value_credit', col('Value_credit').cast('double'))
    .withColumn('Rate_credit', col('Rate_credit').cast('double'))
    .withColumn('Qty_cra', col('Qty_cra').cast('double'))
    .withColumn('Price_cra', col('Price_cra').cast('double'))
    .orderBy('Date_event')
)

df_demanda_credito.write.mode('overwrite').saveAsTable('workspace.default.tb_demanda_credito')
display(df_demanda_credito)

### FATORES PRODUTIVOS AGRÍCOLAS
# ICE - Pontos de Confiança (Agro)
dados_ice = spark.table('workspace.default.tb_ice_fgv')
df_ice = (
    dados_ice
    .withColumnRenamed('Data', 'Date_event')
    .withColumnRenamed('ICE_Agro', 'Points_ice')
)

# Commodities - Preço em reais
dados_commodities = spark.table('workspace.default.tb_commodities_cepea')
df_commodities = (
    dados_commodities
    .withColumnRenamed('Data', 'Date_event')
    .withColumnRenamed('Valor_Soja', 'Price_soy')
    .withColumnRenamed('Valor_Cafe', 'Price_coffee')
    .withColumnRenamed('Valor_Algodao', 'Price_cotton')
)

# Clima - Quantidade, mm, mb, â°c
dados_clima = spark.table('workspace.default.tb_clima_inmet')
df_clima = (
    dados_clima
    .withColumnRenamed('Data_Medicao', 'Date_event')
    .withColumnRenamed('Numero_dias_Precipitacao', 'Days_rainfall')
    .withColumnRenamed('Precipitacao_total', 'Total_rainfall')
    .withColumnRenamed('Pressao_Atmosferica', 'Atmospheric_pressure')
    .withColumnRenamed('Temperatura_media', 'Temperature')
)
    
df_fatores_agricolas = df_ice.join(df_commodities, on='Date_event', how='left').join(df_clima, on='Date_event', how='left')
df_fatores_agricolas = (
    df_fatores_agricolas
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy')).orderBy('Date_event')
    .withColumn('Points_ice', col('Points_ice').cast('double'))
    .withColumn('Price_soy', col('Price_soy').cast('double'))
    .withColumn('Price_coffee', col('Price_coffee').cast('double'))
    .withColumn('Price_cotton', col('Price_cotton').cast('double'))
    .withColumn('Days_rainfall', col('Days_rainfall').cast('double'))
    .withColumn('Total_rainfall', col('Total_rainfall').cast('double'))
    .withColumn('Atmospheric_pressure', col('Atmospheric_pressure').cast('double'))
    .withColumn('Temperature', col('Temperature').cast('double'))
)

df_fatores_agricolas.write.mode('overwrite').saveAsTable('workspace.default.tb_fatores_agricolas')
display(df_fatores_agricolas)

### VARIÁVEIS MACROECONÔMICAS 
# PIB - milhões de reais
df_pib = spark.table('workspace.default.tb_pib_fgv')
df_pib = df_pib.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))

# IGPM - taxa  
df_igpm = spark.table('workspace.default.tb_igpm_fgv')
df_igpm = df_igpm.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))

# Taxa Selic - anualizada acumulada
api_selic = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_final}'
response_selic = requests.get(api_selic)
datas_selic = response_selic.json()
df_selic = spark.createDataFrame(datas_selic)
df_selic = df_selic.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))
df_selicinha = (
    df_selic
    .filter(dayofmonth(col("Data")) == 3)
    .withColumn("Data", trunc(col("Data"), "MM"))
    .join(df_selic, on="Data", how="left_anti")
)
df_selic = df_selic.unionByName(df_selicinha)

# Taxa Câmbio - dolar 
api_cambio = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_final}'
response_cambio = requests.get(api_cambio)
datas_cambio = response_cambio.json()
df_cambio = spark.createDataFrame(datas_cambio)
df_cambio = df_cambio.withColumn('Data', to_date(col('Data'), "dd/MM/yyyy"))
df_cambinhho = (
    df_cambio
    .filter(dayofmonth(col("Data")) == 3)
    .withColumn('Data', trunc(col('Data'), 'MM'))
    .join(df_cambio, on='Data', how='left_anti')
)
df_cambio = df_cambio.unionByName(df_cambinhho)

df_variaveis_macroeconomicas = df_pib.join(df_igpm, on='Data', how='left').join(df_selic.withColumnRenamed('valor', 'Rate_selic'), on='Data', how='left').join(df_cambio.withColumnRenamed('valor', 'Price_dolar'), on='Data', how='left')

df_variaveis_macroeconomicas = (
    df_variaveis_macroeconomicas
    .withColumnRenamed('Data', 'Date_event').orderBy('Date_event')
    .withColumnRenamed('PIB_Agro', 'Agri_pib').withColumn('Agri_pib', col('Agri_pib').cast('double'))
    .withColumnRenamed('PIB_mercado', 'Market_pib').withColumn('Market_pib', col('Market_pib').cast('double'))
    .withColumnRenamed('IGPM_Preco', 'Price_igpm').withColumn('Price_igpm', col('Price_igpm').cast('double'))
    .withColumnRenamed('IGPM_Tx', 'Rate_igpm').withColumn('Rate_igpm', col('Rate_igpm').cast('double'))
    .withColumn('Rate_selic', col('Rate_selic').cast('double'))
    .withColumn('Price_dolar', col('Price_dolar').cast('double'))
    .select('Date_event', 'Agri_pib', 'Market_pib', 'Price_igpm', 'Rate_igpm', 'Rate_selic', 'Price_dolar')
)

df_variaveis_macroeconomicas.write.mode('overwrite').saveAsTable('workspace.default.tb_variaveis_macroeconomicas')
display(df_variaveis_macroeconomicas)