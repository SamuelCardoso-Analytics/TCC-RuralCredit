import requests
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, to_date, dayofmonth, trunc, date_format

start_date = '01/01/2022'
final_date = '31/12/2024'

###  DEMAND FOR RURAL CREDIT  
# Rural Credit Contracts - Quantity and Price
datas_qtycredit = spark.table('workspace.default.tb_credit_agreements')
df_qtycredit = datas_qtycredit
df_qtycredit = (
    df_qtycredit
    .withColumnRenamed('Data', 'Date_event')        
    .withColumnRenamed('QtdInvestimento', 'Qty_credit')
    .withColumnRenamed('VlInvestimento', 'Value_credit')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
)

# Rural Credit Rate - Average market rates (monthly)
api_rtcredit = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.20758/dados?formato=json&dataInicial={start_date}&dataFinal={final_date}'
response_rtcredit = requests.get(api_rtcredit)
datas_rtcredit = response_rtcredit.json()
df_rtcredit = spark.createDataFrame(datas_rtcredit)
df_rtcredit = (
    df_rtcredit
    .withColumnRenamed('data', 'Date_event')
    .withColumnRenamed('valor', 'Rate_credit')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
    .withColumn('Rate_credit', col('Rate_credit').cast('double'))
)

# CRA - Agribusiness Receivables Certificate / Number of Certificates and Total Value
df_cra = spark.table('workspace.default.tb_cra_cvm')
df_cra = (
    df_cra
    .withColumnRenamed('data', 'Date_event')
    .withColumnRenamed('Quantidade_Certificados', 'Qty_cra')
    .withColumnRenamed('Valor_Certificados', 'Price_cra')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
)

df_credit_demand = (df_qtycredit
                      .join(df_rtcredit, on='Date_event', how='left')
                      .join(df_cra, on='Date_event', how='left'))

# Type conversions and sorting
df_credit_demand = (
    df_credit_demand
    .withColumn('Qty_credit', col('Qty_credit').cast('double'))
    .withColumn('Value_credit', col('Value_credit').cast('double'))
    .withColumn('Rate_credit', col('Rate_credit').cast('double'))
    .withColumn('Qty_cra', col('Qty_cra').cast('double'))
    .withColumn('Price_cra', col('Price_cra').cast('double'))
    .orderBy('Date_event')
)

df_credit_demand.write.mode('overwrite').saveAsTable('workspace.default.tb_credit_demand')
display(df_credit_demand)

### AGRICULTURAL PRODUCTIVITY FACTORS
# ICE - Índice de Confiança Empresarial / Business Confidence Index (Agricultural)
datas_ice = spark.table('workspace.default.tb_ice_fgv')
df_ice = (
    datas_ice
    .withColumnRenamed('Data', 'Date_event')
    .withColumnRenamed('ICE_Agro', 'Points_ice')
)

# Commodities - Price (R$)
datas_commodities = spark.table('workspace.default.tb_commodities_cepea')
df_commodities = (
    datas_commodities
    .withColumnRenamed('Data', 'Date_event')
    .withColumnRenamed('Valor_Soja', 'Price_soy')
    .withColumnRenamed('Valor_Cafe', 'Price_coffee')
    .withColumnRenamed('Valor_Algodao', 'Price_cotton')
)

# Weather - Quantity (mm, mb, â°c)
datas_weather = spark.table('workspace.default.tb_clima_inmet')
df_weather = (
    datas_weather
    .withColumnRenamed('Data_Medicao', 'Date_event')
    .withColumnRenamed('Numero_dias_Precipitacao', 'Days_rainfall')
    .withColumnRenamed('Precipitacao_total', 'Total_rainfall')
    .withColumnRenamed('Pressao_Atmosferica', 'Atmospheric_pressure')
    .withColumnRenamed('Temperatura_media', 'Temperature')
)
    
df_product_factors = df_ice.join(df_commodities, on='Date_event', how='left').join(df_weather, on='Date_event', how='left')
df_product_factors = ( 
    df_product_factors
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

df_product_factors.write.mode('overwrite').saveAsTable('workspace.default.tb_product_factors')
display(df_product_factors)

### MACROECONOMIC VARIABLES 
# PIB / GDP - millions (R$)
df_pib = spark.table('workspace.default.tb_pib_fgv')
df_pib = df_pib.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))

# IGPM - taxa  
df_igpm = spark.table('workspace.default.tb_igpm_fgv')
df_igpm = df_igpm.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))

# Selic Rate - annualized accumulated
api_selic = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial={start_date}&dataFinal={final_date}'
response_selic = requests.get(api_selic)
datas_selic = response_selic.json()
df_selicrt = spark.createDataFrame(datas_selic)
df_selicrt = df_selicrt.withColumn('Data', to_date(col('Data'), 'dd/MM/yyyy'))
df_selicrt = (
    df_selicrt
    .filter(dayofmonth(col("Data")) == 3)
    .withColumn("Data", trunc(col("Data"), "MM"))
    .join(df_selicrt, on="Data", how="left_anti")
)
df_selicrt = df_selicrt.unionByName(df_selicrt)

# Exchange Rate - dolar 
api_exchange = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados?formato=json&dataInicial={start_date}&dataFinal={final_date}'
response_exchange = requests.get(api_exchange)
datas_exchange = response_exchange.json()
df_exchangert = spark.createDataFrame(datas_exchange)
df_exchangert = df_exchangert.withColumn('Data', to_date(col('Data'), "dd/MM/yyyy"))
df_exchangert = (
    df_exchangert
    .filter(dayofmonth(col("Data")) == 3)
    .withColumn('Data', trunc(col('Data'), 'MM'))
    .join(df_exchangert, on='Data', how='left_anti')
)
df_exchangert = df_exchangert.unionByName(df_exchangert)

df_variables_macroeconomic = df_pib.join(df_igpm, on='Data', how='left').join(df_selicrt.withColumnRenamed('valor', 'Rate_selic'), on='Data', how='left').join(df_exchangert.withColumnRenamed('valor', 'Price_dolar'), on='Data', how='left')

df_variables_macroeconomic = (
    df_variables_macroeconomic
    .withColumnRenamed('Data', 'Date_event').orderBy('Date_event')
    .withColumnRenamed('PIB_Agro', 'Agri_pib').withColumn('Agri_pib', col('Agri_pib').cast('double'))
    .withColumnRenamed('PIB_mercado', 'Market_pib').withColumn('Market_pib', col('Market_pib').cast('double'))
    .withColumnRenamed('IGPM_Preco', 'Price_igpm').withColumn('Price_igpm', col('Price_igpm').cast('double'))
    .withColumnRenamed('IGPM_Tx', 'Rate_igpm').withColumn('Rate_igpm', col('Rate_igpm').cast('double'))
    .withColumn('Rate_selic', col('Rate_selic').cast('double'))
    .withColumn('Price_dolar', col('Price_dolar').cast('double'))
    .select('Date_event', 'Agri_pib', 'Market_pib', 'Price_igpm', 'Rate_igpm', 'Rate_selic', 'Price_dolar')
)

df_variables_macroeconomic.write.mode('overwrite').saveAsTable('workspace.default.tb_variables_macroeconomic')
display(df_variables_macroeconomic)