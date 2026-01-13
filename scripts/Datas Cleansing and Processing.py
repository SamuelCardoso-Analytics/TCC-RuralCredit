import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions
from pyspark.sql.types import NumericType

df_main = spark.table('workspace.default.tb_demanda_credito') \
    .join(spark.table('workspace.default.tb_fatores_agricolas'), on='Date_event', how='left') \
    .join(spark.table('workspace.default.tb_variaveis_macroeconomicas'), on='Date_event', how='left')

df_padronizado = df_main

colunas_numericas = [
    field.name
    for field in df_main.schema.fields
    if isinstance(field.dataType, NumericType)
]

for coluna in colunas_numericas:
    stats = df_main.select(
        functions.mean(coluna).alias('media'),
        functions.stddev(coluna).alias('desvio')
    ).collect()[0]

    df_padronizado = df_padronizado.withColumn(f'{coluna}_padronizado',
        functions.round((functions.col(coluna) - stats['media']) / stats['desvio'], 2))

for coluna in colunas_numericas:
    df_padronizado = df_padronizado.drop(coluna).withColumnRenamed(f'{coluna}_padronizado', coluna)

display(df_padronizado)

results = []

for coluna in colunas_numericas:

    Q1, Q3 = df_padronizado.approxQuantile(coluna, [0.25, 0.75], 0.01)
    IQR = Q3 - Q1
    limite_inferior = Q1 - (1.5 * IQR)
    limite_superior = Q3 + (1.5 * IQR)
    results.append({
        'coluna': coluna,
        'Q1': round(Q1, 2),
        'Q3': round(Q3, 2),
        'IQR': round(IQR, 2),
        'limite_inferior': round(limite_inferior, 2),
        'limite_superior': round(limite_superior, 2)
    })

df_outliers = pd.DataFrame(results)
display(df_outliers)

df_pd = df_padronizado.select(['Date_event'] + colunas_numericas).toPandas()

for _, row in df_outliers.iterrows():
    coluna = row['coluna']
    limite_inferior = row['limite_inferior']
    limite_superior = row['limite_superior']

for coluna in colunas_numericas:
    plt.figure(figsize=(10, 6)) 
    plt.scatter(df_pd['Date_event'], df_pd[coluna], color='blue', alpha=0.5)
    plt.axhline(y=limite_inferior, color='red', linestyle='--', label='Limite Inferior')
    plt.axhline(y=limite_superior, color='green', linestyle='--', label='Limite Superior')
    plt.xlabel('Date_event')
    plt.ylabel(coluna)
    plt.title(f'Gráfico de Dispersão - {coluna}')
    plt.legend()
    plt.grid(True)
    plt.show()

colunas_para_limitar = ['Qty_credit', 'Value_credit', 'Rate_credit', 'Qty_cra', 'Price_coffee', 'Price_cotton', 'Total_rainfall', 'Atmospheric_pressure', 'Temperature', 'Agri_pib', 'Rate_igpm', 'Price_dolar']

df_outliers_filtrado = df_outliers[df_outliers['coluna'].isin(colunas_para_limitar)]

df_normalizado = df_padronizado

for _, row in df_outliers_filtrado.iterrows():
    coluna = row['coluna']
    limite_superior = row['limite_superior']
    limite_inferior = row['limite_inferior']

    df_normalizado = df_normalizado.withColumn(
        coluna,
        functions.when(functions.col(coluna) > limite_superior, limite_superior) \
            .when(functions.col(coluna) < limite_inferior, limite_inferior) \
            .otherwise(functions.col(coluna))
    )

display(df_normalizado)

df_normalizado.write.mode("overwrite").saveAsTable('workspace.default.tb_dados_normalizados')
df_pd = df_normalizado.select('*').toPandas()

for _, row in df_outliers_filtrado.iterrows():
    coluna = row['coluna']
    limite_inferior = row['limite_inferior']
    limite_superior = row['limite_superior']
    
    plt.figure(figsize=(10, 6))
    plt.scatter(df_pd['Date_event'], df_pd[coluna], color='blue', alpha=0.5)
    plt.axhline(y=limite_inferior, color='red', linestyle='--', label='Limite Inferior')
    plt.axhline(y=limite_superior, color='green', linestyle='--', label='Limite Superior')
    plt.xlabel('Date_event')
    plt.ylabel(coluna)
    plt.title(f'Gráfico de Dispersão - {coluna}')
    plt.legend()
    plt.show()