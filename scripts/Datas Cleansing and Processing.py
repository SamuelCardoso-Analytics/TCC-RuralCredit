import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions
from pyspark.sql.types import NumericType

df_main = spark.table('workspace.default.tb_credit_demand') \
    .join(spark.table('workspace.default.tb_product_factors'), on='Date_event', how='left') \
    .join(spark.table('workspace.default.tb_variables_macroeconomic'), on='Date_event', how='left')

df_standardized = df_main

## Z-SCORE STANDARDIZED
numeric_columns = [
    field.name
    for field in df_main.schema.fields
    if isinstance(field.dataType, NumericType)
]

for columns in numeric_columns:
    stats = df_main.select(
        functions.mean(columns).alias('average'),
        functions.stddev(columns).alias('deviation')
    ).collect()[0]

    df_standardized = df_standardized.withColumn(f'{columns}_standardized',
        functions.round((functions.col(columns) - stats['average']) / stats['deviation'], 2))

for columns in numeric_columns:
    df_standardized = df_standardized.drop(columns).withColumnRenamed(f'{columns}_standardized', columns)

display(df_standardized)

## INTERQUARTILE RANGE
results = []
for columns in numeric_columns:
    Q1, Q3 = df_standardized.approxQuantile(columns, [0.25, 0.75], 0.01)
    IQR = Q3 - Q1
    lower_limit = Q1 - (1.5 * IQR)
    upper_limit = Q3 + (1.5 * IQR)
    results.append({
        'columns': columns,
        'Q1': round(Q1, 2),
        'Q3': round(Q3, 2),
        'IQR': round(IQR, 2),
        'Lower_limit': round(lower_limit, 2),
        'Upper_limit': round(upper_limit, 2)
    })

df_outliers = pd.DataFrame(results)
display(df_outliers)

df_pd = df_standardized.select(['Date_event'] + numeric_columns).toPandas()

# Outiliers Visualization
for _, row in df_outliers.iterrows():
    columns = row['columns']
    lower_limit = row['Lower_limit']
    upper_limit = row['Upper_limit']

for columns in numeric_columns:
    plt.figure(figsize=(10, 6)) 
    plt.scatter(df_pd['Date_event'], df_pd[columns], color='blue', alpha=0.5)
    plt.axhline(y=lower_limit, color='red', linestyle='--', label='Lower Limit')
    plt.axhline(y=upper_limit, color='green', linestyle='--', label='Upper Limit')
    plt.xlabel('Date_event')
    plt.ylabel(columns)
    plt.title(f'Scatter Plot - {columns}')
    plt.legend()
    plt.grid(True)
    plt.show()

## OUTLIERS CLEANSING
process_columns = ['Qty_credit', 'Value_credit', 'Rate_credit', 'Qty_cra', 'Price_coffee', 'Price_cotton', 'Total_rainfall', 'Atmospheric_pressure', 'Temperature', 'Agri_pib', 'Rate_igpm', 'Price_dolar']

df_process_outilers = df_outliers[df_outliers['columns'].isin(process_columns)]
df_normalize = df_standardized

# Replace Outliers datas
for _, row in df_process_outilers.iterrows():
    columns = row['columns']
    lower_limit = row['Lower_limit']
    upper_limit = row['Upper_limit']

    df_normalize = df_normalize.withColumn(
        columns,
        functions.when(functions.col(columns) > upper_limit, upper_limit) \
            .when(functions.col(columns) < lower_limit, lower_limit) \
            .otherwise(functions.col(columns))
    )

display(df_normalize)

df_normalize.write.mode("overwrite").saveAsTable('workspace.default.tb_standardized_data')
df_pd = df_normalize.select('*').toPandas()

# Results Visualization
for _, row in df_process_outilers.iterrows():
    columns = row['columns']
    lower_limit = row['Lower_limit']
    upper_limit = row['Upper_limit']
    
    plt.figure(figsize=(10, 6))
    plt.scatter(df_pd['Date_event'], df_pd[columns], color='blue', alpha=0.5)
    plt.axhline(y=lower_limit, color='red', linestyle='--', label='Lower Limit')
    plt.axhline(y=upper_limit, color='green', linestyle='--', label='Upper Limit')
    plt.xlabel('Date_event')
    plt.ylabel(columns)
    plt.title(f'Scatter Plot - {columns}')
    plt.legend()
    plt.grid(True)
    plt.show()