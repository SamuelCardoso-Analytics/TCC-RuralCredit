import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.types import NumericType
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

df_normalizado = spark.table('workspace.default.tb_dados_normalizados')

colunas_numericas = [
    field.name
    for field in df_normalizado.schema.fields
    if isinstance(field.dataType, NumericType)
]

correlacao = df_normalizado.select(colunas_numericas).toPandas().corr()
correlacao.insert(0, '', correlacao.index)
display(correlacao.round(2))

# 
dependente = 'Qty_credit'
independentes = ['Value_credit','Market_pib','Rate_igpm']  

X = df_normalizado.select(independentes).toPandas().values  
Y = df_normalizado.select(dependente).toPandas().values.flatten()

modelo = LinearRegression()
modelo.fit(X, Y)
Y_pred = modelo.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Dados Originais', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Linha de Regressão', lw=2)
plt.title('Regressão Linear Múltipla')
plt.xlabel(dependente)
plt.ylabel(independentes)
plt.legend()
plt.grid(True)
plt.show()

print(f'Erro Absoluto Médio (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Erro Quadrático Médio (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f"Raiz do Erro Quadrático Médio (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}") 

# 
dependente = 'Value_credit'
independentes = ['Qty_credit','Points_ice','Rate_selic']  

X = df_normalizado.select(independentes).toPandas().values  
Y = df_normalizado.select(dependente).toPandas().values.flatten()

modelo = LinearRegression()
modelo.fit(X, Y)
Y_pred = modelo.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Dados Originais', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Linha de Regressão', lw=2)
plt.title('Regressão Linear Múltipla')
plt.xlabel(dependente)
plt.ylabel(independentes)
plt.legend()
plt.grid(True)
plt.show()

print(f'Erro Absoluto Médio (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Erro Quadrático Médio (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f"Raiz do Erro Quadrático Médio (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}") 