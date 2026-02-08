import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.types import NumericType
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

df_standardized = spark.table('workspace.default.tb_standardized_data')

numeric_columns = [
    field.name
    for field in df_standardized.schema.fields
    if isinstance(field.dataType, NumericType)
]

correlation = df_standardized.select(numeric_columns).toPandas().corr()
correlation.insert(0, '', correlation.index)
display(correlation.round(2))

# 
dependent = 'Qty_credit'
independents = ['Value_credit','Market_pib','Rate_igpm']  

X = df_standardized.select(independents).toPandas().values  
Y = df_standardized.select(dependent).toPandas().values.flatten()

model = LinearRegression()
model.fit(X, Y)
Y_pred = model.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Dados Originais', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Linha de Regressão', lw=2)
plt.title('Regressão Linear Múltipla')
plt.xlabel(dependent)
plt.ylabel(independents)
plt.legend()
plt.grid(True)
plt.show()

print(f'Erro Absoluto Médio (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Erro Quadrático Médio (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f"Raiz do Erro Quadrático Médio (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}") 

# 
dependent = 'Value_credit'
independents = ['Qty_credit','Points_ice','Rate_selic']  

X = df_standardized.select(independents).toPandas().values  x'
Y = df_standardized.select(dependent).toPandas().values.flatten()

model = LinearRegression()
model.fit(X, Y)
Y_pred = model.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Dados Originais', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Linha de Regressão', lw=2)
plt.title('Regressão Linear Múltipla')
plt.xlabel(dependent)
plt.ylabel(independents)
plt.legend()
plt.grid(True)
plt.show()

print(f'Erro Absoluto Médio (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Erro Quadrático Médio (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f"Raiz do Erro Quadrático Médio (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}") 