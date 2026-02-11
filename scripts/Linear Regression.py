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

# Correlation Matrix 
correlation = df_standardized.select(numeric_columns).toPandas().corr()
correlation.insert(0, '', correlation.index)
display(correlation.round(2))

# Linear Regression and Metrics
title = 'Frist Test - Multiple Linear Regression'
dependent = 'Qty_credit'
independents = ['Value_credit','Market_pib','Rate_igpm']  

X = df_standardized.select(independents).toPandas().values  
Y = df_standardized.select(dependent).toPandas().values.flatten()

model = LinearRegression()
model.fit(X, Y)
Y_pred = model.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Original Data', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Regression Line', lw=2)
plt.title(title)
plt.xlabel(dependent)
plt.ylabel(independents)
plt.legend()
plt.grid(True)
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()

print(f'Mean Absolute Error (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Mean Squared Error (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f'Root Mean Squared Error (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}') 

# Linear Regression and Metrics
title = 'Second Test - Multiple Linear Regression'
dependent = 'Value_credit'
independents = ['Qty_credit','Points_ice','Rate_selic']  

X = df_standardized.select(independents).toPandas().values
Y = df_standardized.select(dependent).toPandas().values.flatten()

model = LinearRegression()
model.fit(X, Y)
Y_pred = model.predict(X)

plt.scatter(Y, Y_pred, color='blue', label='Original Data', alpha=0.5)
plt.plot([Y.min(), Y.max()], [Y.min(), Y.max()], 'r--', label='Regression Line', lw=2)
plt.title(title)
plt.xlabel(dependent)
plt.ylabel(independents)
plt.legend()
plt.grid(True)
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()

print(f'Mean Absolute Error (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Mean Squared Error (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f'Root Mean Squared Error (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}') 