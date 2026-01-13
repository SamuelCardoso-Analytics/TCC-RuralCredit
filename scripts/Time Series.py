import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from statsmodels.tsa.stattools import adfuller 
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.statespace.sarimax import SARIMAX

df_normalizado = spark.table('workspace.default.tb_dados_normalizados')

df_serie = pd.DataFrame({
    'Data': df_normalizado.select('Date_event').toPandas()['Date_event'],
    'Valores': df_normalizado.select('Qty_credit').toPandas()['Qty_credit']
})

plt.figure(figsize=(10, 5))
plt.plot(df_serie['Data'], df_serie['Valores'], marker='o')
plt.title('Série Temporal: Verificação Visual')
plt.xlabel('Data')
plt.ylabel('Valores')
plt.grid()
plt.show()

# 
plt.figure(figsize=(12, 4))
plot_acf(df_serie['Valores'], lags=12)
plt.title('ACF')
plt.tight_layout()
plt.show()

max_lag = min(12, len(df_serie) // 2 - 1)
fig, ax = plt.subplots(figsize=(7, 4))
plot_pacf(df_serie['Valores'], lags=max_lag, ax=ax)
plt.title('PACF - Função de Autocorrelação Parcial')
plt.tight_layout()
plt.show()

resultado = adfuller(df_serie['Valores']) 
print('Estatística do Teste ADF:', round(resultado[0], 4)) 
print('p-valor:', round(resultado[1], 4)) 
if resultado[1] < 0.05: 
    print('A série é estacionária') 
else:
    print('A série não é estacionária') 

# 
models = [
    ((1, 0, 0), (1, 0, 0, 12)), 
    ((1, 0, 1), (1, 0, 0, 12)),  
    ((2, 0, 0), (1, 0, 0, 12)),   
    ((1, 0, 1), (1, 0, 1, 12)),  
    ((2, 0, 1), (1, 0, 0, 12)),  
]

results = []
for order, seasonal_order in models:
    try:
        model = SARIMAX(df_serie['Valores'], order=order, seasonal_order=seasonal_order)
        fitted = model.fit(disp=False)
        results.append({
            'model': f'SARIMA{order}{seasonal_order}',
            'AIC': round(fitted.aic, 4),
            'BIC': round(fitted.bic, 4),
            'RMSE': round(np.sqrt(fitted.mse), 4)
        })
    except:
        continue

df_resultados = pd.DataFrame(results).sort_values('AIC')
display(df_resultados)

modelo = SARIMAX(df_serie['Valores'], order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
ajuste = modelo.fit(disp=False)
print(ajuste.summary())

# 
y = df_serie['Valores']

modelo = SARIMAX(y, order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
ajuste = modelo.fit(disp=False)

n_steps = 12  
forecast = ajuste.forecast(steps=n_steps)
plt.figure(figsize=(14, 7))

plt.plot(y.index, y.values, label='Dados Históricos', linewidth=2.5, color='#1f77b4')
plt.plot(y.index, ajuste.fittedvalues, label='Valores Ajustados', alpha=0.8, linestyle='--', color='#ff7f0e', linewidth=2)

last_date = y.index[-1]
if isinstance(last_date, pd.Timestamp):
    forecast_index = pd.date_range(start=last_date, periods=n_steps+1, freq='M')[1:]
else:
    forecast_index = range(len(y), len(y) + n_steps)

plt.plot(forecast_index, forecast, label='Previsão', color='#d62728', linewidth=2.5, marker='o', markersize=6)
plt.title('Previsão de Demanda de Crédito - SARIMA', fontsize=16, fontweight='bold')
plt.xlabel('Período', fontsize=12)
plt.ylabel('Demanda de Crédito', fontsize=12)
plt.legend(loc='best', fontsize=11)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()