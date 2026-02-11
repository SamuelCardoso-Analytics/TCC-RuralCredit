import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller 
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.statespace.sarimax import SARIMAX

df_standardized = spark.table('workspace.default.tb_standardized_data')

df_series = pd.DataFrame({
    'Date': df_standardized.select('Date_event').toPandas()['Date_event'],
    'Values': df_standardized.select('Qty_credit').toPandas()['Qty_credit']
})

## 1 - SIMPLE TIME SERIES
# Check if there series is temporal trend  
title = 'Simple Time Series'
plt.figure(figsize=(10, 5))
plt.plot(df_series['Date'], df_series['Values'], marker='o')
plt.title(title)
plt.xlabel('Date')
plt.ylabel('Values')
plt.grid()
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()

# Identify Seasonality
title = 'ACF - Autocorrelation Function '
plt.figure(figsize=(12, 4))
plot_acf(df_series['Values'], lags=12)
plt.title(title)
plt.tight_layout()
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()

# Identify Stationarity
title = 'PACF - Partial Autocorrelation Function'
max_lag = min(12, len(df_series) // 2 - 1)
fig, ax = plt.subplots(figsize=(7, 4))
plot_pacf(df_series['Values'], lags=max_lag, ax=ax)
plt.title(title)
plt.tight_layout()
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()

result = adfuller(df_series['Values']) 
print('ADF Test:', round(result[0], 4)) # Augmented Dickey-Fuller
print('p-value:', round(result[1], 4)) 
if result[1] < 0.05: 
    print('The series is stationary') 
else:
    print('The series is not stationary') 

## 2 - CHOICE OF MODEL
models = [
    ((1, 0, 0), (1, 0, 0, 12)), 
    ((1, 0, 1), (1, 0, 0, 12)),  
    ((2, 0, 0), (1, 0, 0, 12)),   
    ((1, 0, 1), (1, 0, 1, 12)),  
    ((2, 0, 1), (1, 0, 0, 12)),  
]

# Metrics for calculating the ideal model
results = []
for order, seasonal_order in models:
    try: 
        model = SARIMAX(df_series['Values'], order=order, seasonal_order=seasonal_order)
        fitted = model.fit(disp=False)
        results.append({
            'model': f'SARIMA{order}{seasonal_order}', 
            'AIC': round(fitted.aic, 4),             # Akaike Information Criterion
            'BIC': round(fitted.bic, 4),             # Bayesian Information Criterion
            'RMSE': round(np.sqrt(fitted.mse), 4)    # Root Mean Square Error
        })
    except:
        continue

df_results = pd.DataFrame(results).sort_values('AIC')
display(df_results)

# SARIMA Statistical Summary
model = SARIMAX(df_series['Values'], order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
correction = model.fit(disp=False)
print(correction.summary())

## 3 - MODELING THE FORECAST
# Applying the Forecast
y = df_series['Values']
model = SARIMAX(y, order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
correction = model.fit(disp=False)
n_steps = 6  
forecast = correction.forecast(steps=n_steps)

plt.figure(figsize=(14, 7))
plt.plot(y.index, y.values, label='Historical Data', linewidth=2.5, color='#1f77b4')
plt.plot(y.index, correction.fittedvalues, label='Adjusted Values', alpha=0.8, linestyle='--', color='#ff7f0e', linewidth=2)

# Cleasing for the Plot
last_date = y.index[-1]
if isinstance(last_date, pd.Timestamp):
    forecast_index = pd.date_range(start=last_date, periods=n_steps+1, freq='M')[1:]
else:
    forecast_index = range(len(y), len(y) + n_steps)

title = 'Credit Demand Forecast - SARIMA'
plt.plot(forecast_index, forecast, label='Forecast', color='#d62728', linewidth=2.5, marker='o', markersize=6)
plt.title(title, fontsize=16)
plt.xlabel('Time Period', fontsize=12)
plt.ylabel('Credit Demand', fontsize=12)
plt.legend(loc='best', fontsize=11)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(f'/Workspace/Users/samuucardosoo@gmail.com/TCC-RuralCredit/visualizations/{title}.png')
plt.show()