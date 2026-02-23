# 🎓 Impact of Macroeconomic Variables and Agricultural Production Factors on the Demand for Rural Credit

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Status](https://img.shields.io/badge/Status-Completed-success.svg)]()

## 📝 Context

### Description

Developed as a Final Course Project (Business Management Technology), this project seeks to understand the sensitivity of rural credit demand to macroeconomic variables and productive factors. Additionally, it explores how it is possible to prepare for different economic scenarios using statistical models. 

### Problem

Agriculture, the main sector of the Brazilian economy, is very sensitive to rural credit, therefore, knowing which indicators have the greatest impact on demand is important to prepare for adverse situations and take advantage of market opportunities. 

### v

Identify and quantify the relationships between macroeconomic variables and agricultural production indicators with the demand for rural credit, and use statistical models to find the best scenarios.

### Fundamental Concepts

**Macroeconomic**: They are indicators or tools for controlling economic and monetary policy, such as GDP (Gross Domestic Product), interest rates, exchange rates, and inflation.

**Agriculture**: They are variables that affect agricultural production, such as temperature and rainfall, for example, as they are natural physical factors that have an influence.

**Rural Credit**: Financing modality aimed at rural producers, based on production and price expectations, such as commodity prices and harvest volume. 

## 🎲 Data Used 

### Data Sources

- [**Banco Central do Brasil (BCB)**](https://dadosabertos.bcb.gov.br/): Selic rate (Brazil's base interest rate), Exchange rate and Credit Contracts - API
- [**Fundação Getulio Vargas (FGV)**](https://autenticacao-ibre.fgv.br/ProdutosDigitais/Login): IGPM (price inflation index), ICE (business confidence index) and PIB (GDP) - CSV
- [**Centro de Estudos Avançados em Economia Aplicada (CEPEA)**](https://www.cepea.org.br/br/consultas-ao-banco-de-dados-do-site.aspx): Commodity prices - CSV
- [**Instituto Nacional de Meteorologia (INMET)**](https://bdmep.inmet.gov.br/): Rainfall, Atmospheric pressure e Temperature - CSV
- [**Comissão de Valores Mobiliários (CVM)**](https://dados.cvm.gov.br/): CRA (Agribusiness Receivables Certificate) - CSV

**Period analyzed**: January 2022 to December 2024

### Tools and Libraries
- [**Databricks**](https://login.databricks.com/signup?provider=DB&l=pt-BR&scid=7018Y000001Fi0cQAC&utm_medium=paid+search&utm_source=google&utm_campaign=19829725165&utm_adgroup=147439757256&utm_content=trial&utm_offer=trial&utm_ad=731917902921&utm_term=databricks&gad_source=1&gad_campaignid=19829725165&gbraid=0AAAAABYBeAhHda15kPPFIDjUtw9-JMztZ&gclid=CjwKCAiAjojLBhAlEiwAcjhrDlPAAhrvFx0npdc3nhzsuLzZN0CJWRF5uHdoSWv184ta5Lkxm7qHHxoCMlMQAvD_BwE&tuuid=fbfd3b91-9b8a-430c-b85a-198942daaf68&intent=SIGN_UP&dbx_source=direct&rl_aid=8df46689-73f9-42c6-b642-698357080083&sisu_state=eyJsZWdhbFRleHRTZWVuIjp7Ii9zaWdudXAiOnsidG9zIjp0cnVlLCJwcml2YWN5Ijp0cnVlLCJjb3Jwb3JhdGVFbWFpbFNoYXJpbmciOnRydWV9fX0%3D) - Python 3.11 and SQL 
- [**pyspark**](https://spark.apache.org/docs/latest/api/python/index.html) - SQL functions
- [**requests**](https://pypi.org/) - API acess
- [**pandas**](https://pandas.pydata.org/) - Data processing
- [**numpy**](https://numpy.org/) - Numerical operations
- [**matplotlib**](https://matplotlib.org/) - Visualization
- [**scikit-learn**](https://scikit-learn.org/stable/) - Linear Regression and statistical modeling
- [**statsmodels**](https://www.statsmodels.org/stable/index.html) - Time series and correlation matrix 

## 📁 Repository Structure

```
📦 TCC-RuralCredit 
├── 📂 datas                                      # Processed data
│   ├── tb_clima_inmet.csv              
│   ├── tb_commodities_cepea.csv              
│   ├── tb_cra_cvm.csv
│   ├── tb_credit_agreements.csv              
│   ├── tb_ice_fgv.csv              
│   ├── tb_igpm_fgv.csv              
│   └── tb_pib_fgv.csv                                       
├── 📂 scripts                                    # Python scripts 
│   ├── Collection and Creation of Tables.py
│   ├── Datas Cleansing and Processing.py
│   ├── Linear Regression.py
│   └── Time Series.py
├── 📂 visualizations                             # Graphs and figures
│   ├── ACF - Autocorrelation Function.png
│   ├── Correlation Matrix.png
│   ├── Credit Demand Forecast - SARIMA.png
│   ├── Frist Test - Multiple Linear Regression.png
│   ├── PACF - Partial Autocorrelation Function.png
│   ├── Scatter Plot Clean:(...).png         # more 31 plots
│   ├── Second Test - Multiple Linear Regression.png
│   └── Simple Time Series.png  
└── README.md                                     # This file
```

## 📊 Methodology

### 1° - Collection and Processing

The first step was to collect reliable, original, and uptodate data from the sources cited above, and in processing, converting and renaming columns. The data will be divided into dependent variables, about rural credit, and for variables independent, about the macroeconomic indicators and agricultural factors. 

**`Code example:`**
```python 
datas_qtycredit = spark.table('workspace.default.tb_credit_agreements')
df_qtycredit = datas_qtycredit
df_qtycredit = (
    df_qtycredit
    .withColumnRenamed('Data', 'Date_event')        
    .withColumnRenamed('QtdInvestimento', 'Qty_credit')
    .withColumnRenamed('VlInvestimento', 'Value_credit')
    .withColumn('Date_event', to_date(col('Date_event'), 'dd/MM/yyyy'))
)

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

df_credit_demand = (df_qtycredit.join(df_rtcredit, on='Date_event', how='left'))
df_credit_demand = (
    df_credit_demand
    .withColumn('Qty_credit', col('Qty_credit').cast('double'))
    .withColumn('Value_credit', col('Value_credit').cast('double'))
    .withColumn('Rate_credit', col('Rate_credit').cast('double'))
    .orderBy('Date_event')
)
df_credit_demand.write.mode('overwrite').saveAsTable('workspace.default.tb_credit_demand')
display(df_credit_demand)
```

### 2° - Z-Score Standardized

The Z-score is a statistical tool for standardized, where variables to a common scale by transforming the data to a mean of 0 and a standard deviation of 1. It remove the focus from absolute value and places it in context, showing how many standard deviation one data point is far from the average of a data set, creating comparisons.

**`Code example:`**
```python 
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
```

### 3° - Interquartile Range

The Interquartile Range is a measure that shows the value below the observations found, and is used to find outliers. This value is calculated for two observed groups, 25% and 75% of the sample. It is the dispersion between the data, and to identify the possible normal dispersion limits, the lower and upper limits are calculated.

**`Code example:`**
```python 
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
```

The frist plot, the last two observecions are above the upper limits, are outilers. The second plot, the outilers are replaced by the average of the observations and readjusted for use in models. 

<p align='center'>
<img src='visualizations/Scatter Plot: Price_coffee.png' width='49.5%'>
<img src='visualizations/Scatter Plot Clean: Price_coffee.png' width='49.5%'>

### 4° - Correlation Matrix 

The Correlation Matrix also is a statistical tool used to find the variables most correlated with the dependent variable. It displays the correlation coefficients between different pairs of data, showing the strength of one variable in relation to another, where the closer to 1, positive correlation, and the closes -1, negative correlation.

**`Code example:`**
```python 
df_standardized = spark.table('workspace.default.tb_standardized_data')

numeric_columns = [
    field.name
    for field in df_standardized.schema.fields
    if isinstance(field.dataType, NumericType)
]

correlation = df_standardized.select(numeric_columns).toPandas().corr()
correlation.insert(0, '', correlation.index)
display(correlation.round(2))
``` 

Below this Correlation Matrix in Heat Map, for better visualization. The three frists columns are the study variables, quantity, values and average rates. The frist column is demand, and how is percepibile, the credit value, CRA price, PIB and ICE has a positive correlation.

<p align='center'>
<img src='visualizations/Correlation Matrix.png' width='100%'>

### 5° - Linear Regression

The Multiple Linear Regression has as Goal to identify the variables with the greatest impact on demand, because they show linear relationships and there are multiple independent variables. However, the data were chosen to avoid the problem of multicollinearity, what happens when the dependent variables are higher  correlated with each other than with the independent variable, creating a false model result. 

**`Code example:`**
```python
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
plt.show()

print(f'Mean Absolute Error (MAE): {round(mean_absolute_error(Y, Y_pred),2)}') 
print(f'Mean Squared Error (MSE): {round(mean_squared_error(Y, Y_pred),2)}') 
print(f'Root Mean Squared Error (RMSE): {round(mean_squared_error(Y, Y_pred),2):.2f}') 
```

Both plots show the relationship between the original data and the regression line, with controlled dispersion. The metrics for the first test indicate an MAE of 0.4 and an RMSE of 0.26. The second test, the MAE fell to 0.36 and the RMSE to 0.20. The reduction in error variance means that the final model has fewer problems and better models new data, without creating overfitting.

<p align='center'>
<img src='visualizations/Frist Test - Multiple Linear Regression.png' width='49.5%'>
<img src='visualizations/Second Test - Multiple Linear Regression.png' width='49.5%'>

### 6° - Time Series

The Time Series is a function to predict the behavior of a variable. For check the type of time series, it is necessary to perform the ACF and PACF stationarity tests, that identify if a series has the same statistical properties over time, enabling forecasting.

**`Code example:`**
```python
title = 'ACF - Autocorrelation Function'
plt.figure(figsize=(12, 4))
plot_acf(df_series['Values'], lags=12)
plt.title(title)
plt.tight_layout()
plt.show()

title = 'PACF - Partial Autocorrelation Function'
max_lag = min(12, len(df_series) // 2 - 1)
fig, ax = plt.subplots(figsize=(7, 4))
plot_pacf(df_series['Values'], lags=max_lag, ax=ax)
plt.title(title)
plt.tight_layout()
plt.show()

result = adfuller(df_series['Values']) 
print('ADF Test:', round(result[0], 4)) # Augmented Dickey-Fuller
print('p-value:', round(result[1], 4)) 
if result[1] < 0.05: 
    print('The series is stationary') 
else:
    print('The series is not stationary') 
```

After checking the stationarity of the data, is necessary to choose the best model for forecasting. The predictive model is selected by comparing the two indicators, AIC and BIC, between different combinations of SARIMA parameters, choosing the model with the lowest values.

**`Code example:`**
```python
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

model = SARIMAX(df_series['Values'], order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
correction = model.fit(disp=False)
print(correction.summary())
```

Por o conjunto de dados ser de um período restrito de apenas 3 anos, a previsão se restringiu para olhar somente 6 meses a frente, ou seja, o primeiro semestre de 2025. Estabelecido isso, basta simular e colocar no gráfico. 

**`Code example:`**
```python
y = df_series['Values']
model = SARIMAX(y, order=(1, 0, 0), seasonal_order=(1, 0, 0, 12))  
correction = model.fit(disp=False)
n_steps = 6  
forecast = correction.forecast(steps=n_steps)

plt.figure(figsize=(14, 7))
plt.plot(y.index, y.values, label='Historical Data', linewidth=2.5, color='#1f77b4')
plt.plot(y.index, correction.fittedvalues, label='Adjusted Values', alpha=0.8, linestyle='--', color='#ff7f0e', linewidth=2)

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
plt.show()
```

O gráfico mostra as previsões geradas pelo modelo, a série histórica em azul, os valores ajustados em laranja tracejado e a previsão futura em vermelho. A projeção em vermelho indica uma tendência de queda para o primeiro semestre de 2025.

<p align='center'>
<img src='visualizations/Credit Demand Forecast - SARIMA.png' width='100%'>

## 📚 Results 

### Impacto no Crédito 

Através da Regressão Linear, identificou-se que os indicadores que mais impactam o valor de crédito rural, são a quantidade de crédito disponivel no mercado, a taxa Selic e ICE. A análise confirma a hipotese que juros elevados encarecem o capital e reduzem a oferta, gerando um ciclo de desconfiança que afeta diretamente na queda do ICE e na retração de novos investimentos.  

### Previsão de Crédito

A previsão com Serie Temporal projetou uma tendencia de queda na quantidade de crédito para o primeiro semestre de 2025. Esse movimento é reflexo direto do comportamento dos indicadores usados na regressão: a manutenção da Selic em patamares altos e a deterioração do ICE, que juntos atuaram como freios para a expansão do crédito no longo prazo.

### Conclusão

Embora o estudo tenha enfrentado limitações típicas de dados reais, como janelas temporais restritas, a robustez do modelo foi comprovada pela convergência com o cenário atual. A precisão do estudo é evidenciada por manchetes recentes: enquanto meu modelo previa a retração, a **CNN** confirmou que a [Concessão de crédito rural cai 16% no 1º semestre de 2025](https://www.youtube.com/watch?v=03ZWEk-Xmlg). Além disso, a **Serasa** apontava que a [Recuperação judicial cresce no agro em primeiro trimestre de 2025](https://www.serasaexperian.com.br/sala-de-imprensa/agronegocios/recuperacao-judicial-cresce-no-agro-em-primeiro-trimestre-de-2025-o-que-ainda-afeta-fatia-pequena-dentro-do-universo-de-credito-rural-revela-serasa-experian/), reforçando o impacto do cenário macroeconômico modelado neste estudo, validando a eficácia das variáveis escolhidas para antecipar crises no setor.

---

⭐ **If this project was helpful to you, consider giving the repository a star!**