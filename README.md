# 📊 Impact of Macroeconomic Variables and Agricultural Production Factors on the Demand for Rural Credit

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Status](https://img.shields.io/badge/Status-Completed-success.svg)]()

## 📝 Context

### Description

Developed as a Final Course Project (Business Management Technology), This project seeks to understand the sensitivity of rural credit demand to macroeconomic variables and productive factors. Additionally, it explores how it is possible to prepare for different economic scenarios using statistical models. 

### Problem

Agriculture, the main sector of the Brazilian economy, is very sensitive to rural credit, therefore, knowing which indicators have the greatest impact on demand is important to prepare for adverse situations and take advantage of market opportunities. 

### Goal

Identify and quantify the relationships between macroeconomic variables and agricultural production indicators with the demand for rural credit, and use statistical models to find the best scenarios.

### Fundamental Concepts

**Macroeconomic**: They are indicators or tools for controlling economic and monetary policy, such as GDP (Gross Domestic Product), interest rates, exchange rates, and inflation.

**Agriculture**: They are variables that affect agricultural production, such as temperature and rainfall, for example, as they are natural physical factors that have an influence.

**Rural Credit**: Financing modality aimed at rural producers, based on production and price expectations, such as commodity prices and harvest volume. 

## 📂 Data Used 

### Data Sources

- [**Banco Central do Brasil (BCB)**](https://dadosabertos.bcb.gov.br/): Selic rate (Brazil's base interest rate), Exchange rate and Credit Contracts - API
- [**Fundação Getulio Vargas (FGV)**](https://autenticacao-ibre.fgv.br/ProdutosDigitais/Login): IGPM (price inflation index), ICE (business confidence index) e PIB (GDP) - CSV
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
│   ├── tb_ice_fgv.csv              
│   ├── tb_igpm_fgv.csv              
│   └── tb_pib_fgv.csv                                       
├── 📂 scripts                                    # Python scripts 
│   ├── Collection and Creation of Tables.py
│   ├── Datas Cleansing and Processing.py
│   ├── Linear Regression.py
│   └── Time Series.py
├── 📂 visualizations                             # Graphs and figures
│   ├── correlacao.png
│   ├── serie_temporal.png
│   └── others.png
└── README.md                                     # This file
```


---

⭐ **If this project was helpful to you, consider giving the repository a star!**