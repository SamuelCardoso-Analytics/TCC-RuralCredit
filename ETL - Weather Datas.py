import pandas as pd
import glob

csv_settings = {
    'delimiter': ';',
    'decimal': ',',
    'header': None,
    'skiprows': 11,
    'usecols': [0, 1, 2, 3, 4]
}  # Dictionary for reading CSV

datas = []
path_file = r'C:\Users\your\path\dados_climaticos'

for file in glob.glob(f'{path_file}/*.csv'):

    # Collect the city name in the first line
    city = pd.read_csv(file, delimiter=';', header=None, nrows=1).iloc[0, 0][6:]
    df = pd.read_csv(file, **csv_settings)

    # Name columns and add them to a dataframe
    df.columns = ['Date_event', 'Days_rainfall', 'Total_rainfall', 'Atmospheric_pressure', 'Temperature']
    df['City'] = city
    datas.append(df)

table = pd.concat(datas, ignore_index=True)
columns = ['City'] + [col for col in table.columns if col != 'City']
table = table[columns]  # Change the order of columns

table.to_csv(r'C:\User\your\path\tb_indicadores_climaticos_inmet.csv', index=False)