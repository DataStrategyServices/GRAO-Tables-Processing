import urllib.request
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile

#url = "https://www.grao.bg/tna/tadr2020.txt"
#url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"
from pandas.io.common import urlopen

url = "https://www.grao.bg/tna/tadr-2005.txt"

data = urllib.request.urlopen(url)

a = ""
sep = '|'
reg_str = ''
for line in data:
    decoded_line = line.decode("windows-1251")
    decoded_line = decoded_line.replace('!', '|')
    if 'ОБЛАСТ:' in decoded_line:
        region_arg = decoded_line.split()
        region = region_arg[0].replace('ОБЛАСТ:', '').strip()
        reg_str = region
    elif 'ОБЩИНА:' in decoded_line:
        mun_arg = decoded_line.split(' НА НАСЕЛЕНИЕТО')
        municipality= mun_arg[0].replace('ОБЩИНА:', '').strip()
        reg_str = reg_str + sep + municipality
    elif 0 < decoded_line.count('община'):
        region_arg = decoded_line.split('община')
        region = region_arg[0].replace('област', '').strip()
        municipality = region_arg[1].strip()
        reg_str = region + sep + municipality
    elif (decoded_line.startswith('|С.')) or (' С.' in decoded_line):
        a = a + sep + reg_str + decoded_line.lstrip()
    elif 'ГР.' in decoded_line:
        a = a + sep + reg_str + decoded_line.lstrip()
    else:
        pass

#TO-DO Q codes, Ekatte, complete dataframes

#TO-DO date from inside markdown or URL, date into CSV -> export dataframe to CSV, for each quarter or year


df = pd.DataFrame([x.split('|') for x in a.split('\r\n')])


if len(df.columns) > 8:
    column_list = [0, 5, 6, 7, 9, 10, 11, 12]
    df.drop(df.columns[column_list],
            axis=1,
            inplace=True)
else:
    df = df.iloc[:, 1:-2]

df.columns = ['region', 'municipality', 'settlement', 'permanent_population', 'current_population']

df['region'] = df['region'].str.lower()
df['municipality'] = df['municipality'].str.lower()
df['settlement'] = df['settlement'].str.lower()
df['settlement'] = df['settlement'].str.replace('гр.', '', regex=False)
df['settlement'] = df['settlement'].str.replace('с.', '', regex=False)
df['settlement'] = df['settlement'].str.strip()

df['settlement'] = df['settlement'].str.replace('ь', 'ъ', regex=False)
df['settlement'] = df['settlement'].str.replace('ъо', 'ьо', regex=False)


###### GET EKATTE CODES ##############

ekatte_url = 'http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip'
content = requests.get(ekatte_url)
first_zip = ZipFile(BytesIO(content.content))

with first_zip as z:
    with z.open('Ekatte_xlsx.zip') as second_zip:
        z2_filedata = BytesIO(second_zip.read())
        with ZipFile(z2_filedata) as second_zip:
            df_ekate = pd.read_excel(second_zip.open('Ek_atte.xlsx'), converters={'ekatte': str})
            df_ek_obl = pd.read_excel(second_zip.open('Ek_obl.xlsx'), converters={'ekatte': str})
            df_ek_obst = pd.read_excel(second_zip.open('Ek_obst.xlsx'), converters={'ekatte': str})

df_ekate['name'] = df_ekate['name'].str.lower()
df_ek_obl['name'] = df_ek_obl['name'].str.lower()
df_ek_obst['name'] = df_ek_obst['name'].str.lower()

print(df.head(10))
print(df_ekate.head(10))
print(df_ek_obl.head(10))
print(df_ek_obst.head(10))

df = pd.merge(df, df_ek_obl[['name', 'oblast']], how='left', left_on = 'region', right_on = 'name').drop(columns= ['name'])
df = pd.merge(df, df_ek_obst[['name', 'obstina']], how='left', left_on = 'municipality', right_on = 'name').drop(columns= ['name'])

df = pd.merge(df, df_ekate[['ekatte','name', 'oblast', 'obstina']], how='left', left_on = ['settlement','oblast','obstina'], right_on = ['name','oblast','obstina']).drop(columns= ['name'])

print(df.head(1000))
#TO-DO DICTIONARY FOR MISSTYPED NAMES, REPLACEMENT IN DATAFRAMES TO EQUALIZE