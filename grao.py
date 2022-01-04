import urllib.request
import fuzzymatcher
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile


#url = "https://www.grao.bg/tna/tadr2020.txt"
url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"

#url = "https://www.grao.bg/tna/tadr-2005.txt"

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
    elif "МАН." in decoded_line:
        a = a + sep + reg_str + decoded_line.lstrip()
    else:
        pass

#TO-DO Q codes, Ekatte, complete dataframes

#TO-DO date from inside markdown or URL, date into CSV -> export dataframe to CSV, for each quarter or year


df = pd.DataFrame([x.split('|') for x in a.split('\r\n')])

#check if raw data was properly saved
#df.to_csv('df_check.csv', sep=',', encoding='utf-8', index=False)

if len(df.columns) > 8:
    column_list = [0, 5, 6, 7, 9, 10, 11, 12]
    df.drop(df.columns[column_list],
            axis=1,
            inplace=True)
else:
    df = df.iloc[:, 1:-2]

df.columns = ['region', 'municipality', 'settlement', 'permanent_population', 'current_population']

# a lot of repeating code to turn into 2-3 functions
columns_to_transform = ['region', 'municipality', 'settlement']
for column in columns_to_transform:
    df[column] = df[column].str.lower()


for column in columns_to_transform:
    df[column] = df[column].str.replace('гр.', '', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('с.', '', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('ман.', '', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('ь', 'ъ', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('ъо', 'ьо', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('добричка', 'добрич', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('добрич-селска', 'добрич', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('добрич-град', 'добрич', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('софийска', 'софия', regex=False)

for column in columns_to_transform:
    df[column] = df[column].str.replace('столична', 'софия', regex=False)

df['settlement'] = df['settlement'].str.strip()

#combined columns
df['combined_column'] = df['region'] + '_' + df['municipality'] + '_' + df['settlement']


#TO DO SAVE DATAFRAME TO CSV WITH DATE AS FILENAME

###### GET EKATTE CODES ##############

ekatte_url = 'http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip'
content = requests.get(ekatte_url)
first_zip = ZipFile(BytesIO(content.content))

with first_zip as z:
    with z.open('Ekatte_xlsx.zip') as second_zip:
        z2_filedata = BytesIO(second_zip.read())
        with ZipFile(z2_filedata) as second_zip:
            df_ekatte = pd.read_excel(second_zip.open('Ek_atte.xlsx'), converters={'ekatte': str})
            df_ek_obl = pd.read_excel(second_zip.open('Ek_obl.xlsx'), converters={'ekatte': str})
            df_ek_obst = pd.read_excel(second_zip.open('Ek_obst.xlsx'), converters={'ekatte': str})


# EKATTE DATAFRAME
df_ekatte['name'] = df_ekatte['name'].str.lower()
df_ekatte = df_ekatte.iloc[1: , :]
columns_list_ek = ['t_v_m', 'kmetstvo', 'kind', 'category', 'altitude', 'document', 'tsb', 'abc']
df_ekatte.drop(columns_list_ek,
        axis=1,
        inplace=True)
df_ekatte = df_ekatte.rename({'ekatte': 'ekatte', 'name': 'settlement', 'oblast': 'region_code', 'obstina': 'mun_code'}, axis=1)


# REGION DATAFRAME
df_ek_obl['name'] = df_ek_obl['name'].str.lower()
columns_list_obl = ['document', 'abc', 'region', 'ekatte']
df_ek_obl.drop(columns_list_obl,
        axis=1,
        inplace=True)
df_ek_obl = df_ek_obl.rename({'oblast': 'region_code', 'name': 'region'}, axis=1)


# MUNICIPALITY DATAFRAME
df_ek_obst['name'] = df_ek_obst['name'].str.lower()
columns_list_obst = ['ekatte', 'category', 'document', 'abc']
df_ek_obst.drop(columns_list_obst,
        axis=1,
        inplace=True)
df_ek_obst = df_ek_obst.rename({'obstina': 'mun_code', 'name': 'municipality'}, axis=1)

# MERGED DATAFRAME WITH EKATTE CODES AND SETTLEMENT, REGION, MUNICIPALITY
df_ekatte = pd.merge(df_ekatte, df_ek_obl,
                         how='left')
df_ekatte = pd.merge(df_ekatte, df_ek_obst, how='left')
df_ekatte = df_ekatte[['ekatte', 'region', 'municipality', 'settlement', 'region_code', 'mun_code']]


for column in columns_to_transform:
    df_ekatte[column] = df_ekatte[column].str.replace('добрич-селска', 'добрич', regex=False)

for column in columns_to_transform:
    df_ekatte[column] = df_ekatte[column].str.replace('софийска', 'софия', regex=False)

for column in columns_to_transform:
    df_ekatte[column] = df_ekatte[column].str.replace('столична', 'софия', regex=False)

for column in columns_to_transform:
    df_ekatte[column] = df_ekatte[column].str.replace('софия (столица)', 'софия', regex=False)

# combined columns
df_ekatte['combined_column'] = df_ekatte['region'] + '_' + df_ekatte['municipality'] + '_' + df_ekatte['settlement']

# merge main df and ekatte to combine the matches and avoid mistakes in the next merges
df4 = pd.merge(df, df_ekatte, how='left')

#df4['combined_column'] = df4['region'] + '_' + df4['municipality'] + '_' + df4['settlement']

# Fuzzy matching between df_ekatte and df4 so as to avoid mismatching the "duplicate" values

left_on = ['ekatte', 'combined_column']

right_on = ['ekatte', 'combined_column']

# Matching the previously 1:1 matched initial dataframe with the ekatte dataframe,
# and then fuzzy matching to ekatte one, again to fill gaps
matched_results = fuzzymatcher.fuzzy_left_join(df4, df_ekatte, left_on, right_on, right_id_col= 'ekatte')

matched_results.to_csv('matched_results.csv', sep=',', encoding='utf-8', index=False)
df_ekatte.to_csv('df_ekatte.csv', sep=',', encoding='utf-8', index=False)



