import urllib.request
import codecs
import pandas as pd

url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"
data = urllib.request.urlopen(url)

a = ""
sep='|'
obl_str=''
for line in data:
    decoded_line = line.decode("windows-1251")
    if decoded_line.count('област') > 0:
        obl_arr = decoded_line.split('община')
        obl_str = obl_arr[0].replace('област','').strip()+sep+obl_arr[1].strip()
    elif decoded_line.startswith('|С.') == True:
        a = a + sep+obl_str + decoded_line
    elif decoded_line.startswith('|ГР.') == True:
        a = a + sep + obl_str + decoded_line
    else:
        pass

with codecs.open("dump.txt", "w", "utf-8") as stream:   # or utf-8
    stream.write(a)

df = pd.read_csv('dump.txt', sep='|', header = None)
df = df.iloc[:, 1:-2]
df.columns = ['Област', 'Община', 'Населено място', 'Постоянен адрес', 'Настоящ адрес']

breakpoint()