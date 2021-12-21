import urllib.request
import codecs
import pandas as pd

url = "https://www.grao.bg/tna/tadr2020.txt" #https://www.grao.bg/tna/t41nm-15.12.2021_2.txt
data = urllib.request.urlopen(url)

a = ""
sep='|'
obl_str=''
for line in data:
    decoded_line = line.decode("windows-1251")
    if 0 < decoded_line.count('област'):
        obl_arr = decoded_line.split('община')
        oblast = obl_arr[0].replace('област', '').strip()
        #breakpoint()
        if len(obl_arr) > 2:
            obshtina = obl_arr[1].strip()
        else:
            obshtina = oblast
        obl_str = oblast + sep + obshtina
    elif decoded_line.startswith('|С.'):
        a = a + sep+obl_str + decoded_line
    elif decoded_line.startswith('|ГР.'):
        a = a + sep + obl_str + decoded_line
    else:
        pass

#print(a)


# with codecs.open("dump.txt", "w", "utf-8") as stream:   # or utf-8
#     stream.write(a)

df = pd.DataFrame([x.split('|') for x in a.split('\r\n')])
df = df.iloc[:, 1:-2]
#df.columns = ['Област', 'Община', 'Населено място', 'Постоянен адрес', 'Настоящ адрес']
print(df)

# breakpoint()