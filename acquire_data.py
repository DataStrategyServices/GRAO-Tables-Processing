from datetime import datetime
import requests

currentMonth = datetime.now().month
currentYear = datetime.now().year

first_quarter = [3, 4, 5]
second_quarter = [6, 7, 8]
third_quarter = [9, 10, 11]
fourth_quarter = [1, 2]

if currentMonth in first_quarter:
    month = '03'
elif currentMonth in second_quarter:
    month = '06'
elif currentMonth in third_quarter:
    month = '09'
elif currentMonth in fourth_quarter:
    month = '12'
    currentYear -= 1
else:
    month = '12'

separators = ['-', '.', '_']


for sep in separators:
    url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{currentYear}_2.txt'
    url_to_test = requests.get(url)
    if url_to_test.status_code == 200:
        file_source = url
        break
    else:
        continue
