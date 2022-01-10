from datetime import datetime
import requests

currentMonth = datetime.now().month
currentMonth = str(currentMonth).rjust(2, '0')
currentYear = datetime.now().year

first_quarter = [3, 4, 5]
second_quarter = [6, 7, 8]
third_quarter = [9, 10, 11]
fourth_quarter = [12, 1, 2]

url_to_test = requests.get(f'https://www.grao.bg/tna/t41nm-15-{currentMonth}-{currentYear}_2.txt')


if url_to_test.status_code == 200:
    sep = '-'
else:
    sep = '.'

file_source = f'https://www.grao.bg/tna/t41nm-15{sep}{currentMonth}{sep}{currentYear}_2.txt'
print(file_source)