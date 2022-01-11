from datetime import datetime
import requests
from pathlib import Path

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

def generate_link(separators):
    for sep in separators:
        url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{currentYear}_2.txt'
        url_to_test = requests.get(url)
        if url_to_test.status_code == 200:
            link_source = url
            return link_source
        else:
            continue

separators = ['-', '.', '_']
file_source_date = f'15-{month}-{currentYear}'
date_file = Path("most_recent_report.txt")
if date_file.is_file():
    with open(date_file, "r") as file:
        reader = "".join(file.readlines())
    if reader == file_source_date:
        print('This report is already uploaded.')
    else:
        file_source = generate_link(separators)
        print(file_source)
        with open(date_file, 'w') as file:
            file.write(f'15-{month}-{currentYear}')
else:
    with open("most_recent_report.txt", "w") as file:
        file_source = generate_link(separators)
        print(file_source)
        file.write(f'15-{month}-{currentYear}')

