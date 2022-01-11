from datetime import datetime, timedelta
import requests
from pathlib import Path
from dateutil.relativedelta import relativedelta


def is_in_quarter(month, year):
    if month in (1, 2):
        year -=1
    month = month//3*3
    month = str(month).rjust(2, "0")
    return month, year


def is_skipped_report(file_date, current_date):
    date_object = datetime.strptime(file_date, '%d-%m-%Y')
    date_object = date_object.date() + relativedelta(months=3)
    if current_date > date_object:
        return date_object.month, date_object.year


def generate_link(separators, month, year):
    for sep in separators:
        url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{year}_2.txt'
        url_to_test = requests.get(url)
        if url_to_test.status_code == 200:
            link_source = url
            return link_source
        else:
            continue


currentDate = datetime.today().date()
currentMonth = currentDate.month
currentYear = currentDate.year

date_file = Path("most_recent_report.txt")
if date_file.is_file():
    with open(date_file, "r") as file:
        reader = "".join(file.readlines())
        if len(reader) == 0:
            month_to_use, year_to_use = is_in_quarter(currentMonth, currentYear)
        else:
            try:
                month, year = is_skipped_report(reader, currentDate)
            except TypeError as e:
                with open("processed_dates_log.log", "a") as file:
                    file.write(f'Improper loading on {datetime.today()}' + "\n")
                raise RuntimeError("Most recent report already processed. Run at a later date!")
            month_to_use, year_to_use = is_in_quarter(month, year)
else:
    month_to_use, year_to_use = is_in_quarter(currentMonth, currentYear)

seps = ['-', '.', '_']

file_source = generate_link(seps, month_to_use, year_to_use)
print(file_source)
with open(date_file, 'w') as file:
    file.write(f'15-{month_to_use}-{year_to_use}')
with open("processed_dates_log.log", "a") as file:
    file.write(f'{file_source}' + "\n")


