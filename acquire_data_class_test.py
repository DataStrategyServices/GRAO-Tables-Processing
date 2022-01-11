from datetime import datetime
import requests
from pathlib import Path
from dateutil.relativedelta import relativedelta

class DataURL:
    def __init__(self, current_date, current_month, current_year, date_file):
        self.current_date = current_date
        self.current_month = current_month
        self.current_year = current_year
        self.date_file = date_file
        self.url = self.url_constructor()

    @staticmethod
    def is_in_quarter(month, year):
        if month in (1, 2):
            year -= 1
            month = '12'
        else:
            month = (month//3)*3
            month = str(month).rjust(2, "0")
        return month, year

    @staticmethod
    def is_skipped_report(file_date, date_now):
        date_object = datetime.strptime(file_date, '%d-%m-%Y')
        date_object = date_object.date() + relativedelta(months=3)
        if date_now > date_object:
            return date_object.month, date_object.year

    @staticmethod
    def generate_link(month, year):
        separators = ['-', '.', '_']
        for sep in separators:
            url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{year}_2.txt'
            url_to_test = requests.get(url)
            if url_to_test.status_code == 200:
                link_source = url
                return link_source
            else:
                continue

    def url_constructor(self):
        if self.date_file.is_file():
            with open(self.date_file, "r") as file:
                reader = "".join(file.readlines())
                if len(reader) == 0:
                    month_to_use, year_to_use = self.is_in_quarter(self.current_month, self.current_year)
                else:
                    try:
                        month, year = self.is_skipped_report(reader, self.current_date)
                    except TypeError:
                        with open("processed_dates_log.log", "a") as log_file:
                            log_file.write(f'Improper loading on {datetime.today()}' + "\n")
                        raise RuntimeError("Most recent report already processed. Run at a later date!")
                    month_to_use, year_to_use = self.is_in_quarter(month, year)
        else:
            month_to_use, year_to_use = self.is_in_quarter(self.current_month, self.current_year)
        file_source = self.generate_link(month_to_use, year_to_use)
        with open(self.date_file, 'w') as file:
            file.write(f'15-{month_to_use}-{year_to_use}')
        with open("processed_dates_log.log", "a") as file:
            file.write(f'{file_source}' + "\n")
        return file_source

    # def get_data_link(self):
    #     link = self.url_constructor()
    #     return link
current_date = datetime.today().date()
current_month = current_date.month
current_year = current_date.year
date_file = Path("most_recent_report.txt")

url_object = DataURL(current_date, current_month, current_year, date_file)
