from datetime import datetime
import requests
from pathlib import Path
from dateutil.relativedelta import relativedelta


class DataURL:
    current_date = datetime.today().date()
    current_month = current_date.month
    current_year = current_date.year
    date_file = Path("most_recent_report.txt")

    @staticmethod
    def is_in_quarter(month: int, year: int) -> [str, int]:
        if month in (1, 2):
            year -= 1
            month = '12'
        else:
            month = (month//3)*3
            month = str(month).rjust(2, "0")
        return month, year

    @staticmethod
    def is_skipped_report(file_date: str, date_now: datetime.date) -> [datetime.date, datetime.date]:
        date_object = datetime.strptime(file_date, '%d-%m-%Y')
        date_object = date_object.date() + relativedelta(months=3)
        if date_now > date_object:
            return date_object.month, date_object.year

    @staticmethod
    def find_correct_link(month: str, year: int) -> str:
        separators = ['-', '.', '_']
        for sep in separators:
            url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{year}_2.txt'
            url_to_test = requests.get(url)
            if url_to_test.status_code == 200:
                link_source = url
                return link_source
            else:
                continue

    def url_constructor(self) -> [datetime.date, datetime.date]:
        if self.date_file.is_file():
            with open(self.date_file, "r") as file:
                reader = "".join(file.readlines())
                if len(reader) == 0:
                    return self.is_in_quarter(self.current_month, self.current_year)
                else:
                    try:
                        month, year = self.is_skipped_report(reader, self.current_date)
                    except TypeError:
                        with open("processed_dates_log.log", "a") as log_file:
                            log_file.write(f'Improper loading on {datetime.today()}' + "\n")
                        raise RuntimeError("Most recent report already processed. Run at a later date!")
                    return self.is_in_quarter(month, year)
        else:
            return self.is_in_quarter(self.current_month, self.current_year)

    def generate_data_url(self) -> str:
        month_to_use, year_to_use = self.url_constructor()
        file_source = self.find_correct_link(month_to_use, year_to_use)
        return file_source

    def update_date_file(self) -> None:
        month_to_use, year_to_use = self.url_constructor()
        file_source = self.generate_data_url()
        with open(self.date_file, 'w') as file:
            file.write(f'15-{month_to_use}-{year_to_use}')
        with open("processed_dates_log.log", "a") as file:
            file.write(f'{file_source}' + "\n")
        return print('Date File Updated!')



