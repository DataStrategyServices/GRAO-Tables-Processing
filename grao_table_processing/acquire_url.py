from datetime import datetime
import requests
from pathlib import Path
from dateutil.relativedelta import relativedelta


class DataURL:
    """
    The class takes today's date, month and year and uses them
    to compare against a log file of the most recent processed GRAO data.
    It then produces a working link according to a pattern. The pattern
    has some versatility, and it can be made even more versatile, if necessary.
    Over the last two years there seems to be a consistency in GRAO tables file,
    which is a promising tendency.
    The entire class produces a single URL string, which is used to load a markdown table
    that undergoes transformations until its data is ready to be uploaded to WikiData.
    It also has a log of which dates have passed and of errors that happened.
    If there is no most recent file, it will attempt to upload the closest report to the current date,
    which relates to the nearest quarter 03/06/09/12.
    If it reaches a date beyond the current, the cycle breaks and it will be ready to run
    after a while, so that it doesn't spend processing power and bandwidth uploading
    the same data twice.
    Attributes:
        current_date: Class attribute - today's date
        current_month: Class attribute - the current month
        current_year: Class attribute - the current year
    """
    current_date = datetime.today().date()
    current_month = current_date.month
    current_year = current_date.year
    date_file = Path("logs/most_recent_report.txt")

    @staticmethod
    def is_in_quarter(month: int, year: int) -> [str, int]:
        """
        The static method takes the current month, or the month of the last update
        and checks in which quarter it falls - 03/06/09/12. When the month is 1 or 2,
        the initial logic to divide by 3 and then multiply doesn't work,
        as a result it needs a different check, it also needs to go back 1 year, because
        the first two months fall into the 4th quarter report of the previous year,
        as Q1 report is released on month 3.

        Args:
            month: integer, the month
            year: integer, the year

        Return:
            month, year - a tuple of str, int, used in the link generation
        """
        if month in (1, 2):
            year -= 1
            month = '12'
        else:
            month = (month//3)*3
            month = str(month).rjust(2, "0")
        return month, year

    @staticmethod
    def last_processed_report(file_date: str, date_now: datetime.date) -> [datetime.date, datetime.date]:
        """
        The static method takes a date from the helper file, which shows the last processed GRAO report
        it compares it to the current date, and if it's prior to today and further than 3 months away
        returns the date and year of the next report that needs to be loaded in the URL generation.

        Args:
            file_date: str, a simple DD-MM-YYYY string read from a helper file
            date_now: a datetime.date object, shows the current date

        Return:
            date_object.month, date_object.year - a tuple of two datetime objects, for month and year
        """
        date_object = datetime.strptime(file_date, '%d-%m-%Y')
        date_object = date_object.date() + relativedelta(months=3)
        if date_now > date_object:
            return date_object.month, date_object.year

    @staticmethod
    def find_correct_link(month: str, year: int) -> str:
        """
        The links from GRAO follow a certain pattern that appears to be the same
        for as far as quarterly reports are still online. As a result,
        the logic checks whether the link is active for the specific month and year,
        and with the specific separators. It returns a working link that can be used to extract data.

        Args:
            month: string, the month but with a 0 in front for the single-digit months
            year: integer, the year
        Return:
            url - a string which is the url that will be used to extract data
        """
        separators = ['-', '.', '_']
        for sep in separators:
            url = f'https://www.grao.bg/tna/t41nm-15{sep}{month}{sep}{year}_2.txt'
            url_to_test = requests.get(url)
            if url_to_test.status_code == 200:
                return url
            else:
                continue

    def url_constructor(self) -> [datetime.date, datetime.date]:
        """
        This method is called URL Constructor because it does the necessary checks
        to inspect whether there is a log file with a most recent report,
        and uses the next possible dates. If there's no helper file, or it is empty
        it instead loads the most recent report and attempts to upload it.
        The WikiData uploading logic works in such a way that data that's already present
        will not be re-uploaded.
        Having an empty most recent file or it missing entirely are very rare scenarios.
        Each processed date is saved to a separate log with its entire link, failures to load
        are also recorded.
        If a date that's already processed is loaded, it throws a RuntimeError warning that
        it is already processed.

        Return:
            month, year: returns a tuple of datetime objects that are used in link generation
        """
        if self.date_file.is_file():
            with open(self.date_file, "r") as file:
                reader = "".join(file.readlines())
                if len(reader) == 0:
                    return self.is_in_quarter(self.current_month, self.current_year)
                else:
                    try:
                        month, year = self.last_processed_report(reader, self.current_date)
                    except TypeError:
                        with open("logs/processed_dates.log", "a+") as log_file:
                            log_file.write(f'Improper loading on {datetime.today()}' + "\n")
                        raise RuntimeError("Most recent report already processed. Run at a later date!")
                    return self.is_in_quarter(month, year)
        else:
            return self.is_in_quarter(self.current_month, self.current_year)

    def generate_data_url(self) -> str:
        """
        This is a wrapper method that calls the url constructor and find correct link methods
        it simply uses the results of both the methods to build the file source, so that it can
        send it forward.

        Return:
            file_source - a string, that is a functional URL to extract data from.
        """
        month_to_use, year_to_use = self.url_constructor()
        file_source = self.find_correct_link(month_to_use, year_to_use)
        return file_source

    def update_date_file(self) -> None:
        """
        After the entire process is ran and the link is generated,
        the log files and the helper files are uploaded with the proper most recent report.
        The log gets the entire URL for logging and archive purposes.

        Return:
            None
        """
        month_to_use, year_to_use = self.url_constructor()
        file_source = self.generate_data_url()
        with open(self.date_file, 'w') as file:
            file.write(f'15-{month_to_use}-{year_to_use}')
        with open("logs/processed_dates.log", "a+") as file:
            file.write(f'{file_source}' + "\n")
        return print('Date File Updated!')
