import urllib.request
import pandas as pd
import datetime

class ReadMarkdownTable:
    """
        Class that reads data from url (format markdown)
        Returns the date from the markdown table
        Cleans labels for a merge needed in next step
        Removes unnecessary records
        Returns the data in pandas dataframe
    """
    def __init__(self, url, sep):
        self.url = url
        self.sep = sep
        self.markdown_df = self.markdown_table_to_df()

    def extract_date(self) -> datetime.date:
        """
            Extracts datе from markdown initial table,
            used for output file naming

            Return:
            pandas date
                """
        read_data = urllib.request.urlopen(self.url).read().decode('windows-1251')
        start_date = read_data.find('дата ')
        end_date = read_data.find('\r\n', start_date)
        date_var = read_data[start_date + 5:end_date]

        return pd.to_datetime(date_var, format="%d.%m.%Y").date()

    def markdown_table_to_df(self) -> pd.DataFrame:
        """
            Converts marksdown table to pandas dataframe
            - Extract the data from url
            - Loop through each line in file, searches for particular strings and
                creates a dataframe attribute accoraing to the strings found
            - Attributes added: Region / Municipality / Settlement / Permanent_population / Current_population

            Return: pandas DataFrame
        """
        data = urllib.request.urlopen(self.url)
        md_data = ""
        reg_str = ''
        for line in data:
            decoded_line = line.decode("windows-1251")
            decoded_line = decoded_line.replace('!', '|')
            if 'ОБЛАСТ:' in decoded_line:
                region_arg = decoded_line.split(' Т А Б Л И Ц А')
                region = region_arg[0].replace('ОБЛАСТ:', '').strip()
                reg_str = region
            elif 'ОБЩИНА:' in decoded_line:
                mun_arg = decoded_line.split(' НА НАСЕЛЕНИЕТО')
                municipality = mun_arg[0].replace('ОБЩИНА:', '').strip()
                reg_str = reg_str + self.sep + municipality
            elif 0 < decoded_line.count('община'):
                region_arg = decoded_line.split('община')
                region = region_arg[0].replace('област', '').strip()
                municipality = region_arg[1].strip()
                reg_str = region + self.sep + municipality
            elif (decoded_line.startswith('|С.')) or (' С.' in decoded_line):
                md_data = md_data + self.sep + reg_str + decoded_line.lstrip()
            elif 'ГР.' in decoded_line:
                md_data = md_data + self.sep + reg_str + decoded_line.lstrip()
            else:
                pass

        markdown_df = pd.DataFrame([x.split(self.sep) for x in md_data.split('\r\n')])
        return markdown_df

    def drop_attributes(self, list_to_drop: list) -> None:
        """
            Drops attributes that are not needed.
            Depending on the number of columns, different logic is applied (year / quarterly reports).
            # In future only quarterly reports would be available and the method might be updated.

            Args:
            list_to_drop: list of attributes in a df;

            Returns:
            None
        """
        if len(self.markdown_df.columns) > 8:
            self.markdown_df.drop(self.markdown_df.columns[list_to_drop],
                                  axis=1,
                                  inplace=True)
        else:
            self.markdown_df = self.markdown_df.iloc[:, 1:-2]

    def strip_columns(self, attributes: list) -> None:
        """
            Each item's label of the list is stripped as there are excess spaces
            to avoid merging issues

            Args:
                attributes: list of attributes in a df;

            Return:
                None
        """
        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.strip()

    def lower_columns(self, attributes: list) -> None:
        """
            Each item's label of the list is lowered, to avoid issues in merging

            Args:
                attributes: list of attributes in a df;

            Return:
                None
        """
        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.lower()

    def replace_letters(self, attributes: list, replace_values) -> None:
        """
            Replaces certain character with another;
            Specifically needed for inconsistencies in the data
            Example 'ъ' - 'ь'

        Args:
            attributes - list of attributes in a df;
            replace_values - two strings should be separated with comma
                - first - representing the string that needs to be replaces
                - second - representing the string that is replacing the previous.
        Return:
            None
        """
        split_replace_values = replace_values.split(',')
        value_to_rep = split_replace_values[0]
        value_replacing = split_replace_values[1]

        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.replace \
                (value_to_rep, value_replacing, regex=False)

    def change_labels(self, attribute: str, initial_label: str, change_label: str) -> None:
        """
            Changes an attribute value

        Args:
            attribute - certain attribute in a df - called as string;
            initial_label - value as string - available in the initial data - to be changed
            change_label - value that replaces the previous as string

        Returns: None
        """
        self.markdown_df.loc[self.markdown_df[attribute] ==
                             initial_label, attribute] = change_label

    def clear_kv_zk(self, attribute: str, string_list: list) -> None:
        """
            Filters out records that have no ekatte codes in NSI, or cannot be disambiguated
            Such records contain 'к.в, ж.к, к.к'
            and a problematic village so far (neighborhoods and one settlement)

            Args:
                attribute: an attribute with no needed value (ex: settlement)
                string_list: if the string from the list exists - the record should be filtered out

            Return:
                None
                """
        for string in string_list:
            filter = self.markdown_df[attribute].str.contains(string, na=False, regex=False)
            self.markdown_df = self.markdown_df[~filter]
