import urllib.request
import pandas as pd


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

    def extract_date(self):
        read_data = urllib.request.urlopen(self.url).read().decode('windows-1251')
        start_date = read_data.find('дата ')
        end_date = read_data.find('\r\n', start_date)
        date_var = read_data[start_date + 5:end_date]

        return pd.to_datetime(date_var, format="%d.%m.%Y").date()

    def markdown_table_to_df(self):
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

    def drop_attributes(self, list_to_drop):
        if len(self.markdown_df.columns) > 8:
            self.markdown_df.drop(self.markdown_df.columns[list_to_drop],
                                  axis=1,
                                  inplace=True)
        else:
            self.markdown_df = self.markdown_df.iloc[:, 1:-2]

    def strip_columns(self, attributes):
        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.strip()

    def lower_columns(self, attributes):
        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.lower()

    def replace_letters(self, attributes, replace_values):
        split_replace_values = replace_values.split(',')
        value_to_rep = split_replace_values[0]
        value_replacing = split_replace_values[1]

        for att in attributes:
            self.markdown_df[att] = self.markdown_df[att].str.replace \
                (value_to_rep, value_replacing, regex=False)

    def change_labels(self, attribute, initial_label, change_label):
        self.markdown_df.loc[self.markdown_df[attribute] ==
                             initial_label, attribute] = change_label

    # def remove_settlement_by_index(self, attributes, string_to_be_dropped_set, municipality_label):
    #     indexes = self.markdown_df.index[(self.markdown_df[attributes[0]] == string_to_be_dropped_set) &
    #                                      (self.markdown_df[attributes[1]] == municipality_label)].tolist()
    #     self.markdown_df.drop(indexes, inplace=True)

    def clear_kv_zk(self, attribute, string_list):
        for string in string_list:
            filter = self.markdown_df[attribute].str.contains(string, na=False, regex=False)
            self.markdown_df = self.markdown_df[~filter]
