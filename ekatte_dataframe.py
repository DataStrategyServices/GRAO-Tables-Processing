import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile


class EkatteDataframe:
    ekatte_url = "http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip"

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def unzip_files(self):
        content = requests.get(self.ekatte_url)
        first_zip = ZipFile(BytesIO(content.content))
        with first_zip as z:
            with z.open("Ekatte_xlsx.zip") as second_zip:
                z2_filedata = BytesIO(second_zip.read())
                with ZipFile(z2_filedata) as third_zip:
                    df_ekatte = pd.read_excel(third_zip.open("Ek_atte.xlsx"),
                                              converters={"ekatte": str})
                    df_ek_obl = pd.read_excel(third_zip.open("Ek_obl.xlsx"),
                                              converters={"ekatte": str})
                    df_ek_obst = pd.read_excel(third_zip.open("Ek_obst.xlsx"),
                                               converters={"ekatte": str})
        return df_ekatte, df_ek_obl, df_ek_obst

    def transformations(self):
        df_ek, df_obl, df_obst = self.unzip_files()
        df_ek["name"] = df_ek["name"].str.lower()
        df_ek = df_ek.iloc[1:, :]
        columns_list_ek = ["kmetstvo", "kind", "category", "altitude", "document", "tsb", "abc"]
        df_ek.drop(columns_list_ek, axis=1, inplace=True)
        df_ek = df_ek.rename({"ekatte": "ekatte", "name": "settlement",
                              "oblast": "region_code",
                              "obstina": "mun_code",
                              "t_v_m": "settlement_type"}, axis=1)

        # REGION DATAFRAME
        df_obl["name"] = df_obl["name"].str.lower()
        columns_list_obl = ["document", "abc", "region", "ekatte"]
        df_obl.drop(columns_list_obl, axis=1, inplace=True)
        df_obl = df_obl.rename({"oblast": "region_code", "name": "region"}, axis=1)

        # MUNICIPALITY DATAFRAME
        df_obst["name"] = df_obst["name"].str.lower()
        columns_list_obst = ["ekatte", "category", "document", "abc"]
        df_obst.drop(columns_list_obst, axis=1, inplace=True)

        df_obst = df_obst.rename({"obstina": "mun_code", "name": "municipality"}, axis=1)

        return df_ek, df_obl, df_obst

    def merge_frames(self):
        # MERGED DATAFRAME WITH EKATTE CODES AND SETTLEMENT, REGION, MUNICIPALITY
        df_ek, df_obl, df_obst = self.transformations()

        df_ek = pd.merge(df_ek, df_obl, how="left")
        df_ek = pd.merge(df_ek, df_obst, how="left")

        df_ek['settlement'] = df_ek['settlement_type'] + df_ek['settlement']

        columns_to_drop = ['settlement_type', 'region_code', 'mun_code']
        df_ek.drop(columns_to_drop, axis=1, inplace=True)

        df_ek = df_ek[["ekatte", "region", "municipality", "settlement"]]
        return df_ek

    def remove_ambigious_names(self):
        df_ekatte = self.merge_frames()
        columns_to_fix = ['region', 'municipality', 'settlement']

        # HARD CODE DUPLICATE NAMES TO AVOID MISTAKES
        df_ekatte.loc[df_ekatte["ekatte"] == "14461", "settlement"] = "с.бов (гара бов)"
        df_ekatte.loc[df_ekatte["ekatte"] == "14489", "settlement"] = "с.орешец (гара орешец)"
        df_ekatte.loc[df_ekatte["ekatte"] == "14475", "settlement"] = "с.лакатник(гара лакатник)"
        df_ekatte.loc[df_ekatte["ekatte"] == "18490", "settlement"] = "с.елин пелин (гара елин п"

        # replace wrong names to adjust for codes in NSI
        for column in columns_to_fix:
            df_ekatte[column] = df_ekatte[column].str.replace("добрич-селска", "добрич", regex=False)
            df_ekatte[column] = df_ekatte[column].str.replace("софийска", "софия", regex=False)
            df_ekatte[column] = df_ekatte[column].str.replace("столична", "софия", regex=False)
            df_ekatte[column] = df_ekatte[column].str.replace("софия (столица)", "софия", regex=False)
        return df_ekatte

    def merge_with_main(self):
        # merge main df and ekatte to combine the matches
        df_ekatte = self.remove_ambigious_names()
        df_with_ekattes = pd.merge(self.dataframe, df_ekatte, how="left")
        return df_with_ekattes
