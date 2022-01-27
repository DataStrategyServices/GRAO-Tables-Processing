import unittest

from grao_table_processing.wikidata_uploader import *

class LoginWithCredentialsTestCase(unittest.TestCase):
    def setUp(self):
        self.credentials_path = "data/credentials.csv"

    def test_login(self):
        result = login_with_credentials(self.credentials_path)
        self.assertTrue(isinstance(result, wdi_login.WDLogin))

        #   TODO: Test uploading dummy data to wikidata to see if a login is successful

