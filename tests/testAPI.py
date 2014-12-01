import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../modules/"))
print sys.path

from ks_filehandler import filehandler
from ks_merge import merge
from ks_analytics import analytics
from ks_precompute import precompute
import ks_db_settings
import time
import MySQLdb
import unittest


# import API
from register_raw_files import register_url_data
from load_precompute_normalize import load_precompute_normalize_URL
from user_analytics import measure_data
from register_raw_files import registerFormula


import unittest
import urllib2

class TestAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #----------------------
        # set up db
        #----------------------

        cls.db = MySQLdb.connect(
                ks_db_settings.setting('host'), 
                ks_db_settings.setting('user'), 
                ks_db_settings.setting('password'), 
                ks_db_settings.setting('database'))



    def test_load_files(self):
        ks_fh = filehandler(self.db)
        ks_fh.reset()
        company_id = 1
        first_table = "http://199.127.226.118/sqlengine/Sales.csv"
        second_table = "http://199.127.226.118/sqlengine/Currencyv2.csv"
        third_table = "http://199.127.226.118/sqlengine/CountryRegion.csv"
        fourth_table = "http://199.127.226.118/sqlengine/ComissionTax.csv"
        register_url_data(first_table, "Sales", company_id, self.db)
        register_url_data(second_table, "Currencyv2", company_id, self.db)
        register_url_data(third_table, "CountryRegion", company_id, self.db)
        register_url_data(fourth_table, "ComissionTax",company_id, self.db)

        ks_precompute = precompute(self.db)
        ks_precompute.reset()
        precompute
        ks_merge = merge(self.db)
        load_precompute_normalize_URL(company_id, self.db)

        ks_fh.registerFormula("", "Plus", "Plus", "Units+RoyaltyPrice", "sum")
        ks_fh.registerFormula("", "Mult", "Mult", "Units*RoyaltyPrice", "sum")

        plus_id = ks_fh.getMeasureID("Plus")
        mult_id = ks_fh.getMeasureID("Mult")
        units_id = ks_fh.getMeasureID("Units")
        royality_id = ks_fh.getMeasureID("RoyaltyPrice")

        # MEASURE DATA DEMO raw_facts + measures with formulas
        print(measure_data(self.db, company_id, [plus_id,mult_id,units_id,royality_id],"day","2014-06-01","2014-06-01"))

        # MEASURE DATA DEMO raw_facts group by
        print(measure_data(self.db, company_id, [units_id, royality_id],"day","2014-06-01","2014-06-01","Region"))

        self.db.commit()

if __name__ == '__main__':
    unittest.main()
