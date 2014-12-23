from ks_filehandler import filehandler
from ks_analytics import analytics
from ks_precompute import precompute
import ks_db_settings
import unittest

# import API
from register_raw_files import register_raw_filesCsvPy
from load_precompute_normalize import load_precompute_normalize_CsvPy
from user_analytics import measure_data


class TestUserAnaltics(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        #----------------------
        # set up db
        #----------------------
        cls.db = ks_db_settings.connect()

        cls.ks_fh = filehandler(cls.db)
        cls.ks_fh.reset()
        cls.company_id = 1
        
        register_raw_filesCsvPy("Sales",cls.company_id, cls.db)
        register_raw_filesCsvPy("CurrencyV2",cls.company_id, cls.db)
        register_raw_filesCsvPy("ComissionTax",cls.company_id, cls.db)
        register_raw_filesCsvPy("CountryRegion",cls.company_id, cls.db)
    
        ks_precompute = precompute(cls.db)
        ks_precompute.reset()
        #precompute
        load_precompute_normalize_CsvPy(cls.company_id, cls.db)
        newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(cls.company_id))
        cls.ks_analytics = analytics(cls.db)
        cls.ks_fh.registerFormula("", "UnitsSUM", "UnitsSUM", "sum(Units)", "sum")
                
                
    @classmethod        
    def tearDownClass(cls):
        #ks_db_settings.reset_all(cls.db)
        cls.db.close()

    def test_measure_data_day_output(self):
        m_id = self.ks_fh.getMeasureID("UnitsSUM")
        result = measure_data(self.db, self.company_id, [m_id], "day", "2014-06-01", "2014-06-02","ks_date")
        self.assertAlmostEqual(result['UnitsSUM']['2014-06-01'], 12.0)
        self.assertAlmostEqual(result['UnitsSUM']['2014-06-02'], 4.0)

    def test_measure_data_month_output(self):
        m_id = self.ks_fh.getMeasureID("UnitsSUM")
        result = measure_data(self.db, self.company_id, [m_id], "month", "2014-06-01", "2014-06-30","ks_date")
        print result
        self.assertAlmostEqual(result['UnitsSUM']['2014-06'], 16.0)

    def test_measure_data_quarter_output(self):
        m_id = self.ks_fh.getMeasureID("UnitsSUM")
        result = measure_data(self.db, self.company_id, [m_id], "quarter", "2014-04-01", "2014-06-30","ks_date")
        print result
        self.assertAlmostEqual(result['UnitsSUM']['2014-Q2'], 16.0)

if __name__ == '__main__':
    unittest.main()