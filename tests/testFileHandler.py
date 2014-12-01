from ks_graph import generalLinks
from ks_merge import merge
import MySQLdb
import unittest
import ks_db_settings
from ks_filehandler import filehandler

# import API
from register_raw_files import register_raw_files
from load_precompute_normalize import load_precompute_normalize

class TestMerge(unittest.TestCase):
    
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

    
    def test_precompute(self):
        ks_fh = filehandler(self.db)
        #ks_fh.reset()
        company = "company 1"
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        ks_fh.updateMeasureTable(first_table, company)
        
      
        
    @classmethod    
    def tearDownClass(cls):
        cls.db.close()    

 
if __name__ == '__main__':
    unittest.main()
