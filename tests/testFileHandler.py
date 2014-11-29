from ks_graph import generalLinks
from ks_merge import merge
import MySQLdb
import unittest
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
        with open('tests/mysql_setting.txt', 'r') as f:
            mysql_config = f.readline()

            mysql_params = mysql_config.split(",")  
            localhost = mysql_params[0]
            user = mysql_params[1]
            password = mysql_params[2]
            db_name = mysql_params[3]
            cls.db = MySQLdb.connect(localhost, user, password, db_name)
            

    
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
