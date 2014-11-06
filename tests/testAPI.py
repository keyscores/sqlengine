from ks_graph import generalLinks
from ks_merge import precompute
from ks_merge import merge
import MySQLdb
import unittest
from ks_filehandler import filehandler

# import API
from register_raw_files import register_raw_files
from load_precompute_normalize import load_precompute_normalize
from user_analytics import measure_data
class TestMerge(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        #----------------------
        # set up db
        #----------------------
        with open('mysql_setting.txt', 'r') as f:
            mysql_config = f.readline()

            mysql_params = mysql_config.split(",")  
            localhost = mysql_params[0]
            user = mysql_params[1]
            password = mysql_params[2]
            db_name = mysql_params[3]
            cls.db = MySQLdb.connect(localhost, user, password, db_name)
            
    
    def test_register_files(self):
        #company_name = "2"
        #first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        #second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"    
        #third_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        #fourth_table = "./ks_filehandler/ks_filehandler/data/ComissionTax.csv"    
        #register_raw_files(first_table,company_name, self.db)
        #register_raw_files(second_table,company_name, self.db)
        #register_raw_files(third_table,company_name, self.db)
        #register_raw_files(fourth_table,company_name, self.db)
    
        ##ks_precompute = precompute(self.db)
        ##ks_precompute.reset()
        #precompute
        #ks_fh = filehandler(self.db)
        #company_name = "2"
        #load_precompute_normalize(company_name, self.db)
        
        # get measure
        #ks_fh = filehandler(self.db)
        #company_id = "2"
        
        print(measure_data(self.db, 2, [3,4],"day","0006-01-14","0006-01-14"))
        print(measure_data(self.db, 2, [3,4],"day","0006-02-14","0006-02-14"))
        print(measure_data(self.db, 2, [3,4],"day","0006-01-14","0006-02-14"))
        
        
        print(measure_data(self.db, 2, [3,4],"day","0006-01-14","0006-02-14","Region"))
        print(measure_data(self.db, 2, [3,4],"day","0006-01-14","0006-02-14","RightsHolder"))
        print(measure_data(self.db, 2, [3,4],"day","0006-01-14","0006-02-14","CountryCode"))
        
        
    
        
      
        
    @classmethod    
    def tearDownClass(cls):
        cls.db.close()    

 
if __name__ == '__main__':
    unittest.main()
