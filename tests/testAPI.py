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
from register_raw_files import registerFormula


class TestAPI(unittest.TestCase):
    
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
        ks_fh = filehandler(self.db)
        ks_fh.reset()
        company_id = 2
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"    
        third_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        fourth_table = "./ks_filehandler/ks_filehandler/data/ComissionTax.csv"    
        register_raw_files(first_table,company_id, self.db)
        register_raw_files(second_table,company_id, self.db)
        register_raw_files(third_table,company_id, self.db)
        register_raw_files(fourth_table,company_id, self.db)
    
        ks_precompute = precompute(self.db)
        ks_precompute.reset()
        #precompute
        load_precompute_normalize(company_id, self.db)
        
        # get measure
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
        
        # MEASURE DATA DEMO measures with formulas group by NOT IMPLEMENTED YET
        
    @classmethod    
    def tearDownClass(cls):
        cls.db.close()    

 
if __name__ == '__main__':
    unittest.main()
