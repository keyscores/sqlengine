from ks_merge import precompute
from ks_merge import merge
import MySQLdb
import unittest

class TestPrecompute(unittest.TestCase):
    
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
            
     
    def test_AddBigTable(self):
        # compute BigTable
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"    
        third_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        fourth_table = "./ks_filehandler/ks_filehandler/data/ComissionTax.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"Currencyv2")
        ks_merge.addTable(third_table,"CountryRegion")
        ks_merge.addTable(fourth_table,"ComissionTax")
        #ks_merge.automaticMerge()
        ks_precompute = precompute(self.db)
        meta_data = {'VendorId': 'dim', 
                     'ProductType':'dim',
                     'Units':'fact',
                     'RoyaltyPrice':'fact',
                     'DownloadDate':'date',
                     'CustomerCurrency':'dim',
                     'CountryCode':'dim',
                     'Region':'dim',
                     'ExchangeRate':'fact',
                     'TaxRate':'fact',
                     'RightsHolder':'dim',
                     'ComissionRate':'fact',
                     'id':'sys'}
        ks_precompute.reset()
        ks_precompute.addBigTable(meta_data,"Sales",1)
        
    @unittest.skip("demonstrating skipping")
    def test_GetMeasure(self):
        ks_precompute = precompute(self.db)
        data = ks_precompute.getMeasureData([4,32], 1)
        print data
        
        
    @classmethod    
    def tearDownClass(cls):
        cls.db.close()        
         
if __name__ == '__main__':
    unittest.main()
