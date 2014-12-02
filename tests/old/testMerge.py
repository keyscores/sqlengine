from ks_graph import generalLinks
from ks_merge import merge
import ks_db_settings
import MySQLdb
import unittest

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
            
     
    def test_IS_UNIQUE_COL_CASE1(self):
        first_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"CountryRegion")
        self.assertEqual(True, ks_merge.isUniqueCol("CountryRegion","CountryCode"))
        
      
    def test_IS_UNIQUE_COL_CASE2(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        self.assertEqual(False, ks_merge.isUniqueCol("Sales","CountryCode"))
        
     
    def test_Two_Tables_One_Link1(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        self.assertEqual(True, ks_merge.isUniqueOneLink("Sales", "CountryRegion"))
    
     
    def test_JOIN_TWO_TABLES_ONE_UNIQUE_LINK(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        print (ks_merge.getLinks())
        ks_merge.joinUniqueOneLink("Sales", "CountryRegion")
        print (ks_merge.getLinks())
 
      
    def test_JOIN_TWO_TABLES_TWO_UNIQUE_LINK(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"Currencyv2")
        print (ks_merge.isUniqueTwoLinks("Sales", "Currencyv2"))
        ks_merge.joinUniqueTwoLinks("Sales", "Currencyv2")
    
     
    def test_JOIN_TABLEWISE(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"Currencyv2")
        ks_merge.joinUniqueTwoLinks("Sales", "Currencyv2")
        
        third_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge.addTable(third_table,"CountryRegion")
        ks_merge.joinUniqueOneLink("Sales", "CountryRegion")
        
        fourth_table = "./ks_filehandler/ks_filehandler/data/ComissionTax.csv"
        ks_merge.addTable(fourth_table,"ComissionTax")
        ks_merge.joinUniqueTwoLinks("Sales", "ComissionTax")
    
            
    def test_Two_Tables_One_Link2(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"Currencyv2")
        self.assertEqual(False, ks_merge.isUniqueOneLink(first_table, second_table))
    
     
    def test_AutomaticFourTableMerge(self):
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
        ks_merge.automaticMerge()
        
    def test_MetaData(self):
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
        ks_merge.automaticMerge()
        print(ks_merge.getMetaDataFromTable("Sales"))
        
    @classmethod    
    def tearDownClass(cls):
        ks_db_settings.reset_all(cls.db)
        cls.db.close()    
    

 
if __name__ == '__main__':
    unittest.main()
