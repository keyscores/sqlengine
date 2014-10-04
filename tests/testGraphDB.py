from ks_graph import generalLinksDB
from ks_merge import merge
import unittest
import MySQLdb

class TestGraph(unittest.TestCase):
    
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

    
    def test_Two_Tables_One_Link_Case1(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        general_links = generalLinksDB(["Sales","CountryRegion"], ks_merge)
        print(general_links.getLinks())
        self.assertEqual(True, general_links.isEdge('Sales:CountryCode', 'CountryRegion:CountryCode'))
        self.db.commit()
    
            
    def test_Two_Tables_One_Link_Case2(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        general_links = generalLinksDB(["Sales","CountryRegion"], ks_merge)
        self.assertEqual(True, general_links.isEdge('CountryRegion:CountryCode','Sales:CountryCode',))
        self.db.commit()
    
    
    def test_Two_Tables_One_Link_Case3(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/CountryRegion.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        general_links = generalLinksDB(["Sales","CountryRegion"], ks_merge)
        self.assertEqual(False, general_links.isEdge('Sales.csv:Cou ntryCode', 'CountryRegion.csv:CountryCode'))
        self.db.commit()
            
    
    def test_Four_Tables_Five_Links(self):
        first_table = "./ks_filehandler/ks_filehandler/data/Sales.csv"
        second_table = "./ks_filehandler/ks_filehandler/data/graph/CountryRegion.csv"
        third_table = "./ks_filehandler/ks_filehandler/data/ComissionTax.csv"
        fourth_table = "./ks_filehandler/ks_filehandler/data/graph/Currencyv2.csv"
        ks_merge = merge(self.db)
        ks_merge.reset()
        ks_merge.addTable(first_table,"Sales")
        ks_merge.addTable(second_table,"CountryRegion")
        ks_merge.addTable(third_table,"ComissionTax")
        ks_merge.addTable(fourth_table,"Currencyv2")
        
        
        general_links = generalLinksDB(["Sales","Currencyv2","ComissionTax","CountryRegion"], ks_merge)
        
        print ("Links:")
        print(general_links.getLinks())
        self.assertEqual(True, general_links.isEdge('ComissionTax:Region', 'CountryRegion:Region'))
        self.assertEqual(True, general_links.isEdge('ComissionTax:VendorId', 'Sales:VendorId'))
        self.assertEqual(True, general_links.isEdge('Sales:CountryCode', 'CountryRegion:CountryCode'))
        self.assertEqual(True, general_links.isEdge('Sales:DownloadDate', 'Currencyv2:DownloadDate'))
        self.assertEqual(True, general_links.isEdge('Currencyv2:CustomerCurrency', 'Sales:CustomerCurrency'))
        self.db.commit()


 
if __name__ == '__main__':
    unittest.main()
