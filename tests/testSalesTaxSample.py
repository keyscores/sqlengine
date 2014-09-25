from ks_fileHandler import filehandler
from ks_merge import merge
from ks_analytics import analytics
import time
import MySQLdb
import unittest

class TestSalesTaxSample(unittest.TestCase):
    
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


            #----------------------
            # filehandler
            #----------------------
            file_handler = filehandler(cls.db)
            file_handler.reset()
            file_handler.addTable("Sales", "1","Sales.csv")
            file_handler.addTable("Sales", "2","SalesCustomerTwo.csv")

            file_handler.addTable("ComissionTax", "1","ComissionTax.csv")
            file_handler.addTable("ComissionTax","2","ComissionTaxCustomerTwo.csv")

            file_handler.addTable("CountryRegion", "1","CountryRegion.csv")
            file_handler.addTable("CountryRegion","2","CountryRegionCustomerTwo.csv")

            file_handler.addTable("Currency2","1","Currencyv2.csv")
            time.sleep(1)
            file_handler.addTable("Sales", "1","SalesNewVersion.csv")
            print(file_handler.getLatestTable("Sales", "2"))

            #----------------------
            # merge
            #----------------------
            ks_merge = merge(cls.db)
            ks_merge.reset()
            ks_merge.addTable("./modules/filehandler/data/Sales.csv","Sales")
            ks_merge.addTable("./modules/filehandler/data/CountryRegion.csv","CountryRegion")
            ks_merge.addTable("./modules/filehandler/data/ComissionTax.csv","ComissionTax")
            sql_BigTable = "CREATE TABLE BigTable(id INT PRIMARY KEY AUTO_INCREMENT, \
                 VendorId VARCHAR(25), \
                 ProductType VARCHAR(25), \
                 Units FLOAT, \
                 RoyaltyPrice FLOAT, \
                 DownloadDate VARCHAR(25), \
                 CustomerCurrency VARCHAR(25), \
                 CountryCode VARCHAR(25), \
                 Region VARCHAR(25), \
                 RightsHolder VARCHAR(25), \
                 ComissionRate VARCHAR(25), \
                 TaxRate VARCHAR(25))"
        

        sql_join = "insert into BigTable select S.id,S.VendorId,S.ProductType, "\
            "S.Units, S.RoyaltyPrice, S.DownloadDate, S.CustomerCurrency, "\
            "S.CountryCode, C.Region, T.RightsHolder, T.ComissionRate, "\
            "T.TaxRate from Sales S Inner Join CountryRegion C on "\
            "S.CountryCode=C.CountryCode Inner join ComissionTax T on " \
            "S.VendorId = T.VendorIdentifier and C.Region = T.Region;"
            
        ks_merge.join(sql_join, sql_BigTable)
        
        #----------------------
        # clean up
        #----------------------
        cursor = cls.db.cursor()
        sql = "use merge;"
        cursor.execute(sql)        
        sql = "ALTER TABLE BigTable change ComissionRate ComissionRate FLOAT;"
        cursor.execute(sql)
        sql = "ALTER TABLE BigTable change TaxRate TaxRate FLOAT;"
        cursor.execute(sql)
        sql ="update BigTable set TaxRate = TaxRate/100;"
        cursor.execute(sql)
        
        
        #----------------------
        # analytics
        #----------------------
        cls.ks_analytics = analytics(cls.db)
        cls.ks_analytics.addFactUsingBinaryOp("NET_REVENUE", "Units", "RoyaltyPrice", "*") 
        cls.ks_analytics.addFactUsingBinaryOp("TAXES", "NET_REVENUE","TaxRate","*")
        cls.ks_analytics.addFactUsingBinaryOp("REVENUE_AFTER_TAX", "NET_REVENUE","TAXES","-")
        
    @classmethod    
    def tearDownClass(cls):
        cls.db.close()
        
    def test_NETREVENUE(self):
        self.assertEqual(24, self.ks_analytics.calculate("NET_REVENUE","6/1/14"))

    def test_NETREVENUE_FILTER(self):
        self.assertEqual(12, self.ks_analytics.calculate("NET_REVENUE","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))

    def test_TAXES(self):
        self.assertEqual(1.2576, self.ks_analytics.calculate("TAXES","6/1/14"))
      
    def test_TAXES_FILTER(self):
        self.assertAlmostEqual(0.6288, self.ks_analytics.calculate("TAXES","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
        
    def test_TAXES_BROADCAST1(self):
        self.assertAlmostEqual(1.2, self.ks_analytics.calculate("TAXES","6/1/14","Region:Latam"))
    
    def test_TAXES_BROADCAST2(self):
        self.assertAlmostEqual(0.6288, self.ks_analytics.calculate("TAXES","6/1/14","ProductType:D"))
      
    def test_REVENUE_AFTER_TAX(self):
        self.assertEqual(22.7424, self.ks_analytics.calculate("REVENUE_AFTER_TAX","6/1/14"))
        
    def test_KPI_MARGIN(self):
        self.assertAlmostEqual(0.9226, self.ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",None))
        
    def test_KPI_FILTER(self):
        self.assertAlmostEqual(0.9476, self.ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",
                                                                      "VendorId:0268_20140114_SOFA_ENGLIS"))
        


if __name__ == '__main__':
    unittest.main()
