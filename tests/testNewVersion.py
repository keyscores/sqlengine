from ks_filehandler import filehandler
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
        with open('tests/mysql_setting.txt', 'r') as f:
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
        print(file_handler.getLatestTable("Sales", "1"))

        #----------------------
        # merge
        #----------------------
        ks_merge = merge(cls.db)
        ks_merge.reset()
        latest_sales_version = file_handler.getLatestTable("Sales", "1")
        print (latest_sales_version)
        latest_sales_name = "./ks_filehandler/ks_filehandler/data/%s"%(latest_sales_version)
        print latest_sales_name
        ks_merge.addTable(latest_sales_name,"Sales")
        ks_merge.addTable("./ks_filehandler/ks_filehandler/data/CountryRegion.csv","CountryRegion")
        ks_merge.addTable("./ks_filehandler/ks_filehandler/data/ComissionTax.csv","ComissionTax")
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
        "S.VendorId = T.VendorId and C.Region = T.Region;"
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

    @classmethod    
    def tearDownClass(cls):
        cls.db.close()
        
    def test_NETREVENUE(self):
        self.assertEqual(120, self.ks_analytics.calculate("Units","6/1/14"))

    def test_NETREVENUE_FILTER(self):
        self.assertEqual(2400, self.ks_analytics.calculate("NET_REVENUE","6/1/14"))

    def test_TAXES(self):
        self.assertEqual(56, self.ks_analytics.calculate("TAXES","6/1/14"))

 
if __name__ == '__main__':
    unittest.main()
