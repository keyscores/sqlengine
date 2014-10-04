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
            ks_merge.addTable("./ks_filehandler/ks_filehandler/data/Sales.csv","Sales")
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
        # ProductType changed from D to M see documentation of test case
        sql ="update BigTable set ProductType = 'M' where VendorId='0268_20140114_SOFA_ENGLIS' and DownloadDate='6/1/14';"
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
    
    # Binary Op    
    def test_Binary_Op_Aggregate(self):
        self.assertEqual(6, self.ks_analytics.calculate("Units","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))

    def test_Binary_Op_Multiplication_without_groupby_per_record(self):
        self.assertEqual(12, self.ks_analytics.calculate("NET_REVENUE","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))

    def test_Binary_Op_Multiplication_with_groupby(self):
        self.assertEqual(24, self.ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                                                "VendorId, ProductType, DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    def test_Binary_Op_Multiplication_with_groupby_date(self):
        self.assertEqual(24, self.ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                                                " DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    def test_Binary_Op_Addition_without_groupby_per_record(self):
        self.assertEqual(10, self.ks_analytics.calculate("Units + RoyaltyPrice","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
    
    def test_Binary_Op_Addition_with_groupby(self):
        self.assertEqual(10, self.ks_analytics.calculateGroupBy("sum(Units) + sum(RoyaltyPrice)",
                                                                "VendorId, ProductType, DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    def test_Binary_Op_Addition_with_groupby_date(self):
        self.assertEqual(10, self.ks_analytics.calculateGroupBy("sum(Units) + sum(RoyaltyPrice)",
                                                                " DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    # Intertable
    def test_Intertable_Multiplication_without_groupby_per_record(self):
        self.assertAlmostEqual(0.2096, self.ks_analytics.calculate("RoyaltyPrice*TaxRate","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))

    def test_Intertable_Multiplication_with_groupby(self):
        self.assertAlmostEqual(0.4192, self.ks_analytics.calculateGroupBy("sum(TaxRate)* sum(RoyaltyPrice)",
                                                                "VendorId, ProductType, DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS'"))
    def test_Intertable_Multiplication_with_groupby_date(self):
        self.assertAlmostEqual(0.8384, self.ks_analytics.calculateGroupBy("sum(RoyaltyPrice) * sum(TaxRate*RoyaltyPrice)",
                                   "DownloadDate"," DownloadDate='6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
                               
     
    def test_Intertable_Addition_without_groupby_per_record(self):
        self.assertAlmostEqual(4.104800001252443, self.ks_analytics.calculate("TaxRate + RoyaltyPrice","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
    
    def test_Intertable_Addition_with_groupby(self):
        self.assertAlmostEqual(4.104800001252443, self.ks_analytics.calculateGroupBy("sum(TaxRate) + sum(RoyaltyPrice)",
                                                                "VendorId, ProductType, DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    def test_Intertable_Addition_with_groupby_date(self):
        self.assertAlmostEqual(4.104800001252443, self.ks_analytics.calculateGroupBy("sum(TaxRate) + sum(RoyaltyPrice)",
                                                                " DownloadDate",
                                                                " DownloadDate= '6/1/14' and VendorId='0268_20140114_SOFA_ENGLIS' "))
    # Chained
    def test_Chained_Intertable(self):
        self.assertEqual(11.37120008468628, self.ks_analytics.calculate("REVENUE_AFTER_TAX","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
    
    def test_Chained_Intertable_sm2(self):
        self.assertAlmostEqual(0.9476000070571899, self.ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",
                                                                    "VendorId:0268_20140114_SOFA_ENGLIS"))
    @unittest.skip("demonstrating skipping")                     
    def test_Chained_Intertable_sm2_sandwich(self):
        self.assertEqual(1234, self.ks_analytics.calculateGroupBy("sum(TaxRate) + sum(RoyaltyPrice)",
                                                                " DownloadDate",
                                                                " DownloadDate= '6/1/14'"))

    
    
        
    
    
    
        


if __name__ == '__main__':
    unittest.main()
    
