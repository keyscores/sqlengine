from ks_fileHandler import filehandler
from ks_merge import merge
from ks_analytics import analytics
import time
import MySQLdb


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
db = MySQLdb.connect(localhost, user, password, db_name)


#----------------------
# filehandler
#----------------------
file_handler = filehandler(db)
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
ks_merge = merge(db)
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
cursor = db.cursor()
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
ks_analytics = analytics(db)
ks_analytics.addFactUsingBinaryOp("NET_REVENUE", "Units", "RoyaltyPrice", "*") 
ks_analytics.addFactUsingBinaryOp("TAXES", "NET_REVENUE","TaxRate","*")
ks_analytics.addFactUsingBinaryOp("REVENUE_AFTER_TAX", "NET_REVENUE","TAXES","-")
print ks_analytics.calculate("NET_REVENUE","6/1/14") 
print ks_analytics.calculate("TAXES","6/1/14") 
print ks_analytics.calculate("REVENUE_AFTER_TAX","6/1/14") 
print ks_analytics.calculate("REVENUE_AFTER_TAX",None,"Region:World") 
print ks_analytics.calculate("TAXES", "6/1/14","VendorId:0268_20140114_SOFA_ENGLIS") 
print ks_analytics.calculate("NET_REVENUE", "6/1/14","VendorId:0268_20140114_SOFA_ENGLIS")
print ks_analytics.calculate("TAXES", "6/1/14","Region:Latam")
print ks_analytics.calculate("TAXES", "6/1/14","ProductType:D")
print ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",None)
print ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14","VendorId:0268_20140114_SOFA_ENGLIS")

print "NEW TEST CASES"
print "(1)" + str(ks_analytics.calculate("Units","6/1/14"))
print "-------*-----------"
print "(2)" + str(ks_analytics.calculate("NET_REVENUE","6/1/14"))
print "(3)" + str(ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 "))
print "(4)" + str(ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                   "DownloadDate"," id < 5 "))
print "-------+-----------"
print "(5)" + str(ks_analytics.calculate("Units + RoyaltyPrice","6/1/14"))
print "(6)" + str(ks_analytics.calculateGroupBy("sum(Units)+ sum(RoyaltyPrice)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 "))
print "(7)" + str(ks_analytics.calculateGroupBy("sum(Units)+ sum(RoyaltyPrice)",
                                   "DownloadDate"," id < 5 "))

print "INTERTABLE *"
print "(8 ???)" + str(ks_analytics.calculate("RoyaltyPrice*TaxRate","6/1/14"))
print "(9)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) * sum(TaxRate)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 "))
print "(10)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) * sum(TaxRate*RoyaltyPrice)",
                                   "DownloadDate"," id < 5 "))
print "INTERTABLE +"
print "(11)" + str(ks_analytics.calculate("RoyaltyPrice+TaxRate","6/1/14"))
print "(12)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) + sum(TaxRate)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 "))
print "(13)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) + sum(TaxRate)",
                                   "DownloadDate"," id < 5 "))
print "CHAINED +"
print "(14)" + str(ks_analytics.calculate("REVENUE_AFTER_TAX","6/1/14"))
print ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",None)
print "16 todo)" 

print "-------------FILTER--------------------"
print "(1)" + str(ks_analytics.calculate("Units", "6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print "-------*-----------"
print "(2)" + str(ks_analytics.calculate("NET_REVENUE","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print "(3)" + str(ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))
print "(4)" + str(ks_analytics.calculateGroupBy("sum(Units)* sum(RoyaltyPrice)",
                                   "DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS'"))
print "-------+-----------"
print "(5)" + str(ks_analytics.calculate("Units + RoyaltyPrice","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print "(6)" + str(ks_analytics.calculateGroupBy("sum(Units)+ sum(RoyaltyPrice)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))
print "(7)" + str(ks_analytics.calculateGroupBy("sum(Units)+ sum(RoyaltyPrice)",
                                   "DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))

print "INTERTABLE *"
print "(8 ???)" + str(ks_analytics.calculate("RoyaltyPrice*TaxRate","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print "(9)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) * sum(TaxRate)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))
print "(9)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) * sum(TaxRate*RoyaltyPrice)",
                                   "DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))
print "INTERTABLE +"
print "(11)" + str(ks_analytics.calculate("RoyaltyPrice+TaxRate","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print "(12)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) + sum(TaxRate)",
                                   "VendorId, ProductType, DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))
print "(13)" + str(ks_analytics.calculateGroupBy("sum(RoyaltyPrice) + sum(TaxRate)",
                                   "DownloadDate"," id < 5 and VendorId='0268_20140114_SOFA_ENGLIS' "))


print "CHAINED +"
print "(14)" + str(ks_analytics.calculate("REVENUE_AFTER_TAX","6/1/14","VendorId:0268_20140114_SOFA_ENGLIS"))
print ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14","VendorId:0268_20140114_SOFA_ENGLIS")
print "16 todo)" 




