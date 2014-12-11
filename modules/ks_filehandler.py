import csv
import time
import urllib2
import csvPy


try:
    from google.appengine.ext import blobstore
except ImportError:
    gae_available = False
    
class filehandler:
    """
    Used to register the source of customer data. 
    """

    def __init__(self,db):
        self.db = db
        self.cursor = self.db.cursor()
    
    def addTable(self, table_name, company, file_name):
        sql1 =  "insert into files (table_name, company_name, stamp, file_name) values ("
        sql2 = "'%s','%s',CURRENT_TIMESTAMP,'%s')"%(table_name, company, file_name)
        self.cursor.execute("use filehandler")
        print sql1 + sql2
        self.cursor.execute(sql1 + sql2)
        self.db.commit()
            
    
    def getLatestTable(self, table_name, company_name):    
        # get lates timestamp for table and company
        self.cursor.execute("use filehandler")
        sql_latest_stamp1 = "select max(stamp) from files where company_name = "
        sql_latest_stamp2 = "'%s' and table_name = '%s'"%(company_name, table_name)
        sql_latest_stamp = sql_latest_stamp1 + sql_latest_stamp2
        self.cursor.execute(sql_latest_stamp)
        time_stamp = self.cursor.fetchone()[0]
                 
        # get latest table for a given name and company
        sql_latest_file_name1 = "select file_name from files where company_name = "
        sql_latest_file_name2 = \
            "'%s' and table_name = '%s' and stamp='%s'"%(company_name, table_name, time_stamp)
        sql_latest_file_name = sql_latest_file_name1 + sql_latest_file_name2
        self.cursor.execute(sql_latest_file_name)
        table_id = self.cursor.fetchone()[0]
        return table_id
   
    def getLatestTablesByCompany(self, company_name):
        self.cursor.execute("use filehandler")
        sql = "select max(stamp),table_name,file_name from files where company_name='%s' group by table_name ;"%(company_name)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return rows
        
    def updateMeasureTable(self, file_name, company):
        self.cursor.execute("use filehandler")
        row_counter = 0
        for row in csv.reader(open(file_name)):
            if row_counter == 0:
                header = row
                break
        for header_col in header:
            print header_col
            sql_1 = "insert into ks_measures (company_name, name, alias, formula,agg_type) values "
            sql_2 = " ('%s','%s','%s','%s','%s');"%(company, header_col, header_col, "","")
            sql = sql_1 + sql_2
            try:
                self.cursor.execute(sql)
            except self.db.IntegrityError as e:
                print (e)
        self.db.commit()
        
    def updateMeasureTableCsvPy(self, table_name, company):
        self.cursor.execute("use filehandler")
        row_counter = 0
        for row in csvPy.csv_dict[table_name]:
            if row_counter == 0:
                header = row
                break
        for header_col in header:
            print header_col
            sql_1 = "insert into ks_measures (company_name, name, alias, formula,agg_type) values "
            sql_2 = " ('%s','%s','%s','%s','%s');"%(company, header_col, header_col, "","")
            sql = sql_1 + sql_2
            try:
                self.cursor.execute(sql)
            except self.db.IntegrityError as e:
                print (e)
        self.db.commit()
    
    def updateMeasureTableURL(self, url, company):
        self.cursor.execute("use filehandler")
        row_counter = 0
        data = urllib2.urlopen(url)
        for row in csv.reader(data):
            if row_counter == 0:
                header = row
                break
        for header_col in header:
            print header_col
            sql_1 = "insert into ks_measures (company_name, name, alias, formula,agg_type) values "
            sql_2 = " ('%s','%s','%s','%s','%s');"%(company, header_col, header_col, "","")
            sql = sql_1 + sql_2
            try:
                self.cursor.execute(sql)
            except self.db.IntegrityError as e:
                print (e)
        self.db.commit()
        
    def registerFormula(self, company_id, formula_name, alias, formula, agg_type):
        self.cursor.execute("use filehandler")
        sql_1 = "insert into ks_measures (company_name, name, alias, formula,agg_type) values "
        sql_2 = " ('%s','%s','%s','%s','%s');"%(company_id, formula_name, formula_name, formula, agg_type)
        sql = sql_1 + sql_2
        try:
            self.cursor.execute(sql)
        except self.db.IntegrityError as e:
            print (e)
        self.db.commit()
        

    def updateMeasureTableBlob(self, blob_key, company):
        self.cursor.execute("use filehandler")
        row_counter = 0
        blob_reader = blobstore.BlobReader(blob_key)
        reader = csv.reader(blob_reader, delimiter=',')
        for row in reader:
            if row_counter == 0:
                header = row
                break
        for header_col in header:
            print header_col
            sql_1 = "insert into ks_measures (company_name, name, alias, formula) values "
            sql_2 = " ('%s','%s','%s','%s');"%(company, header_col, header_col, "")
            sql = sql_1 + sql_2
            try:
                self.cursor.execute(sql)
            except self.db.IntegrityError as e:
                print (e)
        self.db.commit()

    def getMeasureDataByID(self, id):
        measure_data ={}
        self.cursor.execute("use filehandler")
        sql = "select company_name, name, alias, formula, agg_type,id from ks_measures where id = %s;"%(id)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        measure_data["company_id"] = row[0][0]
        measure_data["name"] = row[0][1]
        measure_data["alias"] = row[0][2]
        measure_data["formula"] = row[0][3]
        measure_data["agg_type"] = row[0][4]
        measure_data["agg_type"] = row[0][5]
        self.db.commit()
        return measure_data

    def getMeasureID(self, measure):
        measure_data ={}
        self.cursor.execute("use filehandler")
        sql = "select id from ks_measures where name = '%s';"%(measure)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        measure_id = row[0][0]
        self.db.commit()
        return measure_id

    def getMeasureNameById(self, id):
        self.cursor.execute("use filehandler")
        sql = "select name from ks_measures where Id =  %s;"%(id)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        id = row[0][0]
        return id
    
    def getFormulaByMeasureId(self, id):
        self.cursor.execute("use filehandler")
        sql = "select formula from ks_measures where Id =  %s;"%(id)
        self.cursor.execute(sql)
        row = self.cursor.fetchall()
        formula = row[0][0]
        return formula
    
    def getAllMeasures(self):
        self.cursor.execute("use filehandler")
        sql = "select name, id from ks_measures" 
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        measures = []
        for row in rows:
            measures.append("%s [id=%s]"%(row[0],row[1]))
        return measures

            
    def reset(self):
        self.cursor.execute("drop database if exists filehandler")
        self.cursor.execute("create database filehandler")
        self.cursor.execute("use filehandler")
        sql = "CREATE TABLE files(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 table_name VARCHAR(50), \
                 file_name VARCHAR(2000), \
                 company_name VARCHAR(50), stamp TIMESTAMP);"
        print sql         
        self.cursor.execute(sql)
        sql = "CREATE TABLE ks_measures(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 company_name VARCHAR(100), \
                 name VARCHAR(100) NOT NULL UNIQUE, \
                 alias VARCHAR(100), \
                 formula VARCHAR(200), \
                 agg_type VARCHAR(10));"
        print sql         
        self.cursor.execute(sql)
        self.db.commit()

        

            




