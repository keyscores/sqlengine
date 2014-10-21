import csv

class filehandler:

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
            sql_1 = "insert into ks_measures (company_name, name, alias, formula) values "
            sql_2 = " ('%s','%s','%s','%s');"%(company, header_col, header_col, "")
            sql = sql_1 + sql_2
            try:
                self.cursor.execute(sql)
            except self.db.IntegrityError as e:
                print (e)
        self.db.commit()
        


            
    def reset(self):
        self.cursor.execute("drop database if exists filehandler")
        self.cursor.execute("create database filehandler")
        self.cursor.execute("use filehandler")
        sql = "CREATE TABLE files(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 table_name VARCHAR(50), \
                 file_name VARCHAR(200), \
                 company_name VARCHAR(50), stamp TIMESTAMP);"
        print sql         
        self.cursor.execute(sql)
        sql = "CREATE TABLE ks_measures(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 company_name VARCHAR(100), \
                 name VARCHAR(100) NOT NULL UNIQUE, \
                 alias VARCHAR(100), \
                 formula VARCHAR(200));"
        print sql         
        self.cursor.execute(sql)
        self.db.commit()

        

            




