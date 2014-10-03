
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
   
        
        
    def reset(self):
        self.cursor.execute("drop database if exists filehandler")
        self.cursor.execute("create database filehandler")
        self.cursor.execute("use filehandler")
        sql = "CREATE TABLE files(Id INT PRIMARY KEY AUTO_INCREMENT, \
                 table_name VARCHAR(50), \
                 file_name VARCHAR(50), \
                 company_name VARCHAR(50), stamp TIMESTAMP);"
        print sql         
        self.cursor.execute(sql)
        self.db.commit()

        

            




