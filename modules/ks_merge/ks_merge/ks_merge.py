import csv

class merge:

    def __init__(self,db):
        self.db = db
        self.cursor = self.db.cursor()
    
    def addTable(self, file_name, table_name):
        self.cursor.execute("use merge")
        row_counter = 0
        sql_insert = ""
        for row in csv.reader(open(file_name)):
            if row_counter == 0:
                header = row
                print header
                col_types = []
                for col in row:
                    col_types.append("VARCHAR(50)")
                sql_insert = merge.getInsert(table_name,  header)
                self.cursor.execute(merge.getSchema(table_name, header, col_types))
            else:    
                print sql_insert
                print row     
                self.cursor.execute(sql_insert%tuple(map(repr,row)))
            row_counter += 1    
        self.db.commit()
        
    def addTableCompanyCross(self, file_name, table_name, company_name):
        self.cursor.execute("use merge")
        row_counter = 0
        sql_insert = ""
        for row in csv.reader(open(file_name)):
            if row_counter == 0:
                header = row
                header.append("company_name")
                print header
                col_types = []
                for col in row:
                    col_types.append("VARCHAR(50)")
                sql_insert = merge.getInsert(table_name,  header)
                self.cursor.execute(merge.getSchema(table_name, header, col_types))
            else:    
                print sql_insert
                print row     
                row.append(company_name)
                self.cursor.execute(sql_insert%tuple(map(repr,row)))
            row_counter += 1    
        self.db.commit()
                   
    def reset(self):
        self.cursor.execute("drop database if exists merge")
        self.cursor.execute("create database merge")
        self.db.commit()
    
    def join(self, sql_join, sql_BigTable):
        self.cursor.execute("use merge")
        self.cursor.execute(sql_BigTable)
        self.cursor.execute(sql_join)
        self.db.commit()
    
    @staticmethod 
    def getInsert(table, header):
        field_names = ', '.join(header)
        field_markers = ', '.join('%s' for col in header)
        return 'INSERT INTO %s (%s) VALUES (%s);' % \
            (table, field_names, field_markers)
    
    @staticmethod    
    def getSchema(table, header, col_types):
        schema_sql = """CREATE TABLE IF NOT EXISTS %s (id int NOT NULL AUTO_INCREMENT,""" % table 
        for col_name, col_type in zip(header, col_types):
            schema_sql += '%s %s,' % (col_name, col_type)
        schema_sql += """PRIMARY KEY (id)) DEFAULT CHARSET=utf8;"""
        return schema_sql    
