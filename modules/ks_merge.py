import csv
from ks_graphDB import generalLinksDB
import types
import time
import urllib2

try:
    from google.appengine.ext import blobstore
except ImportError:
    gae_available = False

class merge:
    """
    Merges all tables in a given database based on their relations into a BigTable.
    """
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
                    col_types.append("VARCHAR(25)")
                sql_insert = merge.getInsert(table_name,  header)
                self.cursor.execute(merge.getSchema(table_name, header, col_types))
            else:    
                print sql_insert
                print row     
                self.cursor.execute(sql_insert%tuple(map(repr,row)))
            row_counter += 1
        sql = "alter table %s convert to character set utf8 collate utf8_unicode_ci"%table_name
        self.cursor.execute(sql)
 
        self.db.commit()
        
    def addTableURL(self, url, table_name):
        self.cursor.execute("use merge")
        row_counter = 0
        sql_insert = ""
        data = urllib2.urlopen(url)
        for row in csv.reader(data):
            if row_counter == 0:
                header = row
                col_types = []
                for col in row:
                    col_types.append("VARCHAR(25)")
                sql_insert = merge.getInsert(table_name,  header)
                print sql_insert
                self.cursor.execute(merge.getSchema(table_name, header, col_types))
            else:    
                print sql_insert
                print row     
                self.cursor.execute(sql_insert%tuple(map(repr,row)))
            row_counter += 1    
        self.db.commit()
        
        
    def addTableBlob(self, blob_key, table_name):
       self.cursor.execute("use merge")
       row_counter = 0
       sql_insert = ""
       blob_reader = blobstore.BlobReader(blob_key)
       reader = csv.reader(blob_reader, delimiter=',')
       for row in reader:
           if row_counter == 0:
               header = row
               print header
               col_types = []
               for col in row:
                   col_types.append("VARCHAR(25)")
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
    
    def getTables(self):
        self.cursor.execute("use merge")
        self.cursor.execute("show tables in merge")
        numrows = self.cursor.rowcount
        tables = []
        for x in xrange(0,numrows):
            row = self.cursor.fetchone()
            tables.append(row[0])
        self.db.commit()
        return tables
    
    def getLinks(self):
        general_links = generalLinksDB(self.getTables(),self)
        links = general_links.getLinks()
        return links
        
    
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
        
    # FIXXXMEE NoneType is not enough to check for uniquness!
    def isUniqueCol(self, table, col):
        sql = "select %s, count(*) as count from %s group by %s  having count(*) > 1;"%(col, table, col)
        self.cursor.execute("use merge")
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        print type(result)
        if isinstance(result, types.NoneType):
            return True
        else:
            return False
        
    # FIXXXMEE NoneType is not enough to check for uniquness!
    def isUniqueColTwo(self, table, col1, col2):
        sql = "select count(*) as count from %s group by %s,%s  having count(*) > 1;"%(table, col1, col2)
        self.cursor.execute("use merge")
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        print type(result)
        if isinstance(result, types.NoneType):
            return True
        else:
            return False
        
    def getJoinColUniqueOneLink(self, first_table, second_table):
        general_links = generalLinksDB([first_table, second_table], self)
        links = general_links.getLinks()
        edge = links[0]
        first_node = edge[0]
        [first_table, join_col] = first_node.split(':')
        return join_col
    
    def getJoinColUniqueTwoLinks(self, first_table, second_table):
        general_links = generalLinksDB([first_table, second_table],self)
        links = general_links.getLinks()
        edge1 = links[0]
        first_node = edge1[0]
        [first_table, join_col1] = first_node.split(':')
        edge2 = links[1]
        second_node = edge2[0]
        [first_table, join_col2] = second_node.split(':')
        return [join_col1, join_col2]
    
    def joinUniqueTwoLinks(self, first_table, second_table):
                
        self.cursor.execute("use merge")
        join_col1, join_col2 = self.getJoinColUniqueTwoLinks(first_table, second_table)
        # if first table has unique cols then swap tables
        if self.isUniqueColTwo(first_table, join_col1, join_col2):
            first_table, second_table = second_table, first_table
        first_header = self.getHeader(first_table)
        second_header = self.getHeader(second_table)
        second_header_duplicates = list(set(first_header) & set(second_header))
        second_header_uniques = list(set(second_header) - set(second_header_duplicates))
        union_header = first_header
        first_header = self.getHeader(first_table)
        union_header.extend(second_header_uniques)
        self.addTableByCols(union_header)
                
        first_headerPlusTable = map(lambda x: "%s.%s,"%(first_table,x), first_header)
        last_second_header_uniques = second_header_uniques.pop()
        if len(second_header_uniques) > 0:
            second_header_uniques_PlusTable = map(lambda x: "%s.%s,"%(second_table,x), second_header_uniques)
        else:
            second_header_uniques_PlusTable = [""]
        
        join_cols_first = "".join(first_headerPlusTable)
        print join_cols_first
        join_cols_second = "".join(second_header_uniques_PlusTable) + "%s.%s"%(second_table, last_second_header_uniques)
        print join_cols_second
        join_cols = join_cols_first + join_cols_second
        sql_select = "select %s from %s inner join %s on %s.%s = %s.%s and %s.%s = %s.%s"\
            %(join_cols, first_table, second_table,
              first_table, join_col1, second_table, join_col1,
              first_table, join_col2, second_table, join_col2)
        sql_insert = "insert into TempJoin " + sql_select;
        print (sql_insert)
        sql_drop1 = "drop table %s"%first_table
        sql_drop2 = "drop table %s"%second_table
        sql_rename = "rename table TempJoin to %s"%first_table
        self.cursor.execute(sql_insert)
        self.cursor.execute(sql_drop1)
        self.cursor.execute(sql_drop2)
        self.cursor.execute(sql_rename)
        self.db.commit()
       
    
        
    def joinUniqueOneLink(self, first_table, second_table):
                
        self.cursor.execute("use merge")
        join_col = self.getJoinColUniqueOneLink(first_table, second_table)
        
        # if first table has unique col then swap tables
        if self.isUniqueCol(first_table, join_col):
            first_table, second_table = second_table, first_table
        first_header = self.getHeader(first_table)
        second_header = self.getHeader(second_table)
        second_header_duplicates = list(set(first_header) & set(second_header))
        second_header_uniques = list(set(second_header) - set(second_header_duplicates))
        union_header = first_header
        union_header.extend(second_header_uniques)
        self.addTableByCols(union_header)
                
        first_headerPlusTable = map(lambda x: "%s.%s,"%(first_table,x), first_header)
        last_second_header_uniques = second_header_uniques.pop()
        if len(second_header_uniques) > 0:
            second_header_uniques_PlusTable = map(lambda x: "%s.%s,"%(second_table,x), second_header_uniques)
        else:
            second_header_uniques_PlusTable = [""]
        
        join_cols_first = "".join(first_headerPlusTable)
        join_cols_second = "".join(second_header_uniques_PlusTable) + "%s.%s"%(second_table, last_second_header_uniques)
        join_cols = join_cols_first + join_cols_second
        sql_select = "select %s from %s inner join %s on %s.%s = %s.%s "\
            %(join_cols, first_table, second_table,
              first_table, join_col, second_table, join_col)
              
        sql_insert = "insert into TempJoin " + sql_select;
        print (sql_insert)
        sql_drop1 = "drop table %s"%first_table
        sql_drop2 = "drop table %s"%second_table
        sql_rename = "rename table TempJoin to %s"%first_table
        self.cursor.execute(sql_insert)
        self.cursor.execute(sql_drop1)
        self.cursor.execute(sql_drop2)
        self.cursor.execute(sql_rename)
        self.db.commit()
        
        
    def isUniqueOneLink(self, first_table, second_table):
        general_links = generalLinksDB([first_table, second_table],self)
        links = general_links.getLinks()
        print len(links)
        print links
        if len(links) == 1:
            edge = links[0]
            first_node = edge[0]
            second_node = edge[1]
            [first_table, first_col] = first_node.split(':')
            [second_table, second_col] = second_node.split(':')
            if (self.isUniqueCol(first_table, first_col)) or (self.isUniqueCol(second_table, second_col)):
                return True
            else:
                return False
        else:
            return False
        
    def isUniqueTwoLinks(self, first_table, second_table):
        general_links = generalLinksDB([first_table, second_table],self)
        links = general_links.getLinks()
        if len(general_links.getLinks()) == 2:
            [join_col1, join_col2] = self.getJoinColUniqueTwoLinks(first_table, second_table)
            is_unique_table1 = self.isUniqueColTwo(first_table, join_col1, join_col2)
            is_unique_table2 = self.isUniqueColTwo(second_table, join_col1, join_col2)
            if is_unique_table1 or is_unique_table2:
                return True
            else:
                return False
        else:
            return False    
    #----------------------------------------
    def getHeader(self, table_name):
        self.cursor.execute("use merge")
        self.db.commit()
        self.cursor.execute("SELECT `COLUMN_NAME`  FROM `INFORMATION_SCHEMA`." \
                   "`COLUMNS`  WHERE `TABLE_SCHEMA`='merge' AND " \
                   "`TABLE_NAME`='"+ table_name+"';")
        numrows = self.cursor.rowcount
        header = []
        for x in xrange(0,numrows):
            row = self.cursor.fetchone()
            header.append(row[0])
        self.db.commit()
        return header
    
    #----------------------------------------
    def getMetaDataFromTable(self, table_name):
        header = self.getHeader(table_name)
        sql = "select * from %s limit 1"%(table_name)
        self.cursor.execute("use merge")
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        first_row = rows[0]
        self.db.commit()
        counter = 0
        meta_data = {}
        for col in first_row:
            header_str =  header[counter]
            # FIXXXXXME
            try:
                col = col.replace("%","")
            except Exception as e:
                print e
                
            print col
            try:
                date = time.strptime(col, '%m/%d/%y')
                type = "date"
            except ValueError:
                try:
                    float(col)
                    type = "fact" 
                except ValueError:
                    type = "dim"
            counter = counter + 1
            # FIXXME
            if header_str == "id":
                type = "sys"
            meta_data[header_str]=type
        return meta_data
            
    
    
    #----------------------------------------
    def addTableByCols(self, cols):
        self.cursor.execute("use merge")
        last_col = cols.pop()
        colsPlusType = map(lambda x: x+ " VARCHAR(25), ", cols)
        colsPlusType.append(last_col + " VARCHAR(25))")
        sql_cols =  "".join(colsPlusType)
        sql_start = "CREATE TABLE TempJoin("
        sql = sql_start + sql_cols
        self.cursor.execute(sql)
    
    def automaticMerge(self):
        print ("AUTOMATIC MERGE:")
        links = self.getLinks()
        
        #apply tablewise onelink reduction
        for link in links:
            node1 = link[0]
            node2 = link[1]
            first_table, col1 = node1.split(":")
            second_table, col2 = node2.split(":")
            uniqueOneLink = self.isUniqueOneLink(first_table, second_table)
            if (uniqueOneLink == True):
                self.joinUniqueOneLink(first_table, second_table)
                
        #apply tablewise twolinks reduction
        for link in links:
            node1 = link[0]
            node2 = link[1]
            first_table, col1 = node1.split(":")
            second_table, col2 = node2.split(":")
            uniqueOneLink = self.isUniqueTwoLinks(first_table, second_table)
            if (uniqueOneLink == True):
                print first_table, second_table
                self.joinUniqueTwoLinks(first_table, second_table)
        
        