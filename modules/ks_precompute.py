import types
from ks_filehandler import filehandler
from formula_parser import parse
from formula_parser import tree_names

class precompute:
    """
    Stores a BigTable in a normalised way. This class is used to handle
    many companies and different versions of a BigTable for one company.
    """
    def __init__(self,db):
        self.db = db
        self.cursor = self.db.cursor()
    
    def addBigTable(self, meta_data, table_name, company_id):
        print meta_data
        version_id = self.addVersion()
        self.addRows(version_id, table_name)
        self.addCols(table_name, version_id, meta_data, company_id)
        
    def addRows(self, version_id, table_name):
        sql = "insert into ks_rows(version_id,row_id) select %s,id from merge.%s"%(version_id, table_name)
        self.cursor.execute(sql)
        self.db.commit()
        
    def addFactCol(self, col, table, version_id, company_id):
        # fetch measure_id
        sql_id = "select id from filehandler.ks_measures where name = '%s';"%(col)
        self.cursor.execute(sql_id)
        rows = self.cursor.fetchall()
        row = rows[0]
        measure_id = row[0]
        
        sql1 = "insert into ks_fact "
        sql2 = "select ks_rows.id,%s,'%s','%s','%s' from ks_rows inner join "%(col,measure_id, company_id, version_id)  
        sql3 = "merge.%s on merge.%s.id=ks_rows.row_id where version_id=%s"%(table, table, version_id)
        sql = sql1 + sql2 + sql3
        
        print "add Fact: " + col
        print sql
        self.cursor.execute(sql)
        
    def addDimCol(self, col, table, version_id,company_id):
        sql1 = "insert into ks_dim_level "
        sql2 = "select ks_rows.id,'%s',%s from ks_rows inner join "%(col, col)  
        sql3 = "merge.%s on merge.%s.id=ks_rows.row_id where version_id=%s"%(table, table, version_id)
        sql = sql1 + sql2 + sql3
        print sql
        self.cursor.execute(sql)
        
    def addDateCol(self, col, table, version_id, company_id):
        sql1 = "insert into ks_date "
        sql2 = "select ks_rows.id,STR_TO_DATE(%s, "%(col) 
        sql3= "'%m/%d/%y') AS date from ks_rows inner join "  
        sql4 = "merge.%s on merge.%s.id=ks_rows.row_id where version_id=%s"%(table, table, version_id)
        sql = sql1 + sql2 + sql3 + sql4
        print "********************"
        print sql
        self.cursor.execute(sql)
    
    def addSysCol(self, col, table, version_id, company_id):
        print "add Fact: " + col
    
    def addCols(self, table_name, version_id, meta_data, company_id):
        header = self.getHeader(table_name)
        print "header: %s"%(header)
        addCol = {"fact":self.addFactCol,"dim":self.addDimCol,"date":self.addDateCol,"sys":self.addSysCol}
        for col in header:
            print "col:%s, meta:%s"%(col, meta_data[col])
            addCol[meta_data[col]](col, table_name, version_id, company_id)
        self.db.commit()    


    def getRawDataFormula(self, formula_data, version, measure_id, start_date, end_date):
        #FIXME
        op_dict = {}
        op_dict["Add"]="+"
        op_dict["Mult"]="*"
        op = op_dict[formula_data["op"]]
        date_format = "DATE_FORMAT(date,'%m/%d/%y')"
        sql = "select  %s,sum(rhs.value) %s sum(lhs.value) from ks_fact as rhs inner join ks_fact as lhs "%(date_format, op) +\
            "on rhs.link_id = lhs.link_id inner join ks_date on rhs.link_id = ks_date.link_id " +\
            "where lhs.measure_id = %s and rhs.measure_id = %s group by date;"%(formula_data["lhs"], formula_data["rhs"])
        self.cursor.execute(sql)
        #print sql
        rows = self.cursor.fetchall()
        code_data ={}
        for row in rows:
            code_data[str(row[0])]= row[1]
            code_data
        return code_data

    def getRawData(self, version, measure_id, start_date, end_date):
        date_format = "DATE_FORMAT(date,'%m/%d/%y')"
        sql = "select %s, sum(value) from ks_fact inner join ks_date on ks_fact.link_id = "% date_format +\
            "  ks_date.link_id where ks_fact.big_table_version_id = " +\
            "%s and measure_id = %s and date>='%s' and date<='%s' group by date"%(version, measure_id, start_date, end_date)
        self.cursor.execute(sql)
        #print sql
        rows = self.cursor.fetchall()
        code_data ={}
        for row in rows:
            code_data[str(row[0])]= row[1]
            code_data
        return code_data
        
        
    def getMeasureData(self, measure_ids, company_id, start_date, end_date):
        sql_version = "select max(big_table_version_id) from ks_fact where company_id = %s"%(company_id)
        self.cursor.execute(sql_version)
        rows = self.cursor.fetchall()
        row = rows[0]
        version = row[0]
        data = {}
        for measure_id in measure_ids:
            ks_fh = filehandler(self.db)
            measure_data = ks_fh.getMeasureDataByID(measure_id)
            if len(measure_data["formula"])>0:
                formula_data = {}
                formula_tree = parse(measure_data["formula"])
                facts = tree_names(formula_tree)
                formula_data["op"] = formula_tree[0]
                formula_data["rhs"] = ks_fh.getMeasureID(list(facts)[0])
                formula_data["lhs"] = ks_fh.getMeasureID(list(facts)[1])
                formula_data["agg_type"] = measure_data["agg_type"]
                data[measure_id] = self.getRawDataFormula(formula_data,version,  measure_id, start_date, end_date)
                
            else:
                data[measure_id] = self.getRawData(version, measure_id, start_date, end_date)
        return data
    
    

    #----------------------------------------
    def getMeasureDataGroupBy(self, measure_ids, company_id, start_date, end_date,dim):
        sql_version = "select max(big_table_version_id) from ks_fact where company_id = %s"%(company_id)
        self.cursor.execute(sql_version)
        rows = self.cursor.fetchall()
        row = rows[0]
        version = row[0]
        data = {}
        date_format = "DATE_FORMAT(date,'%m/%d/%y')"
        for measure_id in measure_ids:
            sql = "select %s, sum(value),level from ks_fact inner join ks_date on ks_fact.link_id = "% (date_format) +\
                "  ks_date.link_id inner join ks_dim_level on ks_fact.link_id = ks_dim_level.link_id where " +\
                " ks_fact.big_table_version_id = " +\
                "%s and measure_id = %s and dim='%s' and date>='%s'  and date<='%s' group by level,date"%(version, measure_id,dim, start_date, end_date)
            self.cursor.execute(sql)
            #print sql
            rows = self.cursor.fetchall()
            date_dict ={}
            last_level = rows[0][2]
            level_dict = {}
            for row in rows:
                if row[2] != last_level:
                     level_dict[last_level] = date_dict
                     date_dict ={}
                     last_level = row[2]
                date_dict[str(row[0])]= row[1] 
            level_dict[last_level] = date_dict
            data[measure_id] = level_dict
        return data
        
    #----------------------------------------
    def getHeader(self, table_name):
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
    
    def addVersion(self):
        self.cursor.execute("insert into ks_big_table (version) values(current_timestamp)")
        # get max link _id
        sql = "select max(id) from ks_big_table;"
        self.cursor.execute(sql)
        max_version_id = self.cursor.fetchone()[0]
        if isinstance(max_version_id, types.NoneType):
                max_version_id = 0
        self.db.commit()
        return max_version_id
        
    def reset(self):
        for table in ['ks_big_table', 'ks_rows', 'ks_dim_level', 'ks_fact', 'ks_date']:
            self.cursor.execute("drop table if exists %s" % table)
        self.cursor.execute("CREATE TABLE ks_big_table(id INT PRIMARY KEY AUTO_INCREMENT, \
                 version timestamp)")
        
        self.cursor.execute("CREATE TABLE ks_rows(id INT PRIMARY KEY AUTO_INCREMENT, version_id INT, row_id INT)")
                
        self.cursor.execute("CREATE TABLE ks_dim_level(link_id INT, dim VARCHAR(50), level VARCHAR(50))")
        self.cursor.execute("CREATE TABLE ks_fact(link_id INT, value float,"+
                            "measure_id INT, company_id INT, big_table_version_id INT)")
        self.cursor.execute("CREATE TABLE ks_date(link_id INT, date Date)")
        self.db.commit()
        
    def getMaxBigTableIdForCompany(self, company_id):
        sql = "select max(big_table_version_id) from ks_fact where company_id = %s;"%(company_id)
        print sql
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        id = rows[0][0]
        return id
        

    