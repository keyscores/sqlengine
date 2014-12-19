from ks_filehandler import filehandler

class analytics:
    """
    Used to compute measures on a given BigTable.
    """
    def __init__(self,db):
        self.db = db
        self.cursor = self.db.cursor()
        self.ks_fh = filehandler(db)


    def reset(self):
        self.cursor.execute("drop database if exists analytics")
        self.cursor.execute("create database analytics")
        self.db.commit()

    #----------------------------------------
    def addFactUsingBinaryOp(self,
                             new_fact_name,
                             left_hand_fact,
                             right_hand_fact,
                             op):

        sql = "use analytics;"
        self.cursor.execute(sql)
        sql =  "alter table BigTable add column %s FLOAT;"%(new_fact_name)
        print sql
        self.cursor.execute(sql)
        sql = "update BigTable set %s = %s%s%s;"%(new_fact_name,
                                                  left_hand_fact,
                                                  op,
                                                  right_hand_fact)
        self.cursor.execute(sql)
        print sql

    #----------------------------------------
    def addFactUsingBinaryOpAPI(self,
                             new_fact_name,
                             left_hand_fact,
                             right_hand_fact,
                             op, bigtable):

        self.cursor.execute("use analytics;")
        sql =  "alter table %s add column %s FLOAT;"%(bigtable, new_fact_name)
        print sql
        self.cursor.execute(sql)
        sql = "update %s set %s = %s%s%s;"%(bigtable, new_fact_name,
                                                  left_hand_fact,
                                                  op,
                                                  right_hand_fact)
        self.cursor.execute(sql)
        self.cursor.execute(sql)
        self.db.commit()
        print sql


    def addFactWithFormula(self,
                             new_fact_name,
                             formula):

        sql =  "alter table BigTable add column %s FLOAT;"%(new_fact_name)
        print sql
        self.cursor.execute(sql)
        sql = "update BigTable set %s = %s;"%(new_fact_name, formula)
        self.cursor.execute(sql)
        print sql


    def getMeasureData(self,
                       bigTable,
                       measures,
                       start_date,
                       end_date,
                       groupby,
                       dimension_filters,
                       frequency):

        data = {}
        for measure_id in measures:
            measure_name = self.ks_fh.getMeasureNameById(measure_id)
            measure_formula = self.ks_fh.getFormulaByMeasureId(measure_id)
            measure_sql = measure_name
            if len(measure_formula) > 0:
                measure_sql = measure_formula

            print measure_name
            date_condition = 'ks_date >="%s" and ks_date<="%s"'%(start_date, end_date)
            if groupby == None:
                if dimension_filters != None:
                    date_condition = date_condition + dimension_filters
                code_data = self.calculateAPI(bigTable, measure_sql, date_condition)
            else:
                where_condition = date_condition
                if dimension_filters != None:
                    where_condition = date_condition + dimension_filters
                code_data = self.calculateGroupByAPI(bigTable, measure_sql, groupby, where_condition, frequency)

            data[measure_name] = code_data
        return data


    #----------------------------------------
    def calculate(self,fact_name, fact_date=None, dim_level=None):
        sql = "select sum(%s) from BigTable;"%(fact_name)
        if fact_date != None :
            sql = 'select sum(%s) from BigTable where DownloadDate = "%s"'%(fact_name,fact_date);
        if dim_level != None:
            dim_name, level_name = dim_level.split(":")
            sql = 'select sum(%s) from BigTable where %s = "%s"'%(fact_name,dim_name, level_name);
        if (fact_date != None) & (dim_level != None):
            sql = 'select sum(%s) from BigTable where %s = "%s" and DownloadDate = "%s"'\
                %(fact_name,dim_name, level_name,fact_date);
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    #----------------------------------------
    def calculateAPI(self, bigTable,fact_name, date_condition=None, dim_level=None):
        sql = "use analytics;"
        self.cursor.execute(sql)
        sql = "select sum(%s) from BigTable;"%(fact_name)
        if date_condition != None :
            sql = 'select sum(%s) from %s where  %s'%(fact_name, bigTable, date_condition);
        if dim_level != None:
            dim_name, level_name = dim_level.split(":")
            sql = 'select sum(%s) from %s where %s = "%s"'%(fact_name, bigTable, dim_name, level_name);
        if (date_condition != None) & (dim_level != None):
            sql = 'select sum(%s) from %s where %s = "%s" and  %s'\
                %(fact_name, bigTable, dim_name, level_name,date_condition);
        self.cursor.execute(sql)
        rows =  self.cursor.fetchone()
        code_data ={}
        code_data["total"]= rows[0]
        return code_data


    def parseGroupBy(self, group_by):
        dims = group_by.split(",")
        dims_without_date = []
        for dim in dims:
            if dim == "ks_date":
                continue
            dims_without_date.append(dim)
        dims_without_date_str = ",".join(dims_without_date)
        return dims_without_date_str


#----------------------------------------
    def calculateGroupByAPI(self,bigTable,fact_name, group_by, where_str, frequency):
        date_format_dict = {"day":"ks_date",
                            "month":"DATE_FORMAT(ks_date,'%Y-%m')",
                            "quarter":"""concat(DATE_FORMAT(ks_date,'%Y'),"-Q",quarter(ks_date))"""}
        
        ks_date_format = date_format_dict[frequency]
        
        sql = "use analytics;"
        self.cursor.execute(sql)
        group_no_date = self.parseGroupBy(group_by)
        if group_no_date == "":
            group_no_date = ks_date_format
        order_by = group_no_date + "," + ks_date_format
        group_by = group_no_date + "," + ks_date_format
        sql = "select %s,%s,concat(%s) from %s group by %s order by %s"%(ks_date_format,fact_name,group_no_date, bigTable, group_by, order_by)
        if where_str != None:
            sql = "select %s,%s, concat(%s) from %s where %s group by %s order by %s"%(ks_date_format,fact_name,group_no_date, bigTable, where_str, group_by, order_by)
        print sql
        self.cursor.execute(sql)
        rows =  self.cursor.fetchall()
        print "**************************************"
        print "**************************************"
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
        level_dict
        return level_dict


    #----------------------------------------
    def calculateGroupBy(self,fact_name, group_by, where_str):
        sql = "select sum(subtotal) from (select %s as subtotal from BigTable group by %s) as total;"\
            %(fact_name, group_by)
        if where_str != None:
            sub_sql = "select sum(subtotal) from (select %s as subtotal " +\
                "from BigTable where %s group by %s) as total;"
            sql = sub_sql%(fact_name, where_str, group_by)
        print sql
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    #----------------------------------------
    def calculateGroupByAVG(self,fact_name, group_by, where_str):
        sql = "select avg(subtotal) from (select %s as subtotal from BigTable group by %s) as total;"\
            %(fact_name, group_by)
        if where_str != None:
            sub_sql = "select avg(subtotal) from (select %s as subtotal " +\
                "from BigTable where %s group by %s) as total;"
            sql = sub_sql%(fact_name, where_str, group_by)
        print sql
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    #----------------------------------------
    def calculateSm2(self,left_hand_fact, op, right_hand_fact, fact_date=None, dim_level=None):
        sql = "select sum(%s) %s sum(%s) from BigTable;"%(left_hand_fact, op, right_hand_fact)
        if fact_date != None :
            sql = 'select sum(%s) %s sum(%s) from BigTable where DownloadDate = "%s"'\
                %(left_hand_fact, op, right_hand_fact,fact_date);
        if dim_level != None:
            dim_name, level_name = dim_level.split(":")
            sql = 'select sum(%s) %s sum(%s) from BigTable where %s = "%s"'\
                %(left_hand_fact, op, right_hand_fact,dim_name, level_name);
        if (fact_date != None) & (dim_level != None):
            sql = 'select sum(%s) %s sum(%s) from BigTable where %s = "%s" and DownloadDate = "%s"'\
                %(left_hand_fact, op, right_hand_fact,dim_name, level_name,fact_date);
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]


    #----------------------------------------
    def addBigTable(self, mergeTable, newBigTable, metaData):
        """
        Move merged BigTable to analytics and adjust table given metaData.
        MetaData: (each col of the BigTable is a dimension, a fact or a date.
        """
        sql = "CREATE TABLE analytics.%s  SELECT * FROM merge.%s;"%(newBigTable, mergeTable)
        print sql
        self.cursor.execute(sql)
        self.changeFactType2Float(newBigTable, metaData)
        self.addDate2BigTable(newBigTable, metaData)
        self.db.commit()

    #----------------------------------------
    def changeFactType2Float(self, bigTable, metaData):
        """
        Adjust fact cols in BigData.
        """
        sql = "use analytics;"
        self.cursor.execute(sql)
        for metaDatum in metaData:
            if metaData[metaDatum] == "fact":
                sql = "ALTER TABLE %s change %s %s FLOAT;"%(bigTable, metaDatum, metaDatum)
                self.cursor.execute(sql)
    #----------------------------------------
    def addDate2BigTable(self, bigTable, metaData):
        """
        Import date format from BigTable and store it into the col ks_date.
        """
        sql = "use analytics;"
        self.cursor.execute(sql)
        sql =  "alter table %s add column ks_date Date;"%(bigTable)
        self.cursor.execute(sql)
        for metaDatum in metaData:
            if metaData[metaDatum] == "date":
                sql1 = "update %s set ks_date = STR_TO_DATE(%s, "%(bigTable, metaDatum)
                sql2= "'%m/%d/%y')"
                sql = sql1 + sql2
                print "************************************"
                print sql
                self.cursor.execute(sql)
