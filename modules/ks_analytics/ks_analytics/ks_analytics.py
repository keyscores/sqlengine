class analytics:

    def __init__(self,db):
        self.db = db
        self.cursor = self.db.cursor()


    #----------------------------------------
    def addFactUsingBinaryOp(self,
                             new_fact_name,
                             left_hand_fact,
                             right_hand_fact,
                             op):

        sql =  "alter table BigTable add column %s FLOAT;"%(new_fact_name)
        print sql
        self.cursor.execute(sql)
        sql = "update BigTable set %s = %s%s%s;"%(new_fact_name,
                                                  left_hand_fact,
                                                  op,
                                                  right_hand_fact)
        self.cursor.execute(sql)
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
