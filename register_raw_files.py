#Interface to import customer files .csv .xls and register them in appropriate tables
from ks_filehandler import filehandler
import os

def register_raw_files(file_path, company_name, db):
    ks_fh = filehandler(db)
    file_name = os.path.basename(file_path)
    pre_fix, post_fix = file_name.split(".")
    table_name = pre_fix
    ks_fh.addTable(table_name, company_name, file_path)
    ks_fh.updateMeasureTable(file_path, company_name)


def register_raw_files2table(file_path, company_name, table_name, db):
    ks_fh = filehandler(db)
    ks_fh.addTable(table_name, company_name, file_path)
    ks_fh.updateMeasureTableBlob(file_path, company_name)
    
def registerFormula(company_id, formula_name, alias, formula, db, agg_type):
    ks_fh = filehandler(db)
    ks_fh.registerFormula(company_id, formula_name, alias, formula, agg_type)
        


    

    

