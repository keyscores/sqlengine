#API to load, precompute and normalize
from ks_filehandler import filehandler
from ks_merge import merge
from ks_precompute import precompute
from ks_analytics import analytics

def load_precompute_normalize(company_name, db):
    ks_fh = filehandler(db)
    rows = ks_fh.getLatestTablesByCompany(company_name)
    ks_merge = merge(db)
    ks_merge.reset()
    for row in rows:
        table_name =row[1]
        file_path = row[2]
        ks_merge.addTable(file_path, table_name)

    ks_merge.automaticMerge()

    mergeBigTable = ks_merge.getTables()
    ks_precompute = precompute(db)
    meta_data = ks_merge.getMetaDataFromTable(mergeBigTable[0])
    ks_precompute.reset()
    ks_precompute.addBigTable(meta_data,mergeBigTable[0],company_name)

    id = ks_precompute.getMaxBigTableIdForCompany(company_name)


    metaData = ks_merge.getMetaDataFromTable(mergeBigTable[0])

    ks_analytics = analytics(db)
    newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(company_name))
    ks_analytics.reset()
    ks_analytics.addBigTable(mergeBigTable[0], newBigTable, metaData)
    

def load_precompute_normalize_blob(company_name, db):
    ks_fh = filehandler(db)
    rows = ks_fh.getLatestTablesByCompany(company_name)
    ks_merge = merge(db)
    ks_merge.reset()
    for row in rows:
        table_name =row[1]
        file_path = row[2]
        ks_merge.addTableBlob(file_path, table_name)

    ks_merge.automaticMerge()

    ks_precompute = precompute(db)
    meta_data = ks_merge.getMetaDataFromTable("Sales")
    ks_precompute.reset()
    ks_precompute.addBigTable(meta_data,"Sales",company_name)

    id = ks_precompute.getMaxBigTableIdForCompany(company_name)

    mergeBigTable = ks_merge.getTables()
    metaData = ks_merge.getMetaDataFromTable(mergeBigTable[0])

    ks_analytics = analytics(db)
    newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(company_name))
    ks_analytics.reset()
    ks_analytics.addBigTable(mergeBigTable[0], newBigTable, metaData)

def load_precompute_normalize_URL(company_name, db):
    ks_fh = filehandler(db)
    rows = ks_fh.getLatestTablesByCompany(company_name)
    ks_merge = merge(db)
    ks_merge.reset()
    for row in rows:
        table_name =row[1]
        file_path = row[2]
        ks_merge.addTableURL(file_path, table_name)

    ks_merge.automaticMerge()

    mergeBigTable = ks_merge.getTables()
    ks_precompute = precompute(db)
    meta_data = ks_merge.getMetaDataFromTable(mergeBigTable[0])
    ks_precompute.reset()
    ks_precompute.addBigTable(meta_data,mergeBigTable[0],company_name)

    id = ks_precompute.getMaxBigTableIdForCompany(company_name)


    metaData = ks_merge.getMetaDataFromTable(mergeBigTable[0])

    ks_analytics = analytics(db)
    newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(company_name))
    ks_analytics.reset()
    ks_analytics.addBigTable(mergeBigTable[0], newBigTable, metaData)
