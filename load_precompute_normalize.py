#API to load, precompute and normalize
from ks_filehandler import filehandler
from ks_merge import merge
from ks_merge import precompute

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
    
    ks_precompute = precompute(db)
    meta_data = {'VendorId': 'dim', 
                 'ProductType':'dim',
                 'Units':'fact',
                 'RoyaltyPrice':'fact',
                 'DownloadDate':'date',
                 'CustomerCurrency':'dim',
                 'CountryCode':'dim',
                 'Region':'dim',
                 'ExchangeRate':'fact',
                 'TaxRate':'fact',
                 'RightsHolder':'dim',
                 'ComissionRate':'fact',
                 'id':'sys'}
    ks_precompute.reset()
    ks_precompute.addBigTable(meta_data,"Sales",1)
    