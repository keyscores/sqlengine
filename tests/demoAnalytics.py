from ks_filehandler import filehandler
from ks_precompute import precompute
from ks_analytics import analytics
import ks_db_settings
from user_analytics import measure_data

# import API
from register_raw_files import register_raw_filesCsvPy
from load_precompute_normalize import load_precompute_normalize_CsvPy


def MeasureName2MeasureIds(name):
        measure_id = ks_fh.getMeasureID(name)
        measure_ids = []
        measure_ids.append(measure_id)
        return measure_ids


#----------------------
# set up db
#----------------------
db = ks_db_settings.connect()

ks_fh = filehandler(db)
ks_fh.reset()
company_id = 1

register_raw_filesCsvPy("Sales",company_id, db)
register_raw_filesCsvPy("CurrencyV2",company_id, db)
register_raw_filesCsvPy("ComissionTax",company_id, db)
register_raw_filesCsvPy("CountryRegion",company_id, db)

ks_precompute = precompute(db)
ks_precompute.reset()
#precompute
load_precompute_normalize_CsvPy(company_id, db)
newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(company_id))
ks_analytics = analytics(db)

ks_analytics.addFactUsingBinaryOpAPI("NET_REVENUE", "Units", "RoyaltyPrice", "*", newBigTable) 
ks_analytics.addFactUsingBinaryOpAPI("TAXES", "NET_REVENUE","TaxRate","*", newBigTable)
ks_analytics.addFactUsingBinaryOpAPI("REVENUE_AFTER_TAX", "NET_REVENUE","TAXES","-", newBigTable)


ks_fh.registerFormula("", "SumMult", "SumMult", "Sum(Units)*Sum(RoyaltyPrice)", "sum")
measure_ids = MeasureName2MeasureIds("SumMult")
group_by = "ks_date, VendorId"

print(ks_analytics.parseGroupBy(group_by))
result = measure_data(db, company_id, measure_ids, "day", "2014-06-01", "2014-06-02",group_by)
print result

    
