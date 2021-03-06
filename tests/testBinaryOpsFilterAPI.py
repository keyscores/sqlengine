from ks_filehandler import filehandler
from ks_analytics import analytics
from ks_precompute import precompute
import ks_db_settings
import unittest

# import API
from register_raw_files import register_raw_filesCsvPy
from load_precompute_normalize import load_precompute_normalize_CsvPy
from user_analytics import measure_data_V1


class TestBinaryOpsAPIFilter(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        #----------------------
        # set up db
        #----------------------
        cls.db = ks_db_settings.connect()

        cls.ks_fh = filehandler(cls.db)
        cls.ks_fh.reset()
        cls.company_id = 1
        register_raw_filesCsvPy("Sales",cls.company_id, cls.db)
        register_raw_filesCsvPy("CurrencyV2",cls.company_id, cls.db)
        register_raw_filesCsvPy("ComissionTax",cls.company_id, cls.db)
        register_raw_filesCsvPy("CountryRegion",cls.company_id, cls.db)
    
        ks_precompute = precompute(cls.db)
        ks_precompute.reset()
        #precompute
        load_precompute_normalize_CsvPy(cls.company_id, cls.db)
        newBigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(cls.company_id))
        cls.ks_analytics = analytics(cls.db)
        
        cls.ks_analytics.addFactUsingBinaryOpAPI("NET_REVENUE", "Units", "RoyaltyPrice", "*", newBigTable) 
        cls.ks_analytics.addFactUsingBinaryOpAPI("TAXES", "NET_REVENUE","TaxRate","*", newBigTable)
        cls.ks_analytics.addFactUsingBinaryOpAPI("REVENUE_AFTER_TAX", "NET_REVENUE","TAXES","-", newBigTable)
        
        
        cls.ks_fh.registerFormula("", "Plus", "Plus", "Units+RoyaltyPrice", "sum")
        cls.ks_fh.registerFormula("", "Mult", "Mult", "Units*RoyaltyPrice", "sum")
        cls.ks_fh.registerFormula("", "Individual_Tax", "Individual_Tax", "RoyaltyPrice*TaxRate", "sum")
        cls.ks_fh.registerFormula("", "NET_REVENUE", "NET_REVENUE", "Units*RoyaltyPrice", "sum")
        cls.ks_fh.registerFormula("", "SumPlus", "SumPlus", "Sum(Units)+Sum(RoyaltyPrice)", "sum")
        cls.ks_fh.registerFormula("", "SumMult", "SumMult", "Sum(Units)*Sum(RoyaltyPrice)", "sum")
        cls.ks_fh.registerFormula("", "Individual_TaxSum", "Individual_TaxSum", "Sum(RoyaltyPrice)*Sum(TaxRate)", "sum")
        cls.ks_fh.registerFormula("", "NonsenseSum", "Individual_Tax", "Sum(RoyaltyPrice)+Sum(TaxRate)", "sum")
        cls.ks_fh.registerFormula("", "Nonsense", "Individual_Tax", "RoyaltyPrice+TaxRate", "sum")
        cls.ks_fh.registerFormula("", "REVENUE_AFTER_TAX", "REVENUE_AFTER_TAX", "", "sum")
        
        #cls.ks_analytics.addFactUsingBinaryOp("NET_REVENUE", "Units", "RoyaltyPrice", "*") 
        #cls.ks_analytics.addFactUsingBinaryOp("TAXES", "NET_REVENUE","TaxRate","*")
        #cls.ks_analytics.addFactUsingBinaryOp("REVENUE_AFTER_TAX", "NET_REVENUE","TAXES","-")
        
        
                
    @classmethod        
    def tearDownClass(cls):
        ks_db_settings.reset_all(cls.db)
        cls.db.close()


    def MeasureName2MeasureIds(self, name):
        measure_id = self.ks_fh.getMeasureID(name)
        measure_ids = []
        measure_ids.append(measure_id)
        return measure_ids
            
    # Binary Op    
    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Aggregate(self):
        measure_ids = self.MeasureName2MeasureIds("Units")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertEqual(6, result["Units"]["total"])
        
    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Multiplication_without_groupby_per_record(self):
        measure_ids = self.MeasureName2MeasureIds("NET_REVENUE")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertEqual(12, result["NET_REVENUE"]["total"])
        

    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Multiplication_with_groupby(self):
        measure_ids = self.MeasureName2MeasureIds("SumMult")
        group_by = "ProductType, ks_date, VendorId "
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        value = 0
        code_result = result["SumMult"]
        for key in code_result:
            value = value + int(code_result[key])
        self.assertEqual(24, value)
        
    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Multiplication_with_groupby_date(self):
        measure_ids = self.MeasureName2MeasureIds("SumMult")
        group_by = "ks_date"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        value = 0
        code_result = result["SumMult"]
        for key in code_result:
            value = value + int(code_result[key])
        self.assertEqual(24, value)
        
        
    #@unittest.skip("demonstrating skipping")    
    def test_Binary_Op_Addition_without_groupby_per_record(self):
        measure_ids = self.MeasureName2MeasureIds("Plus")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertEqual(10, result["Plus"]["total"])
        
    
    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Addition_with_groupby(self):
        measure_ids = self.MeasureName2MeasureIds("SumPlus")
        group_by = "ks_date, VendorId, ProductType"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        value = 0
        code_result = result["SumPlus"]
        for key in code_result:
            value = value + int(code_result[key])
        self.assertEqual(10, value)
        
        
    #@unittest.skip("demonstrating skipping")
    def test_Binary_Op_Addition_with_groupby_date(self):
        measure_ids = self.MeasureName2MeasureIds("SumPlus")
        group_by = "ks_date"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        value = 0
        code_result = result["SumPlus"]
        for key in code_result:
            value = value + int(code_result[key])
        self.assertEqual(10, value)
        
        
        
    # Intertable
    #@unittest.skip("demonstrating skipping")
    def test_Intertable_Multiplication_without_groupby_per_record(self):
        #self.assertAlmostEqual(0.6192, self.ks_analytics.calculate("RoyaltyPrice*TaxRate","6/1/14"))
        measure_ids = self.MeasureName2MeasureIds("Individual_Tax")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertAlmostEqual(0.209600002, result["Individual_Tax"]["total"])

    #@unittest.skip("demonstrating skipping")
    def test_Intertable_Multiplication_with_groupby(self):
        measure_ids = self.MeasureName2MeasureIds("Individual_TaxSum")
        group_by = "ks_date, VendorId, ProductType"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        print result
        value = 0.0
        code_result = result["Individual_TaxSum"]
        for key in code_result:
            value = value + float(code_result[key])
        self.assertAlmostEqual(0.419200005, value)
       
        
    @unittest.skip("demonstrating skipping")    
    def test_Intertable_Multiplication_with_groupby_date(self):
        measure_ids = self.MeasureName2MeasureIds("Individual_TaxSum")
        group_by = "ks_date"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        print result
        value = 0.0
        code_result = result["Individual_TaxSum"]
        for key in code_result:
            value = value + float(code_result[key])
        self.assertAlmostEqual(4.95360006, value)
        
        
    #@unittest.skip("demonstrating skipping")    
    def test_Intertable_Addition_without_groupby_per_record(self):
        measure_ids = self.MeasureName2MeasureIds("Nonsense")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertAlmostEqual(4.104800001, result["Nonsense"]["total"])
    
    #@unittest.skip("demonstrating skipping")
    def test_Intertable_Addition_with_groupby(self):
        measure_ids = self.MeasureName2MeasureIds("NonsenseSum")
        group_by = "ks_date,ProductType, DownloadDate"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        print result
        value = 0.0
        code_result = result["NonsenseSum"]
        for key in code_result:
            value = value + float(code_result[key])
        self.assertAlmostEqual(4.104800001, value)
        
        
    #@unittest.skip("demonstrating skipping")    
    def test_Intertable_Addition_with_groupby_date(self):
        measure_ids = self.MeasureName2MeasureIds("NonsenseSum")
        group_by = "ks_date"
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01", group_by,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        print result
        value = 0.0
        code_result = result["NonsenseSum"]
        for key in code_result:
            value = value + float(code_result[key])
        self.assertAlmostEqual(4.104800001, value)
        
    # Chained
    #@unittest.skip("demonstrating skipping")
    def test_Chained_Intertable(self):
        measure_ids = self.MeasureName2MeasureIds("REVENUE_AFTER_TAX")
        result = measure_data_V1(self.db, self.company_id, measure_ids, "day", "2014-06-01", "2014-06-01",None,None,
                              "and VendorId='0268_20140114_SOFA_ENGLIS'")
        self.assertAlmostEqual(11.37120008468, result["REVENUE_AFTER_TAX"]["total"])
        
    
    @unittest.skip("demonstrating skipping")
    def test_Chained_Intertable_sm2(self):
        self.assertAlmostEqual(0.9226, self.ks_analytics.calculateSm2("REVENUE_AFTER_TAX","/","NET_REVENUE", "6/1/14",None))
    
    @unittest.skip("demonstrating skipping")                     
    def test_Chained_Intertable_sm2_sandwich(self):
        self.assertEqual(1234, self.ks_analytics.calculateGroupBy("sum(TaxRate) + sum(RoyaltyPrice)",
                                                                " DownloadDate",
                                                                " DownloadDate= '6/1/14'"))



if __name__ == '__main__':
    unittest.main()
    
