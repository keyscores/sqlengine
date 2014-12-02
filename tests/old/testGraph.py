from ks_graph import generalLinks
import unittest

class TestGraph(unittest.TestCase):
    
            
    def test_Two_Tables_One_Link_Case1(self):
        general_links = generalLinks(["./tests/data2/Sales.csv", 
                                      "./tests/data2/CountryRegion.csv"])
        self.assertEqual(True, general_links.isEdge('Sales.csv:CountryCode', 'CountryRegion.csv:CountryCode'))
        
    def test_Two_Tables_One_Link_Case2(self):
        general_links = generalLinks(["./tests/data2/Sales.csv", 
                                      "./tests/data2/CountryRegion.csv"])
        self.assertEqual(True, general_links.isEdge('CountryRegion.csv:CountryCode','Sales.csv:CountryCode',))
        
    def test_Two_Tables_One_Link_Case3(self):
        general_links = generalLinks(["./tests/data2/Sales.csv", 
                                      "./tests/data2/CountryRegion.csv"])
        self.assertEqual(False, general_links.isEdge('Sales.csv:CountryCode', 'CountryRegion.csv:CountryCode'))    

    def test_Four_Tables_Five_Links(self):
        general_links = generalLinks(["./tests/data2/Sales.csv", 
                                      "./tests/data2/CountryRegion.csv",
                                      "./tests/data2/ComissionTax.csv",
                                      "./tests/data2/Currencyv2.csv"])
        general_links.getLinks()
        self.assertEqual(True, general_links.isEdge('ComissionTax.csv:Region', 'CountryRegion.csv:Region'))
        self.assertEqual(True, general_links.isEdge('ComissionTax.csv:VendorId', 'Sales.csv:VendorId'))
        self.assertEqual(True, general_links.isEdge('Sales.csv:CountryCode', 'CountryRegion.csv:CountryCode'))
        self.assertEqual(True, general_links.isEdge('Sales.csv:DownloadDate', 'Currencyv2.csv:DownloadDate'))
        self.assertEqual(True, general_links.isEdge('Currencyv2.csv:CustomerCurrency', 'Sales.csv:CustomerCurrency'))



 
if __name__ == '__main__':
    unittest.main()
