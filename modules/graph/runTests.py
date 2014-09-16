from generalLinks import generalLinks

#test 1
print "Test Case 1:\n"
general_links = generalLinks(["Sales.csv", "CountryRegion.csv"])
general_links.getLinks()
print "\n ---- test case 1 ----"

#test 2
print "Test Case 2:\n"
general_links = generalLinks(["Sales.csv", "CountryRegion.csv", "ComissionTax.csv", "Currencyv2.csv"])
general_links.getLinks()
print "\n ---- test case 2 ----"
