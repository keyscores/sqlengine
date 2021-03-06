csv_sales = [['VendorId', 'ProductType', 'Units', 'RoyaltyPrice', 'DownloadDate', 'CustomerCurrency', 'CountryCode'],
              ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '3', '2', '6/1/14', 'USD', 'BR'],
              ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '3', '2', '6/1/14', 'MXN', 'MX'],
              ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'M', '3', '2', '6/1/14', 'MXN', 'MX'],
              ['0314_20140224_SOFA_QUEDAMORTAL', 'D', '3', '2', '6/1/14', 'USD', 'BR'],
              ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '4', '3', '6/2/14', 'MXN', 'MX']]

csv_currencyV2 = [['ExchangeRate', 'DownloadDate', 'CustomerCurrency'],
                  ['0.070', '6/1/14', 'MXN'],
                  ['0.068', '6/2/14', 'MXN'],
                  ['1.000', '6/1/14', 'USD'],
                  ['1.000', '6/2/14', 'USD']]

csv_countryRegion = [['CountryCode', 'Region'], 
                     ['MX', 'Latam'], 
                     ['BR', 'World'], 
                     ['AR', 'Latam']]

csv_comissionTax = [['VendorId', 'Region', 'RightsHolder', 'ComissionRate', 'TaxRate'], 
                    ['0268_20140114_SOFA_ENGLISHTEACHER', 'Latam', 'Studio A', '10%', '0.1'], 
                    ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'Latam', 'Studio A', '20%', '0.2'], 
                    ['0314_20140224_SOFA_QUEDAMORTAL', 'Latam', 'Studio A', '20%', '0.1'], 
                    ['0023_20120510_MOBZ_DAYDREAMNATION', 'Latam', 'Studio A', '20%', '0.2'], 
                    ['0268_20140114_SOFA_ENGLISHTEACHER', 'World', 'B Studio', '5%', '0.0048'], 
                    ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'World', 'B Studio', '5%', '0.0048'], 
                    ['0314_20140224_SOFA_QUEDAMORTAL', 'World', 'B Studio', '5%', '0.0048'], 
                    ['0023_20120510_MOBZ_DAYDREAMNATION', 'World', 'B Studio', '5%', '0.0048']]

csv_sales2 = [['VendorId', 'ProductType', 'Units', 'RoyaltyPrice', 'DownloadDate', 'CustomerCurrency', 'CountryCode'],
              ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '300', '200', '6/1/14', 'USD', 'BR'],
              ['0268_20140114_SOFA_ENGLISHTEACHER', 'D', '0', '0', '6/1/14', 'MXN', 'MX'], 
              ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'M', '0', '0', '6/1/14', 'MXN', 'MX'], 
              ['0314_20140224_SOFA_QUEDAMORTAL', 'D', '3', '2', '6/1/14', 'USD', 'BR']]

csv_currencyV22 = [['ExchangeRate', 'DownloadDate', 'CustomerCurrency'], 
                   ['0.070', '6/1/14', 'MXN'], 
                   ['0.068', '6/2/14', 'MXN'], 
                   ['1.000', '6/1/14', 'USD'], 
                   ['1.000', '6/2/14', 'USD']]

csv_countryRegion2 = [['CountryCode', 'Region'], 
                      ['MX', 'Latam'], 
                      ['BR', 'World'], 
                      ['AR', 'Latam']]

csv_comissionTax2 = [['VendorId', 'Region', 'RightsHolder', 'ComissionRate', 'TaxRate'], 
                    ['0268_20140114_SOFA_ENGLISHTEACHER', 'Latam', 'Studio A', '1%', '70%'], 
                    ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'Latam', 'Studio A', '1%', '70%'], 
                    ['0314_20140224_SOFA_QUEDAMORTAL', 'Latam', 'Studio A', '1%', '70%'], 
                    ['0023_20120510_MOBZ_DAYDREAMNATION', 'Latam', 'Studio A', '1%', '70%'], 
                    ['0268_20140114_SOFA_ENGLISHTEACHER', 'World', 'B Studio', '1%', '70%'], 
                    ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'World', 'B Studio', '1%', '70%'], 
                    ['0314_20140224_SOFA_QUEDAMORTAL', 'World', 'B Studio', '1%', '70%'], 
                    ['0023_20120510_MOBZ_DAYDREAMNATION', 'World', 'B Studio', '1%', '70%']]

csv_sales_new_version = [['VendorId', 'ProductType', 'Units', 'RoyaltyPrice', 'DownloadDate', 'CustomerCurrency', 'CountryCode'], 
                         ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '30', '20', '6/1/14', 'USD', 'BR'], 
                         ['0268_20140114_SOFA_ENGLISHTEACHER', 'D', '30', '20', '6/1/14', 'MXN', 'MX'], 
                         ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'M', '30', '20', '6/1/14', 'MXN', 'MX'], 
                         ['0314_20140224_SOFA_QUEDAMORTAL', 'D', '30', '20', '6/1/14', 'USD', 'BR'], 
                         ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '40', '30', '6/2/14', 'MXN', 'MX']]

csv_comissionTax_new_version = [['VendorId', 'Region', 'RightsHolder', 'ComissionRate', 'TaxRate'], 
                                ['0268_20140114_SOFA_ENGLISHTEACHER', 'Latam', 'Studio A', '1%', '70%'], 
                                ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'Latam', 'Studio A', '1%', '70%'], 
                                ['0314_20140224_SOFA_QUEDAMORTAL', 'Latam', 'Studio A', '1%', '70%'], 
                                ['0023_20120510_MOBZ_DAYDREAMNATION', 'Latam', 'Studio A', '1%', '70%'], 
                                ['0268_20140114_SOFA_ENGLISHTEACHER', 'World', 'B Studio', '1%', '70%'], 
                                ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'World', 'B Studio', '1%', '70%'], 
                                ['0314_20140224_SOFA_QUEDAMORTAL', 'World', 'B Studio', '1%', '70%'], 
                                ['0023_20120510_MOBZ_DAYDREAMNATION', 'World', 'B Studio', '1%', '70%']]

csv_sales_freq = [['VendorId', 'ProductType', 'Units', 'RoyaltyPrice', 'DownloadDate', 'CustomerCurrency', 'CountryCode'], 
                  ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'M', '3', '2', '4/1/14', 'MXN', 'MX'], 
                  ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '3', '2', '6/1/14', 'USD', 'BR'], 
                  ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '3', '2', '6/1/14', 'MXN', 'MX'], 
                  ['0273_20140114_SOFA_ASSAULTONWALLSTREET', 'M', '3', '2', '6/1/14', 'MXN', 'MX'], 
                  ['0314_20140224_SOFA_QUEDAMORTAL', 'D', '3', '2', '6/1/14', 'USD', 'BR'], 
                  ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '4', '3', '6/2/14', 'MXN', 'MX'], 
                  ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '4', '3', '9/1/14', 'MXN', 'MX'], 
                  ['0268_20140114_SOFA_ENGLISHTEACHER', 'M', '4', '3', '12/4/14', 'MXN', 'MX']]

csv_dict = {}
csv_dict["Sales"] = csv_sales
csv_dict["CurrencyV2"] = csv_currencyV2
csv_dict["CountryRegion"] = csv_countryRegion
csv_dict["ComissionTax"] = csv_comissionTax

csv_dict["Sales2"] = csv_sales2
csv_dict["CurrencyV22"] = csv_currencyV22
csv_dict["CountryRegion2"] = csv_countryRegion2
csv_dict["ComissionTax2"] = csv_comissionTax2

csv_dict["Sales_new_version"] = csv_sales_new_version
csv_dict["ComissionTax_new_version"] = csv_comissionTax_new_version
csv_dict["Salesfreq"] = csv_sales_freq

