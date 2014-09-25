import unittest

import testSalesTaxSample
import testNewVersion
import testCrossCompany

loader = unittest.TestLoader()

suite = loader.loadTestsFromModule(testSalesTaxSample)
suite.addTests(loader.loadTestsFromModule(testNewVersion))
suite.addTests(loader.loadTestsFromModule(testCrossCompany))

runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)