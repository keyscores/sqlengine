import unittest

import testNewVersion
import testCrossCompany
import testBinaryOps
import testBinaryOpsFilter

loader = unittest.TestLoader()

suite = loader.loadTestsFromModule(testNewVersion)
suite.addTests(loader.loadTestsFromModule(testCrossCompany))
suite.addTests(loader.loadTestsFromModule(testBinaryOps))
suite.addTests(loader.loadTestsFromModule(testBinaryOpsFilter))

runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)