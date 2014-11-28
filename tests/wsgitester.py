import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app

import os
import sys
import unittest
from StringIO import StringIO

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, 'modules'))


class TestPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'

        test_path = os.path.join(os.path.dirname(__file__), 'testsuite')

        loader = unittest.TestLoader()

        import tests.testFormulaParser

        suite = loader.loadTestsFromModule(tests.testFormulaParser)

        test_out = StringIO()
        unittest.TextTestRunner(stream=test_out, verbosity=2).run(suite)


        self.response.out.write(test_out.getvalue())


application = webapp2.WSGIApplication([
    ('.*', TestPage),
], debug=True)
