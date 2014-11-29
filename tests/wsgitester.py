import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app

import os
import sys
import unittest
import traceback
from glob import glob
from StringIO import StringIO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'modules')))
sys.path.append(os.path.join(os.path.dirname(__file__), "ks_filehandler"))


class TestPage(webapp2.RequestHandler):
    def get(self):
        """
        Returns a text/plain response of running all tests. The last line is
        very important, it contains "OVERALL:OK" if the tests pass
        "OVERALL:ERROR" if any errors occurred and "OVERALL:FAIL" if failures
        (but 0 errors) occurred.
        """
        self.response.headers['Content-Type'] = 'text/plain'

        loader = unittest.TestLoader()

        test_path = os.path.dirname(__file__)

        test_out = StringIO()

        failed_imports = 0
        suite = None

        for test_module_name in glob(os.path.join(test_path, 'test*.py')):
            test_module_name = os.path.split(test_module_name)[1][:-3]
            print>>test_out, '=' * 80
            print>>test_out, 'tests.' + test_module_name
            print>>test_out, '=' * 80

            try:
                test_module = getattr(__import__('tests.' + test_module_name), test_module_name)
            except:

                typ, err, tb = sys.exc_info()
                traceback.print_tb(tb, None, test_out)
                print>>test_out, typ, err
                failed_imports += 1
                continue

            if suite is None:
                suite = loader.loadTestsFromModule(test_module)
                print>>test_out, 'initialized suite', test_module_name
            else:
                suite.addTests(loader.loadTestsFromModule(test_module))
                print>>test_out, 'added to suite', test_module_name

            print>>test_out, 'OK'


        if suite is None:
            self.response.out.write(test_out.getvalue())
            self.response.out.write('OVERALL:ERROR')
            return

        if failed_imports:
            print>>test_out, '=' * 80

        results = unittest.TextTestRunner(
                stream=test_out, verbosity=2).run(suite)


        self.response.out.write(test_out.getvalue())

        # Last line of OVERALL:... is used by test client to report back to CircleCI'
        if failed_imports > 0 or len(results.errors) > 0:
            self.response.out.write('OVERALL:ERROR')
        elif len(results.failures) > 0:
            self.response.out.write('OVERALL:FAIL')
        else:
            self.response.out.write('OVERALL:OK')



application = webapp2.WSGIApplication([
    ('.*', TestPage),
], debug=True)
