import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app

import os
import sys
import unittest
import traceback
from glob import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'modules')))
sys.path.append(os.path.join(os.path.dirname(__file__), "ks_filehandler"))

class ResponseWrapper(object):
    def __init__(self, response):
        self.response = response
    def write(self, *args, **kwargs):
        self.response.write(*args, **kwargs)
    def flush(self, *args, **kwargs):
        pass
    def close(self, *args, **kwargs):
        pass

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


        failed_imports = 0

        test_filter = self.request.get('filter')

        suite = None
        for test_module_name in glob(os.path.join(test_path, 'test*.py')):
            test_module_name = os.path.split(test_module_name)[1][:-3]
            if test_filter:
                if test_filter[0] == '-':
                    if test_module_name == test_filter[1:]:
                        continue
                elif test_module_name != test_filter:
                    continue

            test_out = ResponseWrapper(self.response)
            print>>test_out, '=' * 60
            print>>test_out, 'tests.' + test_module_name
            print>>test_out, '=' * 60

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
                print>>test_out, 'initialized suite with', test_module_name
            else:
                suite.addTests(loader.loadTestsFromModule(test_module))
                print>>test_out, 'added to suite', test_module_name
                

            print>>test_out, 'OK'
            
            

        error_sum, failure_sum = 0, 0
        if suite:
            print>>test_out, '=' * 60
            print>>test_out, 'RUNNING TESTS'
            print>>test_out, '=' * 60
            results = unittest.TextTestRunner(
                stream=ResponseWrapper(test_out), verbosity=2).run(suite)
            error_sum, failure_sum = len(results.errors), len(results.failures)


        # Last line of OVERALL:... is used by test client to report back to CircleCI'
        if failed_imports > 0 or error_sum > 0:
            self.response.out.write('OVERALL:ERROR')
        elif failure_sum > 0:
            self.response.out.write('OVERALL:FAIL')
        else:
            self.response.out.write('OVERALL:OK')



application = webapp2.WSGIApplication([
    ('.*', TestPage),
], debug=True)
