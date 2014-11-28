import unittest


class TestSandboxing(unittest.TestCase):
    def test_chdir_fail(self):
        try:
            import os
            os.chdir('tests')
            self.fail('Should not be able chdir under sandbox')
        except:
            pass


    def test_import_fail(self):
        try:
            import subprocess
            p = subprocess.Popen(['/bin/bash', '-c', 'ls'])
            p.terminate()
            self.fail('Should not be able to use subprocess under sandbox')
        except:
            pass
