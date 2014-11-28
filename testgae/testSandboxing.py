import unittest


class TestSandboxing(unittest.TestCase):
    def test_import_fail(self):
        try:
            import subprocess
            self.fail('Should not be able to import subprocess under sandbox')
        except ImportError:
            pass
