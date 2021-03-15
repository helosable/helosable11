import unittest
import argparse

pasrser = argparse.ArgumentParser()
pasrser.add_argument('-a', '--arg', type = str, default = 'ip_report')

loader = unittest.TestLoader()
testSuite = loader.discover('tests')
testRunner = unittest.TextTestRunner(verbosity=2)
testRunner.run(testSuite)
