from setuptools import setup

setup(name='test',
      version='1.0',
      url = "http://packages.python.org",
      install_requires=['ijson', 'yoyo-migrations',],
      test_suite='test_main'
      )
