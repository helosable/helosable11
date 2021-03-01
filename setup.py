from setuptools import setup

setup(name='test',
      version='1.0',
      url="http://packages.python.org",
      install_requires=['ijson', 'yoyo-migrations', 'jinja2', 'sqlite3', 'numpy', 'hashlib', 'collections'],
      test_suite='test_main')
