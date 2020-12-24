from distutils.core import setup, Extension

setup(name='test',
      version='1.0',
       install_requires=['ijson', 'sqlite3','yoyo','hashlib','unittest'],
       scripts=["test_main"]
      )
