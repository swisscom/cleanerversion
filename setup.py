#!/usr/bin/env python
from setuptools import setup

"""
Documentation can be found at https://docs.python.org/2/distutils/index.html
"""

setup(name='CleanerVersion',
      version='0.1',
      description='A versioning solution for relational data models',
      long_description='',
      author='Manuel Jeckelmann, Jean-Christophe Zulian, Brian King, Andrea Marcacci',
      author_email='engineering.sophia@swisscom.com',
      license='Apache License 2.0',
      packages=['versions'],
      url='https://github.com/swisscom/cleanerversion',
      install_requires=['django'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Framework :: Django',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 2.7',
          'Topic :: Database',
          'Topic :: System :: Archiving',
      ])
