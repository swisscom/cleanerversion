#!/usr/bin/env python
from setuptools import setup, find_packages

"""
Documentation can be found at https://docs.python.org/2/distutils/index.html, but usually you only need to do the
following steps to publish a new package version to PyPI::

    # Update the version tag in this file (setup.py)
    python setup.py register
    python setup.py sdist --formats=gztar,zip upload

That's already it. You should get the following output written to your command line::

    Server response (200): OK

If you get errors, check the following things:

- Are you behind a proxy? --> Try not to be behind a proxy (I don't actually know how to configure setup.py to be proxy-aware)
- Is your command correct? --> Double-check using the reference documentation
- Do you have all the necessary libraries to generate the wanted formats? --> Reduce the set of formats or install libs
"""

setup(name='CleanerVersion',
      version='1.6.1',
      description='A versioning solution for relational data models using the Django ORM',
      long_description='CleanerVersion is a solution that allows you to read and write multiple versions of an entry '
                       'to and from your relational database. It allows to keep track of modifications on an object '
                       'over time, as described by the theory of **Slowly Changing Dimensions** (SCD) **- Type 2**. '
                       ''
                       'CleanerVersion therefore enables a Django-based Datawarehouse, which was the initial idea of '
                       'this package.',
      author='Manuel Jeckelmann, Jean-Christophe Zulian, Brian King, Andrea Marcacci',
      author_email='engineering.sophia@swisscom.com',
      license='Apache License 2.0',
      packages=find_packages(exclude=['cleanerversion', 'cleanerversion.*']),
      url='https://github.com/swisscom/cleanerversion',
      install_requires=['django'],
      package_data={'versions': ['static/js/*.js','templates/versions/*.html']},
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Framework :: Django',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.4',
          'Topic :: Database',
          'Topic :: System :: Archiving',
      ])
