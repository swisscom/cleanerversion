*************************
CleanerVersion for Django
*************************

.. image:: https://img.shields.io/travis/swisscom/cleanerversion/master.svg
    :target: https://travis-ci.org/swisscom/cleanerversion
.. image:: https://img.shields.io/coveralls/swisscom/cleanerversion/master.svg
   :target: https://coveralls.io/r/swisscom/cleanerversion
.. image:: https://img.shields.io/pypi/v/CleanerVersion.svg
   :target: https://pypi.python.org/pypi/CleanerVersion

Abstract
========

CleanerVersion is a solution that allows you to read and write multiple versions of an entry to and from your
relational database. It allows to keep track of modifications on an object over time, as described by the theory of
**Slowly Changing Dimensions** (SCD) **- Type 2**.

CleanerVersion therefore enables a Django-based Datawarehouse, which was the initial idea of this package.

Features
========

CleanerVersion's feature-set includes the following bullet points:

* Simple versioning of an object (according to SCD, Type 2)

  - Retrieval of the current version of the object
  - Retrieval of an object's state at any point in time

* Versioning of One-to-Many relationships

  - For any point in time, retrieval of correct related objects

* Versioning of Many-to-Many relationships

  - For any point in time, retrieval of correct related objects

* Migrations, if using in conjunction with Django 1.7 and upwards

* Integration with Django Admin (Credits to @boydjohnson and @peterfarrell)


Prerequisites
=============

This code was tested with the following technical components

* Python 2.7 & 3.6
* Django 1.11 & 2.0
* PostgreSQL 9.3.4 & SQLite3

Older Django versions
=====================
CleanerVersion was originally written for Django 1.6 and has now been ported up to Django 1.11.

CleanerVersion 2.x releases are compatible with Django 1.11 and 2.0. It may
also work with Django 1.9 and 1.10, but note that these versions are not
officially supported and test cases have been removed.

Old packages compatible with older Django releases:

* Django 1.6 and 1.7: https://pypi.python.org/pypi/CleanerVersion/1.5.4

* Django 1.8: https://pypi.python.org/pypi/CleanerVersion/1.6.2

* Django 1.9 & 1.10: https://pypi.python.org/pypi/CleanerVersion/2.0.0

Documentation
=============

Find a detailed documentation at http://cleanerversion.readthedocs.org/.


Feature requests
================

- Querying for time ranges
