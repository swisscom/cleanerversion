*************************
CleanerVersion for Django
*************************

.. image:: https://travis-ci.org/swisscom/cleanerversion.png?branch=master
    :target: https://travis-ci.org/swisscom/cleanerversion
.. image:: https://coveralls.io/repos/swisscom/cleanerversion/badge.png?branch=master
   :target: https://coveralls.io/r/swisscom/cleanerversion
.. image:: https://pypip.in/v/cleanerversion/badge.png
   :target: https://pypi.python.org/pypi/CleanerVersion
.. image:: https://pypip.in/d/cleanerversion/badge.png
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


Prerequisites
=============

This code was tested with the following technical components

* Python 2.7 & 3.4
* Django 1.6 & 1.7
* PostgreSQL 9.3.4 & SQLite3


Documentation
=============

Find a detailed documentation at http://cleanerversion.readthedocs.org/.


Feature requests
================

- Querying for time ranges
