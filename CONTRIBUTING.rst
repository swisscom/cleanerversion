******************************
Contributing to CleanerVersion
******************************

Contributions are welcome!

How to contribute
=================
#. Fork the `source repository <https://github.com/swisscom/cleanerversion>`_
#. Make your changes
#. If appropriate, add tests that test the feature you have added or the bug that you have fixed
#. Ensure that tests are passing (``python manage.py test``)
#. Optionally, test all target combinations locally using Tox (see Local Testing, below)
#. Create a pull request
#. Check that integration testing was successful.  This takes a few minutes after the pull request
   is made; Travis runs the tests on the target python versions and database types
#. Wait for feedback or merging of your pull request

Style Guide
===========
Cleanerversion aims to follow `PEP8 <https://www.python.org/dev/peps/pep-0008/>`_
including 4 space indents and 79 character line limits.


Testing
=======
All tests are run on Travis and any pull requests are automatically tested by Travis. Any pull
requests without tests will take longer to be integrated and might be refused.

Local Testing
-------------
To test locally on the various environments that are tested by Travis, you can use `tox <https://testrun.org/tox/latest/>`_.
To do this, these dependencies must be installed:

* python 2.7 and python 3.4
* tox (if you're using pip, you can install tox with ``pip install tox``)
* postgresql 9.3.x

``cleanerversion/settings/pg.py`` defines the username and password that will be used for Postgresql.
The Postgresql user must have the createdb permission, because the django tests create a test
database whenever tests are run.

If you directly modify ``cleanerversion/settings/pg.py``, be careful not to add it when doing
a git commit.

A better approach is to copy ``cleanerversion/settings/pg.py`` to ``cleanerversion/settings/pg_local.py``
and edit it to have the database connection information that you would like.  This file will be ignored
by git.  You will then need to set an environment variable when running ``tox`` to let it know what settings
file to use.

Example of creating a postgresql user ``cleanerversionpg`` who has permission to create databases::

    $ sudo su postgres
    $ createuser -Pd cleanerversionpg

Running tox::

    $ tox

If you want to use a custom database settings file for the postgresql tests, do something like this::

    $ export TOX_PG_CONF=cleanerversion.settings.pg_local
    $ tox
