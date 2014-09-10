**************
CleanerVersion
**************

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


Prerequisites
=============

This code was tested with the following technical components

* Python 2.7
* Django 1.6 & 1.7
* PostgreSQL 9.3.4 & SQLite3


Quick Start
===========

Installation
------------

If you don't like to work with the sources directly, you can also install the `CleanerVersion package from PyPI
<https://pypi.python.org/pypi/CleanerVersion>`_ by doing so (you may need superuser privileges, as for every other
pip-installation)::

    pip install cleanerversion

Once you have your Django project in place, register CleanerVersion to the ``INSTALLED_APPS`` variable by adding the 
``versions`` keyword as follows::

    INSTALLED_APPS = (
        ...
        'versions',
        ...
    )

If you want to be sure, whether things work out correctly, run CleanerVersion's unittests from within your Django 
project root::

    python manage.py test versions

If this terminates with a ``OK``, you're all set. Go on and create your models as follows.


A simple versionable model
--------------------------

First, import all the necessary modules. In this example, all the imports are done in the beginning, such that this
would be a working example, if place in the same source file. Here's how::

    from datetime import datetime
    from django.db.models.fields import CharField
    from django.utils.timezone import utc
    from versions.models import Versionable

    class Person(Versionable):
        name = CharField(max_length=200)
        address = CharField(max_length=200)
        phone = CharField(max_length=200)

Assuming you know how to deal with `Django Models <https://docs.djangoproject.com/en/dev/topics/db/models/>`_ (you will need to sync your DB before your
code gets usable; Or you're only testing, then that step is done by Django), the next step is using your model to create
some entries::

    p = Person.objects.create(name='Donald Fauntleroy Duck', address='Duckburg', phone='123456')
    t1 = datetime.utcnow().replace(tzinfo=utc)

    p = p.clone() # Important! Fetch the returned object, it's the current one! Continue work with this one.
    p.address = 'Entenhausen'
    p.save()
    t2 = datetime.utcnow().replace(tzinfo=utc)

    p = p.clone()
    p.phone = '987654'
    p.save()
    t3 = datetime.utcnow().replace(tzinfo=utc)

Now, let's query the entries::

    donald_current = Person.objects.as_of().get(name__startswith='Donald')  # Get the current entry
    print str(donald_current.address)  # Prints 'Entenhausen'
    print str(donald_current.phone)  # Prints '987654'

    donald_t1 = Person.objects.as_of(t1).get(name__startswith='Donald')  # Get a historic entry
    print str(donald_t1.address)  # Prints 'Duckburg'
    print str(donald_t1.phone)  # Prints '123456'

A related versionable model
---------------------------

Here comes the less simple approach. What we are going to set up is both, a Many-to-One- and a
Many-to-Many-relationship. Keep in mind, that this is just an example and we try to focus on the
relationship part, rather than the semantical correctness of the entries' fields::

    from datetime import datetime
    from django.db.models.fields import CharField
    from django.utils.timezone import utc
    from versions.models import Versionable, VersionedManyToManyField, VersionedForeignKey

    class Discipline(Versionable):
        """A sports discipline"""
        name = CharField(max_length=200)
        rules = CharField(max_length=200)

    class SportsClub(Versionable):
        """Sort of an association for practicing sports"""
        name = CharField(max_length=200)
        practice_periodicity = CharField(max_length=200)
        discipline = VersionedForeignKey('Discipline')

    class Person(Versionable):
        name = CharField(max_length=200)
        phone = CharField(max_length=200)
        sportsclubs = VersionedManyToManyField('SportsClub', related_name='members')

Here comes the data loading for demo::

    running = Discipline.objects.create(name='Running', rules='There are none (almost)')
    icehockey = Discipline.objects.create(name='Ice Hockey', rules='There\'s a ton of them')

    stb = SportsClub.objects.create(name='STB', practice_periodicity='tuesday and thursday night', discipline=running)
    hcfg = SportsClub.objects.create(name='HCFG', practice_periodicity='monday, wednesday and friday night', discipline=icehockey)

    peter = Person.objects.create(name='Peter', phone='123456')
    mary = Person.objects.create(name='Mary', phone='987654')

    # Bringing things together
    # Peter wants to run
    peter.sportsclubs.add(stb)

    t1 = datetime.utcnow().replace(tzinfo=utc)

    # Peter later joins HCFG for ice hockey
    hcfg.members.add(peter)

    # Mary joins STB for running
    stb.members.add(mary)

    t2 = datetime.utcnow().replace(tzinfo=utc)

    # HCFG changes the paractice times
    hcfg = hcfg.clone()
    hcfg.practice_periodicity = 'monday, wednesday and thursday'
    hcfg.save()

    # Too bad, new practice times don't work out for Peter anymore, he leaves HCFG
    hcfg.members.remove(peter)
    t3 = datetime.utcnow().replace(tzinfo=utc)

Let's continue with the queries, to check, whether all that story can be reconstructed::

    ### Querying for timestamp t1
    sportsclub = SportsClub.objects.as_of(t1).get(name='HCFG')
    print "Number of " + sportsclub.name + " (" + sportsclub.discipline.name + ") members: " + str(sportsclub.members.count())
    for member in list(sportsclub.members.all()):
        print "- " + str(member.name)  # prints ""

    sportsclub = SportsClub.objects.as_of(t1).get(name='STB')
    print "Number of " + sportsclub.name + " (" + sportsclub.discipline.name + ") members: " + str(sportsclub.members.count())
    for member in list(sportsclub.members.all()):
        print "- " + str(member.name)  # prints "- Peter"


    ### Querying for timestamp t2
    sportsclub = SportsClub.objects.as_of(t2).get(name='HCFG')
    print "Number of " + sportsclub.name + " (" + sportsclub.discipline.name + ") members: " + str(sportsclub.members.count())
    for member in list(sportsclub.members.all()):
        print "- " + str(member.name)  # prints "- Peter"


    ### Querying for timestamp t3
    sportsclub = SportsClub.objects.as_of(t3).get(name='HCFG')
    print "Number of " + sportsclub.name + " (" + sportsclub.discipline.name + ") members: " + str(sportsclub.members.count())
    for member in list(sportsclub.members.all()):
        print "- " + str(member.name)  # prints ""

    sportsclub = SportsClub.objects.as_of(t3).get(name='STB')
    print "Number of " + sportsclub.name + " (" + sportsclub.discipline.name + ") members: " + str(sportsclub.members.count())
    for member in list(sportsclub.members.all()):
        print "- " + str(member.name)  # prints "- Peter\n- Mary"

Pretty easy, isn't it? ;)

Feature requests
================

- Querying for time ranges
