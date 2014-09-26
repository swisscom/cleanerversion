*********************************
Historization with CleanerVersion
*********************************

Disclaimer: This documentation as well as the CleanerVersion application code have been written to work against Django
1.6.x and 1.7. The documentation may not be accurate anymore when using more recent versions of Django.

.. _cleanerversion-quick-starter:

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


HowTo
=====

The first step is to import :class:`versions.models.Versionable`. :class:`~versions.models.Versionable` subclasses
:class:`django.db.models.Model` and can thus be accessed in the same way.

#TODO: add further stuff here or remove this chapter!

Slowly Changing Dimensions - Type 2
===================================

Find the basics of `slowly changing dimensions - type 2`_ and other types at Wikipedia. These concepts were taken
over and extended to cover different types of relationships.

The technical details and assumptions are documented in the following sections.

.. _`slowly changing dimensions - type 2`: http://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2
__ `slowly changing dimensions - type 2`_

Historization of a single entity
================================

The definition of :class:`~versions.models.Versionable` fields is as follows:

id
    The virtual ID of an entry. This field figures also as the primary key (pk) and is randomly created

identity
    Identifies an object over all its versions, i.e. identity does not change from one version to another

version_birth_date (formerly ``created_date``)
    The timestamp at which an object was created. All versions of an object will have the same creation date.

version_start_date (formerly ``version_date``)
    The timestamp at which a version was created.

version_end_date (formerly ``clone_date``)
    The timestamp at which a version was cloned. If a version has not been cloned yet, ``version_end_date`` will be
    set to ``None`` (or NULL) and the entry is considered the most recent entry of an object (i.e. it is the object's
    current version)


Let's assume the following class definition for this hands-on::

    class Item(Versionable):
        name = CharField(max_length="200")  # referred to as the payload data
        version = CharField(max_length="200")  # part of the payload data as well; added for more transparency

Having the class, let's create an instance of it::

    item = Item.objects.create(name="Peter Muster", version="1")

This sequence of commands generated the following DB entry in the table associated to ``Item`` (inheriting from
:class:`~versions.models.Versionable`):

+----------+----------+---------------------+---------------------+------------------+--------------+---------+
| id (pk)  | identity | version_birth_date  | version_start_date  | version_end_date | name         | version |
+==========+==========+=====================+=====================+==================+==============+=========+
| 123      | 123      | 2014-08-14 14:43:00 | 2014-08-14 14:43:00 | None             | Peter Muster | 1       |
+----------+----------+---------------------+---------------------+------------------+--------------+---------+

Once you wish to change some value on your object, do it as follows::

    item = item.clone()
    item.name = "Peter Mauser"
    item.version = "2"
    item.save()

In the first line, we create the new version of the item entry and assign it immediately to the same variable we used
to work with.

On the new version, we can now change the payload data at will and ``save()`` the object, once we're done.

On a DB level, things will look as follows:

+----------+----------+---------------------+---------------------+---------------------+--------------+---------+
| id (pk)  | identity | version_birth_date  | version_start_date  | version_end_date    | name         | version |
+==========+==========+=====================+=====================+=====================+==============+=========+
| 123      | 123      | 2014-08-14 14:43:00 | 2014-08-14 15:09:00 | None                | Peter Mauser | 2       |
+----------+----------+---------------------+---------------------+---------------------+--------------+---------+
| 124      | 123      | 2014-08-14 14:43:00 | 2014-08-14 14:43:00 | 2014-08-14 15:09:00 | Peter Muster | 1       |
+----------+----------+---------------------+---------------------+---------------------+--------------+---------+

Notice the primary key of the current entry did not change. The original ``id`` will always point the current version of
an object.

Revisions of an object (i.e. historic versions) are copies of the current entry at the time pointed by the version's
``version_end_date``.

For making things clearer, we create another version::

    item = item.clone()
    item.name = "Petra Mauser"
    item.version = "3"
    item.save()

Once again, the situation on DB level will present itself as follows:

+----------+----------+---------------------+---------------------+---------------------+--------------+---------+
| id (pk)  | identity | version_birth_date  | version_start_date  | version_end_date    | name         | version |
+==========+==========+=====================+=====================+=====================+==============+=========+
| 123      | 123      | 2014-08-14 14:43:00 | 2014-08-14 15:21:00 | None                | Petra Mauser | 3       |
+----------+----------+---------------------+---------------------+---------------------+--------------+---------+
| 124      | 123      | 2014-08-14 14:43:00 | 2014-08-14 14:43:00 | 2014-08-14 15:09:00 | Peter Muster | 1       |
+----------+----------+---------------------+---------------------+---------------------+--------------+---------+
| 125      | 123      | 2014-08-14 14:43:00 | 2014-08-14 15:09:00 | 2014-08-14 15:21:00 | Peter Mauser | 2       |
+----------+----------+---------------------+---------------------+---------------------+--------------+---------+

On a timeline, the state can be represented as follows:

.. _cleanerversion_example_single_entry_image:

.. image:: ../images/cleanerversion_example_single_entry.png
    :alt: The visual representation of the single entry CleanerVersion example
    :align: center

Many-to-One relationships
=========================

Declaring versioned M2O relationship
------------------------------------

Here's an example with a sportsclub that can practice at most one sporty discipline::

    class SportsClub(Versionable):
        """Sort of an association for practicing sports"""
        name = CharField(max_length=200)
        practice_periodicity = CharField(max_length=200)
        discipline = VersionedForeignKey('Discipline')

    class Discipline(Versionable):
        """A sports discipline"""
        name = CharField(max_length=200)
        rules = CharField(max_length=200)

If a M2O relationship can also be unset, don't forget to set the nullable flag (null=true) as an argument of the
``VersionedForeignKey`` field.

Adding objects to a versioned M2O relationship
----------------------------------------------

Let's create two disciplines and some sportsclubs practicing these disciplines::

    running = Discipline.objects.create(name='Running', rules='There are none (almost)')
    icehockey = Discipline.objects.create(name='Ice Hockey', rules='There\'s a ton of them')

    stb = SportsClub.objects.create(name='STB', practice_periodicity='tuesday and thursday night',
                                                discipline=running)
    hcfg = SportsClub.objects.create(name='HCFG',
                                                 practice_periodicity='monday, wednesday and friday night',
                                                 discipline=icehockey)
    lca = SportsClub.objects.create(name='LCA', practice_periodicity='individual',
                                                discipline=running)

Reading objects from a M2O relationship
---------------------------------------

Assume, timestamps have been created as follows::
    timestamp = datetime.datetime.utcnow().replace(tzinfo=utc)

Now, let's read some stuff previously loaded::

    sportsclubs = SportsClub.objects.as_of(t1)  # This returns all SportsClubs existing at time t1 [returned within a QuerySet]

Many-to-Many relationships
==========================

Declaring versioned M2M relationships
-------------------------------------

Assume a Person can be part of multiple SportsClubs::

    class Person(Versionable):
        name = CharField(max_length=200)
        phone = CharField(max_length=200)
        sportsclubs = VersionedManyToManyField('SportsClub', related_name='members')

    class SportsClub(Versionable):
        """Sort of an association for practicing sports"""
        name = CharField(max_length=200)
        practice_periodicity = CharField(max_length=200)


Adding objects to a versioned M2M relationship
----------------------------------------------

# TODO or remove

Reading objects from a versioned M2M relationship
-------------------------------------------------

# TODO or remove

Versioning objects being part of a versioned M2M relationship
-------------------------------------------------------------

Versioning an object in a ManyToMany relationship requires 3 steps to be done, including the initial setup:

#) Setting up the situation requires to add at least two objects to a M2M relationship

    .. image:: ../images/clone_m2m_item_1.png
        :align: center

#) Further on, let's clone the Item-instance

    .. image:: ../images/clone_m2m_item_2.png
        :align: center

#) CleanerVersion takes care of cloning and re-linking also the relationships

    .. image:: ../images/clone_m2m_item_3.png
        :align: center


Removing objects from a versioned M2M relationship
--------------------------------------------------

# TODO or remove

Known Issues
============

Currently there are no known issues; for a more updated state please check our `project page
<https://github.com/swisscom/cleanerversion>`_.
