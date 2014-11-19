from django.db.models import CharField

from versions.models import Versionable, VersionedManyToManyField, VersionedForeignKey


def versionable_description(obj):
    return "<" + str(obj.__class__.__name__) + " object: " + str(
        obj.name) + " {valid: [" + obj.version_start_date.isoformat() + " | " + (
               obj.version_end_date.isoformat() if obj.version_end_date else "None") + "], created: " + obj.version_birth_date.isoformat() + "}>"


############################################
# The following model is used for:
# - CreationTest
# - DeletionTest
# - CurrentVersionTest
# - VersionedQuerySetTest
# - VersionNavigationTest
# - HistoricObjectsHandling
class B(Versionable):
    name = CharField(max_length=200)

    __str__ = versionable_description


############################################
# OneToManyTest models
class Team(Versionable):
    name = CharField(max_length=200)

    __str__ = versionable_description


class Player(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=True)

    __str__ = versionable_description


############################################
# SelfOneToManyTest models
class Directory(Versionable):
    name = CharField(max_length=100)
    parent = VersionedForeignKey('self', null=True)


# ############################################
# MultiM2MTest models
class Professor(Versionable):
    name = CharField(max_length=200)
    address = CharField(max_length=200)
    phone_number = CharField(max_length=200)

    __str__ = versionable_description


class Classroom(Versionable):
    name = CharField(max_length=200)
    building = CharField(max_length=200)

    __str__ = versionable_description


class Student(Versionable):
    name = CharField(max_length=200)
    professors = VersionedManyToManyField("Professor", related_name='students')
    classrooms = VersionedManyToManyField("Classroom", related_name='students')

    __str__ = versionable_description


############################################
# MultiM2MToSameTest models
class Pupil(Versionable):
    name = CharField(max_length=200)
    phone_number = CharField(max_length=200)
    language_teachers = VersionedManyToManyField('Teacher', related_name='language_students')
    science_teachers = VersionedManyToManyField('Teacher', related_name='science_students')

    __str__ = versionable_description


class Teacher(Versionable):
    name = CharField(max_length=200)
    domain = CharField(max_length=200)

    __str__ = versionable_description


############################################
# ManyToManyFilteringTest models
class C1(Versionable):
    name = CharField(max_length=50)
    c2s = VersionedManyToManyField("C2", related_name='c1s')

    __str__ = versionable_description


class C2(Versionable):
    name = CharField(max_length=50)
    c3s = VersionedManyToManyField("C3", related_name='c2s')

    __str__ = versionable_description


class C3(Versionable):
    name = CharField(max_length=50)

    __str__ = versionable_description


############################################
# HistoricM2MOperationsTests models
class Observer(Versionable):
    name = CharField(max_length=200)

    __str__ = versionable_description


class Subject(Versionable):
    name = CharField(max_length=200)
    observers = VersionedManyToManyField('Observer', related_name='subjects')

    __str__ = versionable_description
