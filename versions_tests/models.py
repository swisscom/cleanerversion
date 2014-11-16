from django.db.models import CharField

from versions.models import Versionable, VersionedManyToManyField, VersionedForeignKey


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


############################################
# OneToManyTest models
class Team(Versionable):
    name = CharField(max_length=200)


class Player(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=True)

    def __str__(self):
        return "<" + str(self.__class__.__name__) + " object: " + str(
            self.name) + " {valid: [" + self.version_start_date.isoformat() + " | " + (
                   self.version_end_date.isoformat() if self.version_end_date else "None") + "], created: " + self.version_birth_date.isoformat() + "}>"


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

    def __str__(self):
        return self.name


class Classroom(Versionable):
    name = CharField(max_length=200)
    building = CharField(max_length=200)

    def __str__(self):
        return self.name


class Student(Versionable):
    name = CharField(max_length=200)
    professors = VersionedManyToManyField("Professor", related_name='students')
    classrooms = VersionedManyToManyField("Classroom", related_name='students')

    def __str__(self):
        return self.name


############################################
# MultiM2MToSameTest models
class Pupil(Versionable):
    name = CharField(max_length=200)
    phone_number = CharField(max_length=200)
    language_teachers = VersionedManyToManyField('Teacher', related_name='language_students')
    science_teachers = VersionedManyToManyField('Teacher', related_name='science_students')


class Teacher(Versionable):
    name = CharField(max_length=200)
    domain = CharField(max_length=200)


############################################
# ManyToManyFilteringTest models
class C1(Versionable):
    name = CharField(max_length=50)
    c2s = VersionedManyToManyField("C2", related_name='c1s')


class C2(Versionable):
    name = CharField(max_length=50)
    c3s = VersionedManyToManyField("C3", related_name='c2s')


class C3(Versionable):
    name = CharField(max_length=50)


############################################
# HistoricM2MOperationsTests models
class Observer(Versionable):
    name = CharField(max_length=200)


class Subject(Versionable):
    name = CharField(max_length=200)
    observers = VersionedManyToManyField('Observer', related_name='subjects')
