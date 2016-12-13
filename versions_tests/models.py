from django.db.models import CharField, IntegerField, Model, ForeignKey
from django.db.models.deletion import DO_NOTHING, PROTECT, SET, SET_NULL
from django.utils.encoding import python_2_unicode_compatible

from versions.models import Versionable, VersionedManyToManyField, VersionedForeignKey


def versionable_description(obj):
    return "<" + str(obj.__class__.__name__) + " object: " + \
           obj.name + " {valid: [" + obj.version_start_date.isoformat() + " | " + (
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
# Models for
# - DeletionHandlerTest
# - OneToManyTest
# - PrefetchingTest
# - VersionNavigationAsOfTest
# - VersionRestoreTest
# - DetachTest
# - DeferredFieldsTest
@python_2_unicode_compatible
class City(Versionable):
    name = CharField(max_length=200)

    __str__ = versionable_description


@python_2_unicode_compatible
class Team(Versionable):
    name = CharField(max_length=200)
    city = VersionedForeignKey(City, null=True)

    __str__ = versionable_description


@python_2_unicode_compatible
class Player(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=True)

    __str__ = versionable_description


class Award(Versionable):
    name = CharField(max_length=200)
    players = VersionedManyToManyField(Player, related_name='awards')


@python_2_unicode_compatible
class Mascot(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=False)

    __str__ = versionable_description


def default_team():
    return Team.objects.current.get(name__startswith='default_team.')


@python_2_unicode_compatible
class Fan(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=False, on_delete=SET(default_team))

    __str__ = versionable_description


@python_2_unicode_compatible
class RabidFan(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=True, on_delete=SET_NULL)

    __str__ = versionable_description


@python_2_unicode_compatible
class WizardFan(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=True, on_delete=PROTECT)

    __str__ = versionable_description


@python_2_unicode_compatible
class NonFan(Versionable):
    name = CharField(max_length=200)
    team = VersionedForeignKey(Team, null=False, on_delete=DO_NOTHING)

    __str__ = versionable_description


############################################
# SelfOneToManyTest models
class Directory(Versionable):
    name = CharField(max_length=100)
    parent = VersionedForeignKey('self', null=True)


# ############################################
# MultiM2MTest models
@python_2_unicode_compatible
class Professor(Versionable):
    name = CharField(max_length=200)
    address = CharField(max_length=200)
    phone_number = CharField(max_length=200)

    __str__ = versionable_description


@python_2_unicode_compatible
class Classroom(Versionable):
    name = CharField(max_length=200)
    building = CharField(max_length=200)

    __str__ = versionable_description


@python_2_unicode_compatible
class Student(Versionable):
    name = CharField(max_length=200)
    professors = VersionedManyToManyField("Professor", related_name='students')
    classrooms = VersionedManyToManyField("Classroom", related_name='students')

    __str__ = versionable_description


############################################
# MultiM2MToSameTest models
@python_2_unicode_compatible
class Pupil(Versionable):
    name = CharField(max_length=200)
    phone_number = CharField(max_length=200)
    language_teachers = VersionedManyToManyField('Teacher', related_name='language_students')
    science_teachers = VersionedManyToManyField('Teacher', related_name='science_students')

    __str__ = versionable_description


@python_2_unicode_compatible
class Teacher(Versionable):
    name = CharField(max_length=200)
    domain = CharField(max_length=200)

    __str__ = versionable_description


############################################
# ManyToManyFilteringTest models
@python_2_unicode_compatible
class C1(Versionable):
    name = CharField(max_length=50)
    c2s = VersionedManyToManyField("C2", related_name='c1s')

    __str__ = versionable_description


@python_2_unicode_compatible
class C2(Versionable):
    name = CharField(max_length=50)
    c3s = VersionedManyToManyField("C3", related_name='c2s')

    __str__ = versionable_description


@python_2_unicode_compatible
class C3(Versionable):
    name = CharField(max_length=50)

    __str__ = versionable_description


############################################
# HistoricM2MOperationsTests models
@python_2_unicode_compatible
class Observer(Versionable):
    name = CharField(max_length=200)

    __str__ = versionable_description


@python_2_unicode_compatible
class Subject(Versionable):
    name = CharField(max_length=200)
    observers = VersionedManyToManyField('Observer', related_name='subjects')

    __str__ = versionable_description


############################################
# VersionUniqueTests models
class ChainStore(Versionable):
    subchain_id = IntegerField()
    city = CharField(max_length=40)
    name = CharField(max_length=40)
    opening_hours = CharField(max_length=40)
    door_frame_color = VersionedForeignKey('Color')
    door_color = VersionedForeignKey('Color', related_name='cs')

    # There are lots of these chain stores.  They follow these rules:
    # - only one store with the same name and subchain_id can exist in a single city
    # - no two stores can share the same door_frame_color and door_color
    # Yea, well, they want to appeal to people who want to be different.
    VERSION_UNIQUE = [['subchain_id', 'city', 'name'], ['door_frame_color', 'door_color']]


class Color(Versionable):
    name = CharField(max_length=40)


############################################
# IntegrationNonVersionableModelsTests models
@python_2_unicode_compatible
class Wine(Model):
    name = CharField(max_length=200)
    vintage = IntegerField()

    def __str__(self):
        return "<" + str(self.__class__.__name__) + " object: " + str(
            self.name) + " (" + str(self.vintage) + ")>"


@python_2_unicode_compatible
class WineDrinker(Versionable):
    name = CharField(max_length=200)
    glass_content = ForeignKey(Wine, related_name='drinkers', null=True)

    __str__ = versionable_description


@python_2_unicode_compatible
class WineDrinkerHat(Model):
    shape_choices = [('Sailor', 'Sailor'),
                     ('Cloche', 'Cloche'),
                     ('Cartwheel', 'Cartwheel'),
                     ('Turban', 'Turban'),
                     ('Breton', 'Breton'),
                     ('Vagabond', 'Vagabond')]
    color = CharField(max_length=40)
    shape = CharField(max_length=200, choices=shape_choices, default='Sailor')
    wearer = VersionedForeignKey(WineDrinker, related_name='hats', null=True)

    def __str__(self):
        return "<" + str(self.__class__.__name__) + " object: " + str(
            self.shape) + " (" + str(self.color) + ")>"


############################################
# SelfReferencingManyToManyTest models
class Person(Versionable):
    name = CharField(max_length=200)
    children = VersionedManyToManyField('self', symmetrical=False, null=True, related_name='parents')
