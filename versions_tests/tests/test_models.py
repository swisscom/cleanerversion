# Copyright 2014 Swisscom, Sophia Engineering
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
from time import sleep
import itertools
from unittest import skip, skipUnless
import re
import uuid

from django import get_version
from django.core.exceptions import SuspiciousOperation, ObjectDoesNotExist, ValidationError
from django.db import connection, IntegrityError, transaction
from django.db.models import Q, Count, Prefetch, Sum
from django.db.models.deletion import ProtectedError
from django.test import TestCase
from django.utils.timezone import utc
from django.utils import six
from django import VERSION

from versions.exceptions import DeletionOfNonCurrentVersionError
from versions.models import get_utc_now, ForeignKeyRequiresValueError, Versionable
from versions_tests.models import (
    Award, B, C1, C2, C3, City, Classroom, Directory, Fan, Mascot, NonFan, Observer, Person, Player, Professor, Pupil,
    RabidFan, Student, Subject, Teacher, Team, Wine, WineDrinker, WineDrinkerHat, WizardFan
)


def get_relation_table(model_class, fieldname):

    if VERSION[:2] >= (1, 8):
        field_object = model_class._meta.get_field(fieldname)
        direct = not field_object.auto_created or field_object.concrete
    else:
        field_object, _, direct, _ = model_class._meta.get_field_by_name(fieldname)

    if direct:
        field = field_object
    else:
        field = field_object.field
    return field.m2m_db_table()


def set_up_one_object_with_3_versions():
    b = B.objects.create(name='v1')

    sleep(0.1)
    t1 = get_utc_now()

    b = b.clone()
    b.name = 'v2'
    b.save()

    sleep(0.1)
    t2 = get_utc_now()

    b = b.clone()
    b.name = 'v3'
    b.save()

    sleep(0.1)
    t3 = get_utc_now()

    return b, t1, t2, t3

def create_three_current_objects():
    b1 = B.objects.create(name = '1')
    b2 = B.objects.create(name = '2')
    b3 = B.objects.create(name = '3')
    return b1, b2, b3


def remove_white_spaces(self, s):
    return re.sub(r'\s+', '', s)


def assertStringEqualIgnoreWhiteSpaces(self, expected, obtained):
    expected = self.remove_white_spaces(expected).lower()
    obtained = self.remove_white_spaces(obtained).lower()
    self.assertEqual(expected, obtained)


TestCase.remove_white_spaces = remove_white_spaces
TestCase.assertStringEqualIgnoreWhiteSpaces = assertStringEqualIgnoreWhiteSpaces


class CreationTest(TestCase):
    def test_create_using_manager(self):
        b = B.objects.create(name='someB')
        self.assertTrue(isinstance(b, Versionable))
        self.assertEqual(b.version_start_date, b.version_birth_date)

        b_new = b.clone()
        self.assertTrue(isinstance(b_new, Versionable))
        self.assertEqual(b_new.version_start_date, b.version_end_date)

    def test_create_using_constructor(self):
        b = B(name='someB')
        b.save()
        self.assertTrue(isinstance(b, Versionable))
        self.assertEqual(b.version_start_date, b.version_birth_date)

        b_new = b.clone()
        self.assertTrue(isinstance(b_new, Versionable))
        self.assertEqual(b_new.version_start_date, b.version_end_date)

    def test_full_clean(self):
        """
        A full clean will fail if some field allows null but not blank, and
        no value is specified (version_end_date, for example).
        """
        b = B(name='someB')
        try:
            b.full_clean()
        except ValidationError:
            self.fail("Full clean did not succeed")


class DeletionTest(TestCase):
    def setUp(self):
        self.b, self.t1, self.t2, self.t3 = set_up_one_object_with_3_versions()

    def test_deleting(self):
        """
        When deleting an object in the database the object count should stay
        constant as we are doing a soft-delete.
        """
        self.assertEqual(3, B.objects.all().count())

        b = B.objects.current.first()
        b.delete()

        self.assertEqual(3, B.objects.all().count())

    def test_deleting_non_current_version(self):
        """
        Deleting a previous version of an object is not possible and an
        exception must be raised if such an action is attempted.
        """
        self.assertEqual(3, B.objects.all().count())

        current = B.objects.current.first()
        previous = B.objects.previous_version(current)

        self.assertRaises(DeletionOfNonCurrentVersionError, previous.delete)

    def test_delete_using_current_queryset(self):
        B.objects.current.all().delete()
        bs = list(B.objects.all())
        self.assertEqual(3, len(bs))
        for b in bs:
            self.assertIsNotNone(b.version_end_date)

    def test_delete_using_non_current_queryset(self):

        B.objects.create(name='Buzz')

        qs = B.objects.all().filter(version_end_date__isnull=True)
        self.assertEqual(2, len(qs))
        pks = [o.pk for o in qs]

        qs.delete()
        bs = list(B.objects.all().filter(pk__in=pks))
        self.assertEqual(2, len(bs))
        for b in bs:
            self.assertIsNotNone(b.version_end_date)

    def test_deleteing_non_current_version_with_queryset(self):
        qs = B.objects.all().filter(version_end_date__isnull=False)
        self.assertEqual(2, qs.count())
        pks = [o.pk for o in qs]

        B.objects.all().filter(pk__in=pks).delete()

        # None of the objects should have been deleted, because they are not current.
        self.assertEqual(2, B.objects.all().filter(pk__in=pks).count())

    def test_delete_related_with_non_versionable(self):
        jackie = WineDrinker.objects.create(name='Jackie')
        red_sailor_hat = WineDrinkerHat.objects.create(shape='Sailor', color='red', wearer=jackie)
        jackie.delete()
        self.assertEqual(WineDrinkerHat.objects.count(), 0)
        self.assertEqual(WineDrinker.objects.current.count(), 0)


class DeletionHandlerTest(TestCase):
    """Tests that the ForeignKey on_delete parameters have the expected effects"""

    def setUp(self):
        self.city = City.objects.create(name='c.v1')
        self.team = Team.objects.create(name='t.v1', city=self.city)
        self.default_team = Team.objects.create(name='default_team.v1')
        self.p1 = Player.objects.create(name='p1.v1', team=self.team)
        self.p2 = Player.objects.create(name='p2.v1', team=self.team)
        self.m1 = Mascot.objects.create(name='m1.v1', team=self.team)
        self.m2 = Mascot.objects.create(name='m2.v1', team=self.team)
        self.f1 = Fan.objects.create(name='f1.v1', team=self.team)
        self.f2 = Fan.objects.create(name='f2.v1', team=self.team)
        self.f3 = Fan.objects.create(name='f3.v1', team=self.team)
        self.rf1 = RabidFan.objects.create(name='rf1.v1', team=self.team)
        self.nf1 = NonFan.objects.create(name='nf1.v1', team=self.team)
        self.a1 = Award.objects.create(name='a1.v1')
        self.a1.players.add(self.p1, self.p2)

    def test_on_delete(self):
        t1 = get_utc_now()
        player_filter = {'pk__in': [self.p1.pk, self.p2.pk]}
        team_filter = {'pk__in': [self.team.pk]}
        mascot_filter = {'pk__in': [self.m1.pk, self.m2.pk]}
        fan_filter = {'pk__in': [self.f1.pk, self.f2.pk, self.f3.pk]}
        rabid_fan_filter = {'pk__in': [self.rf1.pk]}
        non_fan_filter = {'pk__in': [self.nf1.pk]}
        award_qs = Award.objects.current.filter(pk=self.a1.pk)[0]

        self.assertEqual(1, Team.objects.current.filter(**team_filter).count())
        self.assertEqual(2, Player.objects.current.filter(**player_filter).count())
        self.assertEqual(2, award_qs.players.count())
        self.assertEqual(2, Mascot.objects.current.filter(**mascot_filter).count())
        self.assertEqual(3, Fan.objects.current.filter(**fan_filter).count())
        self.assertEqual(1, RabidFan.objects.current.filter(**rabid_fan_filter).count())
        self.assertEqual(1, NonFan.objects.current.filter(**non_fan_filter).count())

        self.city.delete()

        # Cascading deletes are the default behaviour.
        self.assertEqual(0, Team.objects.current.filter(**team_filter).count())
        self.assertEqual(0, Player.objects.current.filter(**player_filter).count())
        self.assertEqual(0, Mascot.objects.current.filter(**mascot_filter).count())

        # Many-to-Many relationships are terminated.
        self.assertEqual(0, award_qs.players.count())
        # But a record of them still exists.
        self.assertEqual(2, Award.objects.as_of(t1).get(pk=self.a1.pk).players.count())

        # The fans picked another team (on_delete=SET(default_team))
        fans = Fan.objects.current.filter(**fan_filter).all()
        self.assertEqual(3, fans.count())
        fans_teams = {f.team for f in fans}
        self.assertEqual({self.default_team}, fans_teams)

        # The rabid fan doesn't go away if he loses his team, he's still rabid, he just
        # doesn't have a team anymore. (on_delete=SET_NULL)
        self.assertEqual(1, RabidFan.objects.current.filter(**rabid_fan_filter).count())
        rabid_fan = RabidFan.objects.current.filter(**rabid_fan_filter)[0]
        self.assertEqual(None, rabid_fan.team)
        self.assertEqual(self.team.identity, RabidFan.objects.previous_version(rabid_fan).team_id)

        # The non-fan isn't affected (on_delete=DO_NOTHING)
        self.assertEqual(1, NonFan.objects.current.filter(**non_fan_filter).count())
        # This leaves a reference to the deleted team ... hey, that's what DO_NOTHING means.
        self.assertEqual(self.team.pk, NonFan.objects.current.filter(**non_fan_filter)[0].team_id)

    def test_protected_delete(self):
        WizardFan.objects.create(name="Gandalf", team=self.team)
        # The wizard does his best to protect his team and it's city. (on_delete=PROTECTED)
        with self.assertRaises(ProtectedError):
            self.city.delete()
        self.assertEqual(1, Team.objects.current.filter(pk=self.team.pk).count())
        self.assertEqual(1, City.objects.current.filter(pk=self.city.pk).count())

    def test_deleting_when_m2m_history(self):
        through = Award._meta.get_field('players').rel.through
        a1 = Award.objects.create(name="bravo")
        p1 = Player.objects.create(name="Jessie")
        a1.players = [p1]
        self.assertEqual(1, through.objects.filter(player_id=p1.pk).count())
        self.assertEqual(1, through.objects.current.filter(player_id=p1.pk).count())
        a1.players = []
        self.assertEqual(1, through.objects.filter(player_id=p1.pk).count())
        self.assertEqual(0, through.objects.current.filter(player_id=p1.pk).count())
        p1.delete()
        self.assertEqual(1, through.objects.filter(player_id=p1.pk).count())
        self.assertEqual(0, through.objects.current.filter(player_id=p1.pk).count())

class CurrentVersionTest(TestCase):
    def setUp(self):
        self.b, self.t1, self.t2, self.t3 = set_up_one_object_with_3_versions()

    def test_simple_case(self):
        should_be_v3 = B.objects.current.first()

        self.assertEqual('v3', should_be_v3.name)

    def test_after_adding_new_version(self):
        """
        Create a new version of an object and tests that it has become the
        'current' version
        """
        b = self.b.clone()
        b.name = 'v4'
        b.save()

        sleep(0.1)

        should_be_v4 = B.objects.current.first()
        self.assertEqual('v4', should_be_v4.name)

    def test_after_deleting_current_version(self):
        """
        Test that after deleting an object there is no 'current' version of
        this object available.
        """
        self.b.delete()

        self.assertIsNone(B.objects.current.first())

    def test_getting_current_version(self):
        """
        Test that we can get the current version of any object by calling
        the current_version() function
        """
        v2 = B.objects.as_of(self.t2).first()

        should_be_v3 = B.objects.current_version(v2)
        self.assertEqual('v3', should_be_v3.name)

    def test_getting_current_version_but_deleted(self):
        """
        Test that the current_version returns nothing when called with a
        deleted object
        :return:
        """
        current = B.objects.current.first()
        previous = B.objects.previous_version(current)
        current.delete()

        self.assertIsNone(B.objects.current_version(previous))
        self.assertIsNone(B.objects.current_version(current))


class VersionedQuerySetTest(TestCase):
    def test_queryset_without_using_as_of(self):
        b = B.objects.create(name='blabla')

        b.name = 'blibli'
        b.save()

        o = B.objects.first()

        self.assertEqual(b.name, o.name)

    def test_queryset_using_as_of(self):
        """
        Creates one object having 3 versions and then tests that the as_of method
        is returning the correct version when given the corresponding timestamp
        """
        b, t1, t2, t3 = set_up_one_object_with_3_versions()

        o = B.objects.as_of(t1).first()
        self.assertEqual('v1', o.name)

        o = B.objects.as_of(t2).first()
        self.assertEqual('v2', o.name)


    def test_queryset_using_delete(self):
        """
        Creates 3 objects with all current and then tests that the delete method
        makes the current versions a historical version (adding a version_end_date)
        """
        b1, b2, b3 = create_three_current_objects()
        self.assertEqual(True, b1.is_current)
        self.assertEqual(True, b2.is_current)
        self.assertEqual(True, b3.is_current)

        qs = B.objects.filter(name__in = ['1','2','3']).all()
        qs.delete()

        b1 = B.objects.get(name = '1')
        b2 = B.objects.get(name = '2')
        b3 = B.objects.get(name = '3')
        self.assertEqual(False, b1.is_current)
        self.assertEqual(False, b2.is_current)
        self.assertEqual(False, b3.is_current)


class VersionNavigationTest(TestCase):
    def setUp(self):
        self.b, self.t1, self.t2, self.t3 = set_up_one_object_with_3_versions()

    def test_getting_next_version(self):
        """
        Get the first version of an object and navigate to the next version
        until we reach the last version.
        """
        self.assertEqual(B.objects.all().count(), 3)

        v1 = B.objects.as_of(self.t1).first()
        self.assertEqual('v1', v1.name)

        should_be_v2 = B.objects.next_version(v1)
        self.assertEqual('v2', should_be_v2.name)
        v2 = should_be_v2

        should_be_v3 = B.objects.next_version(v2)
        self.assertEqual('v3', should_be_v3.name)
        v3 = should_be_v3

        should_still_be_v3 = B.objects.next_version(v3)
        self.assertEqual('v3', should_still_be_v3.name)

    def test_getting_previous_version(self):
        """
        Get the last version of an object and navigate to the previous version
        until we reach the first one.
        """
        v3 = B.objects.as_of(self.t3).first()
        self.assertEqual('v3', v3.name)

        should_be_v2 = B.objects.previous_version(v3)
        self.assertEqual('v2', should_be_v2.name)
        v2 = should_be_v2

        should_be_v1 = B.objects.previous_version(v2)
        self.assertEqual('v1', should_be_v1.name)
        v1 = should_be_v1

        should_still_be_v1 = B.objects.previous_version(v1)
        self.assertEqual('v1', should_still_be_v1.name)

    def test_getting_nonexistent_next_version(self):
        """
        Raise an error when trying to look up the next version of the last version of a deleted object.
        """
        v3 = B.objects.as_of(self.t3).first()
        v3.delete()

        self.assertRaises(ObjectDoesNotExist, lambda: B.objects.next_version(v3))


class VersionNavigationAsOfTest(TestCase):
    def setUp(self):
        city1 = City.objects.create(name='city1')
        city2 = City.objects.create(name='city2')
        team1 = Team.objects.create(name='team1', city=city1)
        team2 = Team.objects.create(name='team2', city=city1)
        team3 = Team.objects.create(name='team3', city=city2)
        # At t1: city1 - (team1, team2) / city2 - (team3)
        self.t1 = get_utc_now()

        sleep(0.01)
        team2 = team2.clone()
        team2.city = city2
        team2.save()
        # At t2: city1 - (team1) / city2 - (team2, team3)
        self.t2 = get_utc_now()

        sleep(0.01)
        city1 = city1.clone()
        city1.name = 'city1.a'
        city1.save()
        # At t3: city1.a - (team1) / city2 - (team1, team2, team3)
        self.t3 = get_utc_now()

        sleep(0.01)
        team1 = team1.clone()
        team1.name = 'team1.a'
        team1.city = city2
        team1.save()
        # At t4: city1.a - () / city2 - (team1.a, team2, team3)
        self.t4 = get_utc_now()

        sleep(0.01)
        team1 = team1.clone()
        team1.city = city1
        team1.name = 'team1.b'
        team1.save()
        # At t5: city1.a - (team1.b) / city2 - (team2, team3)
        self.t5 = get_utc_now()

    def test_as_of_parameter(self):
        city1_t2 = City.objects.as_of(self.t2).get(name__startswith='city1')
        self.assertEqual(1, city1_t2.team_set.all().count())
        self.assertFalse(city1_t2.is_current)

        # as_of 'end' for current version means "current", not a certain point in time
        city1_current = City.objects.next_version(city1_t2, relations_as_of='end')
        self.assertTrue(city1_current.is_current)
        self.assertIsNone(city1_current._querytime.time)
        teams = city1_current.team_set.all()
        self.assertEqual(1, teams.count())
        self.assertEqual('team1.b', teams[0].name)

        # as_of 'end' for non-current version means at a certain point in time
        city1_previous = City.objects.previous_version(city1_current, relations_as_of='end')
        self.assertIsNotNone(city1_previous._querytime.time)

        # as_of 'start': returns version at the very start of it's life.
        city1_latest_at_birth = City.objects.next_version(city1_t2, relations_as_of='start')
        self.assertTrue(city1_latest_at_birth.is_current)
        self.assertEqual(1, city1_latest_at_birth.team_set.count())
        self.assertIsNotNone(city1_latest_at_birth._querytime.time)
        self.assertEqual(city1_latest_at_birth._querytime.time, city1_latest_at_birth.version_start_date)

        # as_of datetime: returns a version at a given point in time.
        city1_t4 = City.objects.next_version(city1_t2, relations_as_of=self.t4)
        self.assertTrue(city1_latest_at_birth.is_current)
        self.assertIsNotNone(city1_latest_at_birth._querytime.time)
        teams = city1_latest_at_birth.team_set.all()
        self.assertEqual(1, teams.count())
        self.assertEqual('team1', teams[0].name)

        # as_of None: returns object without time restriction for related objects.
        # This means, that all other related object versions that have been associated with
        # this object are returned when queried, without applying any time restriction.
        city1_v2 = City.objects.current_version(city1_t2, relations_as_of=None)
        self.assertFalse(city1_v2._querytime.active)
        teams = city1_v2.team_set.all()
        team_names = {team.name for team in teams}
        self.assertEqual(3, teams.count())
        self.assertSetEqual({'team1', 'team2', 'team1.b'}, team_names)

    def test_invalid_as_of_parameter(self):
        city = City.objects.current.get(name__startswith='city1')

        with self.assertRaises(TypeError):
            City.objects.previous_version(city, relations_as_of='endlich')

        # Using an as_of time before the object's validity period:
        with self.assertRaises(ValueError):
            City.objects.current_version(city, relations_as_of=self.t1)

        # Using an as_of time after the object's validity period:
        with self.assertRaises(ValueError):
            City.objects.previous_version(city, relations_as_of=self.t5)


class HistoricObjectsHandling(TestCase):
    t0 = datetime.datetime(1980, 1, 1, tzinfo=utc)
    t1 = datetime.datetime(1984, 4, 23, tzinfo=utc)
    t2 = datetime.datetime(1985, 4, 23, tzinfo=utc)
    in_between_t1_and_t2 = datetime.datetime(1984, 5, 30, tzinfo=utc)
    after_t2 = datetime.datetime(1990, 1, 18, tzinfo=utc)

    def test_create_using_manager(self):
        b = B.objects._create_at(self.t1, name='someB')
        self.assertEqual(self.t1, b.version_birth_date)
        self.assertEqual(self.t1, b.version_start_date)

        b_v2 = b._clone_at(self.t2)
        self.assertEqual(b_v2.version_start_date, b.version_end_date)

        # Query these objects
        b_v1 = B.objects.as_of(self.in_between_t1_and_t2).get(name='someB')
        self.assertFalse(b_v1.is_current)
        self.assertEqual(b_v1.version_birth_date, b_v1.version_start_date)

        b_v2 = B.objects.as_of(self.after_t2).get(name='someB')
        self.assertTrue(b_v2.is_current)
        self.assertNotEqual(b_v2.version_birth_date, b_v2.version_start_date)

    def test_create_using_constructor(self):
        b = B(name='someB').at(self.t1)
        b.save()
        self.assertEqual(self.t1, b.version_birth_date)
        self.assertEqual(self.t1, b.version_start_date)

        b_v2 = b._clone_at(self.t2)
        self.assertEqual(b_v2.version_start_date, b.version_end_date)

        # Query these objects
        b_v1 = B.objects.as_of(self.in_between_t1_and_t2).get(name='someB')
        self.assertFalse(b_v1.is_current)
        self.assertEqual(b_v1.version_birth_date, b_v1.version_start_date)

        b_v2 = B.objects.as_of(self.after_t2).get(name='someB')
        self.assertTrue(b_v2.is_current)
        self.assertNotEqual(b_v2.version_birth_date, b_v2.version_start_date)

    def test_wrong_temporal_moving_of_objects(self):
        """
        Test that the restriction about creating "past objects' are operational:
           - we cannot give something else than a timestamp to at()
           - we cannot move anywhere in time an object
        """
        b = B(name='someB')
        self.assertRaises(ValueError, lambda: b.at('bla'))
        b.at(self.t1)
        b.save()

        b_new = b._clone_at(self.t2)
        self.assertRaises(SuspiciousOperation, lambda: b.at(self.t2))
        self.assertRaises(SuspiciousOperation, lambda: b_new.at(self.t1))

    def test_cloning_before_birth_date(self):
        b = B.objects._create_at(self.t1, name='someB')
        self.assertRaises(ValueError, b._clone_at, *[self.t0])


class OneToManyTest(TestCase):
    def setUp(self):
        self.team = Team.objects.create(name='t.v1')
        self.p1 = Player.objects.create(name='p1.v1', team=self.team)
        self.p2 = Player.objects.create(name='p2.v1', team=self.team)

    def test_simple(self):
        """
        Test that we have 2 players in the team.
        """
        self.assertEqual(2, self.team.player_set.count())

    def test_creating_new_version_of_the_team(self):
        t1 = get_utc_now()
        sleep(0.1)

        team = self.team.clone()
        team.name = 't.v2'
        team.save()

        t2 = get_utc_now()

        self.assertEqual(2, Team.objects.all().count())

        team = Team.objects.current.first()
        # Either we can test the version_end_date...
        self.assertIsNone(team.version_end_date)
        # ...or the is_current property
        self.assertTrue(team.is_current)

        # We didn't change anything to the players so there must be 2 players in
        # the team at time t1...
        team_at_t1 = Team.objects.as_of(t1).first()
        # TODO: Remove the following (useless) line, once Django1.8 is working
        t1_player_queryset = team_at_t1.player_set.all()
        # TODO: [django18 compat] The SQL query in t1_player_queryset.query shows that the Team pk value (team_at_t1.id)
        # is used to look up the players (instead of the identity property value (team_at_t1.identity))
        self.assertEqual(2, team_at_t1.player_set.count())

        # ... and at time t2
        team_at_t2 = Team.objects.as_of(t2).first()
        self.assertEqual(2, team_at_t2.player_set.count())

    def test_finding_object_with_historic_foreign_key(self):
        t1 = get_utc_now()
        sleep(0.01)
        team = self.team.clone()
        team.name = 't.v2'
        team.save()
        t2 = get_utc_now()
        sleep(0.01)
        team = team.clone()
        team.name = 't.v3'
        team.save()
        team_at_t1 = Team.objects.as_of(t1).get(identity=team.identity)
        team_at_t2 = Team.objects.as_of(t2).get(identity=team.identity)
        team_current = Team.objects.current.get(identity=team.identity)

        # self.p1's foreign key to self.team is it's original value, which is equal
        # to team_at_t1's identity, but not (any longer) team_at_t1's id.

        # The following queries should all work to return the self.p1 Player:

        # Using a cross-relation lookup on a non-identity field (team__name):
        player_p1_lookup = Player.objects.as_of(t1).get(team__name=team_at_t1.name, name='p1.v1')
        self.assertEqual(self.p1, player_p1_lookup)

        # Explicitly specifying the identity field in the lookup:
        player_p1_explicit = Player.objects.as_of(t1).get(team__identity=team_at_t1.identity, name='p1.v1')
        self.assertEqual(self.p1, player_p1_explicit)

        # The following three all work because the foreign key actually refers to the identity
        # field of the foreign object (which equals the identity of the current object).

        # Providing the current related object to filter on:
        player_p1_obj_current = Player.objects.as_of(t1).get(team=team_current, name='p1.v1')
        self.assertEqual(self.p1, player_p1_obj_current)
        self.assertEqual(team_at_t1, player_p1_obj_current.team)

        # Providing the related object that existed at the as_of time:
        player_p1_obj_as_of = Player.objects.as_of(t1).get(team=team_at_t1, name='p1.v1')
        self.assertEqual(self.p1, player_p1_obj_as_of)
        self.assertEqual(team_at_t1, player_p1_obj_as_of.team)

        # Providing the related object that is neither current, nor the one that existed
        # at the as_of time, but that has the same identity.
        player_p1_obj_other_version = Player.objects.as_of(t1).get(team=team_at_t2, name='p1.v1')
        self.assertEqual(self.p1, player_p1_obj_other_version)
        self.assertEqual(team_at_t1, player_p1_obj_other_version.team)

    def test_creating_new_version_of_the_player(self):
        t1 = get_utc_now()
        sleep(0.1)

        p1 = self.p1.clone()
        p1.name = 'p1.v2'
        p1.save()

        sleep(0.1)
        t2 = get_utc_now()

        self.assertEqual(3, Player.objects.all().count())

        # at t1 there is no player named 'p1.v2'
        team = Team.objects.as_of(t1).first()
        self.assertEqual(2, team.player_set.count())
        for player in team.player_set.all():
            self.assertNotEqual(u'p1.v2', six.u(str(player.name)))

        # at t2 there must be a 2 players and on of them is named 'p1.v2'
        team = Team.objects.as_of(t2).first()
        self.assertEqual(2, team.player_set.count())

        if six.PY2:
            matches = itertools.ifilter(lambda x: x.name == 'p1.v2', team.player_set.all())
        if six.PY3:
            matches = filter(lambda x: x.name == 'p1.v2', team.player_set.all())
        self.assertEqual(1, len(list(matches)))

    def test_adding_one_more_player_to_the_team(self):
        t1 = get_utc_now()
        sleep(0.1)

        self.assertEqual(2, self.team.player_set.all().count())

        new_player = Player.objects.create(name='p3.v1', team=self.team)
        t2 = get_utc_now()

        # there should be 3 players now in the team
        self.assertEqual(3, self.team.player_set.all().count())

        # there should be 2 players in the team at time t1
        team_at_t1 = Team.objects.as_of(t1).first()
        self.assertEqual(2, team_at_t1.player_set.all().count())

        # there should be 3 players in the team at time t2
        team_at_t2 = Team.objects.as_of(t2).first()
        self.assertEqual(3, team_at_t2.player_set.all().count())

    def test_removing_and_then_adding_again_same_player(self):
        t1 = get_utc_now()
        sleep(0.1)

        p1 = self.p1.clone()
        p1.team = None
        p1.name = 'p1.v2'
        p1.save()

        t2 = get_utc_now()
        sleep(0.1)

        p1 = p1.clone()
        p1.team = self.team
        p1.name = 'p1.v3'
        p1.save()

        t3 = get_utc_now()

        # there should be 2 players in the team if we put ourselves back at time t1
        team_at_t1 = Team.objects.as_of(t1).first()
        self.assertEqual(2, team_at_t1.player_set.all().count())

        # there should be 1 players in the team if we put ourselves back at time t2
        team_at_t2 = Team.objects.as_of(t2).first()
        self.assertEqual(1, team_at_t2.player_set.all().count())
        p1_at_t2 = Player.objects.as_of(t2).get(name__startswith='p1')
        self.assertIsNone(p1_at_t2.team)

        # there should be 2 players in the team if we put ourselves back at time t3
        team_at_t3 = Team.objects.as_of(t3).first()
        self.assertEqual(2, team_at_t3.player_set.all().count())

    def test_removing_and_then_adding_again_same_player_on_related_object(self):
        t1 = get_utc_now()
        sleep(0.1)

        self.team.player_set.remove(self.p1)

        # Remember: self.p1 was cloned while removing and is not current anymore!!
        # This property has to be documented, since it's critical for developers!
        # At this time, there is no mean to replace the contents of self.p1 within the
        # remove method
        p1 = Player.objects.current.get(name__startswith='p1')
        self.assertNotEqual(p1, self.p1)
        p1.name = 'p1.v2'
        p1.save()
        self.p1 = p1

        t2 = get_utc_now()
        sleep(0.1)

        self.team.player_set.add(self.p1)

        # Same thing here! Don't rely on an added value!
        p1 = Player.objects.current.get(name__startswith='p1')
        p1.name = 'p1.v3'
        p1.save()

        t3 = get_utc_now()

        # there should be 2 players in the team if we put ourselves back at time t1
        team_at_t1 = Team.objects.as_of(t1).first()
        self.assertEqual(2, team_at_t1.player_set.all().count())

        # there should be 1 players in the team if we put ourselves back at time t2
        team_at_t2 = Team.objects.as_of(t2).first()
        self.assertEqual(1, team_at_t2.player_set.all().count())

        # there should be 2 players in the team if we put ourselves back at time t3
        team_at_t3 = Team.objects.as_of(t3).first()
        self.assertEqual(2, team_at_t3.player_set.all().count())


class SelfOneToManyTest(TestCase):
    def setUp(self):
        """
        Setting up one parent folder having 2 sub-folders
        """
        parentdir_v1 = Directory.objects.create(name='parent.v1')

        subdir1_v1 = Directory.objects.create(name='subdir1.v1')
        subdir2_v1 = Directory.objects.create(name='subdir2.v1')

        parentdir_v1.directory_set.add(subdir1_v1)
        parentdir_v1.directory_set.add(subdir2_v1)

    def test_creating_new_version_of_parent_directory(self):
        t1 = get_utc_now()
        sleep(0.1)

        parentdir_v1 = Directory.objects.get(name__startswith='parent.v1')
        self.assertTrue(parentdir_v1.is_current)
        parentdir_v2 = parentdir_v1.clone()
        parentdir_v2.name = 'parent.v2'
        parentdir_v2.save()

        t2 = get_utc_now()

        # 1 parent dir, 2 subdirs, 2 new versions after linking then together
        # and 1 new version of the parent dir
        self.assertEqual(6, Directory.objects.all().count())

        self.assertTrue(parentdir_v2.is_current)

        # We didn't change anything to the subdirs so there must be 2 subdirs in
        # the parent at time t1...
        parentdir_at_t1 = Directory.objects.as_of(t1).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t1.directory_set.count())

        # ... and at time t2
        parentdir_at_t2 = Directory.objects.as_of(t2).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t2.directory_set.count())

    def test_creating_new_version_of_the_subdir(self):
        t1 = get_utc_now()

        subdir1_v1 = Directory.objects.current.get(name__startswith='subdir1')
        subdir1_v2 = subdir1_v1.clone()
        subdir1_v2.name = 'subdir1.v2'
        subdir1_v2.save()

        sleep(0.1)
        t2 = get_utc_now()

        # Count all Directory instance versions:
        # 3 initial versions + 2 subdirs added to parentdir (implies a clone) + 1 subdir1 that was explicitely cloned = 6
        self.assertEqual(6, Directory.objects.all().count())

        # at t1 there is no directory named 'subdir1.v2'
        parentdir_at_t1 = Directory.objects.as_of(t1).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t1.directory_set.count())

        for subdir in parentdir_at_t1.directory_set.all():
            self.assertNotEqual('subdir1.v2', subdir.name)

        # at t2 there must be 2 directories and ...
        parentdir_at_t2 = Directory.objects.as_of(t2).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t2.directory_set.count())

        # ... and one of then is named 'subdir1.v2'
        if six.PY2:
            matches = itertools.ifilter(lambda x: x.name == 'subdir1.v2', parentdir_at_t2.directory_set.all())
        if six.PY3:
            matches = filter(lambda x: x.name == 'subdir1.v2', parentdir_at_t2.directory_set.all())
        self.assertEqual(1, len(list(matches)))

    def test_adding_more_subdir(self):
        t1 = get_utc_now()
        sleep(0.1)

        current_parentdir = Directory.objects.current.get(name__startswith='parent')
        self.assertEqual(2, current_parentdir.directory_set.all().count())
        sleep(0.1)

        Directory.objects.create(name='subdir3.v1', parent=current_parentdir)
        t2 = get_utc_now()

        # There must be 3 subdirectories in the parent directory now. Since current_parentdir has never had an as_of
        # specified, it will reflect the current state.
        self.assertEqual(3, current_parentdir.directory_set.all().count())

        # there should be 2 directories in the parent directory at time t1
        parentdir_at_t1 = Directory.objects.as_of(t1).filter(name='parent.v1').first()
        self.assertEqual(2, parentdir_at_t1.directory_set.all().count())

        # there should be 3 directories in the parent directory at time t2
        parentdir_at_t2 = Directory.objects.as_of(t2).filter(name='parent.v1').first()
        self.assertEqual(3, parentdir_at_t2.directory_set.all().count())

    def test_removing_and_then_adding_again_same_subdir(self):
        t1 = get_utc_now()
        sleep(0.1)

        subdir1_v1 = Directory.objects.current.get(name__startswith='subdir1')
        subdir1_v2 = subdir1_v1.clone()
        subdir1_v2.parent = None
        subdir1_v2.name = 'subdir1.v2'
        subdir1_v2.save()

        t2 = get_utc_now()
        sleep(0.1)

        current_parentdir = Directory.objects.current.get(name__startswith='parent')
        subdir1_v3 = subdir1_v2.clone()
        subdir1_v3.parent = current_parentdir
        subdir1_v3.name = 'subdir1.v3'
        subdir1_v3.save()

        t3 = get_utc_now()

        # there should be 2 directories in the parent directory at time t1
        parentdir_at_t1 = Directory.objects.as_of(t1).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t1.directory_set.all().count())

        # there should be 1 directory in the parent directory at time t2
        parentdir_at_t2 = Directory.objects.as_of(t2).get(name__startswith='parent')
        self.assertEqual(1, parentdir_at_t2.directory_set.all().count())

        # there should be 2 directories in the parent directory at time t3
        parentdir_at_t3 = Directory.objects.as_of(t3).get(name__startswith='parent')
        self.assertEqual(2, parentdir_at_t3.directory_set.all().count())


class OneToManyFilteringTest(TestCase):
    def setUp(self):
        team = Team.objects.create(name='t.v1')
        p1 = Player.objects.create(name='p1.v1', team=team)
        p2 = Player.objects.create(name='p2.v1', team=team)

        self.t1 = get_utc_now()
        sleep(0.1)
        # State at t1
        # Players: [p1.v1, p2.v1]
        # Teams: [t.v1]
        # t.player_set = [p1, p2]

        team.player_set.remove(p2)

        p2 = Player.objects.current.get(name='p2.v1')
        p2.name = 'p2.v2'
        p2.save()

        self.t2 = get_utc_now()
        sleep(0.1)
        # State at t2
        # Players: [p1.v1, p2.v1, p2.v2]
        # Teams: [t.v1]
        # t.player_set = [p1]

        team.player_set.remove(p1)

        p1 = Player.objects.current.get(name='p1.v1')
        p1.name = 'p1.v2'
        p1.save()

        self.t3 = get_utc_now()
        sleep(0.1)
        # State at t3
        # Players: [p1.v1, p2.v1, p2.v2, p1.v2]
        # Teams: [t.v1]
        # t.player_set = []

        # Let's get those players back into the game!
        team.player_set.add(p1)
        team.player_set.add(p2)

        p1 = Player.objects.current.get(name__startswith='p1')
        p1.name = 'p1.v3'
        p1.save()

        p2 = Player.objects.current.get(name__startswith='p2')
        p2.name = 'p2.v3'
        p2.save()

        self.t4 = get_utc_now()
        sleep(0.1)
        # State at t4
        # Players: [p1.v1, p2.v1, p2.v2, p1.v2, p2.v3, p1.v3]
        # Teams: [t.v1]
        # t.player_set = [p1, p2]

        p1.delete()

        self.t5 = get_utc_now()
        # State at t4
        # Players: [p1.v1, p2.v1, p2.v2, p1.v2, p2.v3, p1.v3]
        # Teams: [t.v1]
        # t.player_set = [p2]

    def test_filtering_on_the_other_side_of_the_relation(self):
        self.assertEqual(1, Team.objects.all().count())
        self.assertEqual(1, Team.objects.as_of(self.t1).all().count())
        self.assertEqual(3, Player.objects.filter(name__startswith='p1').all().count())
        self.assertEqual(3, Player.objects.filter(name__startswith='p2').all().count())
        self.assertEqual(1, Player.objects.as_of(self.t1).filter(name='p1.v1').all().count())
        self.assertEqual(1, Player.objects.as_of(self.t1).filter(name='p2.v1').all().count())

        # at t1 there should be one team with two players
        team_p1 = Team.objects.as_of(self.t1).filter(player__name='p1.v1').first()
        self.assertIsNotNone(team_p1)
        team_p2 = Team.objects.as_of(self.t1).filter(player__name='p2.v1').first()
        self.assertIsNotNone(team_p2)

        # at t2 there should be one team with one single player called 'p1.v1'
        team_p1 = Team.objects.as_of(self.t2).filter(player__name='p1.v1').first()
        team_p2 = Team.objects.as_of(self.t2).filter(player__name='p2.v2').first()
        self.assertIsNotNone(team_p1)
        self.assertEqual(team_p1.name, 't.v1')
        self.assertEqual(1, team_p1.player_set.count())
        self.assertIsNone(team_p2)

        # at t3 there should be one team with no players
        team_p1 = Team.objects.as_of(self.t3).filter(player__name='p1.v2').first()
        team_p2 = Team.objects.as_of(self.t3).filter(player__name='p2.v2').first()
        self.assertIsNone(team_p1)
        self.assertIsNone(team_p2)

        # at t4 there should be one team with two players again!
        team_p1 = Team.objects.as_of(self.t4).filter(player__name='p1.v3').first()
        team_p2 = Team.objects.as_of(self.t4).filter(player__name='p2.v3').first()
        self.assertIsNotNone(team_p1)
        self.assertEqual(team_p1.name, 't.v1')
        self.assertIsNotNone(team_p2)
        self.assertEqual(team_p2.name, 't.v1')
        self.assertEqual(team_p1, team_p2)
        self.assertEqual(2, team_p1.player_set.count())

    def test_simple_filter_using_q_objects(self):
        """
        This tests explicitely the filtering of a versioned object using Q objects.
        However, since this is done implicetly with every call to 'as_of', this test is redundant but is kept for
        explicit test coverage
        """
        t1_players = list(
            Player.objects.as_of(self.t1).filter(Q(name__startswith='p1') | Q(name__startswith='p2')).values_list(
                'name',
                flat=True))
        self.assertEqual(2, len(t1_players))
        self.assertListEqual(sorted(t1_players), sorted(['p1.v1', 'p2.v1']))

    def test_filtering_for_deleted_player_at_t5(self):
        team_none = Team.objects.as_of(self.t5).filter(player__name__startswith='p1').first()
        self.assertIsNone(team_none)

    @skipUnless(connection.vendor == 'sqlite', 'SQL is database specific, only sqlite is tested here.')
    def test_query_created_by_filtering_for_deleted_player_at_t5(self):
        team_none_queryset = Team.objects.as_of(self.t5).filter(player__name__startswith='p1')
        # Validating the current query prior to analyzing the generated SQL
        self.assertEqual([], list(team_none_queryset))
        team_none_query = str(team_none_queryset.query)

        team_table = Team._meta.db_table
        player_table = Player._meta.db_table
        t5_utc_w_tz = str(self.t5)
        t5_utc_wo_tz = t5_utc_w_tz[:-6]

        expected_query = """
            SELECT
                "{team_table}"."id",
                "{team_table}"."identity",
                "{team_table}"."version_start_date",
                "{team_table}"."version_end_date",
                "{team_table}"."version_birth_date",
                "{team_table}"."name",
                "{team_table}"."city_id"
            FROM "{team_table}"
            INNER JOIN
                "{player_table}" ON (
                    "{team_table}"."identity" = "{player_table}"."team_id"
                    AND ((
                        {player_table}.version_start_date <= {ts}
                        AND (
                            {player_table}.version_end_date > {ts}
                            OR {player_table}.version_end_date is NULL
                        )
                    ))
                )
            WHERE (
                "{player_table}"."name" LIKE p1% ESCAPE '\\\'
                AND (
                    "{team_table}"."version_end_date" > {ts_wo_tz}
                    OR "{team_table}"."version_end_date" IS NULL
                    )
                AND "{team_table}"."version_start_date" <= {ts_wo_tz}
            )
        """.format(ts=t5_utc_w_tz, ts_wo_tz=t5_utc_wo_tz, team_table=team_table, player_table=player_table)
        self.assertStringEqualIgnoreWhiteSpaces(expected_query, team_none_query)


class MultiM2MTest(TestCase):
    """
    Testing multiple ManyToMany-relationships on a same class; the following story was chosen:

        Classroom <--> Student <--> Professor

    """
    t0 = t1 = t2 = t3 = t4 = None

    def setUp(self):
        # -------------- t0:
        mr_biggs = Professor.objects.create(name='Mr. Biggs', address='123 Mainstreet, Somewhere',
                                            phone_number='123')
        ms_piggy = Professor.objects.create(name='Ms. Piggy', address='82 Leicester Street, London',
                                            phone_number='987')

        gym = Classroom.objects.create(name='Sports room', building='The big one over there')
        phylo = Classroom.objects.create(name='Philosophy lectures', building='The old one')

        annika = Student.objects.create(name='Annika')
        annika.professors.add(mr_biggs)
        annika.professors.add(ms_piggy)
        annika.classrooms.add(phylo)
        annika.classrooms.add(gym)

        benny = Student.objects.create(name='Benny')
        benny.professors.add(mr_biggs)
        benny.classrooms.add(gym)

        sophie = Student.objects.create(name='Sophie')
        # Sophie doesn't study at that school yet, but is already subscribed

        self.t0 = get_utc_now()
        sleep(0.1)

        # -------------- t1:
        # Mr. Biggs moves to Berne
        mr_biggs = mr_biggs.clone()
        mr_biggs.address = 'Thunplatz, Bern'
        mr_biggs.save()

        # Mr. Evans gets hired
        mr_evans = Professor.objects.create(name='Mr. Evans', address='lives in a camper',
                                            phone_number='456')

        # A lab gets built
        lab = Classroom.objects.create(name='Physics and stuff', building='The old one')

        self.t1 = get_utc_now()
        sleep(0.1)

        # -------------- t2:
        # Mr. Evans starts to teach sophie in the lab
        mr_evans.students.add(sophie)
        lab.students.add(sophie)

        self.t2 = get_utc_now()
        sleep(0.1)

        # -------------- t3:
        # Annika is joining Sophie
        annika.professors.add(mr_evans)
        annika.classrooms.add(lab)

        self.t3 = get_utc_now()
        sleep(0.1)

        # -------------- t4:
        # Benny cuts that sh*t
        benny.professors.remove(mr_biggs)

        self.t4 = get_utc_now()

    def test_t0(self):
        professors = Professor.objects.as_of(self.t0).all()
        self.assertEqual(len(professors), 2)
        students = Student.objects.as_of(self.t0).all()
        self.assertEqual(len(students), 3)
        classrooms = Classroom.objects.as_of(self.t0).all()
        self.assertEqual(len(classrooms), 2)

        annika_t0 = Student.objects.as_of(self.t0).get(name='Annika')
        annikas_professors_t0 = annika_t0.professors.all()
        annikas_classrooms_t0 = annika_t0.classrooms.all()
        self.assertEqual(len(annikas_professors_t0), 2)
        self.assertEqual(len(annikas_classrooms_t0), 2)

        benny_t0 = Student.objects.as_of(self.t0).get(name='Benny')
        bennys_professors_t0 = benny_t0.professors.all()
        bennys_classrooms_t0 = benny_t0.classrooms.all()
        self.assertEqual(len(bennys_professors_t0), 1)
        self.assertEqual(len(bennys_classrooms_t0), 1)

        mr_biggs_t0 = bennys_professors_t0[0]
        self.assertEqual(mr_biggs_t0.name, 'Mr. Biggs')
        self.assertEqual(mr_biggs_t0.address, '123 Mainstreet, Somewhere')
        self.assertEqual(len(mr_biggs_t0.students.all()), 2)

        for student in mr_biggs_t0.students.all():
            self.assertIn(student.name, ['Annika', 'Benny'])

        gym_t0 = bennys_classrooms_t0[0]
        self.assertEqual(gym_t0.name, 'Sports room')
        self.assertEqual(len(gym_t0.students.all()), 2)
        for student in gym_t0.students.all():
            self.assertIn(student.name, ['Annika', 'Benny'])

        female_professors_t0 = Classroom.objects.as_of(self.t0).get(name__startswith='Philo'). \
            students.first(). \
            professors.filter(name__startswith='Ms')
        self.assertEqual(len(female_professors_t0), 1)
        self.assertEqual(female_professors_t0[0].name, 'Ms. Piggy')
        self.assertEqual(female_professors_t0[0].phone_number, '987')

    def test_t1(self):
        mr_evans_t1 = Professor.objects.as_of(self.t1).get(name='Mr. Evans')
        self.assertEqual(mr_evans_t1.name, 'Mr. Evans')
        self.assertEqual(mr_evans_t1.students.count(), 0)
        self.assertEqual(list(mr_evans_t1.students.all()), [])

        self.assertEqual(Classroom.objects.as_of(self.t1).get(name__startswith="Physics").students.count(),
                         0)

        self.assertEqual(Professor.objects.as_of(self.t1).get(name__contains='Biggs').address,
                         'Thunplatz, Bern')

    def test_t2(self):
        mr_evans_t2 = Professor.objects.as_of(self.t2).get(name='Mr. Evans')
        evans_students = mr_evans_t2.students.all()
        self.assertEqual(len(evans_students), 1)
        self.assertEqual(evans_students[0].name, 'Sophie')
        # Checking Sophie's rooms
        self.assertIn('Physics and stuff', list(evans_students[0].classrooms.values_list('name', flat=True)))
        self.assertEqual(evans_students[0].classrooms.count(), 1)

    def test_t3(self):
        # Find all professors who teach Annika
        annikas_professors_t3 = Professor.objects.as_of(self.t3).filter(students__name='Annika')
        self.assertEqual(annikas_professors_t3.count(), 3)
        self.assertIn('Mr. Evans', list(annikas_professors_t3.values_list('name', flat=True)))

    def test_number_of_queries_stay_constant(self):
        """
        We had a situation where the number of queries to get data from a m2m relations
        was proportional to the number of objects in the relations. For example if one
        object was related with 10 others it will require 2 + 2x10 queries to get data.
        Obviously this is not something one would want and this problem is really
        difficult to find out as the behavior is correct. There is just too many queries
        generated to carry on the work and therefore the system's performance sinks.
        This test is here to make sure we don't go back accidentally to such a situation
        by making sure the number of queries stays the same.
        """
        annika = Student.objects.current.get(name='Annika')
        with self.assertNumQueries(1):
            annika.professors.all().first()

    def test_adding_multiple_related_objects(self):
        # In the setUp, Benny had a professor, and then no more.
        all_professors = list(Professor.objects.current.all())
        benny = Student.objects.current.get(name='Benny')
        benny.professors.add(*all_professors)
        benny.as_of = get_utc_now()
        # This was once failing because _add_items() was filtering out items it didn't need to re-add,
        # but it was not restricting the query to find those objects with any as-of time.
        self.assertSetEqual(set(list(benny.professors.all())), set(all_professors))

    def test_adding_multiple_related_objects_using_a_valid_timestamp(self):
        all_professors = list(Professor.objects.current.all())
        benny = Student.objects.current.get(name='Benny')
        benny.professors.add_at(self.t4, *all_professors)
        # Test the addition of objects in the past
        self.assertSetEqual(set(list(benny.professors.all())), set(all_professors))

    @skip("To be implemented")
    def test_adding_multiple_related_objects_using_an_invalid_timestamp(self):
        # TODO: See test_adding_multiple_related_objects and make use of add_at and a timestamp laying outside the
        # current object's lifetime

        # Create a new version beyond self.t4
        benny = Student.objects.current.get(name='Benny')
        benny = benny.clone()
        benny.name = "Benedict"
        benny.save()

        all_professors = list(Professor.objects.current.all())
        # Test the addition of objects in the past with a timestamp that points before the current
        # versions lifetime
        # TODO: Raise an error when adding objects outside the lifetime of an object (even if it's a discouraged use case)
        self.assertRaises(ValueError, lambda: benny.professors.add_at(self.t4, *all_professors))

    def test_querying_multiple_related_objects_on_added_object(self):
        # In the setUp, Benny had a professor, and then no more.
        all_professors = list(Professor.objects.current.all())
        benny = Student.objects.current.get(name='Benny')
        benny.professors.add(*all_professors)
        # This was once failing because benny's as_of time had been set by the call to Student.objects.current,
        # and was being propagated to the query selecting the relations, which were added after as_of was set.
        self.assertSetEqual(set(list(benny.professors.all())), set(all_professors))

    def test_direct_assignment_of_relations(self):
        """
        Ensure that when relations that are directly set (e.g. not via add() or remove(),
        that their versioning information is kept.
        """
        benny = Student.objects.current.get(name='Benny')
        all_professors = list(Professor.objects.current.all())
        first_professor = all_professors[0]
        last_professor = all_professors[-1]
        some_professor_ids = [o.pk for o in all_professors][:2]
        self.assertNotEqual(first_professor.identity, last_professor.identity)
        self.assertTrue(1 < len(some_professor_ids) < len(all_professors))

        self.assertEqual(benny.professors.count(), 0)
        t0 = get_utc_now()
        benny.professors.add(first_professor)
        t1 = get_utc_now()

        benny.professors = all_professors
        t2 = get_utc_now()

        benny.professors = [last_professor]
        t3 = get_utc_now()

        # Also try assigning with a list of pks, instead of objects:
        benny.professors = some_professor_ids
        t4 = get_utc_now()

        # Benny ain't groovin' it.
        benny.professors = []
        t5 = get_utc_now()

        benny0 = Student.objects.as_of(t0).get(identity=benny.identity)
        benny1 = Student.objects.as_of(t1).get(identity=benny.identity)
        benny2 = Student.objects.as_of(t2).get(identity=benny.identity)
        benny3 = Student.objects.as_of(t3).get(identity=benny.identity)
        benny4 = Student.objects.as_of(t4).get(identity=benny.identity)
        benny5 = Student.objects.as_of(t5).get(identity=benny.identity)

        self.assertSetEqual(set(list(benny0.professors.all())), set())
        self.assertSetEqual(set(list(benny1.professors.all())), set([first_professor]))
        self.assertSetEqual(set(list(benny2.professors.all())), set(all_professors))
        self.assertSetEqual(set(list(benny3.professors.all())), set([last_professor]))
        self.assertSetEqual(set([o.pk for o in benny4.professors.all()]), set(some_professor_ids))
        self.assertSetEqual(set(list(benny5.professors.all())), set())

    def test_annotations_and_aggregations(self):

        # Annotations and aggreagations should work with .current objects as well as historical .as_of() objects.
        self.assertEqual(4,
                         Professor.objects.current.annotate(num_students=Count('students')).aggregate(
                             sum=Sum('num_students'))['sum']
        )
        self.assertTupleEqual((1, 1),
                              (Professor.objects.current.annotate(num_students=Count('students')).get(
                                  name='Mr. Biggs').num_students,
                               Professor.objects.current.get(name='Mr. Biggs').students.count())
        )

        self.assertTupleEqual((2, 2),
                              (Professor.objects.as_of(self.t1).annotate(num_students=Count('students')).get(
                                  name='Mr. Biggs').num_students,
                               Professor.objects.as_of(self.t1).get(name='Mr. Biggs').students.count())
        )

        # Results should include records for which the annotation returns a 0 count, too.
        # This requires that the generated LEFT OUTER JOIN condition includes a clause
        # to restrict the records according to the desired as_of time.
        self.assertEqual(3, len(Student.objects.current.annotate(num_teachers=Count('professors')).all()))

    def test_constant_number_of_queries_when_cloning_m2m_related_object(self):
        """
        This test aims to verify whether the number of queries against the DB remains constant,
        even if the number of M2M relations has grown.
        This test was necessary in order to verify changes from PR #44
        """
        annika = Student.objects.current.get(name='Annika')
        # Annika, at this point, has:
        # - 3 professors
        # - 3 classrooms

        # There are 12 queries against the DB:
        # - 3 for writing the new version of the object itself
        #   o 1 attempt to update the earlier version
        #   o 1 insert of the earlier version
        #   o 1 update of the later version
        # - 5 for the professors relationship
        #   o 1 for selecting all concerned professor objects
        #   o 1 for selecting all concerned intermediate table entries (student_professor)
        #   o 1 for updating current intermediate entry versions
        #   o 1 for non-current rel-entries pointing the annika-object
        #     (there's 1 originating from the clone-operation on mr_biggs)
        #   o 1 for inserting new versions
        # - 4 for the classrooms M2M relationship
        #   o 1 for selecting all concerned classroom objects
        #   o 1 for selecting all concerned intermediate table entries (student_classroom)
        #   o 1 for updating current intermediate entry versions
        #   o 0 for non-current rel-entries pointing the annika-object
        #   o 1 for inserting new versions
        with self.assertNumQueries(12):
            annika.clone()

    def test_no_duplicate_m2m_entries_after_cloning_related_object(self):
        """
        This test ensures there are no duplicate entries added when cloning an object participating
        in a M2M relationship.
        It ensures the absence of duplicate entries on all modified levels:
        - at the object-model level
        - at any relationship level (intermediary tables)
        """
        annika = Student.objects.current.get(name='Annika')
        student_professors_mgr = annika.professors
        student_classrooms_mgr = annika.classrooms
        # Annika, at this point, has:
        # - 3 professors
        # - 3 classrooms

        # Check the PRE-CLONE state
        annika_pre_clone = annika
        # There's 1 Student instance (named Annika)
        self.assertEqual(1, Student.objects.filter(identity=annika.identity).count())
        # There are 4 links to 3 professors (Mr. Biggs has been cloned once when setting up, thus 1 additional link)
        student_professor_links = list(student_professors_mgr.through.objects.filter(
            **{student_professors_mgr.source_field_name: annika_pre_clone.id}))
        self.assertEqual(4, len(student_professor_links))
        # There are 3 links to classrooms
        student_classroom_links = list(student_classrooms_mgr.through.objects.filter(
            **{student_classrooms_mgr.source_field_name: annika_pre_clone.id}))
        self.assertEqual(3, len(student_classroom_links))

        # Do the CLONE that also impacts the number of linking entries
        annika_post_clone = annika.clone()

        # Check the POST-CLONE state
        # There are 2 Student instances (named Annika)
        self.assertEqual(2, Student.objects.filter(identity=annika.identity).count())

        # There are 7 links to 3 professors
        # - 4 of them are pointing the previous annika-object (including the non-current link to Mr. Biggs)
        # - 3 of them are pointing the current annika-object (only current links were taken over)
        student_professor_links = list(student_professors_mgr.through.objects.filter(
            Q(**{student_professors_mgr.source_field_name: annika_pre_clone.id}) |
            Q(**{student_professors_mgr.source_field_name: annika_post_clone.id})))
        self.assertEqual(7, len(student_professor_links))
        self.assertEqual(4, student_professors_mgr.through.objects.filter(
            Q(**{student_professors_mgr.source_field_name: annika_pre_clone.id})).count())
        self.assertEqual(3, student_professors_mgr.through.objects.filter(
            Q(**{student_professors_mgr.source_field_name: annika_post_clone.id})).count())

        # There are 6 links to 3 professors
        # - 3 of them are pointing the previous annika-object
        # - 3 of them are pointing the current annika-object
        student_classroom_links = list(student_classrooms_mgr.through.objects.filter(
            Q(**{student_classrooms_mgr.source_field_name: annika_pre_clone.id}) |
            Q(**{student_classrooms_mgr.source_field_name: annika_post_clone.id})))
        self.assertEqual(6, len(student_classroom_links))
        self.assertEqual(3, student_classrooms_mgr.through.objects.filter(
            Q(**{student_classrooms_mgr.source_field_name: annika_pre_clone.id})).count())
        self.assertEqual(3, student_classrooms_mgr.through.objects.filter(
            Q(**{student_classrooms_mgr.source_field_name: annika_post_clone.id})).count())


class MultiM2MToSameTest(TestCase):
    """
    This test case shall test the correct functionality of the following relationship:

        Teacher <--> Pupil <--> Teacher
    """
    t0 = t1 = t2 = t3 = None

    def setUp(self):
        billy = Pupil.objects.create(name='Billy', phone_number='123')
        erika = Pupil.objects.create(name='Erika', phone_number='456')

        ms_sue = Teacher.objects.create(name='Ms. Sue', domain='English')
        ms_klishina = Teacher.objects.create(name='Ms. Klishina', domain='Russian')

        mr_kazmirek = Teacher.objects.create(name='Mr. Kazmirek', domain='Math')
        ms_mayer = Teacher.objects.create(name='Ms. Mayer', domain='Chemistry')

        self.t0 = get_utc_now()
        sleep(0.1)

        billy.language_teachers.add(ms_sue)
        erika.science_teachers.add(mr_kazmirek, ms_mayer)

        self.t1 = get_utc_now()
        sleep(0.1)

        billy.language_teachers.add(ms_klishina)
        billy.language_teachers.remove(ms_sue)

        self.t2 = get_utc_now()
        sleep(0.1)

        erika.science_teachers.remove(ms_mayer)

        self.t3 = get_utc_now()

    def test_filtering_on_the_other_side_of_relation(self):
        language_pupils_count = Pupil.objects.as_of(self.t0).filter(
            language_teachers__name='Ms. Sue').count()
        self.assertEqual(0, language_pupils_count)

        language_pupils_count = Pupil.objects.as_of(self.t1).filter(
            language_teachers__name='Ms. Sue').count()
        self.assertEqual(1, language_pupils_count)

        language_pupils_count = Pupil.objects.as_of(self.t2).filter(
            language_teachers__name='Ms. Sue').count()
        self.assertEqual(0, language_pupils_count)

    def test_t0(self):
        """
        Just some cross-checking...
        """
        billy_t0 = Pupil.objects.as_of(self.t0).get(name='Billy')
        self.assertEqual(billy_t0.language_teachers.count(), 0)

    def test_t1(self):
        billy_t1 = Pupil.objects.as_of(self.t1).get(name='Billy')
        self.assertEqual(billy_t1.language_teachers.count(), 1)
        self.assertEqual(billy_t1.language_teachers.first().name, 'Ms. Sue')

        erika_t1 = Pupil.objects.as_of(self.t1).get(name='Erika')
        self.assertEqual(erika_t1.science_teachers.count(), 2)

    def test_t2(self):
        billy_t2 = Pupil.objects.as_of(self.t2).get(name='Billy')
        self.assertEqual(billy_t2.language_teachers.count(), 1)
        self.assertEqual(billy_t2.language_teachers.first().name, 'Ms. Klishina')

    def test_t3(self):
        erika_t3 = Pupil.objects.as_of(self.t3).get(name='Erika')
        self.assertEqual(erika_t3.science_teachers.count(), 1)
        self.assertEqual(erika_t3.science_teachers.first().name, 'Mr. Kazmirek')


class SelfReferencingManyToManyTest(TestCase):
    def setUp(self):
        maude = Person.objects.create(name='Maude')
        max = Person.objects.create(name='Max')
        mips = Person.objects.create(name='Mips')
        mips.parents.add(maude, max)

    def test_parent_relationship(self):
        mips = Person.objects.current.get(name='Mips')
        parents = mips.parents.all()
        self.assertSetEqual({'Maude', 'Max'}, set([p.name for p in parents]))

    def test_child_relationship(self):
        maude = Person.objects.current.get(name='Maude')
        max = Person.objects.current.get(name='Max')
        for person in [maude, max]:
            self.assertEqual('Mips', person.children.first().name)

    def test_relationship_spanning_query(self):
        mips_parents_qs = Person.objects.current.filter(children__name='Mips')
        self.assertSetEqual({'Max', 'Maude'}, {p.name for p in mips_parents_qs})

class ManyToManyFilteringTest(TestCase):
    def setUp(self):
        c1 = C1(name='c1.v1')
        c2 = C2(name='c2.v1')
        c3 = C3(name='c3.v1')

        c1.save()
        c2.save()
        c3.save()

        # Play on an object's instance
        c2 = c2.clone()
        c2.name = 'c2.v2'
        c2.save()

        self.t0 = get_utc_now()
        sleep(0.1)

        c2.c3s.add(c3)
        c1.c2s.add(c2)

        self.t1 = get_utc_now()
        # at t1:
        # c1.c2s = [c2]
        # c2.c3s = [c3]
        sleep(0.1)

        c3a = C3(name='c3a.v1')
        c3a.save()
        c2.c3s.add(c3a)

        sleep(0.1)
        self.t2 = get_utc_now()
        # at t2:
        # c1.c2s = [c2]
        # c2.c3s = [c3, c3a]

        c1 = c1.clone()
        c1.name = 'c1.v2'
        c1.save()

        c3a.delete()

        sleep(0.1)
        self.t3 = get_utc_now()
        # at t3:
        # c1.c2s = [c2]
        # c2.c3s = [c3]

    def test_filtering_one_jump(self):
        """
        Test filtering m2m relations with 2 models
        """
        should_be_c1 = C1.objects.filter(c2s__name__startswith='c2').first()
        self.assertIsNotNone(should_be_c1)

    def test_inexistent_relations_at_t0(self):
        """
        Test return value when there is no element assigned to a M2M relationship
        """
        c1_at_t0 = C1.objects.as_of(self.t0).get()
        self.assertEqual([], list(c1_at_t0.c2s.all()))

    def test_filtering_one_jump_with_version_at_t1(self):
        """
        Test filtering m2m relations with 2 models with propagation of querytime
        information across all tables
        """
        should_be_c1 = C1.objects.as_of(self.t1) \
            .filter(c2s__name__startswith='c2').first()
        self.assertIsNotNone(should_be_c1)

    def test_filtering_one_jump_with_version_at_t3(self):
        """
        Test filtering m2m reations with 2 models with propagaton of querytime
        information across all tables.
        Also test after an object being in a relationship has been deleted.
        """
        should_be_c2 = C2.objects.as_of(self.t3) \
            .filter(c3s__name__startswith='c3.').first()
        self.assertIsNotNone(should_be_c2)
        self.assertEqual(should_be_c2.name, 'c2.v2')

        should_be_none = C2.objects.as_of(self.t3) \
            .filter(c3s__name__startswith='c3a').first()
        self.assertIsNone(should_be_none)

    @skipUnless(connection.vendor == 'sqlite', 'SQL is database specific, only sqlite is tested here.')
    def test_query_created_by_filtering_one_jump_with_version_at_t1(self):
        """
        Test filtering m2m relations with 2 models with propagation of querytime
        information across all tables
        """
        should_be_c1_queryset = C1.objects.as_of(self.t1) \
            .filter(c2s__name__startswith='c2')
        should_be_c1_query = str(should_be_c1_queryset.query)
        t1_string = self.t1.isoformat().replace('T', ' ')
        t1_no_tz_string = t1_string[:-6]
        expected_query = """
        SELECT "versions_tests_c1"."id", "versions_tests_c1"."identity",
               "versions_tests_c1"."version_start_date",
               "versions_tests_c1"."version_end_date",
               "versions_tests_c1"."version_birth_date", "versions_tests_c1"."name"
          FROM "versions_tests_c1"
    INNER JOIN "versions_tests_c1_c2s" ON (
                  "versions_tests_c1"."id" = "versions_tests_c1_c2s"."c1_id"
                   AND ((
                      versions_tests_c1_c2s.version_start_date <= {time}
                      AND (versions_tests_c1_c2s.version_end_date > {time}
                        OR versions_tests_c1_c2s.version_end_date is NULL
                      )
                   ))
               )
    INNER JOIN "versions_tests_c2" ON (
                  "versions_tests_c1_c2s"."C2_id" = "versions_tests_c2"."id"
                   AND ((
                      versions_tests_c2.version_start_date <= {time}
                      AND (versions_tests_c2.version_end_date > {time}
                        OR versions_tests_c2.version_end_date is NULL
                      )
                   ))
               )
         WHERE (
               "versions_tests_c2"."name" LIKE c2% escape '\\'
           AND ("versions_tests_c1"."version_end_date" > {time_no_tz}
                   OR "versions_tests_c1"."version_end_date" IS NULL)
           AND "versions_tests_c1"."version_start_date" <= {time_no_tz}
               )
        """.format(time=t1_string, time_no_tz=t1_no_tz_string)

        self.assertStringEqualIgnoreWhiteSpaces(expected_query, should_be_c1_query)

    def test_filtering_one_jump_reverse(self):
        """
        Test filtering m2m relations with 2 models but navigating relation in the
        reverse direction
        """
        should_be_c3 = C3.objects.filter(c2s__name__startswith='c2').first()
        self.assertIsNotNone(should_be_c3)

    def test_filtering_one_jump_reverse_with_version_at_t1(self):
        """
        Test filtering m2m relations with 2 models with propagation of querytime
        information across all tables and navigating the relation in the reverse
        direction
        """
        should_be_c3 = C3.objects.as_of(self.t1) \
            .filter(c2s__name__startswith='c2').first()
        self.assertIsNotNone(should_be_c3)
        self.assertEqual(should_be_c3.name, 'c3.v1')

    def test_filtering_two_jumps(self):
        """
        Test filtering m2m relations with 3 models
        """
        with self.assertNumQueries(1) as counter:
            should_be_c1 = C1.objects.filter(c2s__c3s__name__startswith='c3').first()
            self.assertIsNotNone(should_be_c1)

    def test_filtering_two_jumps_with_version_at_t1(self):
        """
        Test filtering m2m relations with 3 models with propagation of querytime
        information across all tables
        """
        with self.assertNumQueries(3) as counter:
            should_be_none = C1.objects.as_of(self.t1) \
                .filter(c2s__c3s__name__startswith='c3a').first()
            self.assertIsNone(should_be_none)

            should_be_c1 = C1.objects.as_of(self.t1) \
                .filter(c2s__c3s__name__startswith='c3').first()
            self.assertIsNotNone(should_be_c1)
            self.assertEqual(should_be_c1.name, 'c1.v1')

            count = C1.objects.as_of(self.t1) \
                .filter(c2s__c3s__name__startswith='c3').all().count()
            self.assertEqual(1, count)

    @skipUnless(connection.vendor == 'sqlite', 'SQL is database specific, only sqlite is tested here.')
    def test_query_created_by_filtering_two_jumps_with_version_at_t1(self):
        """
        Investigate correctness of the resulting SQL query
        """
        should_be_c1_queryset = C1.objects.as_of(self.t1) \
            .filter(c2s__c3s__name__startswith='c3')
        should_be_c1_query = str(should_be_c1_queryset.query)
        t1_string = self.t1.isoformat().replace('T', ' ')
        t1_no_tz_string = t1_string[:-6]
        expected_query = """
        SELECT "versions_tests_c1"."id", "versions_tests_c1"."identity",
               "versions_tests_c1"."version_start_date", "versions_tests_c1"."version_end_date",
               "versions_tests_c1"."version_birth_date", "versions_tests_c1"."name"
          FROM "versions_tests_c1"
    INNER JOIN "versions_tests_c1_c2s" ON (
                   "versions_tests_c1"."id" = "versions_tests_c1_c2s"."c1_id"
              AND ((versions_tests_c1_c2s.version_start_date <= {time}
                     AND (versions_tests_c1_c2s.version_end_date > {time}
                        OR versions_tests_c1_c2s.version_end_date is NULL ))
                  )
               )
    INNER JOIN "versions_tests_c2" ON (
                   "versions_tests_c1_c2s"."C2_id" = "versions_tests_c2"."id"
              AND ((versions_tests_c2.version_start_date <= {time}
                     AND (versions_tests_c2.version_end_date > {time}
                        OR versions_tests_c2.version_end_date is NULL ))
                  )
              )
    INNER JOIN "versions_tests_c2_c3s" ON (
                   "versions_tests_c2"."id" = "versions_tests_c2_c3s"."c2_id"
               AND ((versions_tests_c2_c3s.version_start_date <= {time}
                     AND (versions_tests_c2_c3s.version_end_date > {time}
                        OR versions_tests_c2_c3s.version_end_date is NULL ))
                  )
              )
    INNER JOIN "versions_tests_c3" ON (
                   "versions_tests_c2_c3s"."C3_id" = "versions_tests_c3"."id"
               AND ((versions_tests_c3.version_start_date <= {time}
                     AND (versions_tests_c3.version_end_date > {time}
                      OR versions_tests_c3.version_end_date is NULL ))
                   )
                )
         WHERE (
               "versions_tests_c3"."name" LIKE c3%  escape '\\'
           AND ("versions_tests_c1"."version_end_date" > {time_no_tz}
                 OR "versions_tests_c1"."version_end_date" IS NULL)
           AND "versions_tests_c1"."version_start_date" <= {time_no_tz}
               )
        """.format(time=t1_string, time_no_tz=t1_no_tz_string)
        self.assertStringEqualIgnoreWhiteSpaces(expected_query, should_be_c1_query)

    def test_filtering_two_jumps_with_version_at_t2(self):
        """
        Test filtering m2m relations with 3 models with propagation of querytime
        information across all tables but this time at point in time t2
        """
        with self.assertNumQueries(2) as counter:
            should_be_c1 = C1.objects.as_of(self.t2) \
                .filter(c2s__c3s__name__startswith='c3a').first()
            self.assertIsNotNone(should_be_c1)

            count = C1.objects.as_of(self.t2) \
                .filter(c2s__c3s__name__startswith='c3').all().count()
            self.assertEqual(2, count)

    def test_filtering_two_jumps_with_version_at_t3(self):
        """
        Test filtering m2m relations with 3 models with propagation of querytime
        information across all tables but this time at point in time t3
        """
        with self.assertNumQueries(3) as counter:
            # Should be None, since object 'c3a' does not exist anymore at t3
            should_be_none = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3a').first()
            self.assertIsNone(should_be_none)

            should_be_c1 = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3.').first()
            self.assertIsNotNone(should_be_c1)

            count = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3.').all().count()
            self.assertEqual(1, count)

    def test_filtering_two_jumps_reverse(self):
        """
        Test filtering m2m relations with 3 models but navigating relation in the
        reverse direction
        """
        with self.assertNumQueries(1) as counter:
            should_be_c3 = C3.objects.filter(c2s__c1s__name__startswith='c1').first()
            self.assertIsNotNone(should_be_c3)

    def test_filtering_two_jumps_reverse_with_version_at_t1(self):
        """
        Test filtering m2m relations with 3 models with propagation of querytime
        information across all tables and navigating the relation in the reverse
        direction
        """
        with self.assertNumQueries(2) as counter:
            should_be_c3 = C3.objects.as_of(self.t1). \
                filter(c2s__c1s__name__startswith='c1').first()
            self.assertIsNotNone(should_be_c3)
            self.assertEqual(should_be_c3.name, 'c3.v1')

            count = C3.objects.as_of(self.t1) \
                .filter(c2s__c1s__name__startswith='c1').all().count()
            self.assertEqual(1, count)

    def test_filtering_two_jumps_reverse_with_version_at_t2(self):
        """
        Test filtering m2m relations with 3 models with propagation of querytime
        information across all tables and navigating the relation in the reverse
        direction but this time at point in time t2
        """
        with self.assertNumQueries(2) as counter:
            should_be_c3 = C3.objects.as_of(self.t2) \
                .filter(c2s__c1s__name__startswith='c1').first()
            self.assertIsNotNone(should_be_c3)

            count = C3.objects.as_of(self.t2) \
                .filter(c2s__c1s__name__startswith='c1').all().count()
            self.assertEqual(2, count)


class HistoricM2MOperationsTests(TestCase):
    def setUp(self):
        # Set up a situation on 23.4.1984
        ts = datetime.datetime(1984, 4, 23, tzinfo=utc)
        big_brother = Observer.objects._create_at(ts, name='BigBrother')
        self.big_brother = big_brother
        subject = Subject.objects._create_at(ts, name='Winston Smith')
        big_brother.subjects.add_at(ts, subject)

        # Remove the relationship on 23.5.1984
        ts_a_month_later = ts + datetime.timedelta(days=30)
        big_brother.subjects.remove_at(ts_a_month_later, subject)

    def test_observer_subject_relationship_is_active_in_early_1984(self):
        ts = datetime.datetime(1984, 5, 1, tzinfo=utc)
        observer = Observer.objects.as_of(ts).get()
        self.assertEqual(observer.name, 'BigBrother')
        subjects = observer.subjects.all()
        self.assertEqual(len(subjects), 1)
        self.assertEqual(subjects[0].name, 'Winston Smith')

    def test_observer_subject_relationship_is_inactive_in_late_1984(self):
        ts = datetime.datetime(1984, 8, 16, tzinfo=utc)
        observer = Observer.objects.as_of(ts).get()
        self.assertEqual(observer.name, 'BigBrother')
        subjects = observer.subjects.all()
        self.assertEqual(len(subjects), 0)
        subject = Subject.objects.as_of(ts).get()
        self.assertEqual(subject.name, 'Winston Smith')

    def test_simple(self):
        self.big_brother.subjects.all().first()


class M2MDirectAssignmentTests(TestCase):
    def setUp(self):
        self.o1 = Observer.objects.create(name="1.0")
        self.s1 = Subject.objects.create(name="1.0")
        self.s2 = Subject.objects.create(name="2.0")
        self.t1 = get_utc_now()
        self.o1 = self.o1.clone()
        self.o1.name = "1.1"
        self.o1.save()
        self.o1.subjects.add(self.s1, self.s2)
        self.t2 = get_utc_now()
        self.o1 = self.o1.clone()
        self.o1.name = "1.2"
        self.o1.save()
        self.o1.subjects = []
        self.t3 = get_utc_now()

    def test_t1_relations(self):
        observer = Observer.objects.as_of(self.t1).filter(identity=self.o1.identity).first()
        self.assertEqual(0, observer.subjects.all().count())

    def test_t2_relations(self):
        observer = Observer.objects.as_of(self.t2).filter(identity=self.o1.identity).first()
        self.assertEqual(2, observer.subjects.all().count())

    def test_t3_relations(self):
        observer = Observer.objects.as_of(self.t3).filter(identity=self.o1.identity).first()
        self.assertEqual(0, observer.subjects.all().count())


class ReverseForeignKeyDirectAssignmentTests(TestCase):
    def setUp(self):
        # City is the referenced object, Team in the referring object.
        # c1 will be explicitly cloned, but not it's teams.
        # c10 will not be explicitly cloned, but one of it's teams will be.
        self.c1 = City.objects.create(name="Oakland")
        self.team1 = Team.objects.create(name="As")
        self.team2 = Team.objects.create(name="Raiders")

        self.c10 = City.objects.create(name="San Francisco")
        self.team10 = Team.objects.create(name="Giants")
        self.team11 = Team.objects.create(name="49ers")

        self.t1 = get_utc_now()
        self.c1 = self.c1.clone()
        self.c1.team_set.add(self.team1, self.team2)

        self.team10 = self.team10.clone()
        self.c10.team_set.add(self.team10, self.team11)

        self.t2 = get_utc_now()
        self.c1 = self.c1.clone()
        self.c1.team_set = []

        self.team10 = Team.objects.current.get(identity=self.team10.identity).clone()
        self.c10.team_set = []
        self.t3 = get_utc_now()

    def test_t1_relations_for_cloned_referenced_object(self):
        city = City.objects.as_of(self.t1).filter(identity=self.c1.identity).first()
        self.assertEqual(0, city.team_set.all().count())

    def test_t2_relations_for_cloned_referenced_object(self):
        city = City.objects.as_of(self.t2).filter(identity=self.c1.identity).first()
        self.assertEqual(2, city.team_set.all().count())

    def test_t3_relations_for_cloned_referenced_object(self):
        city = City.objects.as_of(self.t3).filter(identity=self.c1.identity).first()
        self.assertEqual(0, city.team_set.all().count())

    def test_t1_relations_for_cloned_referring_object(self):
        city = City.objects.as_of(self.t1).filter(identity=self.c10.identity).first()
        self.assertEqual(0, city.team_set.all().count())

    def test_t2_relations_for_cloned_referring_object(self):
        city = City.objects.as_of(self.t2).filter(identity=self.c10.identity).first()
        self.assertEqual(2, city.team_set.all().count())

    def test_t3_relations_for_cloned_referring_object(self):
        city = City.objects.as_of(self.t3).filter(identity=self.c10.identity).first()
        self.assertEqual(0, city.team_set.all().count())


class PrefetchingTests(TestCase):
    def setUp(self):
        self.city1 = City.objects.create(name='Chicago')
        self.team1 = Team.objects.create(name='te1.v1', city=self.city1)
        self.p1 = Player.objects.create(name='pl1.v1', team=self.team1)
        self.p2 = Player.objects.create(name='pl2.v1', team=self.team1)
        sleep(0.1)
        self.t1 = get_utc_now()

    def test_select_related(self):
        with self.assertNumQueries(1):
            player = Player.objects.as_of(self.t1).select_related('team').get(name='pl1.v1')
            self.assertIsNotNone(player)
            self.assertEqual(player.team, self.team1)

        p1 = self.p1.clone()
        p1.name = 'pl1.v2'
        p1.team = None
        p1.save()
        t2 = get_utc_now()
        with self.assertNumQueries(1):
            player = Player.objects.current.select_related('team').get(name='pl1.v2')
            self.assertIsNotNone(player)
            self.assertIsNone(player.team)

        # Multiple foreign-key related tables should still only require one query
        with self.assertNumQueries(1):
            player = Player.objects.as_of(t2).select_related('team__city').get(name='pl2.v1')
            self.assertIsNotNone(player)
            self.assertEqual(self.city1, player.team.city)

    @skipUnless(connection.vendor == 'sqlite', 'SQL is database specific, only sqlite is tested here.')
    def test_select_related_query_sqlite(self):
        select_related_queryset = Player.objects.as_of(self.t1).select_related('team').all()
        # Validating the query before verifying the SQL string
        self.assertEqual(['pl1.v1', 'pl2.v1'], [player.name for player in select_related_queryset])
        select_related_query = str(select_related_queryset.query)

        team_table = Team._meta.db_table
        player_table = Player._meta.db_table
        t1_utc_w_tz = str(self.t1)
        t1_utc_wo_tz = t1_utc_w_tz[:-6]
        expected_query = """
            SELECT "{player_table}"."id",
                   "{player_table}"."identity",
                   "{player_table}"."version_start_date",
                   "{player_table}"."version_end_date",
                   "{player_table}"."version_birth_date",
                   "{player_table}"."name",
                   "{player_table}"."team_id",
                   "{team_table}"."id",
                   "{team_table}"."identity",
                   "{team_table}"."version_start_date",
                   "{team_table}"."version_end_date",
                   "{team_table}"."version_birth_date",
                   "{team_table}"."name",
                   "{team_table}"."city_id"
            FROM "{player_table}"
            LEFT OUTER JOIN "{team_table}" ON ("{player_table}"."team_id" = "{team_table}"."identity"
                                                      AND (({team_table}.version_start_date <= {ts}
                                                            AND ({team_table}.version_end_date > {ts}
                                                                 OR {team_table}.version_end_date IS NULL))))
            WHERE
            (
              ("{player_table}"."version_end_date" > {ts_wo_tz}
                    OR "{player_table}"."version_end_date" IS NULL)
              AND "{player_table}"."version_start_date" <= {ts_wo_tz}
            )
        """.format(player_table=player_table, team_table=team_table, ts=t1_utc_w_tz, ts_wo_tz=t1_utc_wo_tz)
        self.assertStringEqualIgnoreWhiteSpaces(expected_query, select_related_query)

    @skipUnless(connection.vendor == 'postgresql', 'SQL is database specific, only PostgreSQL is tested here.')
    def test_select_related_query_postgresql(self):
        select_related_query = str(Player.objects.as_of(self.t1).select_related('team').all().query)

        team_table = Team._meta.db_table
        player_table = Player._meta.db_table
        t1_utc_w_tz = str(self.t1)
        t1_utc_wo_tz = t1_utc_w_tz[:-6]
        expected_query = """
            SELECT "{player_table}"."id",
                   "{player_table}"."identity",
                   "{player_table}"."version_start_date",
                   "{player_table}"."version_end_date",
                   "{player_table}"."version_birth_date",
                   "{player_table}"."name",
                   "{player_table}"."team_id",
                   "{team_table}"."id",
                   "{team_table}"."identity",
                   "{team_table}"."version_start_date",
                   "{team_table}"."version_end_date",
                   "{team_table}"."version_birth_date",
                   "{team_table}"."name",
                   "{team_table}"."city_id"
            FROM "{player_table}"
            LEFT OUTER JOIN "{team_table}" ON ("{player_table}"."team_id" = "{team_table}"."identity"
                                                      AND (({team_table}.version_start_date <= {ts}
                                                            AND ({team_table}.version_end_date > {ts}
                                                                 OR {team_table}.version_end_date IS NULL))))
            WHERE
            (
              ("{player_table}"."version_end_date" > {ts}
                    OR "{player_table}"."version_end_date" IS NULL)
              AND "{player_table}"."version_start_date" <= {ts}
            )
        """.format(player_table=player_table, team_table=team_table, ts=t1_utc_w_tz, ts_wo_tz=t1_utc_wo_tz)
        self.assertStringEqualIgnoreWhiteSpaces(expected_query, select_related_query)

    def test_prefetch_related_via_foreignkey(self):
        with self.assertNumQueries(3):
            team = Team.objects.as_of(self.t1).prefetch_related('player_set', 'city').first()
            self.assertIsNotNone(team)

        with self.assertNumQueries(0):
            p1 = team.player_set.all()[0]
            p2 = team.player_set.all()[1]
            self.assertEqual(self.city1, team.city)

        p3 = Player.objects.create(name='pl3.v1', team=self.team1)
        p2 = self.p2.clone()
        p2.name = 'pl2.v2'
        p2.save()
        p1.delete()

        with self.assertNumQueries(3):
            team = Team.objects.current.prefetch_related('player_set', 'city').first()
            self.assertIsNotNone(team)

        with self.assertNumQueries(0):
            self.assertEqual(2, len(team.player_set.all()))
            p1 = team.player_set.all()[0]
            p2 = team.player_set.all()[1]
            self.assertEqual(self.city1, team.city)

        with self.assertNumQueries(3):
            team = Team.objects.prefetch_related('player_set', 'city').first()
            self.assertIsNotNone(team)

        with self.assertNumQueries(0):
            self.assertEqual(4, len(team.player_set.all()))
            px = team.player_set.all()[1]
            self.assertEqual(self.city1, team.city)

    def test_prefetch_related_via_many_to_many(self):
        # award1 - award10
        awards = [Award.objects.create(name='award' + str(i)) for i in range(1, 11)]
        # city0 - city2
        cities = [City.objects.create(name='city-' + str(i)) for i in range(3)]
        teams = []
        # team-0-0 with city0 - team-2-1 with city1
        for i in range(3):
            for j in range(2):
                teams.append(Team.objects.create(
                    name='team-{}-{}'.format(i, j), city=cities[i]))
        players = []
        for i in range(6):
            for j in range(6):
                p = Player.objects.create(
                    name='player-{}-{}'.format(i, j), team=teams[i])
                if j % 2:
                    p.awards.add(*awards[j - 1:j - 9])
                players.append(p)

        t2 = get_utc_now()

        # players is player-0-0 with team-0-0 through player-5-5 with team-2-1
        # players with awards:
        # player-[012345]-1, [012345]-3, [012345]-5,
        # the -1s have awards: 1,2
        # the -3s have awards: 3,4
        # the -5s have awards: 5,6
        with self.assertNumQueries(6):
            players_t2 = list(
                Player.objects.as_of(t2).prefetch_related('team', 'awards').filter(
                    name__startswith='player-').order_by('name')
            )
            players_current = list(
                Player.objects.current.prefetch_related('team', 'awards').filter(
                    name__startswith='player-').order_by('name')
            )

        self.assertSetEqual(set(players_t2), set(players_current))

        award_players = []
        with self.assertNumQueries(0):
            for i in range(len(players_current)):
                t2_p = players_t2[i]
                current_p = players_current[i]
                self.assertEqual(t2_p.team.name, current_p.team.name)
                if i % 2:
                    self.assertGreater(len(t2_p.awards.all()), 0)
                    self.assertSetEqual(set(t2_p.awards.all()), set(current_p.awards.all()))
                    award_players.append(current_p)

        name_list = []
        for p in award_players:
            p.awards.remove(p.awards.all()[0])
            name_list.append(p.name)

        with self.assertNumQueries(2):
            updated_award_players = list(
                Player.objects.current.prefetch_related('awards').filter(
                    name__in=name_list).order_by('name')
            )

        with self.assertNumQueries(0):
            for i in range(len(award_players)):
                old = len(award_players[i].awards.all())
                new = len(updated_award_players[i].awards.all())
                self.assertTrue(new == old - 1)


class PrefetchingHistoricTests(TestCase):
    def setUp(self):
        self.c1 = City.objects.create(name='city.v1')
        self.t1 = Team.objects.create(name='team1.v1', city=self.c1)
        self.t2 = Team.objects.create(name='team2.v1', city=self.c1)
        self.p1 = Player.objects.create(name='pl1.v1', team=self.t1)
        self.p2 = Player.objects.create(name='pl2.v1', team=self.t1)
        self.time1 = get_utc_now()
        sleep(0.001)

    def modify_objects(self):
        # Clone the city (which is referenced by a foreign key in the team object).
        self.c1a = self.c1.clone()
        self.c1a.name = 'city.v2'
        self.c1a.save()
        self.t1a = self.t1.clone()
        self.t1a.name = 'team1.v2'
        self.t1a.save()
        self.p1a = self.p1.clone()
        self.p1a.name = 'pl1.v2'
        self.p1a.save()

    def test_reverse_fk_prefetch_queryset_with_historic_versions(self):
        """
        prefetch_related with Prefetch objects that specify querysets.
        """
        historic_cities_qs = City.objects.as_of(self.time1).filter(name='city.v1').prefetch_related(
            Prefetch(
                'team_set',
                queryset=Team.objects.as_of(self.time1),
                to_attr='prefetched_teams'
            ),
            Prefetch(
                'prefetched_teams__player_set',
                queryset=Player.objects.as_of(self.time1),
                to_attr='prefetched_players'
            )
        )
        with self.assertNumQueries(3):
            historic_cities = list(historic_cities_qs)
            self.assertEquals(1, len(historic_cities))
            historic_city = historic_cities[0]
            self.assertEquals(2, len(historic_city.prefetched_teams))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in historic_city.prefetched_teams})
            team = [t for t in historic_city.prefetched_teams if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.prefetched_players})

        # For the 'current' case:
        current_cities_qs = City.objects.current.filter(name='city.v1').prefetch_related(
            Prefetch(
                'team_set',
                queryset=Team.objects.current,
                to_attr='prefetched_teams'
            ),
            Prefetch(
                'prefetched_teams__player_set',
                queryset=Player.objects.current,
                to_attr='prefetched_players'
            )
        )
        with self.assertNumQueries(3):
            current_cities = list(current_cities_qs)
            self.assertEquals(1, len(current_cities))
            current_city = current_cities[0]
            self.assertEquals(2, len(current_city.prefetched_teams))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in current_city.prefetched_teams})
            team = [t for t in current_city.prefetched_teams if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.prefetched_players})

        self.modify_objects()

        historic_cities_qs = City.objects.as_of(self.time1).filter(name='city.v1').prefetch_related(
            Prefetch(
                'team_set',
                queryset=Team.objects.as_of(self.time1),
                to_attr='prefetched_teams'
            ),
            Prefetch(
                'prefetched_teams__player_set',
                queryset=Player.objects.as_of(self.time1),
                to_attr='prefetched_players'
            )
        )
        with self.assertNumQueries(3):
            historic_cities = list(historic_cities_qs)
            self.assertEquals(1, len(historic_cities))
            historic_city = historic_cities[0]
            self.assertEquals(2, len(historic_city.prefetched_teams))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in historic_city.prefetched_teams})
            team = [t for t in historic_city.prefetched_teams if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.prefetched_players})

        # For the 'current' case:
        current_cities_qs = City.objects.current.filter(name='city.v2').prefetch_related(
            Prefetch(
                'team_set',
                queryset=Team.objects.current,
                to_attr='prefetched_teams'
            ),
            Prefetch(
                'prefetched_teams__player_set',
                queryset=Player.objects.current,
                to_attr='prefetched_players'
            ),
        )
        with self.assertNumQueries(3):
            current_cities = list(current_cities_qs)
            self.assertEquals(1, len(current_cities))
            current_city = current_cities[0]
            self.assertEquals(2, len(current_city.prefetched_teams))
            self.assertSetEqual({'team1.v2', 'team2.v1'}, {t.name for t in current_city.prefetched_teams})
            team = [t for t in current_city.prefetched_teams if t.name == 'team1.v2'][0]
            self.assertSetEqual({'pl1.v2', 'pl2.v1'}, {p.name for p in team.prefetched_players})

    def test_reverse_fk_simple_prefetch_with_historic_versions(self):
        """
        prefetch_related with simple lookup.
        """
        historic_cities_qs = City.objects.as_of(self.time1).filter(name='city.v1').prefetch_related(
            'team_set', 'team_set__player_set')
        with self.assertNumQueries(3):
            historic_cities = list(historic_cities_qs)
            self.assertEquals(1, len(historic_cities))
            historic_city = historic_cities[0]
            self.assertEquals(2, len(historic_city.team_set.all()))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in historic_city.team_set.all()})
            team = [t for t in historic_city.team_set.all() if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.player_set.all()})

        # For the 'current' case:
        current_cities_qs = City.objects.current.filter(name='city.v1').prefetch_related(
            'team_set', 'team_set__player_set')
        with self.assertNumQueries(3):
            current_cities = list(current_cities_qs)
            self.assertEquals(1, len(current_cities))
            current_city = current_cities[0]
            self.assertEquals(2, len(current_city.team_set.all()))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in current_city.team_set.all()})
            team = [t for t in current_city.team_set.all() if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.player_set.all()})

        # Now, we'll clone the city (which is referenced by a foreign key in the team object).
        # The queries above, when repeated, should work the same as before.
        self.modify_objects()

        historic_cities_qs = City.objects.as_of(self.time1).filter(name='city.v1').prefetch_related(
            'team_set', 'team_set__player_set')
        with self.assertNumQueries(3):
            historic_cities = list(historic_cities_qs)
            self.assertEquals(1, len(historic_cities))
            historic_city = historic_cities[0]
            self.assertEquals(2, len(historic_city.team_set.all()))
            self.assertSetEqual({'team1.v1', 'team2.v1'}, {t.name for t in historic_city.team_set.all()})
            team = [t for t in historic_city.team_set.all() if t.name == 'team1.v1'][0]
            self.assertSetEqual({'pl1.v1', 'pl2.v1'}, {p.name for p in team.player_set.all()})

        # For the 'current' case:
        current_cities_qs = City.objects.current.filter(name='city.v2').prefetch_related(
            'team_set', 'team_set__player_set')
        with self.assertNumQueries(3):
            current_cities = list(current_cities_qs)
            self.assertEquals(1, len(current_cities))
            current_city = current_cities[0]
            self.assertEquals(2, len(current_city.team_set.all()))
            self.assertSetEqual({'team1.v2', 'team2.v1'}, {t.name for t in current_city.team_set.all()})
            team = [t for t in current_city.team_set.all() if t.name == 'team1.v2'][0]
            self.assertSetEqual({'pl1.v2', 'pl2.v1'}, {p.name for p in team.player_set.all()})


class IntegrationNonVersionableModelsTests(TestCase):
    def setUp(self):
        self.bordeaux = Wine.objects.create(name="Bordeaux", vintage=2004)
        self.barolo = Wine.objects.create(name="Barolo", vintage=2010)
        self.port = Wine.objects.create(name="Port wine", vintage=2014)

        self.jacques = WineDrinker.objects.create(name='Jacques', glass_content=self.bordeaux)
        self.alfonso = WineDrinker.objects.create(name='Alfonso', glass_content=self.barolo)
        self.jackie = WineDrinker.objects.create(name='Jackie', glass_content=self.port)

        self.red_sailor_hat = WineDrinkerHat.objects.create(shape='Sailor', color='red', wearer=self.jackie)
        self.blue_turban_hat = WineDrinkerHat.objects.create(shape='Turban', color='blue', wearer=self.alfonso)
        self.green_vagabond_hat = WineDrinkerHat.objects.create(shape='Vagabond', color='green', wearer=self.jacques)
        self.pink_breton_hat = WineDrinkerHat.objects.create(shape='Breton', color='pink')

        self.t1 = get_utc_now()
        sleep(0.1)

        self.jacques = self.jacques.clone()
        # Jacques wants to try the italian stuff...
        self.jacques.glass_content = self.barolo
        self.jacques.save()

        self.t2 = get_utc_now()
        sleep(0.1)

        # Jacques gets a bit dizzy and pinches Jackie's hat
        self.red_sailor_hat.wearer = self.jacques
        self.red_sailor_hat.save()

        self.t3 = get_utc_now()
        sleep(0.1)

    def test_accessibility_of_versions_and_non_versionables_via_plain_fk(self):
        # Access coming from a Versionable (reverse access)
        jacques_current = WineDrinker.objects.current.get(name='Jacques')
        jacques_t2 = WineDrinker.objects.as_of(self.t2).get(name='Jacques')
        jacques_t1 = WineDrinker.objects.as_of(self.t1).get(name='Jacques')

        self.assertEqual(jacques_current, jacques_t2)

        self.assertEqual('Barolo', jacques_t2.glass_content.name)
        self.assertEqual('Bordeaux', jacques_t1.glass_content.name)

        # Access coming from plain Models (direct access)
        barolo = Wine.objects.get(name='Barolo')
        all_time_barolo_drinkers = barolo.drinkers.all()
        self.assertEqual({'Alfonso', 'Jacques'}, {winedrinker.name for winedrinker in all_time_barolo_drinkers})

        t1_barolo_drinkers = barolo.drinkers.as_of(self.t1).all()
        self.assertEqual({'Alfonso'}, {winedrinker.name for winedrinker in t1_barolo_drinkers})

        t2_barolo_drinkers = barolo.drinkers.as_of(self.t2).all()
        self.assertEqual({'Alfonso', 'Jacques'}, {winedrinker.name for winedrinker in t2_barolo_drinkers})

        bordeaux = Wine.objects.get(name='Bordeaux')
        t2_bordeaux_drinkers = bordeaux.drinkers.as_of(self.t2).all()
        self.assertEqual(set([]), {winedrinker.name for winedrinker in t2_bordeaux_drinkers})

    def test_accessibility_of_versions_and_non_versionables_via_versioned_fk(self):
        jacques_current = WineDrinker.objects.current.get(name='Jacques')
        jacques_t1 = WineDrinker.objects.as_of(self.t1).get(name='Jacques')

        # Testing direct access
        # We're not able to track changes in objects that are not versionables, pointing objects that are versionables
        # Therefore, it seems like Jacques always had the same combination of hats (even though at t1 and t2, he had
        # one single hat)
        self.assertEqual({'Vagabond', 'Sailor'}, {hat.shape for hat in jacques_current.hats.all()})
        self.assertEqual({hat.shape for hat in jacques_t1.hats.all()},
                         {hat.shape for hat in jacques_current.hats.all()})
        # Fetch jackie-object; at that point, jackie still had her Sailor hat
        jackie_t2 = WineDrinker.objects.as_of(self.t2).get(name='Jackie')
        self.assertEqual(set([]), {hat.shape for hat in jackie_t2.hats.all()})

        # Testing reverse access
        green_vagabond_hat = WineDrinkerHat.objects.get(shape='Vagabond')
        should_be_jacques = green_vagabond_hat.wearer
        self.assertIsNotNone(should_be_jacques)
        self.assertEqual('Jacques', should_be_jacques.name)
        self.assertTrue(should_be_jacques.is_current)

        red_sailor_hat = WineDrinkerHat.objects.get(shape='Sailor')
        should_be_jacques = red_sailor_hat.wearer
        self.assertIsNotNone(should_be_jacques)
        self.assertEqual('Jacques', should_be_jacques.name)
        self.assertTrue(should_be_jacques.is_current)

        # For the records: navigate to a prior version of a versionable object ('Jacques') as follows
        # TODO: Issue #33 on Github aims for a more direct syntax to get to another version of the same object
        should_be_jacques_t1 = should_be_jacques.__class__.objects.as_of(self.t1).get(identity=should_be_jacques.identity)
        self.assertEqual(jacques_t1, should_be_jacques_t1)

    def test_filter_on_fk_versioned_and_nonversioned_join(self):
        # Get non-versioned objects, filtering on a FK-related versioned object
        jacques_hats = WineDrinkerHat.objects.filter(wearer__name='Jacques').distinct()
        self.assertEqual(set(jacques_hats), set([self.green_vagabond_hat, self.red_sailor_hat]))

        # Get all versions of a Versionable by filtering on a FK-related non-versioned object
        person_versions = WineDrinker.objects.filter(hats__shape='Vagabond')
        self.assertIn(self.jacques, person_versions)


class FilterOnForeignKeyRelationTest(TestCase):
    def test_filter_on_fk_relation(self):
        team = Team.objects.create(name='team')
        player = Player.objects.create(name='player', team=team)
        t1 = get_utc_now()
        sleep(0.1)
        l1 = len(Player.objects.as_of(t1).filter(team__name='team'))
        team.clone()
        l2 = len(Player.objects.as_of(t1).filter(team__name='team'))
        self.assertEqual(l1, l2)


class SpecifiedUUIDTest(TestCase):

    @staticmethod
    def uuid4(uuid_value=None):
        if not uuid_value:
            return uuid.uuid4()
        if isinstance(uuid_value, uuid.UUID):
            return uuid_value
        return uuid.UUID(uuid_value)

    def test_create_with_uuid(self):
        p_id = self.uuid4()
        p = Person.objects.create(id=p_id, name="Alice")
        self.assertEqual(str(p_id), str(p.id))
        self.assertEqual(str(p_id), str(p.identity))

        p_id = uuid.uuid5(uuid.NAMESPACE_OID, 'bar')
        with self.assertRaises(ValueError):
            Person.objects.create(id=p_id, name="Alexis")

    def test_create_with_forced_identity(self):

        # This test does some artificial manipulation of versioned objects, do not use it as an example
        # for real-life usage!

        p = Person.objects.create(name="Abela")

        # Postgresql will provide protection here, since util.postgresql.create_current_version_unique_identity_indexes
        # has been invoked in the post migration handler.
        if connection.vendor == 'postgresql' and get_version() >= '1.7':
            with self.assertRaises(IntegrityError):
                with transaction.atomic():
                    ident = self.uuid4(p.identity)
                    Person.objects.create(forced_identity=ident, name="Alexis")

        p.delete()
        sleep(0.1)  # The start date of p2 does not necessarily have to equal the end date of p.

        ident = self.uuid4(p.identity)
        p2 = Person.objects.create(forced_identity=ident, name="Alexis")
        p2.version_birth_date = p.version_birth_date
        p2.save()
        self.assertEqual(p.identity, p2.identity)
        self.assertNotEqual(p2.id, p2.identity)

        # Thanks to that artificial manipulation, p is now the previous version of p2:
        self.assertEqual(p.name, Person.objects.previous_version(p2).name)


class VersionRestoreTest(TestCase):

    def setup_common(self):
        sf = City.objects.create(name="San Francisco")
        forty_niners = Team.objects.create(name='49ers', city=sf)
        player1 = Player.objects.create(name="Montana", team=forty_niners)
        best_quarterback = Award.objects.create(name="Best Quarterback")
        best_attitude = Award.objects.create(name="Best Attitude")
        player1.awards.add(best_quarterback, best_attitude)

        self.player1 = player1
        self.awards = {
            'best_quarterback': best_quarterback,
            'best_attitude': best_attitude,
        }
        self.forty_niners = forty_niners

    def test_restore_latest_version(self):
        self.setup_common()
        self.player1.delete()
        deleted_at = self.player1.version_end_date
        player1_pk = self.player1.pk

        restored = self.player1.restore()
        self.assertEqual(player1_pk, restored.pk)
        self.assertIsNone(restored.version_end_date)
        self.assertEqual(2, Player.objects.filter(name=restored.name).count())

        # There should be no relationships restored:
        self.assertIsNone(restored.team_id)
        self.assertListEqual([], list(restored.awards.all()))

        # The relationships are still present on the previous version.
        previous = Player.objects.previous_version(restored)
        self.assertEqual(deleted_at, previous.version_end_date)
        self.assertSetEqual(set(previous.awards.all()), set(self.awards.values()))
        self.assertEqual(self.forty_niners, previous.team)

    def test_restore_previous_version(self):
        self.setup_common()
        p1 = self.player1.clone()
        p1.name = 'Joe'
        p1.save()
        player1_pk = self.player1.pk

        self.player1.restore()

        with self.assertRaises(ObjectDoesNotExist):
            Player.objects.current.get(name='Joe')

        restored = Player.objects.current.get(name='Montana')
        self.assertEqual(player1_pk, restored.pk)
        self.assertIsNone(restored.version_end_date)
        self.assertEqual(2, Player.objects.filter(name=restored.name).count())

        # There should be no relationships restored:
        self.assertIsNone(restored.team_id)
        self.assertListEqual([], list(restored.awards.all()))

        # The relationships are also present on the previous version.
        previous = Player.objects.previous_version(restored)
        self.assertSetEqual(set(previous.awards.all()), set(self.awards.values()))
        self.assertEqual(self.forty_niners, previous.team)

        # There should be no overlap of version periods.
        self.assertEquals(previous.version_end_date, restored.version_start_date)

    def test_restore_with_required_foreignkey(self):
        team = Team.objects.create(name="Flying Pigs")
        mascot_v1 = Mascot.objects.create(name="Curly", team=team)
        mascot_v1.delete()

        # Restoring without supplying a value for the required foreign key will fail.
        with self.assertRaises(ForeignKeyRequiresValueError):
            mascot_v1.restore()

        self.assertEqual(1, Mascot.objects.filter(name=mascot_v1.name).count())

        mascot2_v1 = Mascot.objects.create(name="Big Ham", team=team)
        mascot2_v1.clone()
        with self.assertRaises(ForeignKeyRequiresValueError):
            mascot2_v1.restore()

        self.assertEqual(2, Mascot.objects.filter(name=mascot2_v1.name).count())
        self.assertEqual(1, Mascot.objects.current.filter(name=mascot2_v1.name).count())

        # If a value (object or pk) is supplied, the restore will succeed.
        team2 = Team.objects.create(name="Submarine Sandwiches")
        restored = mascot2_v1.restore(team=team2)
        self.assertEqual(3, Mascot.objects.filter(name=mascot2_v1.name).count())
        self.assertEqual(team2, restored.team)

        restored.delete()
        rerestored = mascot2_v1.restore(team_id=team.pk)
        self.assertEqual(4, Mascot.objects.filter(name=mascot2_v1.name).count())
        self.assertEqual(team, rerestored.team)

    def test_over_time(self):
        team1 = Team.objects.create(name='team1.v1')
        team2 = Team.objects.create(name='team2.v1')
        p1 = Player.objects.create(name='p1.v1', team=team1)
        p2 = Player.objects.create(name='p2.v1', team=team1)
        a1 = Award.objects.create(name='a1.v1')
        t1 = get_utc_now()
        sleep(0.001)

        p1 = p1.clone()
        p1.name = 'p1.v2'
        p1.save()
        t2 = get_utc_now()
        sleep(0.001)

        p1.delete()
        a1.players.add(p2)
        t3 = get_utc_now()
        sleep(0.001)

        a1.players = []
        t4 = get_utc_now()
        sleep(0.001)

        p1 = Player.objects.get(name='p1.v2').restore(team=team2)

        # p1 did exist at t2, but not at t3.
        self.assertIsNotNone(Player.objects.as_of(t2).filter(name='p1.v2').first())
        self.assertIsNone(Player.objects.as_of(t3).filter(name='p1.v2').first())

        # p1 re-appeared later with team2, though.
        self.assertEqual(team2, Player.objects.current.get(name='p1.v2').team)

        # many-to-many relations
        self.assertEqual([], list(Award.objects.as_of(t2).get(name='a1.v1').players.all()))
        self.assertEqual('p2.v1', Award.objects.as_of(t3).get(name='a1.v1').players.first().name)
        self.assertEqual([], list(Award.objects.current.get(name='a1.v1').players.all()))

        # Expected version counts:
        self.assertEqual(1, Team.objects.filter(name='team1.v1').count())
        self.assertEqual(1, Team.objects.filter(name='team2.v1').count())
        self.assertEqual(3, Player.objects.filter(identity=p1.identity).count())
        self.assertEqual(1, Player.objects.filter(name='p2.v1').count())
        m2m_manager = Award._meta.get_field('players').rel.through.objects
        self.assertEqual(1, m2m_manager.all().count())

    def test_restore_two_in_memory_objects(self):
        # Tests issue #90
        # Restoring two in-memory objects with the same identity, which, according
        # to their in-memory state, are both the current version, should not
        # result in having more than one current object with the same identity
        # present in the database.
        a = City(name="A")
        a.save()
        b = a.clone()
        b.name = "B"
        b.save()
        a = City.objects.get(name="A")
        a.restore()
        b = City.objects.get(name="B")
        b2 = b.restore()
        current_objects = City.objects.filter(version_end_date=None, identity=b.identity)
        self.assertEqual(1, len(current_objects))
        self.assertEqual(b2.pk, current_objects[0].pk)


class DetachTest(TestCase):

    def test_simple_detach(self):
        c1 = City.objects.create(name="Atlantis").clone()
        c1_identity = c1.identity
        c2 = c1.detach()
        c2.save()
        c1 = City.objects.current.get(pk=c1_identity)
        self.assertEqual(c1.name, c2.name)
        self.assertEqual(c2.id, c2.identity)
        self.assertNotEqual(c1.id, c2.id)
        self.assertNotEqual(c1.identity, c2.identity)
        self.assertEqual(2, City.objects.filter(identity=c1_identity).count())
        self.assertEqual(1, City.objects.filter(identity=c2.identity).count())

    def test_detach_with_relations(self):
        """
        ManyToMany and reverse ForeignKey relationships are not kept. ForeignKey relationships are kept.
        """
        t = Team.objects.create(name='Raining Rats')
        t_pk = t.pk
        m = Mascot.objects.create(name="Drippy", team=t)
        p = Player.objects.create(name="Robby", team=t)
        p_pk = p.pk
        a = Award.objects.create(name="Most slippery")
        a.players.add(p)

        p2 = p.detach()
        p2.save()
        p = Player.objects.current.get(pk=p_pk)
        self.assertEqual(t, p.team)
        self.assertEqual(t, p2.team)
        self.assertListEqual([a], list(p.awards.all()))
        self.assertListEqual([], list(p2.awards.all()))

        t2 = t.detach()
        t2.save()
        t = Team.objects.current.get(pk=t_pk)
        self.assertEqual({p, p2}, set(t.player_set.all()))
        self.assertEqual([], list(t2.player_set.all()))


class DeferredFieldsTest(TestCase):

    def setUp(self):
        self.c1 = City.objects.create(name="Porto")
        self.team1 = Team.objects.create(name="Tigers", city=self.c1)

    def test_simple_defer(self):
        limited = City.objects.current.only('name').get(pk=self.c1.pk)
        deferred_fields = set(Versionable.VERSIONABLE_FIELDS)
        deferred_fields.remove('id')
        self.assertSetEqual(deferred_fields, set(limited.get_deferred_fields()))
        for field_name in deferred_fields:
            self.assertNotIn(field_name, limited.__dict__ )

        deferred_fields = ['version_start_date', 'version_end_date']
        deferred = City.objects.current.defer(*deferred_fields).get(pk=self.c1.pk)
        self.assertSetEqual(set(deferred_fields), set(deferred.get_deferred_fields()))
        for field_name in deferred_fields:
            self.assertNotIn(field_name, deferred.__dict__ )

        # Accessing deferred fields triggers queries:
        with self.assertNumQueries(2):
            self.assertEquals(self.c1.version_start_date, deferred.version_start_date)
            self.assertEquals(self.c1.version_end_date, deferred.version_end_date)
        # If already fetched, no query is made:
        with self.assertNumQueries(0):
            self.assertEquals(self.c1.version_start_date, deferred.version_start_date)

    def test_deferred_foreign_key_field(self):

        team_full = Team.objects.current.get(pk=self.team1.pk)
        self.assertIn('city_id', team_full.__dict__ )
        team_light = Team.objects.current.only('name').get(pk=self.team1.pk)
        self.assertNotIn('city_id', team_light.__dict__ )
        with self.assertNumQueries(2):
            # One query to get city_id, and one query to get the related City object.
            self.assertEquals(self.c1.name, team_light.city.name)

    def test_reverse_foreign_key_access(self):
        city = City.objects.current.only('name').get(identity=self.c1.identity)
        with self.assertNumQueries(2):
            # One query to get the identity, one query to get the related objects.
            self.assertSetEqual({self.team1.pk}, {o.pk for o in city.team_set.all()})

    def test_many_to_many_access(self):
        player1 = Player.objects.create(name='Raaaaaow', team=self.team1)
        player2 = Player.objects.create(name='Pssshh', team=self.team1)
        award1 = Award.objects.create(name='Fastest paws')
        award1.players.add(player2)
        award2 = Award.objects.create(name='Frighteningly fast')
        award2.players.add(player1, player2)

        player2_light = Player.objects.current.only('name').get(identity=player2.identity)
        with self.assertNumQueries(1):
            # Many-to-many fields use the id field, which is always fetched, so only one query
            # should be made to get the related objects.
            self.assertSetEqual({award1.pk, award2.pk}, {o.pk for o in player2_light.awards.all()})

        # And from the other direction:
        award2_light = Award.objects.current.only('name').get(identity=award2.identity)
        with self.assertNumQueries(1):
            self.assertSetEqual({player1.pk, player2.pk}, {o.pk for o in award2_light.players.all()})

    def test_clone_of_deferred_object(self):
        c1_v1_partial = City.objects.current.defer('name').get(pk=self.c1.pk)
        self.assertRaisesMessage(
            ValueError,
            'Can not clone a model instance that has deferred fields',
            c1_v1_partial.clone
        )

    def test_restore_of_deferred_object(self):
        t1 = get_utc_now()
        sleep(0.001)
        c1_v2 = self.c1.clone()
        c1_v1 = City.objects.as_of(t1).defer('name').get(identity=c1_v2.identity)
        self.assertRaisesMessage(
            ValueError,
            'Can not restore a model instance that has deferred fields',
            c1_v1.restore
        )
