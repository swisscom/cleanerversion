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
from django.core.exceptions import SuspiciousOperation, ObjectDoesNotExist, MultipleObjectsReturned, ValidationError
from django.db.models import Q, Count, Sum
from django.db.models.fields import CharField
from django.test import TestCase, TransactionTestCase
from django.utils.timezone import utc
from django.utils import six
from unittest import skip

from versions.models import Versionable, get_utc_now
from versions_tests.models import Professor, Classroom, Student, Pupil, Teacher, Observer, B, Subject, Team, Player, \
    Directory, C1, C2, C3


def get_relation_table(model_class, fieldname):
    field_object, model, direct, m2m = model_class._meta.get_field_by_name(fieldname)
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

        self.assertRaises(Exception, previous.delete)


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

    def test_getting_two_next_versions(self):
        """
        This should never happen, unless something went wrong REALLY bad;
        For setting up this test case, we have to go under the hood of CleanerVersion and modify some timestamps.
        Only like this it is possible to have two versions that follow one first version.
        """
        v1 = B.objects.as_of(self.t1).first()
        v2 = B.objects.as_of(self.t2).first()
        v3 = B.objects.as_of(self.t3).first()

        v3.version_start_date = v2.version_start_date
        v3.save()

        self.assertRaises(MultipleObjectsReturned, lambda: B.objects.next_version(v1))

    def test_getting_nonexistent_previous_version(self):
        """
        Raise an error when trying to look up the previous version of a version floating in emptyness.
        This test case implies BAD modification under the hood of CleanerVersion, interrupting the continuity of an
        object's versions through time.
        """
        v1 = B.objects.as_of(self.t1).first()
        v2 = B.objects.as_of(self.t2).first()
        v3 = B.objects.as_of(self.t3).first()

        v2.version_end_date = v1.version_end_date
        v2.save()

        self.assertRaises(ObjectDoesNotExist, lambda: B.objects.previous_version(v3))

    def test_getting_two_previous_versions(self):
        """
        This should never happen, unless something went wrong REALLY bad;
        For setting up this test case, we have to go under the hood of CleanerVersion and modify some timestamps.
        Only like this it is possible to have two versions that precede one last version.
        """
        v1 = B.objects.as_of(self.t1).first()
        v2 = B.objects.as_of(self.t2).first()
        v3 = B.objects.as_of(self.t3).first()

        v1.version_end_date = v2.version_end_date
        v1.save()

        self.assertRaises(MultipleObjectsReturned, lambda: B.objects.previous_version(v3))


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
        self.assertEqual(2, team_at_t1.player_set.count())

        # ... and at time t2
        team_at_t2 = Team.objects.as_of(t2).first()
        self.assertEqual(2, team_at_t2.player_set.count())

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

        team.player_set.remove(p2)

        p2 = Player.objects.current.get(name='p2.v1')
        p2.name = 'p2.v2'
        p2.save()

        self.t2 = get_utc_now()
        sleep(0.1)

        team.player_set.remove(p1)

        p1 = Player.objects.current.get(name='p1.v1')
        p1.name = 'p1.v2'
        p1.save()

        self.t3 = get_utc_now()
        sleep(0.1)

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
            Player.objects.as_of(self.t1).filter(Q(name__startswith='p1') | Q(name__startswith='p2')).values_list('name',
                                                                                                             flat=True))
        self.assertEqual(2, len(t1_players))
        self.assertListEqual(sorted(t1_players), sorted(['p1.v1', 'p2.v1']))


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
        self.assertNotEquals(first_professor.identity, last_professor.identity)
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
            Professor.objects.current.annotate(num_students=Count('students')).aggregate(sum=Sum('num_students'))['sum']
        )
        self.assertTupleEqual((1,1),
            (Professor.objects.current.annotate(num_students=Count('students')).get(name='Mr. Biggs').num_students,
             Professor.objects.current.get(name='Mr. Biggs').students.count())
        )

        self.assertTupleEqual((2,2),
            (Professor.objects.as_of(self.t1).annotate(num_students=Count('students')).get(name='Mr. Biggs').num_students,
             Professor.objects.as_of(self.t1).get(name='Mr. Biggs').students.count())
        )

        # Results should include records for which the annotation returns a 0 count, too.
        # This requires that the generated LEFT OUTER JOIN condition includes a clause
        # to restrict the records according to the desired as_of time.
        self.assertEqual(3, len(Student.objects.current.annotate(num_teachers=Count('professors')).all()))


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
        sleep(0.1)

        c3a = C3(name='c3a.v1')
        c3a.save()
        c2.c3s.add(c3a)

        sleep(0.1)
        self.t2 = get_utc_now()

        c1 = c1.clone()
        c1.name = 'c1.v2'
        c1.save()

        c3a.delete()

        sleep(0.1)
        self.t3 = get_utc_now()

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

    @skip("Expected SQL query still needs to be defined")
    def test_query_created_by_filtering_one_jump_with_version_at_t1(self):
        """
        Test filtering m2m relations with 2 models with propagation of querytime
        information across all tables
        """
        should_be_c1_queryset = C1.objects.as_of(self.t1) \
            .filter(c2s__name__startswith='c2')
        should_be_c1_query = should_be_c1_queryset.query
        print should_be_c1_query
        self.assertTrue(False)

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

    @skip("Expected SQL query still needs to be defined")
    def test_query_created_by_filtering_two_jumps_with_version_at_t1(self):
        """
        Investigate correctness of the resulting SQL query
        """
        should_be_c1_queryset = C1.objects.as_of(self.t1) \
            .filter(c2s__c3s__name__startswith='c3')
        should_be_c1_query = should_be_c1_queryset.query
        print should_be_c1_query
        self.assertTrue(False)

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
        with self.assertNumQueries(1) as counter:
            # Should be None, since object 'c3a' does not exist anymore at t3
            should_be_none = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3a').first()
            self.assertIsNone(should_be_none)

            should_be_c1 = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3a').first()
            self.assertIsNotNone(should_be_c1)

            count = C1.objects.as_of(self.t3) \
                .filter(c2s__c3s__name__startswith='c3').all().count()
            self.assertEqual(2, count)

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
