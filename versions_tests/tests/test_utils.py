from unittest import skipUnless
from django.db import connection
from django.test import TestCase, TransactionTestCase
from django.db import IntegrityError
from versions_tests.models import ChainStore, Color
from versions.util.postgresql import get_uuid_like_indexes_on_table


@skipUnless(connection.vendor == 'postgresql', "Postgresql-specific test")
class PostgresqlVersionUniqueTests(TransactionTestCase):
    def setUp(self):
        self.red = Color.objects.create(name='red')
        self.green = Color.objects.create(name='green')
        self.black = Color.objects.create(name='black')
        self.yellow = Color.objects.create(name='yellow')

        # - only one store with the same name and subchain_id can exist in a single city
        # - no two stores can share the same door_frame_color and door_color
        store = {
            'subchain_id': 1,
            'city': 'Santa Barbara',
            'name': 'Barbara style',
            'opening_hours': '9-9 everyday',
            'door_frame_color': self.red,
            'door_color': self.black,
        }

        self.sb1 = ChainStore.objects.create(**store)

    def test_version_unique(self):

        # It should not be possible to create another store with the same name, city, and subchain_id
        with self.assertRaises(IntegrityError):
            sb2 = ChainStore.objects.create(
                subchain_id = self.sb1.subchain_id,
                city = self.sb1.city,
                name = self.sb1.name,
                door_frame_color = self.sb1.door_frame_color,
                door_color = self.green
            )

        # It should not be possible to create another store with the same door and door_frame color
        with self.assertRaises(IntegrityError):
            sb3 = ChainStore.objects.create(
                subchain_id = self.sb1.subchain_id,
                city = self.sb1.city,
                name = "Bearded Bob's style",
                door_frame_color = self.sb1.door_frame_color,
                door_color = self.sb1.door_color
            )

        # It should be possible to create objects as long as they follow the unique constraints, though:
        sb4 = ChainStore.objects.create(
            subchain_id = self.sb1.subchain_id,
            city = self.sb1.city,
            name = "Bearded Bob's style",
            door_frame_color = self.sb1.door_frame_color,
            door_color = self.green
        )

        sb5 = ChainStore.objects.create(
            subchain_id = sb4.subchain_id + 1,
            city = sb4.city,
            name = sb4.name,
            door_frame_color = sb4.door_frame_color,
            door_color = self.yellow
        )

        # If a version is soft-deleted, it should be possible to create a new object with the
        # value of that old version
        sb4.delete()
        sb6 = ChainStore.objects.create(
            subchain_id = sb4.subchain_id,
            city = sb4.city,
            name = sb4.name,
            door_frame_color = sb4.door_frame_color,
            door_color = sb4.door_color
        )

    def test_identity_unique(self):
        c = Color.objects.create(name='sky blue')
        c.identity = self.green.identity

        # It should not be possible to have two "current" objects with the same identity:
        with self.assertRaises(IntegrityError):
            c.save()


@skipUnless(connection.vendor == 'postgresql', "Postgresql-specific test")
class PostgresqlUuidLikeIndexesTest(TestCase):
    def test_no_like_indexes_on_uuid_columns(self):
        # Django creates like indexes on char columns.  In Django 1.7.x and below, there is no
        # support for native uuid columns, so CleanerVersion uses a CharField to store the
        # uuid values.  For postgresql, Django creates special indexes for char fields so that
        # like searches (e.g. WHERE foo like '%bar') are fast.
        # Those indexes are not going to be used in our case, and extra indexes will slow down
        # updates and inserts.  So, they should have been removed by the post_migrate handler in
        # versions_tests.apps.VersionsTestsConfig.ready.
        self.assertEqual(0, len(get_uuid_like_indexes_on_table(ChainStore)))
