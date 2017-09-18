from django.core.management import call_command
from django.test import TestCase

APP_NAME = 'versions_tests'


class TestMigrations(TestCase):
    def test_makemigrations_command(self):
        call_command('makemigrations', APP_NAME, dry_run=True, verbosity=0)
