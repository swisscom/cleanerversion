import django
from django.core.management import call_command
from django.test import TestCase

APP_NAME = 'versions_tests'

if django.VERSION[:2] >= (1, 7):
    class TestMigrations(TestCase):
        def test_makemigrations_command(self):
            call_command('makemigrations', APP_NAME, dry_run=True, verbosity=0)
