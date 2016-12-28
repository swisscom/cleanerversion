from unittest.case import skip

import django
from django.core.management import call_command
from django.test import TestCase

APP_NAME = 'versions_tests'

class TestMigrations(TestCase):
    @skip("Migrations for M2M intermediary models not properly handled yet")
    def test_makemigrations_command(self):
        call_command('makemigrations', APP_NAME, dry_run=True, verbosity=0)
