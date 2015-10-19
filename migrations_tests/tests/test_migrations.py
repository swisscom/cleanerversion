import os
import re

from django.core.management import call_command
from django_migration_testcase import MigrationTest


class M2MRelatedNameMigration(MigrationTest):
    app = 'migrations_tests'
    before = [(app, '0001_initial')]
    after = []
    after_migration_file = None

    def call_makemigrations(self, app_name=None):
        if not app_name:
            app_name = self.app
        try:
            mig_filename_holder = "/tmp/migrations_filename.txt"
            with open(mig_filename_holder, 'w') as f:
                call_command('makemigrations', app_name,
                             verbosity=1, no_initial_data=True, stdout=f)
            fh = open(mig_filename_holder, 'r')
            out = fh.read().split('\n')[1]
            fh.close()
            match = re.match(r'.*((\d{4}_auto\w+)\.py)', out)
            out_mig = match.group(2)
            self.after_migration_file = "./%s/migrations/%s" % (app_name, match.group(1))
        finally:
            fh.close()
            os.remove(mig_filename_holder)
        with open(self.after_migration_file, 'r') as fh:
            self.after_migration_content = fh.read()
        self.after = [(app_name, out_mig)]

    def tearDown(self):
        try:
            os.remove(self.after_migration_file)
        except Exception as e:
            print(e)
        super(M2MRelatedNameMigration, self).tearDown()

    def test_migration(self):
        MyMigratingModelA = self.get_model_before('migrations_tests.MyMigratingModelA')
        MyMigratingModelB = self.get_model_before('migrations_tests.MyMigratingModelB')
        PlainModelB = self.get_model_before('migrations_tests.MyPlainModelB')

        # self.assertIn('mymigratingmodelb_set', MyMigratingModelA.__dict__)

        # Make migration file
        self.call_makemigrations()
        print("----------\n%s\n----------\n" % self.after_migration_content)

        # self.run_migration runs the newly created migration and thus provokes an error:
        # "AttributeError: 'VersionedManyToManyField' object has no attribute '_m2m_reverse_name_cache'"
        self.assertRaises(AttributeError, self.run_migration)

        # MyMigratingModelA = self.get_model_after('migrations_tests.MyMigratingModelA')
        # MyMigratingModelB = self.get_model_after('migrations_tests.MyMigratingModelB')
        # self.assertIn('b_models', MyMigratingModelA.__dict__)
