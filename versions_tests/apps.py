from django.apps import AppConfig
from django.db.models.signals import post_migrate
from django.db import connection

def index_adjustments(sender, using=None, **kwargs):
    """
    Remove -like indexes (varchar_pattern_ops) on UUID fields and create
    version-unique indexes for models that have a VERSION_UNIQUE attribute.
    :param AppConfig sender:
    :param str sender: database alias
    :param kwargs:
    """
    from versions.util.postgresql import (
        remove_uuid_id_like_indexes,
        create_current_version_unique_indexes,
        create_current_version_unique_identity_indexes
    )
    remove_uuid_id_like_indexes(sender.name, using)
    create_current_version_unique_indexes(sender.name, using)
    create_current_version_unique_identity_indexes(sender.name, using)

class VersionsTestsConfig(AppConfig):
    name = 'versions_tests'
    verbose_name = "Versions Tests default application configuration"

    def ready(self):
        """
        For postgresql only, remove like indexes for uuid columns and
        create version-unique indexes.

        This will only be run in django >= 1.7.

        :return: None
        """
        if connection.vendor == 'postgresql':
            post_migrate.connect(index_adjustments, sender=self)