from django.db.models.fields import CharField
from versions.models import Versionable, VersionedManyToManyField


############################################
# MigrationTest models
class MyMigratingModelA(Versionable):
    name = CharField(max_length=10)

class MyMigratingModelB(Versionable):
    identifier = CharField(max_length=10)
    a_models = VersionedManyToManyField('MyMigratingModelA', related_name='b_models')
