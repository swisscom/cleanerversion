from django.db.models import Model
from django.db.models.fields import CharField
from django.db.models.fields.related import ManyToManyField
from versions.models import Versionable, VersionedManyToManyField


############################################
# MigrationTest models
class MyMigratingModelA(Versionable):
    name = CharField(max_length=10)


class MyMigratingModelB(Versionable):
    identifier = CharField(max_length=10)
    a_models = VersionedManyToManyField('MyMigratingModelA', related_name='b_models')

    def __init__(self):
        super(MyMigratingModelB, self).__init__()


class MyPlainModelA(Model):
    property_one = CharField(max_length=10)


class MyPlainModelB(Model):
    property_one = CharField(max_length=10)
    a_models = ManyToManyField('MyPlainModelA', related_name="plain_b_models")