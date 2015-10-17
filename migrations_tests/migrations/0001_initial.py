# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import versions.models


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='MyMigratingModelA',
            fields=[
                ('id', models.CharField(max_length=36, serialize=False, primary_key=True)),
                ('identity', models.CharField(max_length=36)),
                ('version_start_date', models.DateTimeField()),
                ('version_end_date', models.DateTimeField(default=None, null=True, blank=True)),
                ('version_birth_date', models.DateTimeField()),
                ('name', models.CharField(max_length=10)),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='MyMigratingModelB',
            fields=[
                ('id', models.CharField(max_length=36, serialize=False, primary_key=True)),
                ('identity', models.CharField(max_length=36)),
                ('version_start_date', models.DateTimeField()),
                ('version_end_date', models.DateTimeField(default=None, null=True, blank=True)),
                ('version_birth_date', models.DateTimeField()),
                ('identifier', models.CharField(max_length=10)),
                ('a_models', versions.models.VersionedManyToManyField(to='migrations_tests.MyMigratingModelA')),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.AlterUniqueTogether(
            name='mymigratingmodelb',
            unique_together=set([('id', 'identity')]),
        ),
        migrations.AlterUniqueTogether(
            name='mymigratingmodela',
            unique_together=set([('id', 'identity')]),
        ),
    ]
