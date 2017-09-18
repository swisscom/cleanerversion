from __future__ import absolute_import

from versions.models import Versionable

from django import VERSION
from django.db import connection, connections

if VERSION >= (1, 7):
    from django.apps import apps
else:
    from django.db.models import get_app, get_models


def database_connection(dbname=None):
    if dbname:
        return connections[dbname]
    else:
        return connection


def get_app_models(app_name, include_auto_created=False):
    if VERSION >= (1, 7):
        return apps.get_app_config(app_name).get_models(
            include_auto_created=include_auto_created)
    else:
        return get_models(get_app(app_name),
                          include_auto_created=include_auto_created)


def versionable_models(app_name, include_auto_created=False):
    return [m for m in get_app_models(app_name, include_auto_created) if
            issubclass(m, Versionable)]
