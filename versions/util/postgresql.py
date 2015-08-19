from __future__ import absolute_import
from django.db import connection as default_connection
from versions.models import VersionedForeignKey
from .helper import database_connection, versionable_models


def index_exists(cursor, index_name):
    """
    Checks if an index with the given name exists in the database

    :param cursor: database connection cursor
    :param index_name: string
    :return: boolean
    """
    cursor.execute("SELECT COUNT(1) FROM pg_indexes WHERE indexname = %s", [index_name])
    return cursor.fetchone()[0] > 0


def remove_uuid_id_like_indexes(app_name, database=None):
    """
    Remove all of varchar_pattern_ops indexes that django created for uuid columns.
    A search is never done with a filter of the style (uuid__like='1ae3c%'), so
    all such indexes can be removed from Versionable models.
    This will only try to remove indexes if they exist in the database, so it
    should be safe to run in a post_migrate signal handler.  Running it several
    times should leave the database in the same state as running it once.
    :param str app_name: application name whose Versionable models will be acted on.
    :param str database: database alias to use.  If None, use default connection.
    :return: number of indexes removed
    :rtype: int
    """

    removed_indexes = 0
    with database_connection(database).cursor() as cursor:
        for model in versionable_models(app_name, include_auto_created=True):
            indexes = select_uuid_like_indexes_on_table(model, cursor)
            if indexes:
                index_list = ','.join(['"%s"' % r[0] for r in indexes])
                cursor.execute("DROP INDEX %s" % index_list)
                removed_indexes += len(indexes)

    return removed_indexes


def get_uuid_like_indexes_on_table(model):
    """
    Gets a list of database index names for the given model for the uuid-containing
    fields that have had a like-index created on them.

    :param model: Django model
    :return: list of database rows; the first field of each row is an index name
    """
    with default_connection.cursor() as c:
        indexes = select_uuid_like_indexes_on_table(model, c)
    return indexes


def select_uuid_like_indexes_on_table(model, cursor):
    """
    Gets a list of database index names for the given model for the uuid-containing
    fields that have had a like-index created on them.

    :param model: Django model
    :param cursor: database connection cursor
    :return: list of database rows; the first field of each row is an index name
    """

    # VersionedForeignKey fields as well as the id fields have these useless like indexes
    field_names = ["'%s'" % f.column for f in model._meta.fields if isinstance(f, VersionedForeignKey)]
    field_names.append("'id'")
    sql = """
                select i.relname as index_name
                from pg_class t,
                     pg_class i,
                     pg_index ix,
                     pg_attribute a
                where t.oid = ix.indrelid
                  and i.oid = ix.indexrelid
                  and a.attrelid = t.oid
                  and a.attnum = ANY(ix.indkey)
                  and t.relkind = 'r'
                  and t.relname = '{0}'
                  and a.attname in ({1})
                  and i.relname like '%_like'
            """.format(model._meta.db_table, ','.join(field_names))
    cursor.execute(sql)
    return cursor.fetchall()


def create_current_version_unique_indexes(app_name, database=None):
    """
    Add unique indexes for models which have a VERSION_UNIQUE attribute.
    These must be defined as partially unique indexes, which django
    does not support.
    The unique indexes are defined so that no two *current* versions can have
    the same value.
    This will only try to create indexes if they do not exist in the database, so it
    should be safe to run in a post_migrate signal handler.  Running it several
    times should leave the database in the same state as running it once.
    :param str app_name: application name whose Versionable models will be acted on.
    :param str database: database alias to use.  If None, use default connection.
    :return: number of partial unique indexes created
    :rtype: int
    """

    indexes_created = 0
    connection = database_connection(database)
    with connection.cursor() as cursor:
        for model in versionable_models(app_name):
            unique_field_groups = getattr(model, 'VERSION_UNIQUE', None)
            if not unique_field_groups:
                continue

            table_name = model._meta.db_table
            for group in unique_field_groups:
                col_prefixes = []
                columns = []
                for field in group:
                    column = model._meta.get_field(field).column
                    col_prefixes.append(column[0:3])
                    columns.append(column)
                index_name = '%s_%s_%s_v_uniq' % (app_name, table_name, '_'.join(col_prefixes))
                if not index_exists(cursor, index_name):
                    cursor.execute("CREATE UNIQUE INDEX %s ON %s(%s) WHERE version_end_date IS NULL"
                                   % (index_name, table_name, ','.join(columns)))
                    indexes_created += 1

    return indexes_created

def create_current_version_unique_identity_indexes(app_name, database=None):
    """
    Add partial unique indexes for the the identity column of versionable models.

    This enforces that no two *current* versions can have the same identity.

    This will only try to create indexes if they do not exist in the database, so it
    should be safe to run in a post_migrate signal handler.  Running it several
    times should leave the database in the same state as running it once.
    :param str app_name: application name whose Versionable models will be acted on.
    :param str database: database alias to use.  If None, use default connection.
    :return: number of partial unique indexes created
    :rtype: int
    """

    indexes_created = 0
    connection = database_connection(database)
    with connection.cursor() as cursor:
        for model in versionable_models(app_name):
            if getattr(model._meta, 'managed', True):
                table_name = model._meta.db_table
                index_name = '%s_%s_identity_v_uniq' % (app_name, table_name)
                if not index_exists(cursor, index_name):
                    cursor.execute("CREATE UNIQUE INDEX %s ON %s(%s) WHERE version_end_date IS NULL"
                                   % (index_name, table_name, 'identity'))
                    indexes_created += 1

    return indexes_created
