# Copyright 2014 Swisscom, Sophia Engineering
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import datetime
import uuid
from collections import namedtuple

from django.db.models.sql.datastructures import Join
from django.apps.registry import apps
from django.core.exceptions import SuspiciousOperation, ObjectDoesNotExist
from django.db import models, router, transaction
from django.db.models.base import Model
from django.db.models import Q
from django.db.models.constants import LOOKUP_SEP
from django.db.models.fields import FieldDoesNotExist
from django.db.models.fields.related import (ForeignKey, ReverseSingleRelatedObjectDescriptor,
                                             ReverseManyRelatedObjectsDescriptor, ManyToManyField,
                                             ManyRelatedObjectsDescriptor, create_many_related_manager,
                                             ForeignRelatedObjectsDescriptor, RECURSIVE_RELATIONSHIP_CONSTANT)
from django.db.models.query import QuerySet, ValuesListQuerySet, ValuesQuerySet
from django.db.models.signals import post_init
from django.db.models.sql import Query
from django.db.models.sql.where import ExtraWhere, WhereNode
from django.utils.functional import cached_property
from django.utils.timezone import utc
from django.utils import six

from versions.settings import get_versioned_delete_collector_class
from versions.settings import settings as versions_settings
from versions.exceptions import DeletionOfNonCurrentVersionError


def get_utc_now():
    return datetime.datetime.utcnow().replace(tzinfo=utc)


def validate_uuid(uuid_obj):
    """
    Check that the UUID object is in fact a valid version 4 uuid.
    """
    return isinstance(uuid_obj, uuid.UUID) and uuid_obj.version == 4

QueryTime = namedtuple('QueryTime', 'time active')


class ForeignKeyRequiresValueError(ValueError):
    pass


class VersionManager(models.Manager):
    """
    This is the Manager-class for any class that inherits from Versionable
    """
    use_for_related_fields = True

    def get_queryset(self):
        """
        Returns a VersionedQuerySet capable of handling version time restrictions.

        :return: VersionedQuerySet
        """
        qs = VersionedQuerySet(self.model, using=self._db)
        if hasattr(self, 'instance') and hasattr(self.instance, '_querytime'):
            qs.querytime = self.instance._querytime
        return qs

    def as_of(self, time=None):
        """
        Filters Versionables at a given time
        :param time: The timestamp (including timezone info) at which Versionables shall be retrieved
        :return: A QuerySet containing the base for a timestamped query.
        """
        return self.get_queryset().as_of(time)

    def next_version(self, object, relations_as_of='end'):
        """
        Return the next version of the given object.

        In case there is no next object existing, meaning the given
        object is the current version, the function returns this version.

        Note that if object's version_end_date is None, this does not check the database to
        see if there is a newer version (perhaps created by some other code), it simply
        returns the passed object.

        ``relations_as_of`` is used to fix the point in time for the version; this affects which related
        objects are returned when querying for object relations. See ``VersionManager.version_as_of``
        for details on valid ``relations_as_of`` values.

        :param Versionable object: object whose next version will be returned.
        :param mixed relations_as_of: determines point in time used to access relations. 'start'|'end'|datetime|None
        :return: Versionable
        """
        if object.version_end_date == None:
            next = object
        else:
            next = self.filter(
                Q(identity=object.identity),
                Q(version_start_date__gte=object.version_end_date)
            ).order_by('version_start_date').first()

            if not next:
                raise ObjectDoesNotExist(
                    "next_version couldn't find a next version of object " + str(object.identity))

        return self.adjust_version_as_of(next, relations_as_of)

    def previous_version(self, object, relations_as_of='end'):
        """
        Return the previous version of the given object.

        In case there is no previous object existing, meaning the given object
        is the first version of the object, then the function returns this version.

        ``relations_as_of`` is used to fix the point in time for the version; this affects which related
        objects are returned when querying for object relations. See ``VersionManager.version_as_of``
        for details on valid ``relations_as_of`` values.

        :param Versionable object: object whose previous version will be returned.
        :param mixed relations_as_of: determines point in time used to access relations. 'start'|'end'|datetime|None
        :return: Versionable
        """
        if object.version_birth_date == object.version_start_date:
            previous = object
        else:
            previous = self.filter(
                Q(identity=object.identity),
                Q(version_end_date__lte=object.version_start_date)
            ).order_by('-version_end_date').first()

            if not previous:
                raise ObjectDoesNotExist(
                    "previous_version couldn't find a previous version of object " + str(object.identity))

        return self.adjust_version_as_of(previous, relations_as_of)

    def current_version(self, object, relations_as_of=None, check_db=False):
        """
        Return the current version of the given object.

        The current version is the one having its version_end_date set to NULL.
        If there is not such a version then it means the object has been 'deleted'
        and so there is no current version available. In this case the function returns None.

        Note that if check_db is False and object's version_end_date is None, this does not
        check the database to see if there is a newer version (perhaps created by some other code),
        it simply returns the passed object.

        ``relations_as_of`` is used to fix the point in time for the version; this affects which related
        objects are returned when querying for object relations. See ``VersionManager.version_as_of``
        for details on valid ``relations_as_of`` values.

        :param Versionable object: object whose current version will be returned.
        :param mixed relations_as_of: determines point in time used to access relations. 'start'|'end'|datetime|None
        :param bool check_db: Whether or not to look in the database for a more recent version
        :return: Versionable
        """
        if object.version_end_date is None and not check_db:
            current = object
        else:
            current = self.current.filter(identity=object.identity).first()

        return self.adjust_version_as_of(current, relations_as_of)

    @staticmethod
    def adjust_version_as_of(version, relations_as_of):
        """
        Adjusts the passed version's as_of time to an appropriate value, and returns it.

        ``relations_as_of`` is used to fix the point in time for the version; this affects which related
        objects are returned when querying for object relations.
        Valid ``relations_as_of`` values and how this affects the returned version's as_of attribute:
        - 'start': version start date
        - 'end': version end date - 1 microsecond (no effect if version is current version)
        - datetime object: given datetime (raises ValueError if given datetime not valid for version)
        - None: unset (related object queries will not be restricted to a point in time)

        :param Versionable object: object whose as_of will be adjusted as requested.
        :param mixed relations_as_of: valid values are the strings 'start' or 'end', or a datetime object.
        :return: Versionable
        """
        if not version:
            return version

        if relations_as_of == 'end':
            if version.is_current:
                # Ensure that version._querytime is active, in case it wasn't before.
                version.as_of = None
            else:
                version.as_of = version.version_end_date - datetime.timedelta(microseconds=1)
        elif relations_as_of == 'start':
            version.as_of = version.version_start_date
        elif isinstance(relations_as_of, datetime.datetime):
            as_of = relations_as_of.astimezone(utc)
            if not as_of >= version.version_start_date:
                raise ValueError(
                    "Provided as_of '{}' is earlier than version's start time '{}'".format(
                        as_of.isoformat(),
                        version.version_start_date.isoformat()
                    )
                )
            if version.version_end_date is not None and as_of >= version.version_end_date:
                raise ValueError(
                    "Provided as_of '{}' is later than version's start time '{}'".format(
                        as_of.isoformat(),
                        version.version_end_date.isoformat()
                    )
                )
            version.as_of = as_of
        elif relations_as_of is None:
            version._querytime = QueryTime(time=None, active=False)
        else:
            raise TypeError("as_of parameter must be 'start', 'end', None, or datetime object")

        return version

    @property
    def current(self):
        return self.as_of(None)

    def create(self, **kwargs):
        """
        Creates an instance of a Versionable
        :param kwargs: arguments used to initialize the class instance
        :return: a Versionable instance of the class
        """
        return self._create_at(None, **kwargs)

    def _create_at(self, timestamp=None, id=None, forced_identity=None, **kwargs):
        """
        WARNING: Only for internal use and testing.

        Create a Versionable having a version_start_date and version_birth_date set to some pre-defined timestamp
        :param timestamp: point in time at which the instance has to be created
        :param id: version 4 UUID unicode object.  Usually this is not specified, it will be automatically created.
        :param forced_identity: version 4 UUID unicode object.  For internal use only.
        :param kwargs: arguments needed for initializing the instance
        :return: an instance of the class
        """
        id = Versionable.uuid(id)
        if forced_identity:
            ident = Versionable.uuid(forced_identity)
        else:
            ident = id

        if timestamp is None:
            timestamp = get_utc_now()
        kwargs['id'] = id
        kwargs['identity'] = ident
        kwargs['version_start_date'] = timestamp
        kwargs['version_birth_date'] = timestamp
        return super(VersionManager, self).create(**kwargs)


class VersionedWhereNode(WhereNode):
    def as_sql(self, qn, connection):
        """
        This method identifies joined table aliases in order for VersionedExtraWhere.as_sql()
        to be able to add time restrictions for those tables based on the VersionedQuery's
        querytime value.

        :param qn: In Django 1.7 & 1.8 this is a compiler
        :param connection: A DB connection
        :return: A tuple consisting of (sql_string, result_params)
        """
        # self.children is an array of VersionedExtraWhere-objects
        for child in self.children:
            if isinstance(child, VersionedExtraWhere) and not child.params:
                _query = qn.query
                query_time = _query.querytime.time
                apply_query_time = _query.querytime.active
                alias_map = _query.alias_map
                self._set_child_joined_alias(child, alias_map)
                if apply_query_time:
                    # Add query parameters that have not been added till now
                    child.set_as_of(query_time)
                else:
                    # Remove the restriction if it's not required
                    child.sqls = []
        return super(VersionedWhereNode, self).as_sql(qn, connection)

    @staticmethod
    def _set_child_joined_alias(child, alias_map):
        """
        Set the joined alias on the child, for Django >= 1.8.0
        :param child:
        :param alias_map:
        """
        for table in alias_map:
            join = alias_map[table]
            if not isinstance(join, Join):
                continue
            lhs = join.parent_alias
            if (lhs == child.alias and table == child.related_alias) \
                    or (lhs == child.related_alias and table == child.alias):
                child.set_joined_alias(table)
                break


class VersionedExtraWhere(ExtraWhere):
    """
    A specific implementation of ExtraWhere;
    Before as_sql can be called on an object, ensure that calls to
    - set_as_of and
    - set_joined_alias
    have been done
    """

    def __init__(self, historic_sql, current_sql, alias, remote_alias):
        super(VersionedExtraWhere, self).__init__(sqls=[], params=[])
        self.historic_sql = historic_sql
        self.current_sql = current_sql
        self.alias = alias
        self.related_alias = remote_alias
        self._as_of_time_set = False
        self.as_of_time = None
        self._joined_alias = None

    def set_as_of(self, as_of_time):
        self.as_of_time = as_of_time
        self._as_of_time_set = True

    def set_joined_alias(self, joined_alias):
        """
        Takes the alias that is being joined to the query and applies the query
        time constraint to its table

        :param str joined_alias: The table name of the alias
        """
        self._joined_alias = joined_alias

    def as_sql(self, qn=None, connection=None):
        sql = ""
        params = []

        # Fail fast for inacceptable cases
        if self._as_of_time_set and not self._joined_alias:
            raise ValueError("joined_alias is not set, but as_of is; this is a conflict!")

        # Set the SQL string in dependency of whether as_of_time was set or not
        if self._as_of_time_set:
            if self.as_of_time:
                sql = self.historic_sql
                params = [self.as_of_time] * 2
                # 2 is the number of occurences of the timestamp in an as_of-filter expression
            else:
                # If as_of_time was set to None, we're dealing with a query for "current" values
                sql = self.current_sql
        else:
            # No as_of_time has been set; Perhaps, as_of was not part of the query -> That's OK
            pass

        # By here, the sql string is defined if an as_of_time was provided
        if self._joined_alias:
            sql = sql.format(alias=self._joined_alias)

        # Set the final sqls
        # self.sqls needs to be set before the call to parent
        if sql:
            self.sqls = [sql]
        else:
            self.sqls = ["1=1"]
        self.params = params
        return super(VersionedExtraWhere, self).as_sql(qn, connection)


class VersionedQuery(Query):
    """
    VersionedQuery has awareness of the query time restrictions.  When the query is compiled,
    this query time information is passed along to the foreign keys involved in the query, so
    that they can provide that information when building the sql.
    """

    def __init__(self, *args, **kwargs):
        kwargs['where'] = VersionedWhereNode
        super(VersionedQuery, self).__init__(*args, **kwargs)
        self.querytime = QueryTime(time=None, active=False)

    def clone(self, *args, **kwargs):
        _clone = super(VersionedQuery, self).clone(*args, **kwargs)
        try:
            _clone.querytime = self.querytime
        except AttributeError:
            # If the caller is using clone to create a different type of Query, that's OK.
            # An example of this is when creating or updating an object, this method is called
            # with a first parameter of sql.UpdateQuery.
            pass
        return _clone

    def get_compiler(self, *args, **kwargs):
        """
        Add the query time restriction limit at the last moment.  Applying it earlier
        (e.g. by adding a filter to the queryset) does not allow the caching of related
        object to work (they are attached to a queryset; filter() returns a new queryset).
        """
        if self.querytime.active and (not hasattr(self, '_querytime_filter_added') or not self._querytime_filter_added):
            time = self.querytime.time
            if time is None:
                self.add_q(Q(version_end_date__isnull=True))
            else:
                self.add_q(
                    (Q(version_end_date__gt=time) | Q(version_end_date__isnull=True))
                    & Q(version_start_date__lte=time)
                )
            # Ensure applying these filters happens only a single time (even if it doesn't falsify the query, it's
            # just not very comfortable to read)
            self._querytime_filter_added = True
        return super(VersionedQuery, self).get_compiler(*args, **kwargs)

    def build_filter(self, filter_expr, **kwargs):
        """
        When a query is filtered with an expression like .filter(team=some_team_object),
        where team is a VersionedForeignKey field, and some_team_object is a Versionable object,
        adapt the filter value to be (team__identity=some_team_object.identity).

        When the query is built, this will enforce that the tables are joined and that
        the identity column and the as_of restriction is used for matching.

        For example, the generated SQL will be like:

           SELECT ... FROM foo INNER JOIN team ON (
                foo.team_id == team.identity
                AND foo.version_start_date <= [as_of]
                AND (foo.version_end_date > [as_of] OR foo.version_end_date IS NULL)) ...

        This is not necessary, and won't be applied, if any of these are true:
        - no as_of is in effect
        - the current objects are being queried (e.g. foo.objects.current.filter(...))
        - a terminal object is being used as the lookup value (e.g. .filter(team=the_deleted_team_version)
        - the lookup value is not a Versionable (e.g. .filter(foo='bar') or .filter(team=non_versionable_team)

        Note that this has the effect that Foo.objects.as_of(t1).filter(team=team_object_at_t3) will
        return the Foo objects at t1, and that accessing their team field (e.g. foo.team) will return
        the team object that was associated with them at t1, which may be a different object than
        team_object_at_t3.

        The goal is to make expressions like Foo.objects.as_of(tx).filter(team=some_team_object) work as
        closely as possible to standard, non-versioned Django querysets like Foo.objects.filter(team=some_team_object).

        :param filter_expr:
        :param kwargs:
        :return: tuple
        """
        lookup, value = filter_expr
        if self.querytime.active and isinstance(value, Versionable) and not value.is_latest:
            new_lookup = lookup + LOOKUP_SEP + Versionable.OBJECT_IDENTIFIER_FIELD
            filter_expr = (new_lookup, value.identity)
        return super(VersionedQuery, self).build_filter(filter_expr, **kwargs)


class VersionedQuerySet(QuerySet):
    """
    The VersionedQuerySet makes sure that every objects retrieved from it has
    the added property 'query_time' added to it.
    For that matter it override the __getitem__, _fetch_all and _clone methods
    for its parent class (QuerySet).
    """

    def __init__(self, model=None, query=None, *args, **kwargs):
        """
        Overridden so that a VersionedQuery will be used.
        """
        if not query:
            query = VersionedQuery(model)
        super(VersionedQuerySet, self).__init__(model=model, query=query, *args, **kwargs)
        self.querytime = QueryTime(time=None, active=False)

    @property
    def querytime(self):
        return self._querytime

    @querytime.setter
    def querytime(self, value):
        """
        Sets self._querytime as well as self.query.querytime.
        :param value: None or datetime
        :return:
        """
        self._querytime = value
        self.query.querytime = value

    def __getitem__(self, k):
        """
        Overrides the QuerySet.__getitem__ magic method for retrieving a list-item out of a query set.
        :param k: Retrieve the k-th element or a range of elements
        :return: Either one element or a list of elements
        """
        item = super(VersionedQuerySet, self).__getitem__(k)
        if isinstance(item, (list,)):
            for i in item:
                self._set_item_querytime(i)
        else:
            self._set_item_querytime(item)
        return item

    def _fetch_all(self):
        """
        Completely overrides the QuerySet._fetch_all method by adding the timestamp to all objects
        :return: See django.db.models.query.QuerySet._fetch_all for return values
        """
        if self._result_cache is None:
            self._result_cache = list(self.iterator())
            if not isinstance(self, ValuesListQuerySet):
                for x in self._result_cache:
                    self._set_item_querytime(x)
        if self._prefetch_related_lookups and not self._prefetch_done:
            self._prefetch_related_objects()

    def _clone(self, *args, **kwargs):
        """
        Overrides the QuerySet._clone method by adding the cloning of the VersionedQuerySet's query_time parameter
        :param kwargs: Same as the original QuerySet._clone params
        :return: Just as QuerySet._clone, this method returns a clone of the original object
        """
        clone = super(VersionedQuerySet, self)._clone(**kwargs)
        clone.querytime = self.querytime
        return clone

    def _set_item_querytime(self, item, type_check=True):
        """
        Sets the time for which the query was made on the resulting item
        :param item: an item of type Versionable
        :param type_check: Check the item to be a Versionable
        :return: Returns the item itself with the time set
        """
        if isinstance(item, Versionable):
            item._querytime = self.querytime
        elif isinstance(item, VersionedQuerySet):
            item.querytime = self.querytime
        elif isinstance(self, ValuesQuerySet):
            # When we are dealing with a ValueQuerySet there is no point in
            # setting the query_time as we are returning an array of values
            # instead of a full-fledged model object
            pass
        else:
            if type_check:
                raise TypeError("This item is not a Versionable, it's a " + str(type(item)))
        return item

    def as_of(self, qtime=None):
        """
        Sets the time for which we want to retrieve an object.
        :param qtime: The UTC date and time; if None then use the current state (where version_end_date = NULL)
        :return: A VersionedQuerySet
        """
        clone = self._clone()
        clone.querytime = QueryTime(time=qtime, active=True)
        return clone

    def delete(self):
        """
        Deletes the records in the QuerySet.
        """
        assert self.query.can_filter(), \
            "Cannot use 'limit' or 'offset' with delete."

        # Ensure that only current objects are selected.
        del_query = self.filter(version_end_date__isnull=True)

        # The delete is actually 2 queries - one to find related objects,
        # and one to delete. Make sure that the discovery of related
        # objects is performed on the same database as the deletion.
        del_query._for_write = True

        # Disable non-supported fields.
        del_query.query.select_for_update = False
        del_query.query.select_related = False
        del_query.query.clear_ordering(force_empty=True)

        collector_class = get_versioned_delete_collector_class()
        collector = collector_class(using=del_query.db)
        collector.collect(del_query)
        collector.delete(get_utc_now())

        # Clear the result cache, in case this QuerySet gets reused.
        self._result_cache = None

    delete.alters_data = True
    delete.queryset_only = True


class VersionedForeignKey(ForeignKey):
    """
    We need to replace the standard ForeignKey declaration in order to be able to introduce
    the VersionedReverseSingleRelatedObjectDescriptor, which allows to go back in time...
    We also want to allow keeping track of any as_of time so that joins can be restricted
    based on that.
    """

    def __init__(self, *args, **kwargs):
        super(VersionedForeignKey, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name, virtual_only=False):
        super(VersionedForeignKey, self).contribute_to_class(cls, name, virtual_only)
        setattr(cls, self.name, VersionedReverseSingleRelatedObjectDescriptor(self))

    def contribute_to_related_class(self, cls, related):
        """
        Override ForeignKey's methods, and replace the descriptor, if set by the parent's methods
        """
        # Internal FK's - i.e., those with a related name ending with '+' -
        # and swapped models don't get a related descriptor.
        super(VersionedForeignKey, self).contribute_to_related_class(cls, related)
        accessor_name = related.get_accessor_name()
        if hasattr(cls, accessor_name):
            setattr(cls, accessor_name, VersionedForeignRelatedObjectsDescriptor(related))

    def get_extra_restriction(self, where_class, alias, remote_alias):
        """
        Overrides ForeignObject's get_extra_restriction function that returns an SQL statement which is appended to a
        JOIN's conditional filtering part

        :return: SQL conditional statement
        :rtype: WhereNode
        """
        historic_sql = '''{alias}.version_start_date <= %s
                 AND ({alias}.version_end_date > %s OR {alias}.version_end_date is NULL )'''
        current_sql = '''{alias}.version_end_date is NULL'''
        # How 'bout creating an ExtraWhere here, without params
        return where_class([VersionedExtraWhere(historic_sql=historic_sql, current_sql=current_sql, alias=alias,
                                                remote_alias=remote_alias)])

    def get_joining_columns(self, reverse_join=False):
        """
        Get and return joining columns defined by this foreign key relationship

        :return: A tuple containing the column names of the tables to be joined (<local_col_name>, <remote_col_name>)
        :rtype: tuple
        """
        source = self.reverse_related_fields if reverse_join else self.related_fields
        joining_columns = tuple()
        for lhs_field, rhs_field in source:
            lhs_col_name = lhs_field.column
            rhs_col_name = rhs_field.column
            # Test whether
            # - self is the current ForeignKey relationship
            # - self was not auto_created (e.g. is not part of a M2M relationship)
            if self is lhs_field and not self.auto_created:
                if rhs_col_name == Versionable.VERSION_IDENTIFIER_FIELD:
                    rhs_col_name = Versionable.OBJECT_IDENTIFIER_FIELD
            elif self is rhs_field and not self.auto_created:
                if lhs_col_name == Versionable.VERSION_IDENTIFIER_FIELD:
                    lhs_col_name = Versionable.OBJECT_IDENTIFIER_FIELD
            joining_columns = joining_columns + ((lhs_col_name, rhs_col_name),)
        return joining_columns


class VersionedManyToManyField(ManyToManyField):
    def __init__(self, *args, **kwargs):
        super(VersionedManyToManyField, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name):
        """
        Called at class type creation. So, this method is called, when metaclasses get created
        """
        # self.rel.through needs to be set prior to calling super, since super(...).contribute_to_class refers to it.
        # Classes pointed to by a string do not need to be resolved here, since Django does that at a later point in
        # time - which is nice... ;)
        #
        # Superclasses take care of:
        # - creating the through class if unset
        # - resolving the through class if it's a string
        # - resolving string references within the through class
        if not self.rel.through and not cls._meta.abstract and not cls._meta.swapped:
            self.rel.through = VersionedManyToManyField.create_versioned_many_to_many_intermediary_model(self, cls,
                                                                                                         name)
        super(VersionedManyToManyField, self).contribute_to_class(cls, name)

        # Overwrite the descriptor
        if hasattr(cls, self.name):
            setattr(cls, self.name, VersionedReverseManyRelatedObjectsDescriptor(self))

    def contribute_to_related_class(self, cls, related):
        """
        Called at class type creation. So, this method is called, when metaclasses get created
        """
        super(VersionedManyToManyField, self).contribute_to_related_class(cls, related)
        accessor_name = related.get_accessor_name()
        if accessor_name and hasattr(cls, accessor_name):
            descriptor = VersionedManyRelatedObjectsDescriptor(related, accessor_name)
            setattr(cls, accessor_name, descriptor)
            if hasattr(cls._meta, 'many_to_many_related') and isinstance(cls._meta.many_to_many_related, list):
                cls._meta.many_to_many_related.append(descriptor)
            else:
                cls._meta.many_to_many_related = [descriptor]

    @staticmethod
    def create_versioned_many_to_many_intermediary_model(field, cls, field_name):
        # Let's not care too much on what flags could potentially be set on that intermediary class (e.g. managed, etc)
        # Let's play the game, as if the programmer had specified a class within his models... Here's how.

        from_ = cls._meta.model_name
        to_model = field.rel.to

        # Force 'to' to be a string (and leave the hard work to Django)
        if not isinstance(field.rel.to, six.string_types):
            to_model = '%s.%s' % (field.rel.to._meta.app_label, field.rel.to._meta.object_name)
            to = field.rel.to._meta.object_name.lower()
        else:
            to = to_model.lower()
        name = '%s_%s' % (from_, field_name)

        if field.rel.to == RECURSIVE_RELATIONSHIP_CONSTANT or to == cls._meta.object_name:
            from_ = 'from_%s' % to
            to = 'to_%s' % to
            to_model = cls

        # Since Django 1.7, a migration mechanism is shipped by default with Django. This migration module loads all
        # declared apps' models inside a __fake__ module.
        # This means that the models can be already loaded and registered by their original module, when we
        # reach this point of the application and therefore there is no need to load them a second time.
        if cls.__module__ == '__fake__':
            try:
                # Check the apps for an already registered model
                return apps.get_registered_model(cls._meta.app_label, str(name))
            except KeyError:
                # The model has not been registered yet, so continue
                pass

        meta = type('Meta', (object,), {
            # 'unique_together': (from_, to),
            'auto_created': cls,
            'db_tablespace': cls._meta.db_tablespace,
            'app_label': cls._meta.app_label,
        })
        return type(str(name), (Versionable,), {
            'Meta': meta,
            '__module__': cls.__module__,
            from_: VersionedForeignKey(cls, related_name='%s+' % name, auto_created=name),
            to: VersionedForeignKey(to_model, related_name='%s+' % name, auto_created=name),
        })


class VersionedReverseSingleRelatedObjectDescriptor(ReverseSingleRelatedObjectDescriptor):
    """
    A ReverseSingleRelatedObjectDescriptor-typed object gets inserted, when a ForeignKey
    is defined in a Django model. This is one part of the analogue for versioned items.

    Unfortunately, we need to run two queries. The first query satisfies the foreign key
    constraint. After extracting the identity information and combining it with the datetime-
    stamp, we are able to fetch the historic element.
    """

    def __get__(self, instance, instance_type=None):
        """
        The getter method returns the object, which points instance, e.g. choice.poll returns
        a Poll instance, whereas the Poll class defines the ForeignKey.
        :param instance: The object on which the property was accessed
        :param instance_type: The type of the instance object
        :return: Returns a Versionable
        """
        current_elt = super(VersionedReverseSingleRelatedObjectDescriptor, self).__get__(instance, instance_type)

        if instance is None:
            return self

        if not current_elt:
            return None

        if not isinstance(current_elt, Versionable):
            raise TypeError("VersionedForeignKey target is of type "
                            + str(type(current_elt))
                            + ", which is not a subclass of Versionable")

        if hasattr(instance, '_querytime'):
            # If current_elt matches the instance's querytime, there's no need to make a database query.
            if Versionable.matches_querytime(current_elt, instance._querytime):
                current_elt._querytime = instance._querytime
                return current_elt

            return current_elt.__class__.objects.as_of(instance._querytime.time).get(identity=current_elt.identity)
        else:
            return current_elt.__class__.objects.current.get(identity=current_elt.identity)


class VersionedForeignRelatedObjectsDescriptor(ForeignRelatedObjectsDescriptor):
    """
    This descriptor generates the manager class that is used on the related object of a ForeignKey relation
    (i.e. the reverse-ForeignKey field manager).
    """

    @cached_property
    def related_manager_cls(self):
        # return create_versioned_related_manager
        manager_cls = super(VersionedForeignRelatedObjectsDescriptor, self).related_manager_cls
        rel_field = self.related.field

        class VersionedRelatedManager(manager_cls):
            def __init__(self, instance):
                super(VersionedRelatedManager, self).__init__(instance)

                # This is a hack, in order to get the versioned related objects
                for key in self.core_filters.keys():
                    if '__exact' in key or '__' not in key:
                        self.core_filters[key] = instance.identity

            def get_queryset(self):
                queryset = super(VersionedRelatedManager, self).get_queryset()
                # Do not set the query time if it is already correctly set.  queryset.as_of() returns a clone
                # of the queryset, and this will destroy the prefetched objects cache if it exists.
                if isinstance(queryset,
                              VersionedQuerySet) and self.instance._querytime.active and queryset.querytime != self.instance._querytime:
                    queryset = queryset.as_of(self.instance._querytime.time)
                return queryset

            def get_prefetch_queryset(self, instances, queryset=None):
                """
                Overrides RelatedManager's implementation of get_prefetch_queryset so that it works
                nicely with VersionedQuerySets.  It ensures that identities and time-limited where
                clauses are used when selecting related reverse foreign key objects.
                """
                if queryset is None:
                    # Note that this intentionally call's VersionManager's get_queryset, instead of simply calling
                    # the superclasses' get_queryset (as the non-versioned RelatedManager does), because what is
                    # needed is a simple Versioned queryset without any restrictions (e.g. do not
                    # apply self.core_filters).
                    queryset = VersionManager.get_queryset(self)

                queryset._add_hints(instance=instances[0])
                queryset = queryset.using(queryset._db or self._db)

                rel_obj_attr = rel_field.get_local_related_value
                instance_attr = rel_field.get_foreign_related_value
                # Use identities instead of ids so that this will work with versioned objects.
                instances_dict = {(inst.identity,): inst for inst in instances}
                identities = [inst.identity for inst in instances]
                query = {'%s__identity__in' % rel_field.name: identities}
                queryset = queryset.filter(**query)

                # Since we just bypassed this class' get_queryset(), we must manage
                # the reverse relation manually.
                for rel_obj in queryset:
                    instance = instances_dict[rel_obj_attr(rel_obj)]
                    setattr(rel_obj, rel_field.name, instance)
                cache_name = rel_field.related_query_name()
                return queryset, rel_obj_attr, instance_attr, False, cache_name

            def add(self, *objs):
                cloned_objs = ()
                for obj in objs:
                    if not isinstance(obj, Versionable):
                        raise TypeError("Trying to add a non-Versionable to a VersionedForeignKey relationship")
                    cloned_objs += (obj.clone(),)
                super(VersionedRelatedManager, self).add(*cloned_objs)

            # clear() and remove() are present if the FK is nullable
            if 'clear' in dir(manager_cls):
                def clear(self, **kwargs):
                    """
                    Overridden to ensure that the current queryset is used, and to clone objects before they
                    are removed, so that history is not lost.
                    """
                    bulk = kwargs.pop('bulk', True)
                    db = router.db_for_write(self.model, instance=self.instance)
                    queryset = self.current.using(db)
                    with transaction.atomic(using=db, savepoint=False):
                        cloned_pks = [obj.clone().pk for obj in queryset]
                        update_qs = self.current.filter(pk__in=cloned_pks)
                        self._clear(update_qs, bulk)

            if 'remove' in dir(manager_cls):
                def remove(self, *objs):
                    val = rel_field.get_foreign_related_value(self.instance)
                    cloned_objs = ()
                    for obj in objs:
                        # Is obj actually part of this descriptor set? Otherwise, silently go over it, since Django
                        # handles that case
                        if rel_field.get_local_related_value(obj) == val:
                            # Silently pass over non-versionable items
                            if not isinstance(obj, Versionable):
                                raise TypeError(
                                    "Trying to remove a non-Versionable from a VersionedForeignKey realtionship")
                            cloned_objs += (obj.clone(),)
                    super(VersionedRelatedManager, self).remove(*cloned_objs)

        return VersionedRelatedManager


def create_versioned_many_related_manager(superclass, rel):
    """
    The "casting" which is done in this method is needed, since otherwise, the methods introduced by
    Versionable are not taken into account.
    :param superclass: This is usually a models.Manager
    :param rel: Contains the ManyToMany relation
    :return: A subclass of ManyRelatedManager and Versionable
    """
    many_related_manager_klass = create_many_related_manager(superclass, rel)

    class VersionedManyRelatedManager(many_related_manager_klass):
        def __init__(self, *args, **kwargs):
            super(VersionedManyRelatedManager, self).__init__(*args, **kwargs)
            try:
                _ = self.through._meta.get_field('version_start_date')
                _ = self.through._meta.get_field('version_end_date')
            except FieldDoesNotExist as e:
                fields = [f.name for f in self.through._meta.get_fields()]
                print(str(e) + "; available fields are " + ", ".join(fields))
                raise e
                # FIXME: this probably does not work when auto-referencing

        def get_queryset(self):
            """
            Add a filter to the queryset, limiting the results to be pointed by relationship that are
            valid for the given timestamp (which is taken at the current instance, or set to now, if not
            available).
            Long story short, apply the temporal validity filter also to the intermediary model.
            """

            queryset = super(VersionedManyRelatedManager, self).get_queryset()
            if hasattr(queryset, 'querytime'):
                if self.instance._querytime.active and self.instance._querytime != queryset.querytime:
                    queryset = queryset.as_of(self.instance._querytime.time)
            return queryset

        def _remove_items(self, source_field_name, target_field_name, *objs):
            """
            Instead of removing items, we simply set the version_end_date of the current item to the
            current timestamp --> t[now].
            Like that, there is no more current entry having that identity - which is equal to
            not existing for timestamps greater than t[now].
            """
            return self._remove_items_at(None, source_field_name, target_field_name, *objs)

        def _remove_items_at(self, timestamp, source_field_name, target_field_name, *objs):
            if objs:
                if timestamp is None:
                    timestamp = get_utc_now()
                old_ids = set()
                for obj in objs:
                    if isinstance(obj, self.model):
                        if hasattr(self, 'target_field'):
                            fk_val = self.target_field.get_foreign_related_value(obj)[0]
                        else:
                            raise TypeError("We couldn't find the value of the foreign key, this might be due to the "
                                            "use of an unsupported version of Django")
                        old_ids.add(fk_val)
                    else:
                        old_ids.add(obj)
                db = router.db_for_write(self.through, instance=self.instance)
                qs = self.through._default_manager.using(db).filter(**{
                    source_field_name: self.instance.id,
                    '%s__in' % target_field_name: old_ids
                }).as_of(timestamp)
                for relation in qs:
                    relation._delete_at(timestamp)

        if 'add' in dir(many_related_manager_klass):
            def add(self, *objs):
                if not self.instance.is_current:
                    raise SuspiciousOperation(
                        "Adding many-to-many related objects is only possible on the current version")

                # The ManyRelatedManager.add() method uses the through model's default manager to get
                # a queryset when looking at which objects already exist in the database.
                # In order to restrict the query to the current versions when that is done,
                # we temporarily replace the queryset's using method so that the version validity
                # condition can be specified.
                klass = self.through._default_manager.get_queryset().__class__
                __using_backup = klass.using

                def using_replacement(self, *args, **kwargs):
                    qs = __using_backup(self, *args, **kwargs)
                    return qs.as_of(None)

                klass.using = using_replacement
                super(VersionedManyRelatedManager, self).add(*objs)
                klass.using = __using_backup

            def add_at(self, timestamp, *objs):
                """
                This function adds an object at a certain point in time (timestamp)
                """
                # First off, define the new constructor
                def _through_init(self, *args, **kwargs):
                    super(self.__class__, self).__init__(*args, **kwargs)
                    self.version_birth_date = timestamp
                    self.version_start_date = timestamp

                # Through-classes have an empty constructor, so it can easily be overwritten when needed;
                # This is not the default case, so the overwrite only takes place when we "modify the past"
                self.through.__init_backup__ = self.through.__init__
                self.through.__init__ = _through_init

                # Do the add operation
                self.add(*objs)

                # Remove the constructor again (by replacing it with the original empty constructor)
                self.through.__init__ = self.through.__init_backup__
                del self.through.__init_backup__

            add_at.alters_data = True

        if 'remove' in dir(many_related_manager_klass):
            def remove_at(self, timestamp, *objs):
                """
                Performs the act of removing specified relationships at a specified time (timestamp);
                So, not the objects at a given time are removed, but their relationship!
                """
                self._remove_items_at(timestamp, self.source_field_name, self.target_field_name, *objs)

                # For consistency, also handle the symmetrical case
                if self.symmetrical:
                    self._remove_items_at(timestamp, self.target_field_name, self.source_field_name, *objs)

            remove_at.alters_data = True

    return VersionedManyRelatedManager


class VersionedReverseManyRelatedObjectsDescriptor(ReverseManyRelatedObjectsDescriptor):
    """
    Beside having a very long name, this class is useful when it comes to versioning the
    ReverseManyRelatedObjectsDescriptor (huhu!!). The main part is the exposure of the
    'related_manager_cls' property
    """

    def __get__(self, instance, owner=None):
        """
        Reads the property as which this object is figuring; mainly used for debugging purposes
        :param instance: The instance on which the getter was called
        :param owner: no idea... alternatively called 'instance_type by the superclasses
        :return: A VersionedManyRelatedManager object
        """
        return super(VersionedReverseManyRelatedObjectsDescriptor, self).__get__(instance, owner)

    def __set__(self, instance, value):
        """
        Completely overridden to avoid bulk deletion that happens when the parent method calls clear().

        The parent method's logic is basically: clear all in bulk, then add the given objects in bulk.
        Instead, we figure out which ones are being added and removed, and call add and remove for these values.
        This lets us retain the versioning information.

        Since this is a many-to-many relationship, it is assumed here that the django.db.models.deletion.Collector
        logic, that is used in clear(), is not necessary here.  Collector collects related models, e.g. ones that should
        also be deleted because they have a ON CASCADE DELETE relationship to the object, or, in the case of
        "Multi-table inheritance", are parent objects.

        :param instance: The instance on which the getter was called
        :param value: iterable of items to set
        """

        if not instance.is_current:
            raise SuspiciousOperation(
                "Related values can only be directly set on the current version of an object")

        if not self.field.rel.through._meta.auto_created:
            opts = self.field.rel.through._meta
            raise AttributeError(("Cannot set values on a ManyToManyField which specifies an intermediary model. "
                                  "Use %s.%s's Manager instead.") % (opts.app_label, opts.object_name))

        manager = self.__get__(instance)
        # Below comment is from parent __set__ method.  We'll force evaluation, too:
        # clear() can change expected output of 'value' queryset, we force evaluation
        # of queryset before clear; ticket #19816
        value = tuple(value)

        being_removed, being_added = self.get_current_m2m_diff(instance, value)
        timestamp = get_utc_now()
        manager.remove_at(timestamp, *being_removed)
        manager.add_at(timestamp, *being_added)

    def get_current_m2m_diff(self, instance, new_objects):
        """
        :param instance: Versionable object
        :param new_objects: objects which are about to be associated with instance
        :return: (being_removed id list, being_added id list)
        :rtype : tuple
        """
        new_ids = self.pks_from_objects(new_objects)
        relation_manager = self.__get__(instance)

        filter = Q(**{relation_manager.source_field.attname: instance.pk})
        qs = self.through.objects.current.filter(filter)
        target_name = relation_manager.target_field.attname
        current_ids = set(qs.values_list(target_name, flat=True))

        being_removed = current_ids - new_ids
        being_added = new_ids - current_ids
        return list(being_removed), list(being_added)

    def pks_from_objects(self, objects):
        """
        Extract all the primary key strings from the given objects.  Objects may be Versionables, or bare primary keys.
        :rtype : set
        """
        return {o.pk if isinstance(o, Model) else o for o in objects}

    @cached_property
    def related_manager_cls(self):
        return create_versioned_many_related_manager(
            self.field.rel.to._default_manager.__class__,
            self.field.rel
        )


class VersionedManyRelatedObjectsDescriptor(ManyRelatedObjectsDescriptor):
    """
    Beside having a very long name, this class is useful when it comes to versioning the
    ManyRelatedObjectsDescriptor (huhu!!). The main part is the exposure of the
    'related_manager_cls' property
    """

    via_field_name = None

    def __init__(self, related, via_field_name):
        super(VersionedManyRelatedObjectsDescriptor, self).__init__(related)
        self.via_field_name = via_field_name

    def __get__(self, instance, owner=None):
        """
        Reads the property as which this object is figuring; mainly used for debugging purposes
        :param instance: The instance on which the getter was called
        :param owner: no idea... alternatively called 'instance_type by the superclasses
        :return: A VersionedManyRelatedManager object
        """
        return super(VersionedManyRelatedObjectsDescriptor, self).__get__(instance, owner)

    @cached_property
    def related_manager_cls(self):
        return create_versioned_many_related_manager(
            self.related.model._default_manager.__class__,
            self.related.field.rel
        )


class Versionable(models.Model):
    """
    This is pretty much the central point for versioning objects.
    """

    VERSION_IDENTIFIER_FIELD = 'id'
    OBJECT_IDENTIFIER_FIELD = 'identity'
    VERSIONABLE_FIELDS = [VERSION_IDENTIFIER_FIELD, OBJECT_IDENTIFIER_FIELD, 'version_start_date',
                          'version_end_date', 'version_birth_date']

    if versions_settings.VERSIONS_USE_UUIDFIELD:
        id = models.UUIDField(primary_key=True)
        """id stands for ID and is the primary key; sometimes also referenced as the surrogate key"""
    else:
        id = models.CharField(max_length=36, primary_key=True)

    if versions_settings.VERSIONS_USE_UUIDFIELD:
        identity = models.UUIDField()
        """identity is used as the identifier of an object, ignoring its versions; sometimes also referenced as the natural key"""
    else:
        identity = models.CharField(max_length=36)

    version_start_date = models.DateTimeField()
    """version_start_date points the moment in time, when a version was created (ie. an versionable was cloned).
    This means, it points the start of a clone's validity period"""

    version_end_date = models.DateTimeField(null=True, default=None, blank=True)
    """version_end_date, if set, points the moment in time, when the entry was duplicated (ie. the entry was cloned). It
    points therefore the end of a clone's validity period"""

    version_birth_date = models.DateTimeField()
    """version_birth_date contains the timestamp pointing to when the versionable has been created (independent of any
    version); This timestamp is bound to an identity"""

    objects = VersionManager()
    """Make the versionable compliant with Django"""

    as_of = None
    """Hold the timestamp at which the object's data was looked up. Its value must always be in between the
    version_start_date and the version_end_date"""

    class Meta:
        abstract = True
        unique_together = ('id', 'identity')

    def __init__(self, *args, **kwargs):
        super(Versionable, self).__init__(*args, **kwargs)

        # _querytime is for library-internal use.
        self._querytime = QueryTime(time=None, active=False)

        # Ensure that the versionable field values are set.
        # If there are any deferred fields, then this instance is being initialized
        # from data in the database, and thus these values will already be set (unless
        # the fields are deferred, in which case they should not be set here).
        if not self.get_deferred_fields():
            if not getattr(self, 'version_start_date', None):
                setattr(self, 'version_start_date', get_utc_now())
            if not getattr(self, 'version_birth_date', None):
                setattr(self, 'version_birth_date', self.version_start_date)
            if not getattr(self, self.VERSION_IDENTIFIER_FIELD, None):
                setattr(self, self.VERSION_IDENTIFIER_FIELD, self.uuid())
            if not getattr(self, self.OBJECT_IDENTIFIER_FIELD, None):
                setattr(self, self.OBJECT_IDENTIFIER_FIELD, getattr(self, self.VERSION_IDENTIFIER_FIELD))

    def delete(self, using=None):
        using = using or router.db_for_write(self.__class__, instance=self)
        assert self._get_pk_val() is not None, \
            "{} object can't be deleted because its {} attribute is set to None.".format(
                self._meta.object_name, self._meta.pk.attname)

        collector_class = get_versioned_delete_collector_class()
        collector = collector_class(using=using)
        collector.collect([self])
        collector.delete(get_utc_now())

    def _delete_at(self, timestamp, using=None):
        """
        WARNING: This method is only for internal use, it should not be used
        from outside.

        It is used only in the case when you want to make sure a group of
        related objects are deleted at the exact same time.

        It is certainly not meant to be used for deleting an object and giving it
        a random deletion date of your liking.
        """
        if self.version_end_date is None:
            self.version_end_date = timestamp
            self.save(force_update=True, using=using)
        else:
            raise DeletionOfNonCurrentVersionError('Cannot delete anything else but the current version')

    @property
    def is_current(self):
        return self.version_end_date is None

    @property
    def is_latest(self):
        """
        Checks if this is the latest version.

        Note that this will not check the database for a possible newer version.
        It simply inspects the object's in-memory state.

        :return: boolean
        """
        return self.id == self.identity

    @property
    def is_terminated(self):
        """
        Checks if this version has been terminated.

        This will be true if a newer version has been created, or if the version has been "deleted".

        :return: boolean
        """
        return self.version_end_date is not None

    @property
    def as_of(self):
        return self._querytime.time

    @as_of.setter
    def as_of(self, time):
        self._querytime = QueryTime(time=time, active=True)

    @staticmethod
    def uuid(uuid_value=None):
        """
        Returns a uuid value that is valid to use for id and identity fields.

        :return: unicode uuid object if using UUIDFields, uuid unicode string otherwise.
        """
        if uuid_value:
            if not validate_uuid(uuid_value):
                raise ValueError("uuid_value must be a valid UUID version 4 object")
        else:
            uuid_value = uuid.uuid4()

        if versions_settings.VERSIONS_USE_UUIDFIELD:
            return uuid_value
        else:
            return six.u(str(uuid_value))

    def _clone_at(self, timestamp):
        """
        WARNING: This method is only for internal use, it should not be used
        from outside.

        This function is mostly intended for testing, to allow creating
        realistic test cases.
        """
        return self.clone(forced_version_date=timestamp)

    def clone(self, forced_version_date=None, in_bulk=False):
        """
        Clones a Versionable and returns a fresh copy of the original object.
        Original source: ClonableMixin snippet (http://djangosnippets.org/snippets/1271), with the pk/id change
        suggested in the comments

        :param forced_version_date: a timestamp including tzinfo; this value is usually set only internally!
        :param in_bulk: whether not to write this objects to the database already, if not necessary; this value is
        usually set only internally for performance optimization
        :return: returns a fresh clone of the original object (with adjusted relations)
        """
        if not self.pk:
            raise ValueError('Instance must be saved before it can be cloned')

        if self.version_end_date:
            raise ValueError('This is a historical item and can not be cloned.')

        if forced_version_date:
            if not self.version_start_date <= forced_version_date <= get_utc_now():
                raise ValueError('The clone date must be between the version start date and now.')
        else:
            forced_version_date = get_utc_now()

        if self.get_deferred_fields():
            # It would be necessary to fetch the record from the database again for this to succeed.
            # Alternatively, perhaps it would be possible to create a copy of the object after
            # fetching the missing fields.
            # Doing so may be unexpected by the calling code, so raise an exception: the calling
            # code should be adapted if necessary.
            raise ValueError('Can not clone a model instance that has deferred fields')

        earlier_version = self

        later_version = copy.copy(earlier_version)
        later_version.version_end_date = None
        later_version.version_start_date = forced_version_date

        # set earlier_version's ID to a new UUID so the clone (later_version) can
        # get the old one -- this allows 'head' to always have the original
        # id allowing us to get at all historic foreign key relationships
        earlier_version.id = self.uuid()
        earlier_version.version_end_date = forced_version_date

        if not in_bulk:
            # This condition might save us a lot of database queries if we are being called
            # from a loop like in .clone_relations
            earlier_version.save()
            later_version.save()
        else:
            earlier_version._not_created = True

        # re-create ManyToMany relations
        for field_name in self.get_all_m2m_field_names():
            earlier_version.clone_relations(later_version, field_name, forced_version_date)

        return later_version

    def at(self, timestamp):
        """
        Force the create date of an object to be at a certain time; This method can be invoked only on a
        freshly created Versionable object. It must not have been cloned yet. Raises a SuspiciousOperation
        exception, otherwise.
        :param timestamp: a datetime.datetime instance
        """
        # Ensure, it's not a historic item
        if not self.is_current:
            raise SuspiciousOperation(
                "Cannot relocate this Versionable instance in time, since it is a historical item")
        # Ensure it's not a versioned item (that would lead to some ugly situations...
        if not self.version_birth_date == self.version_start_date:
            raise SuspiciousOperation(
                "Cannot relocate this Versionable instance in time, since it is a versioned instance")
        # Ensure the argument is really a timestamp
        if not isinstance(timestamp, datetime.datetime):
            raise ValueError("This is not a datetime.datetime timestamp")
        self.version_birth_date = self.version_start_date = timestamp
        return self

    def clone_relations(self, clone, manager_field_name, forced_version_date):
        # Source: the original object, where relations are currently pointing to
        source = getattr(self, manager_field_name)  # returns a VersionedRelatedManager instance
        # Destination: the clone, where the cloned relations should point to
        destination = getattr(clone, manager_field_name)
        for item in source.all():
            destination.add(item)

        # retrieve all current m2m relations pointing the newly created clone
        # filter for source_id
        m2m_rels = list(source.through.objects.filter(**{source.source_field.attname: clone.id}))
        later_current = []
        later_non_current = []
        for rel in m2m_rels:
            # Only clone the relationship, if it is the current one; Simply adjust the older ones to point the old entry
            # Otherwise, the number of pointers pointing an entry will grow exponentially
            if rel.is_current:
                later_current.append(rel.clone(forced_version_date=self.version_end_date, in_bulk=True))
                # On rel, which is no more 'current', set the source ID to self.id
                setattr(rel, source.source_field_name, self)
            else:
                later_non_current.append(rel)
        # Perform the bulk changes rel.clone() did not perform because of the in_bulk parameter
        # This saves a huge bunch of SQL queries:
        # - update current version entries
        source.through.objects.filter(id__in=[l.id for l in later_current]).update(
            **{'version_start_date': forced_version_date})
        # - update entries that have been pointing the current object, but have never been 'current'
        source.through.objects.filter(id__in=[l.id for l in later_non_current]).update(
            **{source.source_field_name: self})
        # - create entries that were 'current', but which have been relieved in this method run
        source.through.objects.bulk_create([r for r in m2m_rels if hasattr(r, '_not_created') and r._not_created])

    def restore(self, **kwargs):
        """
        Restores this version as a new version, and returns this new version.

        If a current version already exists, it will be terminated before restoring this version.

        Relations (foreign key, reverse foreign key, many-to-many) are not restored with the old
        version.  If provided in kwargs, (Versioned)ForeignKey fields will be set to the provided
        values.  If passing an id for a (Versioned)ForeignKey, use the field.attname.  For example:
           restore(team_id=myteam.pk)
        If passing an object, simply use the field name, e.g.:
           restore(team=myteam)

        If a (Versioned)ForeignKey is not nullable and no value is provided for it in kwargs, a
        ForeignKeyRequiresValueError will be raised.

        :param kwargs: arguments used to initialize the class instance
        :return: Versionable
        """
        if not self.pk:
            raise ValueError('Instance must be saved and terminated before it can be restored.')

        if self.is_current:
            raise ValueError('This is the current version, no need to restore it.')

        if self.get_deferred_fields():
            # It would be necessary to fetch the record from the database again for this to succeed.
            # Alternatively, perhaps it would be possible to create a copy of the object after
            # fetching the missing fields.
            # Doing so may be unexpected by the calling code, so raise an exception: the calling
            # code should be adapted if necessary.
            raise ValueError('Can not restore a model instance that has deferred fields')

        cls = self.__class__
        now = get_utc_now()
        restored = copy.copy(self)
        restored.version_end_date = None
        restored.version_start_date = now

        fields = [f for f in cls._meta.local_fields if f.name not in Versionable.VERSIONABLE_FIELDS]
        for field in fields:
            if field.attname in kwargs:
                setattr(restored, field.attname, kwargs[field.attname])
            elif field.name in kwargs:
                setattr(restored, field.name, kwargs[field.name])
            elif isinstance(field, ForeignKey):
                # Set all non-provided ForeignKeys to None.  If required, raise an error.
                try:
                    setattr(restored, field.name, None)
                except ValueError as e:
                    raise ForeignKeyRequiresValueError(e.args[0])

        self.id = self.uuid()

        with transaction.atomic():
            # If this is not the latest version, terminate the latest version
            latest = cls.objects.current_version(self, check_db=True)
            if latest and latest != self:
                latest.delete()
                restored.version_start_date = latest.version_end_date

            self.save()
            restored.save()

            # Update ManyToMany relations to point to the old version's id instead of the restored version's id.
            for field_name in self.get_all_m2m_field_names():
                manager = getattr(restored, field_name)  # returns a VersionedRelatedManager instance
                manager.through.objects.filter(**{manager.source_field.attname: restored.id}).update(
                    **{manager.source_field_name: self})

            return restored

    def get_all_m2m_field_names(self):
        opts = self._meta
        rel_field_names = [field.attname for field in opts.many_to_many]
        if hasattr(opts, 'many_to_many_related'):
            rel_field_names += [rel.via_field_name for rel in opts.many_to_many_related]

        return rel_field_names

    def detach(self):
        """
        Detaches the instance from its history.

        Similar to creating a new object with the same field values. The id and
        identity fields are set to a new value. The returned object has not been saved,
        call save() afterwards when you are ready to persist the object.

        ManyToMany and reverse ForeignKey relations are lost for the detached object.

        :return: Versionable
        """
        self.id = self.identity = self.uuid()
        self.version_start_date = self.version_birth_date = get_utc_now()
        self.version_end_date = None
        return self

    @staticmethod
    def matches_querytime(instance, querytime):
        """
        Checks whether the given instance satisfies the given QueryTime object.

        :param instance: an instance of Versionable
        :param querytime: QueryTime value to check against
        """
        if not querytime.active:
            return True

        if not querytime.time:
            return instance.version_end_date is None

        return (instance.version_start_date <= querytime.time
                and (instance.version_end_date is None or instance.version_end_date > querytime.time))
