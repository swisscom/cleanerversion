from django.db.models.deletion import DO_NOTHING
from django.db.models.fields.related import ForeignKey, ManyToManyField, \
    resolve_relation, lazy_related_operation
from django.db.models.query_utils import Q
from django.db.models.sql.datastructures import Join
from django.db.models.sql.where import ExtraWhere, WhereNode
from django.db.models.utils import make_model_tuple

from versions.descriptors import (VersionedForwardManyToOneDescriptor,
                                  VersionedReverseManyToOneDescriptor,
                                  VersionedManyToManyDescriptor)
from versions.models import Versionable


class VersionedForeignKey(ForeignKey):
    """
    We need to replace the standard ForeignKey declaration in order to be able
    to introduce the VersionedReverseSingleRelatedObjectDescriptor, which
    allows to go back in time...
    We also want to allow keeping track of any as_of time so that joins can
    be restricted based on that.
    """

    def __init__(self, *args, **kwargs):
        super(VersionedForeignKey, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name, virtual_only=False):
        super(VersionedForeignKey, self).contribute_to_class(cls, name,
                                                             virtual_only)
        setattr(cls, self.name, VersionedForwardManyToOneDescriptor(self))

    def contribute_to_related_class(self, cls, related):
        """
        Override ForeignKey's methods, and replace the descriptor, if set by
        the parent's methods
        """
        # Internal FK's - i.e., those with a related name ending with '+' -
        # and swapped models don't get a related descriptor.
        super(VersionedForeignKey, self).contribute_to_related_class(cls,
                                                                     related)
        accessor_name = related.get_accessor_name()
        if hasattr(cls, accessor_name):
            setattr(cls, accessor_name,
                    VersionedReverseManyToOneDescriptor(related))

    def get_extra_restriction(self, where_class, alias, remote_alias):
        """
        Overrides ForeignObject's get_extra_restriction function that returns
        an SQL statement which is appended to a JOIN's conditional filtering
        part

        :return: SQL conditional statement
        :rtype: WhereNode
        """
        historic_sql = '''{alias}.version_start_date <= %s
                            AND ({alias}.version_end_date > %s
                                OR {alias}.version_end_date is NULL )'''
        current_sql = '''{alias}.version_end_date is NULL'''
        # How 'bout creating an ExtraWhere here, without params
        return where_class([VersionedExtraWhere(historic_sql=historic_sql,
                                                current_sql=current_sql,
                                                alias=alias,
                                                remote_alias=remote_alias)])

    def get_joining_columns(self, reverse_join=False):
        """
        Get and return joining columns defined by this foreign key relationship

        :return: A tuple containing the column names of the tables to be
            joined (<local_col_name>, <remote_col_name>)
        :rtype: tuple
        """
        source = self.reverse_related_fields if reverse_join \
            else self.related_fields
        joining_columns = tuple()
        for lhs_field, rhs_field in source:
            lhs_col_name = lhs_field.column
            rhs_col_name = rhs_field.column
            # Test whether
            # - self is the current ForeignKey relationship
            # - self was not auto_created (e.g. is not part of a M2M
            #   relationship)
            if self is lhs_field and not self.auto_created:
                if rhs_col_name == Versionable.VERSION_IDENTIFIER_FIELD:
                    rhs_col_name = Versionable.OBJECT_IDENTIFIER_FIELD
            elif self is rhs_field and not self.auto_created:
                if lhs_col_name == Versionable.VERSION_IDENTIFIER_FIELD:
                    lhs_col_name = Versionable.OBJECT_IDENTIFIER_FIELD
            joining_columns = joining_columns + ((lhs_col_name, rhs_col_name),)
        return joining_columns

    def get_reverse_related_filter(self, obj):
        base_filter = dict()
        for lh_field, rh_field in self.related_fields:
            if isinstance(obj, Versionable) and \
                            rh_field.attname == \
                            Versionable.VERSION_IDENTIFIER_FIELD:
                base_filter.update(**{
                    Versionable.OBJECT_IDENTIFIER_FIELD:
                        getattr(obj, lh_field.attname)})
            else:
                base_filter.update(
                    **{rh_field.attname: getattr(obj, lh_field.attname)})
        descriptor_filter = self.get_extra_descriptor_filter(obj)
        base_q = Q(**base_filter)
        if isinstance(descriptor_filter, dict):
            return base_q & Q(**descriptor_filter)
        elif descriptor_filter:
            return base_q & descriptor_filter
        return base_q


class VersionedManyToManyField(ManyToManyField):
    def __init__(self, *args, **kwargs):
        super(VersionedManyToManyField, self).__init__(*args, **kwargs)

    def contribute_to_class(self, cls, name, **kwargs):
        """
        Called at class type creation. So, this method is called, when
        metaclasses get created
        """
        # TODO: Apply 3 edge cases when not to create an intermediary model
        # specified in django.db.models.fields.related:1566
        # self.rel.through needs to be set prior to calling super, since
        # super(...).contribute_to_class refers to it.
        # Classes pointed to by a string do not need to be resolved here,
        # since Django does that at a later point in time - which is nice... ;)
        #
        # Superclasses take care of:
        # - creating the through class if unset
        # - resolving the through class if it's a string
        # - resolving string references within the through class
        if not self.remote_field.through and \
                not cls._meta.abstract and \
                not cls._meta.swapped:
            # We need to anticipate some stuff, that's done only later in
            # class contribution
            self.set_attributes_from_name(name)
            self.model = cls
            self.remote_field.through = \
                VersionedManyToManyField \
                .create_versioned_many_to_many_intermediary_model(self,
                                                                  cls,
                                                                  name)
        super(VersionedManyToManyField, self).contribute_to_class(cls, name)

        # Overwrite the descriptor
        if hasattr(cls, self.name):
            setattr(cls, self.name,
                    VersionedManyToManyDescriptor(self.remote_field))

    def contribute_to_related_class(self, cls, related):
        """
        Called at class type creation. So, this method is called, when
        metaclasses get created
        """
        super(VersionedManyToManyField, self). \
            contribute_to_related_class(cls, related)
        accessor_name = related.get_accessor_name()
        if accessor_name and hasattr(cls, accessor_name):
            descriptor = VersionedManyToManyDescriptor(related, accessor_name)
            setattr(cls, accessor_name, descriptor)
            if hasattr(cls._meta, 'many_to_many_related') and isinstance(
                    cls._meta.many_to_many_related, list):
                cls._meta.many_to_many_related.append(descriptor)
            else:
                cls._meta.many_to_many_related = [descriptor]

    @staticmethod
    def create_versioned_many_to_many_intermediary_model(field, cls,
                                                         field_name):
        # TODO: Verify functionality against
        # django.db.models.fields.related:1048
        # Let's not care too much on what flags could potentially be set on
        #   that intermediary class (e.g. managed, etc)
        # Let's play the game, as if the programmer had specified a class
        #   within his models... Here's how.

        # FIXME: VersionedManyToManyModels do not get registered in the
        #   apps models.
        # FIXME: This is usually done at django/db/models/base.py:284,
        # invoked by create_many_to_many_intermediary_model at
        #   django.db.models.fields.related:1048

        def set_managed(model, related, through):
            through._meta.managed = model._meta.managed or \
                                    related._meta.managed

        to_model = resolve_relation(cls, field.remote_field.model)

        name = '%s_%s' % (cls._meta.object_name, field_name)
        lazy_related_operation(set_managed, cls, to_model, name)

        # Force 'to' to be a string (and leave the hard work to Django)
        to = make_model_tuple(to_model)[1]
        from_ = cls._meta.model_name
        if to == from_:
            from_ = 'from_%s' % from_
            to = 'to_%s' % to

        meta = type('Meta', (object,), {
            'db_table': field._get_m2m_db_table(cls._meta),
            'auto_created': cls,
            'app_label': cls._meta.app_label,
            'db_tablespace': cls._meta.db_tablespace,
            # 'unique_together' is not applicable as is, due to multiple
            #   versions to be allowed to exist.
            # 'unique_together': (from_, to),
            'verbose_name': '%(from)s-%(to)s relationship' % {'from': from_,
                                                              'to': to},
            'verbose_name_plural': '%(from)s-%(to)s relationships' % {
                'from': from_, 'to': to},
            'apps': field.model._meta.apps,
        })
        return type(str(name), (Versionable,), {
            'Meta': meta,
            '__module__': cls.__module__,
            from_: VersionedForeignKey(
                cls,
                related_name='%s+' % name,
                db_tablespace=field.db_tablespace,
                db_constraint=field.remote_field.db_constraint,
                auto_created=name,
                on_delete=DO_NOTHING,
            ),
            to: VersionedForeignKey(
                to_model,
                related_name='%s+' % name,
                db_tablespace=field.db_tablespace,
                db_constraint=field.remote_field.db_constraint,
                auto_created=name,
                on_delete=DO_NOTHING,
            ),
        })


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
            raise ValueError(
                "joined_alias is not set, but as_of is; this is a conflict!")

        # Set the SQL string in dependency of whether as_of_time was set or not
        if self._as_of_time_set:
            if self.as_of_time:
                sql = self.historic_sql
                params = [self.as_of_time] * 2
                # 2 is the number of occurences of the timestamp in an
                # as_of-filter expression
            else:
                # If as_of_time was set to None, we're dealing with a query
                # for "current" values
                sql = self.current_sql
        else:
            # No as_of_time has been set; Perhaps, as_of was not part of the
            # query -> That's OK
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


class VersionedWhereNode(WhereNode):
    def as_sql(self, qn, connection):
        """
        This method identifies joined table aliases in order for
        VersionedExtraWhere.as_sql() to be able to add time restrictions for
        those tables based on the VersionedQuery's querytime value.

        :param qn: In Django 1.7 & 1.8 this is a compiler; in 1.6, it's an
            instance-method
        :param connection: A DB connection
        :return: A tuple consisting of (sql_string, result_params)
        """
        # self.children is an array of VersionedExtraWhere-objects
        for child in self.children:
            if isinstance(child, VersionedExtraWhere) and not child.params:
                # Django 1.7 & 1.8 handles compilers as objects
                _query = qn.query
                query_time = _query.querytime.time
                apply_query_time = _query.querytime.active
                alias_map = _query.alias_map
                # In Django 1.8, use the Join objects in alias_map
                self._set_child_joined_alias(child, alias_map)
                if apply_query_time:
                    # Add query parameters that have not been added till now
                    child.set_as_of(query_time)
                else:
                    # Remove the restriction if it's not required
                    child.sqls = []
        return super(VersionedWhereNode, self).as_sql(qn, connection)

    @staticmethod
    def _set_child_joined_alias_using_join_map(child, join_map, alias_map):
        """
        Set the joined alias on the child, for Django <= 1.7.x.
        :param child:
        :param join_map:
        :param alias_map:
        """
        for lhs, table, join_cols in join_map:
            if lhs is None:
                continue
            if lhs == child.alias:
                relevant_alias = child.related_alias
            elif lhs == child.related_alias:
                relevant_alias = child.alias
            else:
                continue

            join_info = alias_map[relevant_alias]
            if join_info.join_type is None:
                continue

            if join_info.lhs_alias in [child.alias, child.related_alias]:
                child.set_joined_alias(relevant_alias)
                break

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
