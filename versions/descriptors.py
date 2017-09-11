from collections import namedtuple

from django.core.exceptions import SuspiciousOperation, FieldDoesNotExist
from django.db import router, transaction
from django.db.models.base import Model
from django.db.models.fields.related import (ForwardManyToOneDescriptor,
                                             ReverseManyToOneDescriptor,
                                             ManyToManyDescriptor)
from django.db.models.fields.related_descriptors import \
    create_forward_many_to_many_manager
from django.db.models.query_utils import Q
from django.utils.functional import cached_property

from versions.util import get_utc_now


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

    return (instance.version_start_date <= querytime.time and (
        instance.version_end_date is None or
        instance.version_end_date > querytime.time))


class VersionedForwardManyToOneDescriptor(ForwardManyToOneDescriptor):
    """
    The VersionedForwardManyToOneDescriptor is used when pointing another
    Model using a VersionedForeignKey;

    For example:

        class Team(Versionable):
            name = CharField(max_length=200)
            city = VersionedForeignKey(City, null=True)

    ``team.city`` is a VersionedForwardManyToOneDescriptor
    """

    def get_prefetch_queryset(self, instances, queryset=None):
        """
         Overrides the parent method to:
         - force queryset to use the querytime of the parent objects
         - ensure that the join is done on identity, not id
         - make the cache key identity, not id.
         """
        if queryset is None:
            queryset = self.get_queryset()
        queryset._add_hints(instance=instances[0])

        # CleanerVersion change 1: force the querytime to be the same as the
        # prefetched-for instance.
        # This is necessary to have reliable results and avoid extra queries
        # for cache misses when accessing the child objects from their
        # parents (e.g. choice.poll).
        instance_querytime = instances[0]._querytime
        if instance_querytime.active:
            if queryset.querytime.active and \
                            queryset.querytime.time != instance_querytime.time:
                raise ValueError(
                    "A Prefetch queryset that specifies an as_of time must "
                    "match the as_of of the base queryset.")
            else:
                queryset.querytime = instance_querytime

        # CleanerVersion change 2: make rel_obj_attr return a tuple with
        # the object's identity.
        # rel_obj_attr = self.field.get_foreign_related_value
        def versioned_fk_rel_obj_attr(versioned_rel_obj):
            return versioned_rel_obj.identity,

        rel_obj_attr = versioned_fk_rel_obj_attr
        instance_attr = self.field.get_local_related_value
        instances_dict = {instance_attr(inst): inst for inst in instances}
        # CleanerVersion change 3: fake the related field so that it provides
        # a name of 'identity'.
        # related_field = self.field.foreign_related_fields[0]
        related_field = namedtuple('VersionedRelatedFieldTuple', 'name')(
            'identity')

        # FIXME: This will need to be revisited when we introduce support for
        # composite fields. In the meantime we take this practical approach to
        # solve a regression on 1.6 when the reverse manager in hidden
        # (related_name ends with a '+'). Refs #21410.
        # The check for len(...) == 1 is a special case that allows the query
        # to be join-less and smaller. Refs #21760.
        if self.field.rel.is_hidden() or len(
                self.field.foreign_related_fields) == 1:
            query = {'%s__in' % related_field.name: set(
                instance_attr(inst)[0] for inst in instances)}
        else:
            query = {'%s__in' % self.field.related_query_name(): instances}
        queryset = queryset.filter(**query)

        # Since we're going to assign directly in the cache,
        # we must manage the reverse relation cache manually.
        if not self.field.rel.multiple:
            rel_obj_cache_name = self.field.rel.get_cache_name()
            for rel_obj in queryset:
                instance = instances_dict[rel_obj_attr(rel_obj)]
                setattr(rel_obj, rel_obj_cache_name, instance)
        return queryset, rel_obj_attr, instance_attr, True, self.cache_name

    def get_queryset(self, **hints):
        queryset = super(VersionedForwardManyToOneDescriptor,
                         self).get_queryset(**hints)
        if hasattr(queryset, 'querytime'):
            if 'instance' in hints:
                instance = hints['instance']
                if hasattr(instance, '_querytime'):
                    if instance._querytime.active and \
                                    instance._querytime != queryset.querytime:
                        queryset = queryset.as_of(instance._querytime.time)
                else:
                    queryset = queryset.as_of(None)
        return queryset


vforward_many_to_one_descriptor_class = VersionedForwardManyToOneDescriptor


def vforward_many_to_one_descriptor_getter(self, instance, instance_type=None):
    """
    The getter method returns the object, which points instance,
    e.g. choice.poll returns a Poll instance, whereas the Poll class defines
    the ForeignKey.

    :param instance: The object on which the property was accessed
    :param instance_type: The type of the instance object
    :return: Returns a Versionable
    """
    from versions.models import Versionable
    current_elt = super(self.__class__, self).__get__(instance, instance_type)

    if instance is None:
        return self

    if not current_elt:
        return None

    if not isinstance(current_elt, Versionable):
        raise TypeError("VersionedForeignKey target is of type " +
                        str(type(current_elt)) +
                        ", which is not a subclass of Versionable")

    if hasattr(instance, '_querytime'):
        # If current_elt matches the instance's querytime, there's no need to
        # make a database query.
        if matches_querytime(current_elt, instance._querytime):
            current_elt._querytime = instance._querytime
            return current_elt

        return current_elt.__class__.objects.as_of(
            instance._querytime.time).get(identity=current_elt.identity)
    else:
        return current_elt.__class__.objects.current.get(
            identity=current_elt.identity)


vforward_many_to_one_descriptor_class.__get__ = \
    vforward_many_to_one_descriptor_getter


class VersionedReverseManyToOneDescriptor(ReverseManyToOneDescriptor):
    @cached_property
    def related_manager_cls(self):
        manager_cls = super(VersionedReverseManyToOneDescriptor,
                            self).related_manager_cls
        rel_field = self.field

        class VersionedRelatedManager(manager_cls):
            def __init__(self, instance):
                super(VersionedRelatedManager, self).__init__(instance)

                # This is a hack, in order to get the versioned related objects
                for key in self.core_filters.keys():
                    if '__exact' in key or '__' not in key:
                        self.core_filters[key] = instance.identity

            def get_queryset(self):
                from versions.models import VersionedQuerySet

                queryset = super(VersionedRelatedManager, self).get_queryset()
                # Do not set the query time if it is already correctly set.
                # queryset.as_of() returns a clone of the queryset, and this
                # will destroy the prefetched objects cache if it exists.
                if isinstance(queryset, VersionedQuerySet) \
                        and self.instance._querytime.active \
                        and queryset.querytime != self.instance._querytime:
                    queryset = queryset.as_of(self.instance._querytime.time)
                return queryset

            def get_prefetch_queryset(self, instances, queryset=None):
                """
                Overrides RelatedManager's implementation of
                get_prefetch_queryset so that it works nicely with
                VersionedQuerySets. It ensures that identities and time-limited
                where clauses are used when selecting related reverse foreign
                key objects.
                """
                if queryset is None:
                    # Note that this intentionally call's VersionManager's
                    # get_queryset, instead of simply calling the superclasses'
                    # get_queryset (as the non-versioned RelatedManager does),
                    # because what is needed is a simple Versioned queryset
                    # without any restrictions (e.g. do not apply
                    # self.core_filters).
                    from versions.models import VersionManager
                    queryset = VersionManager.get_queryset(self)

                queryset._add_hints(instance=instances[0])
                queryset = queryset.using(queryset._db or self._db)
                instance_querytime = instances[0]._querytime
                if instance_querytime.active:
                    if queryset.querytime.active and \
                                    queryset.querytime.time != \
                                    instance_querytime.time:
                        raise ValueError(
                            "A Prefetch queryset that specifies an as_of time "
                            "must match the as_of of the base queryset.")
                    else:
                        queryset.querytime = instance_querytime

                rel_obj_attr = rel_field.get_local_related_value
                instance_attr = rel_field.get_foreign_related_value
                # Use identities instead of ids so that this will work with
                # versioned objects.
                instances_dict = {(inst.identity,): inst for inst in instances}
                identities = [inst.identity for inst in instances]
                query = {'%s__identity__in' % rel_field.name: identities}
                queryset = queryset.filter(**query)

                # Since we just bypassed this class' get_queryset(), we must
                # manage the reverse relation manually.
                for rel_obj in queryset:
                    instance = instances_dict[rel_obj_attr(rel_obj)]
                    setattr(rel_obj, rel_field.name, instance)
                cache_name = rel_field.related_query_name()
                return queryset, rel_obj_attr, instance_attr, False, cache_name

            def add(self, *objs, **kwargs):
                from versions.models import Versionable
                cloned_objs = ()
                for obj in objs:
                    if not isinstance(obj, Versionable):
                        raise TypeError(
                            "Trying to add a non-Versionable to a "
                            "VersionedForeignKey relationship")
                    cloned_objs += (obj.clone(),)
                super(VersionedRelatedManager, self).add(*cloned_objs,
                                                         **kwargs)

            # clear() and remove() are present if the FK is nullable
            if 'clear' in dir(manager_cls):
                def clear(self, **kwargs):
                    """
                    Overridden to ensure that the current queryset is used,
                    and to clone objects before they are removed, so that
                    history is not lost.
                    """
                    bulk = kwargs.pop('bulk', True)
                    db = router.db_for_write(self.model,
                                             instance=self.instance)
                    queryset = self.current.using(db)
                    with transaction.atomic(using=db, savepoint=False):
                        cloned_pks = [obj.clone().pk for obj in queryset]
                        update_qs = self.current.filter(pk__in=cloned_pks)
                        self._clear(update_qs, bulk)

            if 'remove' in dir(manager_cls):
                def remove(self, *objs, **kwargs):
                    from versions.models import Versionable

                    val = rel_field.get_foreign_related_value(self.instance)
                    cloned_objs = ()
                    for obj in objs:
                        # Is obj actually part of this descriptor set?
                        # Otherwise, silently go over it, since Django
                        # handles that case
                        if rel_field.get_local_related_value(obj) == val:
                            # Silently pass over non-versionable items
                            if not isinstance(obj, Versionable):
                                raise TypeError(
                                    "Trying to remove a non-Versionable from "
                                    "a VersionedForeignKey realtionship")
                            cloned_objs += (obj.clone(),)
                    super(VersionedRelatedManager, self).remove(*cloned_objs,
                                                                **kwargs)

        return VersionedRelatedManager


class VersionedManyToManyDescriptor(ManyToManyDescriptor):
    @cached_property
    def related_manager_cls(self):
        model = self.rel.related_model if self.reverse else self.rel.model
        return create_versioned_forward_many_to_many_manager(
            model._default_manager.__class__,
            self.rel,
            reverse=self.reverse,
        )

    def __set__(self, instance, value):
        """
        Completely overridden to avoid bulk deletion that happens when the
        parent method calls clear().

        The parent method's logic is basically: clear all in bulk, then add
        the given objects in bulk.
        Instead, we figure out which ones are being added and removed, and
        call add and remove for these values.
        This lets us retain the versioning information.

        Since this is a many-to-many relationship, it is assumed here that
        the django.db.models.deletion.Collector logic, that is used in
        clear(), is not necessary here. Collector collects related models,
        e.g. ones that should also be deleted because they have
        a ON CASCADE DELETE relationship to the object, or, in the case of
        "Multi-table inheritance", are parent objects.

        :param instance: The instance on which the getter was called
        :param value: iterable of items to set
        """

        if not instance.is_current:
            raise SuspiciousOperation(
                "Related values can only be directly set on the current "
                "version of an object")

        if not self.field.rel.through._meta.auto_created:
            opts = self.field.rel.through._meta
            raise AttributeError((
                                     "Cannot set values on a ManyToManyField "
                                     "which specifies an intermediary model. "
                                     "Use %s.%s's Manager instead.") % (
                                     opts.app_label, opts.object_name))

        manager = self.__get__(instance)
        # Below comment is from parent __set__ method.  We'll force
        # evaluation, too:
        # clear() can change expected output of 'value' queryset, we force
        # evaluation of queryset before clear; ticket #19816
        value = tuple(value)

        being_removed, being_added = self.get_current_m2m_diff(instance, value)
        timestamp = get_utc_now()
        manager.remove_at(timestamp, *being_removed)
        manager.add_at(timestamp, *being_added)

    def get_current_m2m_diff(self, instance, new_objects):
        """
        :param instance: Versionable object
        :param new_objects: objects which are about to be associated with
            instance
        :return: (being_removed id list, being_added id list)
        :rtype : tuple
        """
        new_ids = self.pks_from_objects(new_objects)
        relation_manager = self.__get__(instance)

        filter = Q(**{relation_manager.source_field.attname: instance.pk})
        qs = self.through.objects.current.filter(filter)
        try:
            # Django 1.7
            target_name = relation_manager.target_field.attname
        except AttributeError:
            # Django 1.6
            target_name = relation_manager.through._meta.get_field_by_name(
                relation_manager.target_field_name)[0].attname
        current_ids = set(qs.values_list(target_name, flat=True))

        being_removed = current_ids - new_ids
        being_added = new_ids - current_ids
        return list(being_removed), list(being_added)

    def pks_from_objects(self, objects):
        """
        Extract all the primary key strings from the given objects.
        Objects may be Versionables, or bare primary keys.

        :rtype : set
        """
        return {o.pk if isinstance(o, Model) else o for o in objects}


def create_versioned_forward_many_to_many_manager(superclass, rel,
                                                  reverse=None):
    many_related_manager_klass = create_forward_many_to_many_manager(
        superclass, rel, reverse)

    class VersionedManyRelatedManager(many_related_manager_klass):
        def __init__(self, *args, **kwargs):
            super(VersionedManyRelatedManager, self).__init__(*args, **kwargs)
            # Additional core filters are:
            # version_start_date <= t &
            #   (version_end_date > t | version_end_date IS NULL)
            # but we cannot work with the Django core filters, since they
            # don't support ORing filters, which is a thing we need to
            # consider the "version_end_date IS NULL" case;
            # So, we define our own set of core filters being applied when
            # versioning
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
            Add a filter to the queryset, limiting the results to be pointed
            by relationship that are valid for the given timestamp (which is
            taken at the current instance, or set to now, if not available).
            Long story short, apply the temporal validity filter also to the
            intermediary model.
            """
            queryset = super(VersionedManyRelatedManager, self).get_queryset()
            if hasattr(queryset, 'querytime'):
                if self.instance._querytime.active and \
                                self.instance._querytime != queryset.querytime:
                    queryset = queryset.as_of(self.instance._querytime.time)
            return queryset

        def _remove_items(self, source_field_name, target_field_name, *objs):
            """
            Instead of removing items, we simply set the version_end_date of
            the current item to the current timestamp --> t[now].
            Like that, there is no more current entry having that identity -
            which is equal to not existing for timestamps greater than t[now].
            """
            return self._remove_items_at(None, source_field_name,
                                         target_field_name, *objs)

        def _remove_items_at(self, timestamp, source_field_name,
                             target_field_name, *objs):
            if objs:
                if timestamp is None:
                    timestamp = get_utc_now()
                old_ids = set()
                for obj in objs:
                    if isinstance(obj, self.model):
                        # The Django 1.7-way is preferred
                        if hasattr(self, 'target_field'):
                            fk_val = \
                                self.target_field \
                                    .get_foreign_related_value(obj)[0]
                        else:
                            raise TypeError(
                                "We couldn't find the value of the foreign "
                                "key, this might be due to the use of an "
                                "unsupported version of Django")
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
                        "Adding many-to-many related objects is only possible "
                        "on the current version")

                # The ManyRelatedManager.add() method uses the through model's
                # default manager to get a queryset when looking at which
                # objects already exist in the database.
                # In order to restrict the query to the current versions when
                # that is done, we temporarily replace the queryset's using
                # method so that the version validity condition can be
                # specified.
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
                This function adds an object at a certain point in time
                (timestamp)
                """

                # First off, define the new constructor
                def _through_init(self, *args, **kwargs):
                    super(self.__class__, self).__init__(*args, **kwargs)
                    self.version_birth_date = timestamp
                    self.version_start_date = timestamp

                # Through-classes have an empty constructor, so it can easily
                # be overwritten when needed;
                # This is not the default case, so the overwrite only takes
                # place when we "modify the past"
                self.through.__init_backup__ = self.through.__init__
                self.through.__init__ = _through_init

                # Do the add operation
                self.add(*objs)

                # Remove the constructor again (by replacing it with the
                # original empty constructor)
                self.through.__init__ = self.through.__init_backup__
                del self.through.__init_backup__

            add_at.alters_data = True

        if 'remove' in dir(many_related_manager_klass):
            def remove_at(self, timestamp, *objs):
                """
                Performs the act of removing specified relationships at a
                specified time (timestamp);
                So, not the objects at a given time are removed, but their
                relationship!
                """
                self._remove_items_at(timestamp, self.source_field_name,
                                      self.target_field_name, *objs)

                # For consistency, also handle the symmetrical case
                if self.symmetrical:
                    self._remove_items_at(timestamp, self.target_field_name,
                                          self.source_field_name, *objs)

            remove_at.alters_data = True

    return VersionedManyRelatedManager
