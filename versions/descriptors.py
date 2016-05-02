from django import VERSION
from django.core.exceptions import SuspiciousOperation, FieldDoesNotExist
from django.db import router, transaction
from django.db.models.base import Model
from django.db.models.query_utils import Q

from django.utils.functional import cached_property
from versions.models import Versionable, VersionedQuerySet

from versions.util import get_utc_now

if VERSION[:2] >= (1, 9):
    # With Django 1.9 related descriptor classes have been renamed:
    # ReverseSingleRelatedObjectDescriptor => ForwardManyToOneDescriptor
    # ForeignRelatedObjectsDescriptor => ReverseManyToOneDescriptor
    # ReverseManyRelatedObjectsDescriptor => ManyToManyDescriptor
    # ManyRelatedObjectsDescriptor => ManyToManyDescriptor
    # (new) => ReverseOneToOneDescriptor
    from django.db.models.fields.related import (ForwardManyToOneDescriptor, ReverseManyToOneDescriptor,
                                                 ManyToManyDescriptor, ReverseOneToOneDescriptor)
    from django.db.models.fields.related_descriptors import create_forward_many_to_many_manager
else:
    from django.db.models.fields.related import (ReverseSingleRelatedObjectDescriptor,
                                                 ReverseManyRelatedObjectsDescriptor,
                                                 ManyRelatedObjectsDescriptor,
                                                 ForeignRelatedObjectsDescriptor,
                                                 create_many_related_manager)



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

if VERSION[:2] >= (1,9):
    class VersionedForwardManyToOneDescriptor(ForwardManyToOneDescriptor):
        """

        """
        pass
    vforward_many_to_one_descriptor_class = VersionedForwardManyToOneDescriptor
else:
    class VersionedReverseSingleRelatedObjectDescriptor(ReverseSingleRelatedObjectDescriptor):
        """
        A ReverseSingleRelatedObjectDescriptor-typed object gets inserted, when a ForeignKey
        is defined in a Django model. This is one part of the analogue for versioned items.

        Unfortunately, we need to run two queries. The first query satisfies the foreign key
        constraint. After extracting the identity information and combining it with the datetime-
        stamp, we are able to fetch the historic element.
        """
        pass
    vforward_many_to_one_descriptor_class = VersionedReverseSingleRelatedObjectDescriptor

def vforward_many_to_one_descriptor_getter(self, instance, instance_type=None):
    """
    The getter method returns the object, which points instance, e.g. choice.poll returns
    a Poll instance, whereas the Poll class defines the ForeignKey.
    :param instance: The object on which the property was accessed
    :param instance_type: The type of the instance object
    :return: Returns a Versionable
    """
    current_elt = super(self.__class__, self).__get__(instance, instance_type)

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
        if matches_querytime(current_elt, instance._querytime):
            current_elt._querytime = instance._querytime
            return current_elt

        return current_elt.__class__.objects.as_of(instance._querytime.time).get(identity=current_elt.identity)
    else:
        return current_elt.__class__.objects.current.get(identity=current_elt.identity)

vforward_many_to_one_descriptor_class.__get__ = vforward_many_to_one_descriptor_getter



if VERSION[:2] >= (1,9):
    class VersionedReverseManyToOneDescriptor(ReverseManyToOneDescriptor):
        pass

    vreverse_many_to_one_descriptor_class = VersionedReverseManyToOneDescriptor
else:
    class VersionedForeignRelatedObjectsDescriptor(ForeignRelatedObjectsDescriptor):
        """
        This descriptor generates the manager class that is used on the related object of a ForeignKey relation
        (i.e. the reverse-ForeignKey field manager).
        """
        pass

    vreverse_many_to_one_descriptor_class = VersionedForeignRelatedObjectsDescriptor


def vreverse_many_to_one_descriptor_related_manager_cls_property(self):
    # return create_versioned_related_manager
    manager_cls = super(self.__class__, self).related_manager_cls
    if VERSION[:2] >= (1, 9):
        # TODO: Define, what field has to be taken over here, self.rel/self.field? The WhineDrinker.hats test seems to be a good one for testing this
	rel_field = self.rel
    elif hasattr(self, 'related'):
        rel_field = self.related.field
    else:
        rel_field = self.field

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
            if isinstance(queryset, VersionedQuerySet) \
                    and self.instance._querytime.active \
                    and queryset.querytime != self.instance._querytime:
                queryset = queryset.as_of(self.instance._querytime.time)
            return queryset

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
                    if VERSION[:2] == (1, 6):
                        update_qs.update(**{rel_field.name: None})
                    else:
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

vreverse_many_to_one_descriptor_class.related_manager_cls = cached_property(vreverse_many_to_one_descriptor_related_manager_cls_property)


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
        Extract all the primary key strings from the given objects.  Objects may be Versionables, or bare primary keys.
        :rtype : set
        """
        return {o.pk if isinstance(o, Model) else o for o in objects}

if VERSION[:2] < (1, 9):
    class VersionedReverseManyRelatedObjectsDescriptor(ReverseManyRelatedObjectsDescriptor):
        """
        Beside having a very long name, this class is useful when it comes to versioning the
        ReverseManyRelatedObjectsDescriptor (huhu!!). The main part is the exposure of the
        'related_manager_cls' property
        """

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
            Extract all the primary key strings from the given objects.  Objects may be Versionables, or bare primary keys.
            :rtype : set
            """
            return {o.pk if isinstance(o, Model) else o for o in objects}

        @cached_property
        def related_manager_cls(self):
            return create_versioned_forward_many_to_many_manager(
                self.field.rel.to._default_manager.__class__,
                self.field.rel
            )


    class VersionedManyRelatedObjectsDescriptor(ManyRelatedObjectsDescriptor):
        """
        Beside having a very long name, this class is useful when it comes to versioning the
        ManyRelatedObjectsDescriptor (huhu!!). The main part is the exposure of the
        'related_manager_cls' property
        """

        # via_field_name = None

        def __init__(self, related, via_field_name):
            super(VersionedManyRelatedObjectsDescriptor, self).__init__(related)
            self.via_field_name = via_field_name

        @cached_property
        def related_manager_cls(self):
            return create_versioned_forward_many_to_many_manager(
                self.related.model._default_manager.__class__,
                self.related.field.rel
            )


def create_versioned_forward_many_to_many_manager(superclass, rel, reverse=None):
    if VERSION[:2] >= (1, 9):
        many_related_manager_klass = create_forward_many_to_many_manager(superclass, rel, reverse)
    else:
        many_related_manager_klass = create_many_related_manager(superclass, rel)

    class VersionedManyRelatedManager(many_related_manager_klass):
        def __init__(self, *args, **kwargs):
            super(VersionedManyRelatedManager, self).__init__(*args, **kwargs)
            # Additional core filters are: version_start_date <= t & (version_end_date > t | version_end_date IS NULL)
            # but we cannot work with the Django core filters, since they don't support ORing filters, which
            # is a thing we need to consider the "version_end_date IS NULL" case;
            # So, we define our own set of core filters being applied when versioning
            try:
                version_start_date_field = self.through._meta.get_field('version_start_date')
                version_end_date_field = self.through._meta.get_field('version_end_date')
            except FieldDoesNotExist as e:
                if VERSION[:2] >= (1, 8):
                    fields = [f.name for f in self.through._meta.get_fields()]
                else:
                    fields = self.through._meta.get_all_field_names()
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
                        # The Django 1.7-way is preferred
                        if hasattr(self, 'target_field'):
                            fk_val = self.target_field.get_foreign_related_value(obj)[0]
                        # But the Django 1.6.x -way is supported for backward compatibility
                        elif hasattr(self, '_get_fk_val'):
                            fk_val = self._get_fk_val(obj, target_field_name)
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

# create_versioned_many_related_manager = create_versioned_forward_many_to_many_manager
