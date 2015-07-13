from django import VERSION
from django.db.models.deletion import (
    attrgetter, signals, six, sql, transaction,
    CASCADE,
    Collector,
)
import versions.models


class VersionedCollector(Collector):
    """
    A Collector that can be used to collect and delete Versionable objects.
    The delete operation for Versionable objects is Versionable._delete_at,
    which does not delete the record, it updates it's version_end_date to be
    the timestamp passed to the delete() method.

    Since non-versionable and versionable objects can be related, the delete()
    method handles both of them.  The standard Django behaviour is kept for
    non-versionable objects.  For versionable objects, no pre/post-delete signals
    are sent.  No signal is sent because the object is not being removed from the
    database.  If you want the standard signals to be sent, or custom signals,
    create a subclass of this class and override versionable_pre_delete() and/or
    versionable_post_delete(), and in your settings file specify the dotted path
    to your custom class as a string, e.g.:
    VERSIONED_DELETE_COLLECTOR_CLASS = 'myapp.deletion.CustomVersionedCollector'
    """

    def can_fast_delete(self, objs, from_field=None):
        """Do not fast delete anything"""
        return False

    def is_versionable(self, model):
        return hasattr(model, 'VERSION_IDENTIFIER_FIELD') and hasattr(model, 'OBJECT_IDENTIFIER_FIELD')

    def delete(self, timestamp):
        # sort instance collections
        for model, instances in self.data.items():
            self.data[model] = sorted(instances, key=attrgetter("pk"))

        # if possible, bring the models in an order suitable for databases that
        # don't support transactions or cannot defer constraint checks until the
        # end of a transaction.
        self.sort()

        with transaction.atomic(using=self.using, savepoint=False):
            # send pre_delete signals, but not for versionables
            for model, obj in self.instances_with_model():
                if not model._meta.auto_created:
                    if self.is_versionable(model):
                        # By default, no signal is sent when deleting a Versionable.
                        self.versionable_pre_delete(obj, timestamp)
                    else:
                        signals.pre_delete.send(
                            sender=model, instance=obj, using=self.using
                        )

            # do not do fast deletes
            if self.fast_deletes:
                raise RuntimeError("No fast_deletes should be present; they are not safe for Versionables")

            # update fields
            for model, instances_for_fieldvalues in six.iteritems(self.field_updates):
                id_map = {}
                for (field, value), instances in six.iteritems(instances_for_fieldvalues):
                    if self.is_versionable(model):
                        # Do not set the foreign key to null, which can be the behaviour (depending on DB backend)
                        # for the default CASCADE on_delete method.
                        # In the case of a SET.. method, clone before changing the value (if it hasn't already been
                        # cloned)
                        updated_instances = set()
                        if not(isinstance(field, versions.models.VersionedForeignKey) and field.rel.on_delete == CASCADE):
                            for instance in instances:
                                # Clone before updating
                                cloned = id_map.get(instance.pk, None)
                                if not cloned:
                                    cloned = instance.clone()
                                id_map[instance.pk] = cloned
                                updated_instances.add(cloned)
                                #TODO: instance should get updated with new values from clone ?
                        instances_for_fieldvalues[(field, value)] = updated_instances

                # Replace the instances with their clones in self.data, too
                model_instances = self.data.get(model, {})
                for index, instance in enumerate(model_instances):
                    cloned = id_map.get(instance.pk)
                    if cloned:
                        self.data[model][index] = cloned

                query = sql.UpdateQuery(model)
                for (field, value), instances in six.iteritems(instances_for_fieldvalues):
                    if instances:
                        query.update_batch([obj.pk for obj in instances], {field.name: value}, self.using)

            # reverse instance collections
            for instances in six.itervalues(self.data):
                instances.reverse()

            # delete instances
            for model, instances in six.iteritems(self.data):
                if self.is_versionable(model):
                    for instance in instances:
                        self.versionable_delete(instance, timestamp)
                        if not model._meta.auto_created:
                            # By default, no signal is sent when deleting a Versionable.
                            self.versionable_post_delete(instance, timestamp)
                else:
                    query = sql.DeleteQuery(model)
                    pk_list = [obj.pk for obj in instances]
                    query.delete_batch(pk_list, self.using)

                    if not model._meta.auto_created:
                        for obj in instances:
                            signals.post_delete.send(
                                sender=model, instance=obj, using=self.using
                            )

        # update collected instances
        for model, instances_for_fieldvalues in six.iteritems(self.field_updates):
            for (field, value), instances in six.iteritems(instances_for_fieldvalues):
                for obj in instances:
                    setattr(obj, field.attname, value)

        # Do not set Versionable object ids to None, since they still do have an id.
        # Instead, set their version_end_date.
        for model, instances in six.iteritems(self.data):
            is_versionable = self.is_versionable(model)
            for instance in instances:
                if is_versionable:
                    setattr(instance, 'version_end_date', timestamp)
                else:
                    setattr(instance, model._meta.pk.attname, None)

    def related_objects(self, related, objs):
        """
        Gets a QuerySet of current objects related to ``objs`` via the relation ``related``.

        """
        if VERSION >= (1, 8):
            related_model = related.related_model
        else:
            related_model = related.model
        manager = related_model._base_manager
        if issubclass(related_model, versions.models.Versionable):
            manager = manager.current
        return manager.using(self.using).filter(
            **{"%s__in" % related.field.name: objs}
        )

    def versionable_pre_delete(self, instance, timestamp):
        """
        Override this method to implement custom behaviour.  By default, does nothing.

        :param Versionable instance:
        :param datetime timestamp:
        """
        pass

    def versionable_post_delete(self, instance, timestamp):
        """
        Override this method to implement custom behaviour.  By default, does nothing.

        :param Versionable instance:
        :param datetime timestamp:
        """
        pass

    def versionable_delete(self, instance, timestamp):
        """
        Soft-deletes the instance, setting it's version_end_date to timestamp.

        Override this method to implement custom behaviour.

        :param Versionable instance:
        :param datetime timestamp:
        """
        instance._delete_at(timestamp, using=self.using)
