from django.conf import settings as django_settings
import importlib
from django import VERSION


_cache = {}


class VersionsSettings(object):
    """
    Gets a setting from django.conf.settings if set, otherwise from the defaults
    defined in this class.

    A magic accessor is used instead of just defining module-level variables because
    Django doesn't like attributes of the django.conf.settings object to be accessed in
    module scope.
    """

    defaults = {
        'VERSIONED_DELETE_COLLECTOR': 'versions.deletion.VersionedCollector',
        'VERSIONS_USE_UUIDFIELD': VERSION[:3] >= (1, 8, 3),
    }

    def __getattr__(self, name):
        try:
            return getattr(django_settings, name)
        except AttributeError:
            try:
                return self.defaults[name]
            except KeyError:
                raise AttributeError("{} object has no attribute {}".format(self.__class__, name))


settings = VersionsSettings()


def import_from_string(val, setting_name):
    """
    Attempt to import a class from a string representation.
    Based on the method of the same name in Django Rest Framework.
    """
    try:
        parts = val.split('.')
        module_path, class_name = '.'.join(parts[:-1]), parts[-1]
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except ImportError as e:
        raise ImportError("Could not import '{}' for CleanerVersion setting '{}'. {}: {}.".format(
            (val, setting_name, e.__class__.__name__, e)))


def get_versioned_delete_collector_class():
    """
    Gets the class to use for deletion collection.

    :return: class
    """
    key = 'VERSIONED_DELETE_COLLECTOR'
    try:
        cls = _cache[key]
    except KeyError:
        collector_class_string = getattr(settings, key)
        cls = import_from_string(collector_class_string, key)
        _cache[key] = cls
    return cls
