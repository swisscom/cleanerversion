from django.conf import settings
from django.utils import importlib


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

_cache = {}
def get_versioned_delete_collector_class():
    """
    Gets the class to use for deletion collection.

    This is done as a method instead of just defining a module-level variable because
    Django doesn't like attributes of the django.conf.settings object to be accessed
    in top-level module scope.

    :return: class
    """
    key = 'VERSIONED_DELETE_COLLECTOR'
    try:
        cls = _cache[key]
    except KeyError:
        cls = import_from_string(getattr(settings, key, 'versions.deletion.VersionedCollector'), key)
        _cache[key] = cls
    return cls
