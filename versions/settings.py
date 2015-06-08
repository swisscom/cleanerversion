from django.conf import settings
import importlib


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
_defaults = {
    'VERSIONED_DELETE_COLLECTOR': 'versions.deletion.VersionedCollector'
}

def get_versioned_delete_collector_class():
    """
    Gets the class to use for deletion collection.

    :return: class
    """
    key = 'VERSIONED_DELETE_COLLECTOR'
    try:
        cls = _cache[key]
    except KeyError:
        collector_class_string = get_setting(key)
        cls = import_from_string(collector_class_string, key)
        _cache[key] = cls
    return cls

def get_setting(setting_name):
    """
    Gets a setting from django.conf.settings if set, otherwise from the defaults
    defined in this module.

    A function is used for this instead of just defining a module-level variable because
    Django doesn't like attributes of the django.conf.settings object to be accessed in
    module scope.

    :param string setting_name: setting to take from django.conf.setting
    :return: class
    """
    try:
        return getattr(settings, setting_name)
    except AttributeError:
        return _defaults[setting_name]

