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

VERSIONED_DELETE_COLLECTOR_CLASS = import_from_string(
    getattr(settings, 'VERSIONED_DELETE_COLLECTOR', 'versions.deletion.VersionedCollector'),
    'VERSIONED_DELETE_COLLECTOR'
)
