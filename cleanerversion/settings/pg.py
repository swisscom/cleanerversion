"""
Django settings for testign the CleanerVersion project with TravisCI.
"""

from .base import *

# Database
# https://docs.djangoproject.com/en/dev/ref/settings/#databases
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'psqldb',
        'USER': 'psqluser',
        'PASSWORD': 'psqlpwd',
        'HOST': '127.0.0.1',
        'PORT': '5432',
    },
}
