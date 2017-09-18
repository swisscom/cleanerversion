"""
Django settings for CleanerVersion project.

For more information on this file, see
https://docs.djangoproject.com/en/dev/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/dev/ref/settings/
"""

from __future__ import absolute_import

import os

from django.utils.crypto import get_random_string

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)) + '/..')

# SECURITY WARNING: keep the secret key used in production secret!
chars = 'abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)'
SECRET_KEY = get_random_string(50, chars)

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages'],
        },
    },
]

ALLOWED_HOSTS = []

# Database
# https://docs.djangoproject.com/en/dev/ref/settings/#databases
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'sqlite.db',
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    },
    # 'postgresql': {
    #     'ENGINE': 'django.db.backends.postgresql_psycopg2',
    #     'NAME': 'psqldb',
    #     'USER': 'psqluser',
    #     'PASSWORD': 'psqlpwd',
    #     'HOST': '127.0.0.1',
    #     'PORT': '5432',
    # },
    # 'sqlite3': {
    #     'ENGINE': 'django.db.backends.sqlite3',
    #     'NAME': 'sqlite.db',
    #     'USER': '',
    #     'PASSWORD': '',
    #     'HOST': '',
    #     'PORT': '',
    # }
}

# Application definition
INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'versions',
    'versions_tests.apps.VersionsTestsConfig',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

# Internationalization
# https://docs.djangoproject.com/en/dev/topics/i18n/
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

ROOT_URLCONF = 'cleanerversion.urls'

STATIC_URL = '/static/'

VERSIONS_USE_UUIDFIELD = False
