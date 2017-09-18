import datetime

from django.utils.timezone import utc


def get_utc_now():
    return datetime.datetime.utcnow().replace(tzinfo=utc)
