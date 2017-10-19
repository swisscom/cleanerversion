from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User
from django.test import TestCase
try:
    from django.urls import reverse
except ModuleNotFoundError:
    # Supports backward compatibility with 1.9
    from django.core.urlresolvers import reverse

from versions.admin import VersionedAdmin
from ..models import City


class VersionedAdminTest(TestCase):
    def setUp(self):
        self.city = City.objects.create(name='city')
        self.admin = VersionedAdmin(City, AdminSite)
        self.user = User.objects.create_superuser(
            username='user', password='secret', email='super@example.com')

    def test_identity_shortener(self):
        self.assertEqual(self.admin.identity_shortener(self.city),
                         "..." + str(self.city.identity)[-12:])

    def test_history_view(self):
        self.client.force_login(self.user)
        response = self.client.get(reverse(
            'admin:versions_tests_city_history', args=(self.city.id, )))
        self.assertEqual(response.status_code, 200)
