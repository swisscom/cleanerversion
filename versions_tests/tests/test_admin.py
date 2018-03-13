from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User
from django.test import TestCase
try:
    from django.urls import reverse
except ImportError:
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

    def test_restore_old_version(self):
        new_city = self.city.clone()
        new_city.name = 'new city'
        new_city.save()
        self.assertEquals(City.objects.current_version(
            self.city, check_db=True
        ), new_city)

        self.client.force_login(self.user)
        response = self.client.post(reverse(
            'admin:versions_tests_city_change',
            args=(self.city.id, )) + 'restore/')

        self.assertEqual(response.status_code, 302)
        self.assertEquals(City.objects.all().count(), 3)
        restored_city = City.objects.current_version(self.city, check_db=True)
        self.assertEquals(restored_city.name, self.city.name)

    def test_restore_current_version(self):
        self.client.force_login(self.user)
        with self.assertRaises(ValueError):
            self.client.post(reverse('admin:versions_tests_city_change',
                             args=(self.city.identity, )) + 'restore/')
