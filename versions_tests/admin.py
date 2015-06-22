from django.contrib import admin

from versions.admin import VersionedAdmin
from versions_tests.models import City, Student, Observer, Professor, Subject, Teacher, Team, Player, Award, ChainStore, \
    Classroom, WineDrinker, WineDrinkerHat, Wine

admin.site.register(
    [City, Student, Subject, Teacher, Team, Player, Award, Observer, ChainStore, Professor, Classroom,
     WineDrinker], VersionedAdmin)
admin.site.register([Wine, WineDrinkerHat], admin.ModelAdmin)
