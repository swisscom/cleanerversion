from django.contrib.admin import ModelAdmin



class VersionableAdmin(ModelAdmin):
    readonly_fields = ('id','identity')