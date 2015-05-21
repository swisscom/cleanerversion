from django.contrib.admin import ModelAdmin
from django.contrib.admin.checks import ModelAdminChecks




    #necessary right now because of the error about exclude not being a tuple since we are using @property to dynamicall
    #change it
class VAdminChecks(ModelAdminChecks):
    def _check_exclude(self, cls, model):
        return []








class VersionableAdmin(ModelAdmin):
    readonly_fields = ('id','identity')
    list_display_show_identity = True
    list_display_show_end_date = True
    list_display_show_start_date = True
    ordering = []
    checks_class = VAdminChecks

    def get_readonly_fields(self, request, obj=None):
        """this method is needed so that if a subclass of VersionableAdmin has readonly_fields the
                the ones written above won't be undone"""
        if obj:
            return self.readonly_fields + ('id','identity','is_current')
        return self.readonly_fields

    def get_ordering(self, request):
        return ['identity', '-version_start_date'] + self.ordering

    def get_list_display(self, request):
        """this method determines which fields go in the changelist"""
        if self.list_display_show_identity:
            list_display = ['identity_shortener']
        else:
            list_display = []

        list_display += super(VersionableAdmin,self).get_list_display(request)

        if self.list_display_show_start_date:
            list_display += ['version_start_date']

        if self.list_display_show_end_date:
            list_display += ['version_end_date']

        #force cast to list as super get_list_display could return a tuple
        return list(list_display) + ['is_current']

    @property
    def exclude(self):
        """need a getter for exclude since there is no get_exclude method to be overridden"""
        VERSIONABLE_EXCLUDE = ['id', 'identity', 'version_end_date', 'version_start_date', 'version_birth_date']
        if self.exclude is None:
            return VERSIONABLE_EXCLUDE
        else:
            return list(self.exclude) + VERSIONABLE_EXCLUDE

    def get_form(self, request, obj=None, **kwargs):
        if request.method == 'POST' and obj is not None:
            obj = obj.clone()
        form = super(VersionableAdmin,self).get_form(request,obj,**kwargs)
        return form

    def is_current(self, obj):
        return obj.is_current

    is_current.boolean = True
    is_current.short_description = "Current"

    def identity_shortener(self,obj):
        return "..."+obj.identity[-12:]

    identity_shortener.boolean = False
    identity_shortener.short_description = "Short Identity"