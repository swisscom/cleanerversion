
from django.contrib.admin.checks import ModelAdminChecks
from django.contrib import admin


admin.site.disable_action('delete_selected')

    #necessary right now because of the error about exclude not being a tuple since we are using @property to dynamicall
    #change it
class VAdminChecks(ModelAdminChecks):
    def _check_exclude(self, cls, model):
        return []








class VersionedAdmin(admin.ModelAdmin):
    actions = ['delete_model']
    #these are so that the subclasses can overwrite these attributes
    # to have the identity, end date,or start date column not show
    list_display_show_identity = True
    list_display_show_end_date = True
    list_display_show_start_date = True
    ordering = []
    #new attribute for working with self.exclude method so that the subclass can specify more fields to exclude
    _exclude = None
    checks_class = VAdminChecks

    def get_readonly_fields(self, request, obj=None):
        """this method is needed so that if a subclass of VersionedAdmin has readonly_fields the
                the ones written above won't be undone"""
        if obj:
            return self.readonly_fields + ('id','identity','is_current')
        return self.readonly_fields

    def get_ordering(self, request):
        return ['identity', '-version_start_date'] + self.ordering

    def get_list_display(self, request):
        """this method determines which fields go in the changelist"""

        list_display = super(VersionedAdmin,self).get_list_display(request)
        #force cast to list as super get_list_display could return a tuple
        list_display = list(list_display)
        if self.list_display_show_identity:
            list_display = ['identity_shortener'] + list_display

        if self.list_display_show_start_date:
            list_display += ['version_start_date']

        if self.list_display_show_end_date:
            list_display += ['version_end_date']

        return list_display + ['is_current']

    @property
    def exclude(self):
        """need a getter for exclude since there is no get_exclude method to be overridden"""
        VERSIONED_EXCLUDE = ['id', 'identity', 'version_end_date', 'version_start_date', 'version_birth_date']
        #creating _exclude so that self.exclude doesn't need to be called prompting recursion, and subclasses
        #have a way to change what is excluded
        if self._exclude is None:
            return VERSIONED_EXCLUDE
        else:
            return list(self._exclude) + VERSIONED_EXCLUDE


    def get_object(self, request, object_id, from_field=None):
        obj = super(VersionedAdmin, self).get_object(request, object_id) #from_field breaks in 1.7.8
        #the things tested for in the if are for Updating an object; get_object is called three times: changeview, delete, and history
        if request.method == "POST" and obj and obj.is_latest and not 'delete' in request.path:
            obj = obj.clone()

        return obj

    def delete_model(self, request, queryset):
        """needed for bulk 'delete' of versionable objects"""
        for object in queryset:
            object.delete()




    def is_current(self, obj):
        return obj.is_current

    is_current.boolean = True
    is_current.short_description = "Current"

    def identity_shortener(self,obj):
        return "..."+obj.identity[-12:]

    identity_shortener.boolean = False
    identity_shortener.short_description = "Short Identity"