from django.contrib.admin import ModelAdmin
from django.forms import ModelForm









class VersionableAdmin(ModelAdmin):
    readonly_fields = ('id','identity')
    list_display_show_identity = True
    list_display_show_end_date = True
    list_display_show_start_date = True

    def get_readonly_fields(self, request, obj=None):
        """this method is needed so that if a subclass of VersionableAdmin has readonly_fields the
                the ones written above won't be undone"""
        if obj:
            return self.readonly_fields + ('id','identity','is_current')
        return self.readonly_fields



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


    def save_model(self, request, obj, form, change):
        """this method adds ability for cleanerversion objects to be added and updated from Admin"""
        if change:
            newer_object = obj.clone().clone()
            form.is_valid()
            newer_object.name = form.cleaned_data["name"]
            newer_object.age = form.cleaned_data['age']
            newer_object.save()


    def is_current(self, obj):
        return obj.is_current

    is_current.boolean = True
    is_current.short_description = "Current"

    def identity_shortener(self,obj):
        return obj.identity[:7]

    identity_shortener.boolean = False
    identity_shortener.short_discription = "Short Identity"