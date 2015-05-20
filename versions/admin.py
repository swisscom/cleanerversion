from django.contrib.admin import ModelAdmin
from django.forms import ModelForm









class VersionableAdmin(ModelAdmin):
    readonly_fields = ('id','identity')



    def get_readonly_fields(self, request, obj=None):
        """this method is needed so that if a subclass of VersionableAdmin has readonly_fields the
                the ones written above won't be undone"""
        if obj:
            return self.readonly_fields + ('id','identity','is_current')
        return self.readonly_fields

    def get_list_display(self, request):
        """this method determines which fields go in the changelist"""
        list_display = super(VersionableAdmin,self).get_list_display(request)
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
