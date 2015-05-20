from django.contrib.admin import ModelAdmin
from django.forms import ModelForm









class VersionableAdmin(ModelAdmin):
    readonly_fields = ('id','identity')


    def get_readonly_fields(self, request, obj=None):
        """this method is needed so that if a subclass of VersionableAdmin has readonly_fields the
                the ones written above won't be undone"""
        if obj:
            return self.readonly_fields + ('id','identity')
        return self.readonly_fields




    def save_model(self, request, obj, form, change):
        """this method adds ability for cleanerversion objects to be added and updated from Admin"""
        if change:
            newer_object = obj.clone().clone()
            form.is_valid()
            newer_object.name = form.cleaned_data["name"]
            newer_object.age = form.cleaned_data['age']
            newer_object.save()
