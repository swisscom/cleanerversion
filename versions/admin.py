from django.contrib.admin.widgets import AdminSplitDateTime
from django.contrib.admin.checks import ModelAdminChecks
from django.contrib import admin
from django import forms
from django.contrib.admin.templatetags.admin_static import static
from django.http import HttpResponseRedirect
from django.conf.urls import patterns, url

    #necessary right now because of the error about exclude not being a tuple since we are using @property to dynamicall
    #change it
class VAdminChecks(ModelAdminChecks):
    def _check_exclude(self, cls, model):
        return []





class DateTimeForm(forms.Form):
    def __init__(self, request, *args, **kwargs):
        field_name = kwargs.pop('field_name')
        super(DateTimeForm, self).__init__(*args, **kwargs)
        self.request = request
        self.fields['%s_as_of' % field_name] = forms.SplitDateTimeField(
            label='',
            input_time_formats=['%I:%M %p', '%H:%M:%S'],
            widget=AdminSplitDateTime(
                attrs={'placeholder': ('as of date and time')}
            ),
            localize=True,
            required=True
        )


    @property
    def media(self):
        try:
            if getattr(self.request, 'daterange_filter_media_included'):
                return forms.Media()
        except AttributeError:
            setattr(self.request, 'daterange_filter_media_included', True)

            js = ["calendar.js", "admin/DateTimeShortcuts.js"]
            css = ['widgets.css']

            return forms.Media(
                js=[static("admin/js/%s" % path) for path in js],
                css={'all': [static("admin/css/%s" % path) for path in css]}
            )


class DateTimeFilter(admin.FieldListFilter):
    template = 'datetimefilter.html'

    def __init__(self, field, request, params, model, model_admin, field_path):
        self.lookup_kwarg_as_ofdate = '%s_as_of_0' % field_path
        self.lookup_kwarg_as_oftime = '%s_as_of_1' % field_path
        super(DateTimeFilter, self).__init__(field, request, params, model, model_admin, field_path)
        self.form = self.get_form(request)


    def choices(self, cl):
        return []

    def expected_parameters(self):
        return [self.lookup_kwarg_as_ofdate,self.lookup_kwarg_as_oftime]

    def get_form(self, request):
        return DateTimeForm(request, data=self.used_parameters,
                             field_name=self.field_path)

    def queryset(self, request, queryset):
        if self.form.is_valid() and self.form.cleaned_data.values()[0] is not None:
            filter_params = self.form.cleaned_data.values()[0]
            return queryset.as_of(filter_params)
        else:
            return queryset



class VersionedAdmin(admin.ModelAdmin):
    #actions = ['delete_model']
    #these are so that the subclasses can overwrite these attributes
    # to have the identity, end date,or start date column not show
    list_display_show_identity = True
    list_display_show_end_date = True
    list_display_show_start_date = True
    ordering = []
    #new attribute for working with self.exclude method so that the subclass can specify more fields to exclude
    _exclude = None
    checks_class = VAdminChecks
    change_form_template = 'admin/versions/changeform.html'

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

    def get_list_filter(self, request):
        list_filter = super(VersionedAdmin,self).get_list_filter(request)
        return list_filter + (('version_start_date',DateTimeFilter),)


    def will_not_clone(self, request, *args, **kwargs):
        """needed for save but not clone capability, this is a view"""
        paths = request.path_info.split('/')

        object_id = paths[3]
        self.change_view(request,object_id)
        #this gets the adminsite for the app, and the model name and joins together with /
        path = '/'+'/'.join(paths[1:3])
        return HttpResponseRedirect(path)

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
        if request.method == "POST" and obj and obj.is_latest and not 'will_not_clone' in request.path and not 'delete' in request.path:
            obj = obj.clone()

        return obj


    def get_urls(self):
        not_clone_url = [url(r'^(.+)/will_not_clone/$',admin.site.admin_view(self.will_not_clone),name="willNotClone")]
        return not_clone_url + super(VersionedAdmin,self).get_urls()




    def is_current(self, obj):
        return obj.is_current

    is_current.boolean = True
    is_current.short_description = "Current"

    def identity_shortener(self,obj):
        return "..."+obj.identity[-12:]

    identity_shortener.boolean = False
    identity_shortener.short_description = "Short Identity"





