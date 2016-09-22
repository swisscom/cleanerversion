from django.contrib.admin.widgets import AdminSplitDateTime
from django.contrib.admin.checks import ModelAdminChecks
from django.contrib import admin
from django.contrib.admin.utils import unquote
from django import forms
from django.contrib.admin.templatetags.admin_static import static
from django.http import HttpResponseRedirect
from django.conf.urls import url
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from django.contrib.admin.options import get_content_type_for_model
from django.utils.encoding import force_text
from django.utils.text import capfirst
from django.template.response import TemplateResponse
from datetime import datetime

class DateTimeFilterForm(forms.Form):
    def __init__(self, request, *args, **kwargs):
        field_name = kwargs.pop('field_name')
        super(DateTimeFilterForm, self).__init__(*args, **kwargs)
        self.request = request
        self.fields['%s_as_of' % field_name] = forms.SplitDateTimeField(
            label='',
            input_time_formats=['%I:%M %p', '%H:%M:%S'],
            widget=AdminSplitDateTime(
                attrs={'placeholder': 'as of date and time'}
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

            js = ['calendar.js', 'admin/DateTimeShortcuts.js', ]
            css = ['widgets.css', ]

            return forms.Media(
                js=[static('admin/js/%s' % path) for path in js],
                css={'all': [static('admin/css/%s' % path) for path in css]}
            )


class DateTimeFilter(admin.FieldListFilter):
    template = 'versions/datetimefilter.html'
    title = 'DateTime filter'

    def __init__(self, field, request, params, model, model_admin, field_path):
        self.field_path = field_path
        self.lookup_kwarg_as_ofdate = '%s_as_of_0' % field_path
        self.lookup_kwarg_as_oftime = '%s_as_of_1' % field_path
        super(DateTimeFilter, self).__init__(field, request, params, model, model_admin, field_path)
        self.form = self.get_form(request)

    def choices(self, cl):
        return []

    def expected_parameters(self):
        return [self.lookup_kwarg_as_ofdate, self.lookup_kwarg_as_oftime]

    def get_form(self, request):
        return DateTimeFilterForm(request, data=self.used_parameters, field_name=self.field_path)

    def queryset(self, request, queryset):
        fieldname = '%s_as_of' % self.field_path
        if self.form.is_valid() and fieldname in self.form.cleaned_data:
            filter_params = self.form.cleaned_data.get(fieldname, datetime.utcnow())
            return queryset.as_of(filter_params)
        else:
            return queryset


class IsCurrentFilter(admin.SimpleListFilter):
    title = 'Is Current filter'
    parameter_name = 'is_current'

    def __init__(self, request, params, model, model_admin):
        self.lookup_kwarg = 'is_current'
        self.lookup_val = request.GET.get(self.lookup_kwarg, None)
        super(IsCurrentFilter, self).__init__(request, params, model, model_admin)

    def lookups(self, request, model_admin):
        return [(None, 'All'), ('1', 'Current'), ]

    def choices(self, cl):
        for lookup, title in self.lookup_choices:
            yield {
                'selected': self.value() == lookup,
                'query_string': cl.get_query_string({
                    self.parameter_name: lookup,
                }, []),
                'display': title,
            }

    def queryset(self, request, queryset):
        if self.lookup_val:
            return queryset.as_of()
        else:
            return queryset


class VersionedAdminChecks(ModelAdminChecks):
    def _check_exclude(self, cls, model):
        """
        Required to suppress error about exclude not being a tuple since we are using @property to dynamically change it
        """
        return []


class VersionedAdmin(admin.ModelAdmin):
    """
    VersionedAdmin provides functionality to allow cloning of objects when saving, not cloning if a mistake was
    made, and making a current object historical by deleting it
    """

    VERSIONED_EXCLUDE = ['id', 'identity', 'version_end_date', 'version_start_date', 'version_birth_date']

    # These are so that the subclasses can overwrite these attributes
    # to have the identity, end date, or start date column not show
    list_display_show_identity = True
    list_display_show_end_date = True
    list_display_show_start_date = True
    ordering = []

    checks_class = VersionedAdminChecks

    def get_readonly_fields(self, request, obj=None):
        """
        This is required a subclass of VersionedAdmin has readonly_fields ours won't be undone
        """
        if obj:
            return list(self.readonly_fields) + ['id', 'identity', 'is_current']
        return self.readonly_fields

    def get_ordering(self, request):
        return ['identity', '-version_start_date', ] + self.ordering

    def get_list_display(self, request):
        """
        This method determines which fields go in the changelist
        """

        # Force cast to list as super get_list_display could return a tuple
        list_display = list(super(VersionedAdmin, self).get_list_display(request))

        # Preprend the following fields to list display
        if self.list_display_show_identity:
            list_display = ['identity_shortener', ] + list_display

        # Append the following fields to list display
        if self.list_display_show_start_date:
            list_display += ['version_start_date', ]
        if self.list_display_show_end_date:
            list_display += ['version_end_date', ]

        return list_display + ['is_current', ]

    def get_list_filter(self, request):
        """
        Adds versionable custom filtering ability to changelist
        """
        list_filter = super(VersionedAdmin, self).get_list_filter(request)
        return list(list_filter) + [('version_start_date', DateTimeFilter), IsCurrentFilter]

    def restore(self,request, *args, **kwargs):
        """
        View for restoring object from change view
        """
        paths = request.path_info.split('/')
        object_id_index = paths.index("restore") - 1
        object_id = paths[object_id_index]

        obj = super(VersionedAdmin,self).get_object(request, object_id)
        obj.restore()
        admin_wordIndex = object_id_index - 3
        path = "/%s" % ("/".join(paths[admin_wordIndex:object_id_index]))
        return HttpResponseRedirect(path)

    def will_not_clone(self, request, *args, **kwargs):
        """
        Add save but not clone capability in the changeview
        """
        paths = request.path_info.split('/')
        index_of_object_id = paths.index("will_not_clone")-1
        object_id = paths[index_of_object_id]
        self.change_view(request, object_id)

        admin_wordInUrl = index_of_object_id-3
        # This gets the adminsite for the app, and the model name and joins together with /
        path = '/' + '/'.join(paths[admin_wordInUrl:index_of_object_id])
        return HttpResponseRedirect(path)

    @property
    def exclude(self):
        """
        Custom descriptor for exclude since there is no get_exclude method to be overridden
        """
        exclude = self.VERSIONED_EXCLUDE

        if super(VersionedAdmin, self).exclude is not None:
            # Force cast to list as super exclude could return a tuple
            exclude = list(super(VersionedAdmin, self).exclude) + exclude

        return exclude

    def get_object(self, request, object_id, from_field=None):
        """
        our implementation of get_object allows for cloning when updating an object, not cloning when the button
        'save but not clone' is pushed and at no other time will clone be called
        """
        obj = super(VersionedAdmin, self).get_object(request, object_id)  # from_field breaks in 1.7.8
        # Only clone if update view as get_object() is also called for change, delete, and history views
        if request.method == 'POST' and obj and obj.is_latest and 'will_not_clone' not in request.path \
                and 'delete' not in request.path and 'restore' not in request.path:
            obj = obj.clone()

        return obj

    def history_view(self, request, object_id, extra_context=None):
        "The 'history' admin view for this model."
        from django.contrib.admin.models import LogEntry
        # First check if the user can see this history.
        model = self.model
        obj = get_object_or_404(self.get_queryset(request), pk=unquote(object_id))
        if not self.has_change_permission(request, obj):
            raise PermissionDenied

        # Then get the history for this object.
        opts = model._meta
        app_label = opts.app_label
        action_list = LogEntry.objects.filter(
            object_id=unquote(obj.identity),  # this is the change for our override;
            content_type=get_content_type_for_model(model)
        ).select_related().order_by('action_time')

        ctx = self.admin_site.each_context(request)

        context = dict(ctx,
                       title=('Change history: %s') % force_text(obj),
                       action_list=action_list,
                       module_name=capfirst(force_text(opts.verbose_name_plural)),
                       object=obj,
                       opts=opts,
                       preserved_filters=self.get_preserved_filters(request),
                       )
        context.update(extra_context or {})
        return TemplateResponse(request, self.object_history_template or [
            "admin/%s/%s/object_history.html" % (app_label, opts.model_name),
            "admin/%s/object_history.html" % app_label,
            "admin/object_history.html"
        ], context, current_app=self.admin_site.name)

    def get_urls(self):
        """
        Appends the custom will_not_clone url to the admin site
        """
        not_clone_url = [url(r'^(.+)/will_not_clone/$', admin.site.admin_view(self.will_not_clone))]
        restore_url = [url(r'^(.+)/restore/$', admin.site.admin_view(self.restore))]
        return not_clone_url + restore_url + super(VersionedAdmin, self).get_urls()

    def is_current(self, obj):
        return obj.is_current

    is_current.boolean = True
    is_current.short_description = "Current"

    def identity_shortener(self, obj):
        """
        Shortens identity to the last 12 characters
        """
        return "..." + obj.identity[-12:]

    identity_shortener.boolean = False
    identity_shortener.short_description = "Short Identity"

    class Media():
        # This supports dynamically adding 'Save without cloning' button: http://bit.ly/1T2fGOP
        js = ('js/admin_addon.js',)
