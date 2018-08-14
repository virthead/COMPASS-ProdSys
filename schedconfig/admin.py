# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from ModelAdmin import MultiDBModelAdmin

# Register your models here.

from .models import Schedconfig, Cloudconfig, Siteaccess, Users

class UsersAdmin(MultiDBModelAdmin):
    search_fields = ['name', 'dn']
    list_display = ['name', 'dn']

class SiteaccessAdmin(MultiDBModelAdmin):
    search_fields = ['pandasite', 'dn']
    list_display = ['pandasite', 'dn']

class CloudconfigAdmin(MultiDBModelAdmin):
    search_fields = ['name']

class SchedconfigAdmin(MultiDBModelAdmin):
    search_fields = ['name', 'nickname']
    list_display = ['name', 'nickname']

admin.site.register(Schedconfig, SchedconfigAdmin)
admin.site.register(Cloudconfig, CloudconfigAdmin)
admin.site.register(Siteaccess, SiteaccessAdmin)
admin.site.register(Users, UsersAdmin)
