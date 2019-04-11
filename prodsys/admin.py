# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from django.db.models import Count

from .models import Task, Job

from django.utils.html import format_html
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

def JobsResend(modeladmin, request, queryset):
    queryset.update(status='failed', status_merging_mdst=None, chunk_number_merging_mdst=-1, status_x_check='no',
                    status_merging_histos=None,
                    status_merging_evntdmp=None, chunk_number_merging_evntdmp=-1, status_x_check_evntdmp='no',
                    status_castor_mdst=None, status_castor_histos=None, status_castor_evntdmp=None, 
                    date_updated=timezone.now())
        
JobsResend.short_description = 'Resend selected jobs'

def JobsResendMergingMDST(modeladmin, request, queryset):
    for q in queryset:
        jobs_list = Job.objects.all().filter(task=q.task).filter(run_number=q.run_number).update(status_merging_mdst='ready', 
                        chunk_number_merging_mdst=-1, status_x_check='no', 
                        status_merging_histos=None, 
                        status_merging_evntdmp=None, chunk_number_merging_evntdmp=-1,
                        status_castor_mdst=None, status_castor_histos=None, status_castor_evntdmp=None, 
                        date_updated=timezone.now())

JobsResendMergingMDST.short_description = 'Resend merging mdst of selected jobs'

def JobsResendMergingHIST(modeladmin, request, queryset):
    for q in queryset:
        jobs_list = Job.objects.all().filter(task=q.task).filter(run_number=q.run_number).update(status_merging_histos='ready', 
                        status_castor_mdst=None, status_castor_histos=None, status_castor_evntdmp=None, 
                        date_updated=timezone.now())

JobsResendMergingHIST.short_description = 'Resend merging hist of selected jobs'

def JobsResendXCheck(modeladmin, request, queryset):
    for q in queryset:
        jobs_list = Job.objects.all().filter(task=q.task).filter(run_number=q.run_number).update(status_x_check='no', 
                        status_castor_mdst=None, status_castor_histos=None, status_castor_evntdmp=None, 
                        date_updated=timezone.now())

JobsResendXCheck.short_description = 'Resend x-check of selected jobs'

def JobsResendMergingEVTDMP(modeladmin, request, queryset):
    for q in queryset:
        jobs_list = Job.objects.all().filter(task=q.task).filter(run_number=q.run_number).update(status_merging_evntdmp='ready',
                        chunk_number_merging_evntdmp=-1, status_x_check_evntdmp='no',
                        status_castor_evntdmp=None, 
                        date_updated=timezone.now())

JobsResendMergingEVTDMP.short_description = 'Resend merging eventdump of selected jobs'

def JobsResendArchiveLogs(modeladmin, request, queryset):
    for q in queryset:
        jobs_list = Job.objects.all().filter(task=q.task).filter(run_number=q.run_number).update(status_logs_archived='no',
                        status_logs_castor='no',
                        date_updated=timezone.now())
        task_list = Task.objects.all().filter(id=q.task.id).update(status='archive', date_updated=timezone.now())

JobsResendArchiveLogs.short_description = 'Resend archive logs of selected jobs'

class TaskAdmin(admin.ModelAdmin):
    model = Task
    list_display = ('name', 'production', 'site', 'type', 'prodslt', 'phastver', 'status', 'jobs', 
                    'merging_mdst', 'x_check_mdst', 
                    'merging_hist', 
                    'merging_evntdmp', 'x_check_evntdmp', 
                    'castor_mdst', 'castor_hist', 'castor_dump')
    search_fields = ['name', 'production', 'soft', 'status']
    
    def jobs(self, obj):
        jobs_all = 0
        jobs_staged = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_mcin = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_all = Job.objects.filter(task=obj.id).count()
            jobs_finished = jobs_all
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).values('status').annotate(total=Count('status')))
            for j in jobs_list:
                if j['status'] == u'staged':
                    jobs_staged = j['total']
                if j['status'] == u'sent':
                    jobs_sent = j['total']
                if j['status'] == u'failed':
                    jobs_failed = j['total']
                if j['status'] == u'manual check is needed':
                    jobs_mcin = j['total']
                if j['status'] == u'finished':
                    jobs_finished = j['total']
                jobs_all += j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>all: {}, staged: {}, sent: {}, failed: {}, check: {}, finished: {}</div>', jobs_all, jobs_staged, jobs_sent, jobs_failed, jobs_mcin, jobs_finished)
        
    def merging_mdst(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_merging_mdst__isnull=True).values('status_merging_mdst').annotate(total=Count('status_merging_mdst')))
            for j in jobs_list:
                if j['status_merging_mdst'] == u'ready':
                    jobs_ready = j['total']
                if j['status_merging_mdst'] == u'sent':
                    jobs_sent = j['total']
                if j['status_merging_mdst'] == u'failed':
                    jobs_failed = j['total']
                if j['status_merging_mdst'] == u'finished':
                    jobs_finished = j['total']
            
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)
    
    def x_check_mdst(self, obj):
        jobs_no = 0
        jobs_yes = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_yes = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_x_check__isnull=True).values('status_x_check').annotate(total=Count('status_x_check')))
            for j in jobs_list:
                if j['status_x_check'] == u'no':
                    jobs_no = j['total']
                if j['status_x_check'] == u'yes':
                    jobs_yes = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>no: {}, yes: {}</div>', jobs_no, jobs_yes)

    def merging_hist(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_merging_mdst__isnull=True).values('status_merging_histos').annotate(total=Count('status_merging_histos')))
            for j in jobs_list:
                if j['status_merging_histos'] == u'ready':
                    jobs_ready = j['total']
                if j['status_merging_histos'] == u'sent':
                    jobs_sent = j['total']
                if j['status_merging_histos'] == u'failed':
                    jobs_failed = j['total']
                if j['status_merging_histos'] == u'finished':
                    jobs_finished = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)
        
    def merging_evntdmp(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_merging_evntdmp__isnull=True).values('status_merging_evntdmp').annotate(total=Count('status_merging_evntdmp')))
            for j in jobs_list:
                if j['status_merging_evntdmp'] == u'ready':
                    jobs_ready = j['total']
                if j['status_merging_evntdmp'] == u'sent':
                    jobs_sent = j['total']
                if j['status_merging_evntdmp'] == u'failed':
                    jobs_failed = j['total']
                if j['status_merging_evntdmp'] == u'finished':
                    jobs_finished = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)
    
    def x_check_evntdmp(self, obj):
        jobs_no = 0
        jobs_yes = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_yes = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_x_check_evntdmp__isnull=True).values('status_x_check_evntdmp').annotate(total=Count('status_x_check_evntdmp')))
            for j in jobs_list:
                if j['status_x_check_evntdmp'] == u'no':
                    jobs_no = j['total']
                if j['status_x_check_evntdmp'] == u'yes':
                    jobs_yes = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>no: {}, yes: {}</div>', jobs_no, jobs_yes)
    
    def castor_mdst(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_castor_mdst__isnull=True).values('status_castor_mdst').annotate(total=Count('status_castor_mdst')))
            for j in jobs_list:
                if j['status_castor_mdst'] == u'ready':
                    jobs_ready = j['total']
                if j['status_castor_mdst'] == u'sent':
                    jobs_sent = j['total']
                if j['status_castor_mdst'] == u'failed':
                    jobs_failed = j['total']
                if j['status_castor_mdst'] == u'finished':
                    jobs_finished = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)
    
    def castor_hist(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_castor_histos__isnull=True).values('status_castor_histos').annotate(total=Count('status_castor_histos')))
            for j in jobs_list:
                if j['status_castor_histos'] == u'ready':
                    jobs_ready = j['total']
                if j['status_castor_histos'] == u'sent':
                    jobs_sent = j['total']
                if j['status_castor_histos'] == u'failed':
                    jobs_failed = j['total']
                if j['status_castor_histos'] == u'finished':
                    jobs_finished = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)
    
    def castor_dump(self, obj):
        jobs_ready = 0
        jobs_sent = 0
        jobs_failed = 0
        jobs_finished = 0
        if obj.status == 'done' or obj.status == 'archive' or obj.status == 'archived':
            jobs_finished = Job.objects.filter(task=obj.id).count()
        else:
            jobs_list = list(Job.objects.filter(task=obj.id).exclude(status_castor_evntdmp__isnull=True).values('status_castor_evntdmp').annotate(total=Count('status_castor_evntdmp')))
            for j in jobs_list:
                if j['status_castor_evntdmp'] == u'ready':
                    jobs_ready = j['total']
                if j['status_castor_evntdmp'] == u'sent':
                    jobs_sent = j['total']
                if j['status_castor_evntdmp'] == u'failed':
                    jobs_failed = j['total']
                if j['status_castor_evntdmp'] == u'finished':
                    jobs_finished = j['total']
        
        return format_html('<div style=white-space:nowrap;display:inline-block;>ready: {}, sent: {}, failed: {}, finished: {}</div>', jobs_ready, jobs_sent, jobs_failed, jobs_finished)

class StatusMergingMDSTListFilter(admin.SimpleListFilter):
    title = _('status merging mdst')
    parameter_name = 'status_merging_mdst'

    def lookups(self, request, model_admin):
        return (
            ('ready', _('ready')),
            ('sent', _('sent')),
            ('finished', _('finished')),
            ('failed', _('failed')),
            ('-', _('-')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'ready':
            return queryset.filter(status_merging_mdst='ready')
        if self.value() == 'sent':
            return queryset.filter(status_merging_mdst='sent')
        if self.value() == 'finished':
            return queryset.filter(status_merging_mdst='finished')
        if self.value() == 'failed':
            return queryset.filter(status_merging_mdst='failed')
        if self.value() == '-':
            return queryset.filter(status_merging_mdst__isnull=True)
        
class StatusMergingHISTListFilter(admin.SimpleListFilter):
    title = _('status merging histos')
    parameter_name = 'status_merging_histos'

    def lookups(self, request, model_admin):
        return (
            ('ready', _('ready')),
            ('sent', _('sent')),
            ('finished', _('finished')),
            ('failed', _('failed')),
            ('-', _('-')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'ready':
            return queryset.filter(status_merging_histos='ready')
        if self.value() == 'sent':
            return queryset.filter(status_merging_histos='sent')
        if self.value() == 'finished':
            return queryset.filter(status_merging_histos='finished')
        if self.value() == 'failed':
            return queryset.filter(status_merging_histos='failed')
        if self.value() == '-':
            return queryset.filter(status_merging_histos__isnull=True)

class StatusMergingDUMPListFilter(admin.SimpleListFilter):
    title = _('status merging evntdmp')
    parameter_name = 'status_merging_evntdmp'

    def lookups(self, request, model_admin):
        return (
            ('ready', _('ready')),
            ('sent', _('sent')),
            ('finished', _('finished')),
            ('failed', _('failed')),
            ('-', _('-')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'ready':
            return queryset.filter(status_merging_evntdmp='ready')
        if self.value() == 'sent':
            return queryset.filter(status_merging_evntdmp='sent')
        if self.value() == 'finished':
            return queryset.filter(status_merging_evntdmp='finished')
        if self.value() == 'failed':
            return queryset.filter(status_merging_evntdmp='failed')
        if self.value() == '-':
            return queryset.filter(status_merging_evntdmp__isnull=True)
        
class StatusXCheckFilter(admin.SimpleListFilter):
    title = _('status x check')
    parameter_name = 'status_x_check'

    def lookups(self, request, model_admin):
        return (
            ('yes', _('yes')),
            ('no', _('no')),
            ('manual check is needed', _('manual check is needed')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'yes':
            return queryset.filter(status_x_check='yes')
        if self.value() == 'no':
            return queryset.filter(status_x_check='no')
        if self.value() == 'manual check is needed':
            return queryset.filter(status_x_check='manual check is needed')
        
class StatusXCheckDUMPFilter(admin.SimpleListFilter):
    title = _('status x check evntdmp')
    parameter_name = 'status_x_check_evntdmp'

    def lookups(self, request, model_admin):
        return (
            ('yes', _('yes')),
            ('no', _('no')),
            ('manual check is needed', _('manual check is needed')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'yes':
            return queryset.filter(status_x_check_evntdmp='yes')
        if self.value() == 'no':
            return queryset.filter(status_x_check_evntdmp='no')
        if self.value() == 'manual check is needed':
            return queryset.filter(status_x_check_evntdmp='manual check is needed')

class StatusLogsDeletedFilter(admin.SimpleListFilter):
    title = _('status logs deleted')
    parameter_name = 'status_logs_deleted'

    def lookups(self, request, model_admin):
        return (
            ('yes', _('yes')),
            ('no', _('no')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'yes':
            return queryset.filter(status_logs_deleted='yes')
        if self.value() == 'no':
            return queryset.filter(status_logs_deleted='no')
        
class StatusLogsArchivedFilter(admin.SimpleListFilter):
    title = _('status logs archived')
    parameter_name = 'status_logs_archived'

    def lookups(self, request, model_admin):
        return (
            ('yes', _('yes')),
            ('no', _('no')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'yes':
            return queryset.filter(status_logs_archived='yes')
        if self.value() == 'no':
            return queryset.filter(status_logs_archived='no')

class JobAdmin(admin.ModelAdmin):
    model = Job
    list_display = ('task_name', 'file', 'number_of_events', 'run_number', 'chunk_number', 'panda_id', 'attempt', 'status', 
                    'panda_id_merging_mdst', 'attempt_merging_mdst', 'status_merging_mdst',
                    'chunk_number_merging_mdst', 'status_x_check',
                    'panda_id_merging_histos', 'attempt_merging_histos', 'status_merging_histos', 'chunk_number_merging_histos',
                    'panda_id_merging_evntdmp', 'attempt_merging_evntdmp', 'status_merging_evntdmp', 'chunk_number_merging_evntdmp', 'status_x_check_evntdmp',
                    'status_castor_mdst', 'status_castor_histos', 'status_castor_evntdmp',
                    'status_logs_deleted', 'status_logs_archived'
                    )
    list_filter = ('status', StatusMergingMDSTListFilter, StatusXCheckFilter, StatusMergingHISTListFilter, StatusMergingDUMPListFilter, 
                   StatusXCheckDUMPFilter, StatusLogsDeletedFilter, StatusLogsArchivedFilter)
    search_fields = ['task_name', 'file', 'run_number']
    
    actions = [JobsResend, JobsResendMergingMDST, JobsResendMergingHIST, JobsResendXCheck, JobsResendMergingEVTDMP, JobsResendArchiveLogs, ]

admin.site.register(Task, TaskAdmin)
admin.site.register(Job, JobAdmin)
