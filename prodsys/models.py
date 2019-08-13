# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.template.defaultfilters import default

# Create your models here.

class Task(models.Model):
    type_choices = (
        ('test production', 'test production'),
        ('mass production', 'mass production'),
        ('technical production', 'technical production'),
        ('DDD filtering', 'DDD filtering'),
        ('MC generation', 'MC generation'),
        ('MC reconstruction', 'MC reconstruction')
        )
    
    site_choices = (
        ('CERN_COMPASS_PROD', 'CERN_COMPASS_PROD'),
        ('BW_COMPASS_MCORE', 'BW_COMPASS_MCORE'),
        ('STAMPEDE_COMPASS_MCORE', 'STAMPEDE_COMPASS_MCORE'),
        ('FRONTERA_COMPASS_MCORE', 'FRONTERA_COMPASS_MCORE')
        )
    
    status_choices = (
        ('draft', 'draft'),
        ('ready', 'ready'),
        ('jobs ready', 'jobs ready'),
        ('send', 'send'),
        ('running', 'running'),
        ('paused', 'paused'),
        ('cancelled', 'cancelled'),
        ('done', 'done'),
        ('archive', 'archive'),
        ('archiving', 'archiving'),
        ('archived', 'archived'),
        )
    
    template_choices = (
        ('template.opt', 'template.opt'),
        ('template_mu+.opt', 'template_mu+.opt'),
        ('template_mu-.opt', 'template_mu-.opt'),
        )
    
    files_source_choices = (
        ('files list', 'files list'),
        ('runs list', 'runs list'),
        )
    
    name = models.CharField(max_length=300)
    type = models.CharField(choices=type_choices, max_length=50, default='test production')
    site = models.CharField(choices=site_choices, max_length=50, default='CERN_COMPASS_PROD')
    home = models.CharField(max_length=300, default='/cvmfs/compass.cern.ch/', help_text='mandatory leading and trailing slash')
    path = models.CharField(max_length=300, help_text='no leading slash, mandatory trailing slash')
    soft = models.CharField(max_length=300, help_text='no leading and trailing slash')
    production = models.CharField(max_length=50)
    year = models.PositiveSmallIntegerField(null=True, blank=True)
    period = models.CharField(max_length=50, null=True, blank=True)
    prodslt = models.IntegerField(default=0)
    phastver = models.IntegerField(default=7)
    template = models.CharField(choices=template_choices, max_length=50, default='template.opt')
    files_source = models.CharField(choices=files_source_choices, max_length=50, default='files list')
    filelist = models.TextField(null=True)
    files_home = models.CharField(max_length=300, null=True, blank=True, help_text='For HPC only')
    sw_prefix = models.CharField(max_length=300, null=True, blank=True, help_text='For HPC only')
    max_attempts = models.IntegerField(default=5)
    status = models.CharField(max_length=300, choices=status_choices, default='draft')
    date_added = models.DateTimeField()
    date_updated = models.DateTimeField(null=True, blank=True)
    comment = models.TextField(null=True, blank=True)
    date_processing_start = models.DateTimeField(null=True, blank=True)
    date_processing_finish = models.DateTimeField(null=True, blank=True)
    status_files_deleted = models.CharField(max_length=5, default='no')
    
    def __unicode__(self):
        return self.name

    
class Job(models.Model):
    status_choices = (
        ('defined', 'defined'),
        ('staging', 'staging'),
        ('staged', 'staged'),
        ('sent', 'sent'),
        ('running', 'running'),
        ('failed', 'failed'),
        ('paused', 'paused'),
        ('cancelled', 'cancelled'),
        ('finished', 'finished'),
        ('manual check is needed', 'manual check is needed')
        )
    
    task = models.ForeignKey(Task, on_delete=models.DO_NOTHING)
    file = models.CharField(max_length=300)
    number_of_events = models.IntegerField(default=-1)
    number_of_events_attempt = models.IntegerField(default=0)
    attempt = models.IntegerField(default=0)
    status = models.CharField(max_length=50, choices=status_choices, default='defined')
    run_number = models.IntegerField(default=0)
    chunk_number = models.IntegerField(default=0)
    panda_id = models.BigIntegerField(default=0)
    date_added =  models.DateTimeField()
    date_updated = models.DateTimeField()
    status_merging = models.CharField(max_length=50, null=True, blank=True)
    panda_id_merging_mdst = models.BigIntegerField(default=0)
    attempt_merging_mdst = models.IntegerField(default=0)
    status_merging_mdst = models.CharField(max_length=50, null=True, blank=True)
    chunk_number_merging_mdst = models.IntegerField(default=-1)
    status_castor_mdst = models.CharField(max_length=50, null=True, blank=True)
    attempt_castor_mdst = models.IntegerField(default=0)
    status_x_check = models.CharField(max_length=50, default='no')
    panda_id_merging_histos = models.BigIntegerField(default=0)
    attempt_merging_histos = models.IntegerField(default=0)
    status_merging_histos = models.CharField(max_length=50, null=True, blank=True)
    chunk_number_merging_histos = models.IntegerField(default=-1)
    status_castor_histos = models.CharField(max_length=50, null=True, blank=True)
    attempt_castor_histos = models.IntegerField(default=0)
    panda_id_merging_evntdmp = models.BigIntegerField(default=0)
    attempt_merging_evntdmp = models.IntegerField(default=0)
    status_merging_evntdmp = models.CharField(max_length=50, null=True, blank=True)
    chunk_number_merging_evntdmp = models.IntegerField(default=-1)
    status_x_check_evntdmp = models.CharField(max_length=50, default='no')
    status_castor_evntdmp = models.CharField(max_length=50, null=True, blank=True)
    attempt_castor_evntdmp = models.IntegerField(default=0)
    status_logs_deleted = models.CharField(max_length=5, default='no')
    status_logs_archived = models.CharField(max_length=5, default='no')
    status_logs_castor = models.CharField(max_length=50, null=True, blank=True)
    attempt_logs_castor = models.IntegerField(default=0)
    
    def __unicode__(self):
        return self.file
    
    class Meta:
        unique_together = (('task', 'file'),)
