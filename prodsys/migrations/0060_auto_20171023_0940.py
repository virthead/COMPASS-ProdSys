# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-10-23 09:40
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0059_auto_20171023_0829'),
    ]

    operations = [
        migrations.RenameField(
            model_name='job',
            old_name='status_castor',
            new_name='status_castor_evntdmp',
        ),
        migrations.AddField(
            model_name='job',
            name='status_castor_histos',
            field=models.CharField(default='no', max_length=50),
        ),
        migrations.AddField(
            model_name='job',
            name='status_castor_mdst',
            field=models.CharField(default='no', max_length=50),
        ),
    ]
