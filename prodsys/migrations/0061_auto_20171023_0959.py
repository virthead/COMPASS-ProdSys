# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-10-23 09:59
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0060_auto_20171023_0940'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='attempt_castor_evntdmp',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='job',
            name='attempt_castor_histos',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='job',
            name='attempt_castor_mdst',
            field=models.IntegerField(default=0),
        ),
    ]
