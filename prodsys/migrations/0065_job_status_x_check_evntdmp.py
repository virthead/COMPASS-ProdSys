# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-11-04 12:43
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0064_job_chunk_number_merging_evntdmp'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='status_x_check_evntdmp',
            field=models.CharField(default='no', max_length=50),
        ),
    ]
