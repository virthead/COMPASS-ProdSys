# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-08-16 15:23
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0037_auto_20170816_1514'),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name='job',
            name='prodsys_job_task_id_853b0d_idx',
        ),
    ]
