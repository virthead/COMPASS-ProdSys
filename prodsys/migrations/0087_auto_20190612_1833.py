# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-06-12 18:33
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0086_task_status_files_deleted'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='date_updated',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
