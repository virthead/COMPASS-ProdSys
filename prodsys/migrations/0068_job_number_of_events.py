# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-12-14 13:48
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0067_task_files_source'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='number_of_events',
            field=models.IntegerField(default=0),
        ),
    ]
