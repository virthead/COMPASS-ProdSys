# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-10-29 15:39
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0095_auto_20191025_0812'),
    ]

    operations = [
        migrations.AddField(
            model_name='task',
            name='parent_task_id',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
