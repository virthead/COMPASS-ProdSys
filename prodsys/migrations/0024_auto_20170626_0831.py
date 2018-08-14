# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-06-26 08:31
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0023_auto_20170626_0824'),
    ]

    operations = [
        migrations.RenameField(
            model_name='job',
            old_name='attempt_merge',
            new_name='attempt_merging',
        ),
        migrations.RenameField(
            model_name='job',
            old_name='panda_id_merge',
            new_name='panda_id_merging',
        ),
        migrations.AddField(
            model_name='job',
            name='status_merging',
            field=models.CharField(blank=True, max_length=300, null=True),
        ),
    ]
