# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-08-29 11:50
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0078_auto_20180802_0921'),
    ]

    operations = [
        migrations.RenameField(
            model_name='job',
            old_name='logs_deleted',
            new_name='status_logs_deleted',
        ),
    ]
