# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-08-25 11:27
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0040_auto_20170816_1525'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='job',
            name='chunk_number_merging_mdst',
        ),
    ]
