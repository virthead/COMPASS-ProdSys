# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-06-26 11:14
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0025_job_chunk_number_merging'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='chunk_number_merging',
            field=models.IntegerField(default=0),
        ),
    ]
