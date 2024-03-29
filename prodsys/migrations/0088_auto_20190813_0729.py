# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-08-13 07:29
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0087_auto_20190612_1833'),
    ]

    operations = [
        migrations.AddField(
            model_name='task',
            name='sw_prefix',
            field=models.CharField(blank=True, help_text='For HPC only', max_length=300, null=True),
        ),
        migrations.AlterField(
            model_name='task',
            name='files_home',
            field=models.CharField(blank=True, help_text='For HPC only', max_length=300, null=True),
        ),
        migrations.AlterField(
            model_name='task',
            name='site',
            field=models.CharField(choices=[('CERN_COMPASS_PROD', 'CERN_COMPASS_PROD'), ('BW_COMPASS_MCORE', 'BW_COMPASS_MCORE'), ('STAMPEDE_COMPASS_MCORE', 'STAMPEDE_COMPASS_MCORE'), ('FRONTERA_COMPASS_MCORE', 'FRONTERA_COMPASS_MCORE')], default='CERN_COMPASS_PROD', max_length=50),
        ),
    ]
