# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-09-10 06:46
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0090_task_use_local_generator_file'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='use_local_generator_file',
            field=models.CharField(choices=[('yes', 'yes'), ('no', 'no')], default='yes', max_length=5),
        ),
    ]