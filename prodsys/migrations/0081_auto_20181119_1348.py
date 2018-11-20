# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-11-19 13:48
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0080_auto_20180829_1157'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='status',
            field=models.CharField(choices=[('draft', 'draft'), ('ready', 'ready'), ('jobs ready', 'jobs ready'), ('send', 'send'), ('running', 'running'), ('paused', 'paused'), ('cancelled', 'cancelled'), ('done', 'done'), ('archive', 'archive'), ('archiving', 'archiving'), ('archived', 'archived')], default='draft', max_length=300),
        ),
    ]
