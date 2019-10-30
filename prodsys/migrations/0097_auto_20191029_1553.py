# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2019-10-29 15:53
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0096_task_parent_task_id'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='parent_task_id',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='prodsys.Task'),
        ),
    ]