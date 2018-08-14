# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-09-06 20:15
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0046_auto_20170906_1333'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='task',
            name='phastVer',
        ),
        migrations.RemoveField(
            model_name='task',
            name='prodSlt',
        ),
        migrations.AddField(
            model_name='task',
            name='phastver',
            field=models.IntegerField(default=7),
        ),
        migrations.AddField(
            model_name='task',
            name='prodslt',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterUniqueTogether(
            name='job',
            unique_together=set([('task', 'file')]),
        ),
    ]
