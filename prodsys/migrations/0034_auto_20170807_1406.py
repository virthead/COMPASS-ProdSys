# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-08-07 14:06
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0033_auto_20170707_1044'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='max_attempts',
            field=models.IntegerField(default=5),
        ),
    ]
