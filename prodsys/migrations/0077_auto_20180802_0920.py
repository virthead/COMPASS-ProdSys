# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-08-02 09:20
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prodsys', '0076_auto_20180802_0919'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='soft',
            field=models.CharField(help_text='no leading and trailing slashes', max_length=300),
        ),
    ]
