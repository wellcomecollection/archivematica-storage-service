# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0023_auto_20190222_0749'),
    ]

    operations = [
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='aws_access_key_id',
            field=models.CharField(max_length=64, verbose_name='AWS Access Key ID to authenticate', blank=True),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='aws_secret_access_key',
            field=models.CharField(max_length=256, verbose_name='AWS Secret Access Key to authenticate with', blank=True),
        ),
    ]
