# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0025_allow_blank_aws_auth'),
    ]

    operations = [
        migrations.RenameField(
            model_name='s3',
            old_name='access_key_id',
            new_name='aws_access_key_id',
        ),
        migrations.RenameField(
            model_name='s3',
            old_name='secret_access_key',
            new_name='aws_secret_access_key',
        ),
        migrations.AddField(
            model_name='s3',
            name='aws_assumed_role',
            field=models.CharField(max_length=256, verbose_name='Assumed AWS IAM Role', blank=True),
        ),
    ]
