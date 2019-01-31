# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0021_wellcome'),
    ]

    operations = [
        migrations.RenameField(
            'wellcomestorageservice',
            's3_access_key_id',
            'aws_access_key_id',
        ),
        migrations.RenameField(
            'wellcomestorageservice',
            's3_secret_access_key',
            'aws_secret_access_key',
        ),
        migrations.AddField(
            model_name='wellcomestorageservice',
            name='aws_assumed_role',
            field=models.CharField(max_length=256, verbose_name='Assumed AWS IAM Role', blank=True),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='s3_bucket',
            field=models.CharField(help_text='S3 Bucket for temporary storage', max_length=64, verbose_name='S3 Bucket'),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='s3_region',
            field=models.CharField(help_text='S3 Region in S3. Eg. us-east-2', max_length=64, verbose_name='S3 Region'),
        ),
    ]
