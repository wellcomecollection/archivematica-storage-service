# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0026_s3_aws_fields'),
    ]

    operations = [
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='api_root_url',
            field=models.URLField(help_text='Root URL of the storage service API, e.g. https://api.wellcomecollection.org/storage/v1', max_length=256),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='aws_access_key_id',
            field=models.CharField(max_length=64, verbose_name='Access Key ID to authenticate', blank=True),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='aws_secret_access_key',
            field=models.CharField(max_length=256, verbose_name='Secret Access Key to authenticate with', blank=True),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='s3_bucket',
            field=models.CharField(help_text='S3 Bucket Name', max_length=64, verbose_name='S3 Bucket', blank=True),
        ),
        migrations.AlterField(
            model_name='wellcomestorageservice',
            name='s3_region',
            field=models.CharField(help_text='Region in S3. Eg. us-east-2', max_length=64, verbose_name='Region'),
        ),
    ]
