# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0020_dspace_rest'),
    ]

    operations = [
        migrations.CreateModel(
            name='WellcomeStorageService',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('token_url', models.URLField(help_text='URL of the OAuth token endpoint, e.g. https://auth.wellcomecollection.org/oauth2/token', max_length=256)),
                ('api_root_url', models.URLField(help_text='Root URL of the storage service API, e.g. https://api.wellcomecollection.org/', max_length=256)),
                ('app_client_id', models.CharField(max_length=300, null=True, blank=True)),
                ('app_client_secret', models.CharField(max_length=300, null=True, blank=True)),
            ],
            options={
                'verbose_name': 'Wellcome Storage Service',
            },
        ),
        migrations.AlterField(
            model_name='space',
            name='access_protocol',
            field=models.CharField(help_text='How the space can be accessed.', max_length=8, verbose_name='Access protocol', choices=[(b'ARKIVUM', 'Arkivum'), (b'DV', 'Dataverse'), (b'DC', 'DuraCloud'), (b'DSPACE', 'DSpace via SWORD2 API'), (b'DSPC_RST', 'DSpace via REST API'), (b'FEDORA', 'FEDORA via SWORD2'), (b'GPG', 'GPG encryption on Local Filesystem'), (b'FS', 'Local Filesystem'), (b'LOM', 'LOCKSS-o-matic'), (b'NFS', 'NFS'), (b'PIPE_FS', 'Pipeline Local Filesystem'), (b'SWIFT', 'Swift'), (b'S3', 'S3'), (b'WELLCOME', 'Wellcome Storage Service')]),
        ),
        migrations.AddField(
            model_name='wellcomestorageservice',
            name='space',
            field=models.OneToOneField(to='locations.Space', to_field=b'uuid'),
        ),
    ]
