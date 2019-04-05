#!/usr/bin/env bash

set -o errexit
set -o nounset

echo "== WELLCOME: starting version $GIT_COMMIT =="

/usr/local/bin/gunicorn --config=/etc/archivematica/storage-service.gunicorn-config.py storage_service.wsgi:application $@
