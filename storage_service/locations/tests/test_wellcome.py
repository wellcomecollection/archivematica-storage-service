import os

import boto3
import pytest
from django.test import TestCase

from locations import models


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.abspath(os.path.join(THIS_DIR, '..', 'fixtures'))

def pytest_runtest_setup(item):
    # Set a default region before we start running tests.
    #
    # Without this line, boto3 complains about not having a region defined
    # (despite one being passed in the Travis env variables/local config).
    # TODO: Investigate this properly.
    boto3.setup_default_session(region_name="eu-west-1")


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    #root_docker_compose = pytestconfig.rootdir.join("docker-compose.yml")
    src_docker_compose = pytestconfig.rootdir.join('locations', 'tests', "docker-compose.yml")
    #print(root_docker_compose, src_docker_compose)

   # if root_docker_compose.exists():
   #     return root_docker_compose
    if src_docker_compose.exists():
        return src_docker_compose
    else:
        assert False, "Cannot find docker-compose file!"


#@pytest.fixture(scope="session")
#def s3_endpoint_url(docker_ip):
#    port = 8000
#    return 'http://dummy-aws_s3_1:8000'.format(docker_ip, port)


class TestWellcomeStorage(TestCase):

    fixtures = ['base.json', 'wellcome.json']

    def setUp(self):
        self.wellcome_object = models.WellcomeStorageService.objects.get(id=1)

    def tearDown(self):
        delete_list = [
            os.path.join(FIXTURES_DIR, 'test.txt'),
            os.path.join(FIXTURES_DIR, 'objects.zip'),
            os.path.join(FIXTURES_DIR, 'metadata.zip'),
            os.path.join(FIXTURES_DIR, 'objects.7z'),
            os.path.join(FIXTURES_DIR, 'metadata.7z'),

        ]
        for delete_file in delete_list:
            try:
                os.remove(delete_file)
            except OSError:
                pass

#    def test_move_from_ss(self):
#        #conn = boto3.resource('s3', region_name='us-east-1',
#        #                      )
#        print('connecting')
#        client = boto3.client(
#            "s3",
#            aws_access_key_id="accessKey1",
#            aws_secret_access_key="verySecretKey1",
#            endpoint_url='http://s3:8000',
#            region_name='us-east-1',
#        )
#        print(client)
#        # We need to create the bucwesng tket since this is all in Moto's 'virtual' AWS account
#        bucket = client.create_bucket(Bucket='ingest-bucket')
#        print('bucket', bucket)
#
#        ## Create test.txt
#        with open(os.path.join(FIXTURES_DIR, 'test.txt'), 'w') as f:
#            f.write('test file\n')
#
#        package = models.Package.objects.get(uuid="6465da4a-ea88-4300-ac56-9641125f1276")
#
#        ## Upload
#        self.wellcome_object.move_from_storage_service(
#            os.path.join(FIXTURES_DIR, 'small_compressed_bag.zip'),
#            'irrelevent',
#            package=package
#        )
#
#        ## Verify
#        #assert package.current_path == 'http://demo.dspace.org/swordv2/statement/86.atom'
#        #assert package.misc_attributes['handle'] == '123456789/35'
#        ## FIXME How to verify?
