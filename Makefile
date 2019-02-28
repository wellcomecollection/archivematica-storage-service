IMAGE_BUILDER_IMAGE = wellcome/image_builder:25
PUBLISH_SERVICE_IMAGE = wellcome/publish_service:60

ROOT = $(shell git rev-parse --show-toplevel)
DOCKER_RUN = $(ROOT)/wellcome/docker_run.py

ACCOUNT_ID = 299497370133


# Publish a Docker image to ECR, and put its associated release ID in S3.
#
# Args:
#   $1 - Name of the Docker image.
#
define publish_service
	$(DOCKER_RUN) \
	    --aws --dind -- \
	    $(PUBLISH_SERVICE_IMAGE) \
			--project_id=archivematica \
			--service_id=$(1) \
			--account_id=$(ACCOUNT_ID) \
			--region_id=eu-west-1 \
			--namespace=uk.ac.wellcome
endef


storage_service-build:
	$(DOCKER_RUN) --dind -- $(IMAGE_BUILDER_IMAGE) --name=archivematica_storage_service --path=./Dockerfile

storage_service-publish: storage_service-build
	$(call publish_service,archivematica_storage_service)
