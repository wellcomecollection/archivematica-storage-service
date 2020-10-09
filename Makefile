IMAGE_BUILDER_IMAGE = wellcome/image_builder:25
PUBLISH_SERVICE_IMAGE = wellcome/publish_service:60

ROOT = $(shell git rev-parse --show-toplevel)

ACCOUNT_ID = 299497370133


# Publish a Docker image to ECR, and put its associated release ID in S3.
#
# Args:
#   $1 - Name of the Docker image.
#
define publish_service
	$(ROOT)/docker_run.py \
        --aws --dind -- \
            wellcome/weco-deploy:5.0.2 \
            --project-id=archivematica \
            --verbose \
            publish \
            --image-id="$(1)"
endef


storage_service-build:
	$(DOCKER_RUN) --dind -- $(IMAGE_BUILDER_IMAGE) \
		--name=archivematica_storage_service \
		--build-arg GIT_COMMIT="$(shell git log -1 --pretty=format:'%h -- %ai -- %an -- %s')" \
		--path=./Dockerfile

storage_service-publish: storage_service-build
	$(call publish_service,archivematica_storage_service)
