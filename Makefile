ROOT = $(shell git rev-parse --show-toplevel)
GIT_COMMIT="$(shell git log -1 --pretty=format:'%h -- %ai -- %an -- %s')"

ACCOUNT_ID = 299497370133


# Publish a Docker image to ECR, and put its associated release ID in S3.
#
# Args:
#   $1 - Project identifier.
#   $2 - Name of the Docker image.
#
define publish_service
	$(ROOT)/docker_run.py \
        --aws --dind -- \
            wellcome/weco-deploy:5.0.2 \
            --project-id="$(1)" \
            --verbose \
            publish \
            --image-id="$(2)"
endef

# Build and tag a Docker image.
#
# Args:
#   $1 - Name of the image.
#   $2 - Path to the Dockerfile, relative to the root of the repo.
#
define build_image
	$(ROOT)/docker_run.py \
	    --dind -- \
	    wellcome/image_builder:25 \
            --name="$(1)" \
            --build-arg $(GIT_COMMIT)
            --path="$(2)"
endef

storage_service-build:
    $(call build_image,archivematica_storage_service,./Dockerfile)

storage_service-publish: storage_service-build
	$(call publish_service,archivematica,archivematica_storage_service)
