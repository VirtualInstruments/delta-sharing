# Makefile is at repo root
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
SERVER_VERSION := $(shell grep 'version in ThisBuild' $(REPO_ROOT)version.sbt | sed 's/.*"\(.*\)".*/\1/')
SERVICE_IMAGE := $(shell grep '^SERVICE_IMAGE' $(REPO_ROOT).env | cut -d'=' -f2)
# IMAGE_TAG is normally supplied by the caller (e.g. Jenkins export). ?= leaves an
# already-set/exported value alone and only supplies "dev" for local builds -
IMAGE_TAG ?= dev

.PHONY: image
image:
	@echo "Building Docker image from fork (version $(SERVER_VERSION))"
	@cd $(REPO_ROOT) && build/sbt server/docker:publishLocal
	@echo "Image built: deltaio/delta-sharing-server:$(SERVER_VERSION)"
	@docker tag deltaio/delta-sharing-server:$(SERVER_VERSION) ${SERVICE_IMAGE}:${IMAGE_TAG}

.PHONY: push-dev
push-dev:
	@echo "Pushing Docker image as tag $(IMAGE_TAG)"
	@docker tag ${SERVICE_IMAGE}:dev gcr.io/zing-dev-197522/${SERVICE_IMAGE}:$(IMAGE_TAG)
	@docker push gcr.io/zing-dev-197522/${SERVICE_IMAGE}:$(IMAGE_TAG)

.PHONY: clean
clean::
	@echo "Cleaning up old files (from any previous pull-based builds)"
	@rm -rf delta-sharing-*
	@rm -f v*.tar.gz
	@echo "Cleanup complete. To clean SBT build artifacts, run: build/sbt clean"

.PHONY: deploy
deploy: deploy-dev

.PHONY: deploy-dev
deploy-dev:
	@echo "Deploying image tag $(IMAGE_TAG) to zing-dev"
	@cd ci && ./deploy.sh dev

.PHONY: deploy-preview
deploy-preview:
	@echo "Deploying image tag $(IMAGE_TAG) to zing-preview"
	@cd ci && ./deploy.sh preview

.PHONY: deploy-prod
deploy-prod:
	@echo "Deploying image tag $(IMAGE_TAG) to zcloud-prod"
	@cd ci && ./deploy.sh prod
