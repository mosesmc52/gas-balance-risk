.PHONY: up build daemon down clean logs \
	help shell mongo-export mongo-import mongo-restore-full mongo-clean-backup clean_eqb_pdfs \
	do-fn-validate do-fn-connect do-fn-status do-fn-deploy do-fn-deploy-remote \
	do-fn-list do-fn-get do-fn-invoke do-fn-activations do-fn-logs \
	do-droplet-log do-spaces-log

WORKER_CONTAINER=gas_risk_worker
MONGO_CONTAINER=mongo_gas_risk
MONGO_USER=admin
MONGO_PASS=admin123
MONGO_AUTH_DB=admin
MONGO_BACKUP_DIR=./mongo_backup
MONGO_DUMP_PATH=/data/dump

# List of databases to export
MONGO_DBS=energy_gas_risk

# Default Compose files
MAIN_COMPOSE_FILE := docker-compose.yml

# DigitalOcean Functions
DO_FN_DIR := infra/do-functions
DO_FN_ENV := $(DO_FN_DIR)/.env
DO_FN_NAME := launcher/gas-balance-risk
DROPLET_USER ?= root
DROPLET_LOG_FILE ?= /var/log/job.log
SPACES_ENDPOINT ?= https://zilla.sfo3.digitaloceanspaces.com
SPACES_BUCKET ?= gas-risk

# Quickly view available targets
help:
	@echo "Available make targets:"
	@echo "  build        Build main Airflow image"
	@echo "  up           Start main stack (foreground)"
	@echo "  upd          Start main stack (daemon)"
	@echo "  down         Stop main stack"
	@echo "  clean        Stop and remove volumes for main stack"
	@echo "  logs         Follow logs for main stack"
	@echo "  do-fn-validate      Validate DO Functions project metadata"
	@echo "  do-fn-connect       Connect doctl to a DO Functions namespace"
	@echo "  do-fn-status        Show DO Functions connection status"
	@echo "  do-fn-deploy        Deploy infra/do-functions with runtime env"
	@echo "  do-fn-deploy-remote Deploy infra/do-functions using remote build"
	@echo "  do-fn-list          List deployed DO functions"
	@echo "  do-fn-get           Show deployed function metadata"
	@echo "  do-fn-invoke        Invoke launcher/gas-balance-risk"
	@echo "  do-fn-activations   List recent activations"
	@echo "  do-fn-logs          Show logs for ACTIVATION=<id>"
	@echo "  do-droplet-log      Tail droplet log over SSH with DROPLET_IP=<ip>"
	@echo "  do-spaces-log       Print uploaded log from Spaces with LOG_KEY=<key>"


# ---------------------------------------------------------------------
# MAINCOMMANDS
# ---------------------------------------------------------------------

build:
	docker-compose -f $(MAIN_COMPOSE_FILE) build

up:
	docker-compose -f $(MAIN_COMPOSE_FILE) up

upd:
	docker-compose -f $(MAIN_COMPOSE_FILE) up -d

down:
	docker-compose -f $(MAIN_COMPOSE_FILE) down --remove-orphans

clean:
	docker-compose -f $(MAIN_COMPOSE_FILE) down --volumes --remove-orphans
	#rm -rf airflow/logs airflow/db mongo_data

logs:
	docker-compose -f $(MAIN_COMPOSE_FILE) logs -f

shell:
	docker exec -it $(WORKER_CONTAINER) /bin/bash


mongo-export:
	@mkdir -p $(MONGO_BACKUP_DIR)
	@for DB in $(MONGO_DBS); do \
		echo "📦 Dumping $$DB..."; \
		docker exec $(MONGO_CONTAINER) mongodump \
			--username $(MONGO_USER) \
			--password $(MONGO_PASS) \
			--authenticationDatabase $(MONGO_AUTH_DB) \
			--db $$DB \
			--out $(MONGO_DUMP_PATH); \
		docker cp $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)/$$DB $(MONGO_BACKUP_DIR)/$$DB; \
	done
	@echo "✅ Export complete: $(MONGO_BACKUP_DIR)/"

mongo-import:
	@for DB in $(MONGO_DBS); do \
		echo "♻️  Restoring $$DB..."; \
		docker cp $(MONGO_BACKUP_DIR)/$$DB $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)/$$DB; \
		docker exec $(MONGO_CONTAINER) mongorestore \
			--username $(MONGO_USER) \
			--password $(MONGO_PASS) \
			--authenticationDatabase $(MONGO_AUTH_DB) \
			--db $$DB \
			--drop \
			$(MONGO_DUMP_PATH)/$$DB; \
	done
	@echo "✅ Restore complete."

mongo-restore-full:
	@echo "🚀 Creating dump path inside container..."
	docker exec $(MONGO_CONTAINER) mkdir -p $(MONGO_DUMP_PATH)
	@echo "📦 Copying all backup files to container..."
	docker cp $(MONGO_BACKUP_DIR)/. $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)
	@echo "♻️  Restoring full MongoDB dump..."
	docker exec $(MONGO_CONTAINER) mongorestore \
		--username $(MONGO_USER) \
		--password $(MONGO_PASS) \
		--authenticationDatabase $(MONGO_AUTH_DB) \
		--drop \
		$(MONGO_DUMP_PATH)
	@echo "✅ Full restore complete."

mongo-clean-backup:
	rm -rf $(MONGO_BACKUP_DIR)
	@echo "🗑️  Removed backup folder: $(MONGO_BACKUP_DIR)"

do-fn-validate:
	doctl serverless get-metadata $(DO_FN_DIR)

do-fn-connect:
	doctl serverless connect

do-fn-status:
	doctl serverless status

do-fn-deploy:
	doctl serverless deploy $(DO_FN_DIR) --env $(DO_FN_ENV)

do-fn-deploy-remote:
	doctl serverless deploy $(DO_FN_DIR) --env $(DO_FN_ENV) --remote-build

do-fn-list:
	doctl serverless functions list

do-fn-get:
	doctl serverless functions get $(DO_FN_NAME)

do-fn-invoke:
	doctl serverless functions invoke $(DO_FN_NAME)

do-fn-activations:
	doctl serverless activations list

do-fn-logs:
	test -n "$(ACTIVATION)" || (echo "Set ACTIVATION=<id>" && exit 1)
	doctl serverless activations logs $(ACTIVATION)

do-droplet-log:
	test -n "$(DROPLET_IP)" || (echo "Set DROPLET_IP=<ip>" && exit 1)
	ssh $(DROPLET_USER)@$(DROPLET_IP) "sudo tail -f $(DROPLET_LOG_FILE)"

do-spaces-log:
	test -n "$(LOG_KEY)" || (echo "Set LOG_KEY=<spaces log key>" && exit 1)
	aws --endpoint-url $(SPACES_ENDPOINT) s3 cp s3://$(SPACES_BUCKET)/$(LOG_KEY) -
