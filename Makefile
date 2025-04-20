.PHONY: network build-test-runner test-shell test stop

network: ## Creates test docker network
	@docker network create -d bridge test || true

build-test-runner: ## Builds test_runner container
	@docker compose build test-runner

test-shell: network build-test-runner ## Shells into the test-runner container
	@docker compose run --rm test-runner bash
	@docker compose down --volumes --remove-orphans

test: network build-test-runner ## Runs the test suite
	@docker compose run --rm test-runner sh test-script.sh
	@docker compose down --volumes --remove-orphans

stop: ## Stops both containers
	@docker compose down --remove-orphans --volumes
