.PHONY: demo demo-lite up down logs ps simulate test rebuild seed calibrate clean

# Stream engine: flink (reference, ~2.5GB, amd64-emulated on Apple Silicon) or
# lite (the same topology in one Python process, ~100MB, native). Pick one:
#   make demo              # flink
#   make demo-lite         # lite
#   make up ENGINE=lite    # any target honours ENGINE
ENGINE ?= flink
OTHER_ENGINE = $(if $(filter lite,$(ENGINE)),flink,lite)
COMPOSE = docker compose --profile $(ENGINE)

demo: ## Full local bring-up: infra -> baselines seeded -> pipeline running
	@test -f .env || (echo "!! copy .env.example to .env first" && exit 1)
	@# Never run both engines: both consume news.raw and would double-count.
	@docker compose --profile $(OTHER_ENGINE) stop $(if $(filter lite,$(ENGINE)),flink-jobmanager flink-taskmanager,processor) 2>/dev/null || true
	$(COMPOSE) up -d --build
ifeq ($(ENGINE),lite)
	@echo "waiting for the lite processor to reach RUNNING (first build pulls FinBERT, be patient)..."
	@for i in $$(seq 1 60); do \
		curl -sf http://localhost:8083/stats 2>/dev/null | grep -q '"state": "RUNNING"' && break || sleep 5; \
	done
else
	@echo "waiting for the Flink job to reach RUNNING (first build pulls FinBERT, be patient)..."
	@for i in $$(seq 1 60); do \
		curl -sf http://localhost:8081/jobs/overview 2>/dev/null | grep -q '"state":"RUNNING"' && break || sleep 5; \
	done
endif
	@echo
	@echo "  web        http://localhost:3000"
ifeq ($(ENGINE),lite)
	@echo "  processor  http://localhost:8083/stats"
else
	@echo "  flink      http://localhost:8081"
endif
	@echo "  enrichment http://localhost:8082/stats"

demo-lite: ## Same as demo, on the lightweight Python engine instead of Flink
	@$(MAKE) demo ENGINE=lite

up:
	$(COMPOSE) up -d

down:
	docker compose --profile flink --profile lite down

clean: ## down + delete volumes (kafka data, postgres data, model cache)
	docker compose --profile flink --profile lite down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

rebuild:
	$(COMPOSE) up -d --build --force-recreate

test: ## pure-python unit tests, no containers required
	python -m pytest tests/ -q

seed: ## populate per-ticker baselines from the last 24h of real news
	python tools/seed_baseline.py

calibrate: ## replay a real corpus and report the alert fire rate
	python tools/calibrate.py --hours 6

simulate: ## force a synthetic negative event for TSLA
	curl -s -X POST http://localhost:3000/api/simulate \
		-H 'Content-Type: application/json' -d '{"ticker":"TSLA"}' | python -m json.tool
