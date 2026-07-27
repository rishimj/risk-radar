.PHONY: demo up down logs ps simulate test rebuild seed calibrate clean

demo: ## Full local bring-up: infra -> baselines seeded -> pipeline running
	@test -f .env || (echo "!! copy .env.example to .env first" && exit 1)
	docker compose up -d --build
	@echo "waiting for the Flink job to reach RUNNING (first build pulls FinBERT, be patient)..."
	@for i in $$(seq 1 60); do \
		curl -sf http://localhost:8081/jobs/overview 2>/dev/null | grep -q '"state":"RUNNING"' && break || sleep 5; \
	done
	@echo
	@echo "  web        http://localhost:3000"
	@echo "  flink      http://localhost:8081"
	@echo "  kafka-ui   http://localhost:8090"
	@echo "  enrichment http://localhost:8082/stats"

up:
	docker compose up -d

down:
	docker compose down

clean: ## down + delete volumes (kafka data, dynamo tables, model cache)
	docker compose down -v

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

rebuild:
	docker compose up -d --build --force-recreate

test: ## pure-python unit tests, no containers required
	python -m pytest tests/ -q

seed: ## populate per-ticker baselines from the last 24h of real news
	python tools/seed_baseline.py

calibrate: ## replay a real corpus and report the alert fire rate
	python tools/calibrate.py --hours 6

simulate: ## force a synthetic negative event for TSLA
	curl -s -X POST http://localhost:3000/api/simulate \
		-H 'Content-Type: application/json' -d '{"ticker":"TSLA"}' | python -m json.tool
