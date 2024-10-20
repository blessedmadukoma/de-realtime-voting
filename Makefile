dc_up:
	docker compose up -d

dc_down:
	docker compose down -v

run:
	python main.py

.PHONY: dc_up dc_down run