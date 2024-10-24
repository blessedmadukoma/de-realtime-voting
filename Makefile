dc_up:
	docker compose up -d

dc_down:
	docker compose down -v

run:
	python main.py

kafka_topics:
	# kafka-topics --list --bootstrap-server broker:29092
	# docker exec -it broker /opt/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper:2181

kafka_producer:
	docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic <TOPIC_NAME>

kafka_consumer:
	# kafka-console-consumer --topic aggregated_votes_per_candidate --bootstrap-server broker:29092
	# docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic <TOPIC_NAME> --from-beginning

kafka-delete-topic:
	kafka-topics --delete --topic <TOPIC_NAME> --bootstrap-server broker:29092

.PHONY: dc_up dc_down run kafka_topics kafka_producer kafka_consumer