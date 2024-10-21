import random
import psycopg2
from dotenv import load_dotenv
import simplejson as json
import os
import logging
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from datetime import datetime
from main import delivery_report

load_dotenv()

config = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(config | {
    'group.id': 'voting',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
})

producer = SerializingProducer(config)

DATABASE_URL = os.getenv("DATABASE_URL")

if __name__ == "__main__":
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
    })

    try:
        conn = psycopg2.connect(DATABASE_URL)

        cur = conn.cursor()

        candidates_query = cur.execute(
            """
            SELECT 
                row_to_json(col) 
            FROM 
            (
                SELECT * FROM candidates
            ) col;
            """
        )

        candidates = [candidate[0] for candidate in cur.fetchall()]

        if len(candidates) == 0:
            raise Exception("No candidates found")

        # subscribe to voters topic to get voter data
        consumer.subscribe(["voters_topic"])

        while True:
            try:
                # poll for new messages
                message = consumer.poll(timeout=1.0)

                # check if message is None
                if message is None:
                    continue

                # check if message has an error
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        logging.warning('%% %s [%d] reached end at offset %d\n' %
                                        (message.topic(), message.partition(), message.offset()))

                        continue
                    elif message.error():
                        raise KafkaException(message.error())
                else:
                    voter = json.loads(message.value().decode("utf-8"))

                    # get a random candidate
                    chosen_candidate = random.choice(candidates)

                    vote = voter | chosen_candidate | {
                        "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        'vote': 1
                    }

                    print(
                        f'User {voter["voter_id"]} voted for {chosen_candidate["candidate_name"]}')

                    # insert vote
                    cur.execute(
                        """
                        INSERT INTO votes (voter_id, candidate_id, vote_time)
                        VALUES (%s, %s, %s)
                        """,
                        (vote["voter_id"], vote["candidate_id"],
                         vote["voting_time"])
                    )

                    conn.commit()

                    # send vote data to kafka
                    producer.produce(
                        topic="votes_topic",
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )

                    producer.poll(0)

                    logging.info(
                        f'User {voter["voter_id"]} voted for {chosen_candidate["candidate_name"]}')

            except Exception as e:
                logging.error(f"Error processing message: {e}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"Error connecting to db: {e}")
