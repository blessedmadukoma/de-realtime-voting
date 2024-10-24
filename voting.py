import random
import psycopg2
from dotenv import load_dotenv
import simplejson as json
import os
import logging
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from datetime import datetime
import time
from main import delivery_report
from env import connect_db

load_dotenv()

config = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(config | {
    'group.id': 'voting',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
})


def insert_vote(conn, cur, voter, chosen_candidate):
    try:
        vote = voter | chosen_candidate | {
            "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            'vote': 1
        }

        cur.execute(
            """
            INSERT INTO votes (voter_id, candidate_id, voting_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (voter_id) DO NOTHING
            """,
            (vote["voter_id"], vote["candidate_id"], vote["voting_time"])
        )
        conn.commit()

        print(
            f'User {voter["voter_id"]} voted for {chosen_candidate["candidate_name"]}')

        # Send vote data to Kafka
        producer.produce(
            topic="voters_topic",
            key=vote["voter_id"],
            value=json.dumps(vote),
            on_delivery=delivery_report
        )
        producer.poll(0)

        logging.info(
            f'User {voter["voter_id"]} voted for {chosen_candidate["candidate_name"]}')
    except psycopg2.Error as db_err:
        logging.error(f"Database error: {db_err}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Error processing vote: {e}")


producer = SerializingProducer(config)

if __name__ == "__main__":
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
    })

    try:
        conn = connect_db()
        cur = conn.cursor()

        candidates_query = cur.execute(
            "SELECT row_to_json(col) FROM (SELECT * FROM candidates) col;")
        candidates = [candidate[0] for candidate in cur.fetchall()]

        if len(candidates) == 0:
            raise Exception("No candidates found")

        consumer.subscribe(["voters_topic"])

        while True:
            cur.execute("SELECT COUNT(*) FROM votes")
            votes_count = cur.fetchone()

            if votes_count[0] >= 1000:
                print("1000 votes have been cast")
                logging.info("1000 votes have been cast")
                break

            message = consumer.poll(timeout=5.0)  # Increased timeout

            if message is None:
                continue

            if message.error():
                logging.error(f"Kafka error: {message.error()}")
                continue

            voter = json.loads(message.value().decode("utf-8"))
            chosen_candidate = random.choice(candidates)

            insert_vote(conn, cur, voter, chosen_candidate)

            time.sleep(0.5)

    except Exception as e:
        logging.error(f"Error processing message: {e}")
    finally:
        cur.close()
        conn.close()
