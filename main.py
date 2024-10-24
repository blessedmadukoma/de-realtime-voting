import json
import random
import logging
import requests
from confluent_kafka import SerializingProducer
from env import DATABASE_URL, BASE_URL, connect_db

PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

random.seed(21)


def create_tables(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255) UNIQUE,
            voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            vote int DEFAULT 1,
            primary key (voter_id, candidate_id)
        )
        """
    )

    conn.commit()


def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(
        BASE_URL + f"&gender={'female' if candidate_number % 2 == 1 else 'male'}")

    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "candidate_id": user_data["login"]["uuid"],
            "candidate_name": user_data["name"]["first"] + " " + user_data["name"]["last"],
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": 'A brief biography of the candidate',
            "campaign_platform": "Key campaign promises and platform",
            "photo_url": user_data["picture"]["large"]
        }
    else:
        logging.error(f"Error fetching data from API: {response.status_code}")
        return "Error fetching data"


def generate_voter_data():
    response = requests.get(BASE_URL)

    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "voter_id": user_data["login"]["uuid"],
            "voter_name": user_data["name"]["first"] + " " + user_data["name"]["last"],
            "date_of_birth": user_data["dob"]["date"],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            # "registration_number": user_data['registered']['number'],
            "registration_number": user_data['login']['username'],
            'address': {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data["registered"]["age"]
        }

    else:
        logging.error(f"Error fetching data from API: {response.status_code}")
        return "Error fetching data"


def insert_voters(conn, cur, voter_data):
    cur.execute(
        """
        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, picture, registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (voter_data["voter_id"], voter_data["voter_name"], voter_data["date_of_birth"], voter_data["gender"], voter_data["nationality"], voter_data["registration_number"], voter_data["address"]["street"], voter_data["address"]["city"],
         voter_data["address"]["state"], voter_data["address"]["country"], voter_data["address"]["postcode"], voter_data["email"], voter_data["phone_number"], voter_data["picture"], voter_data["registered_age"])
    )

    conn.commit()


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {msg.value()}: {err.str()}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
    })

    try:
        conn = connect_db()

        cur = conn.cursor()

        # Create tables
        create_tables(conn, cur)

        # get all candidates
        cur.execute("SELECT * FROM candidates")

        candidates = cur.fetchall()
        print(f"Candidates before insert: {candidates}")

        if len(candidates) == 0:
            for i in range(3):
                candidate = generate_candidate_data(i, 3)

                print(candidate)

                cur.execute(
                    """
                    INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (candidate["candidate_id"], candidate["candidate_name"], candidate["party_affiliation"],
                     candidate["biography"], candidate["campaign_platform"], candidate["photo_url"])
                )

            conn.commit()

            candidates = cur.fetchall()
            print(f"Candidates after insert: {candidates}")

        cur.execute("SELECT COUNT(*) FROM voters")
        voters_count = cur.fetchone()

        if voters_count[0] < 1000:
            cur.execute("DELETE FROM voters")
            conn.commit()

            # generate voters data
            for i in range(1000):
                voter_data = generate_voter_data()
                insert_voters(conn, cur, voter_data)

                # send voter data to kafka
                producer.produce(
                    topic="voters_topic",
                    key=voter_data["voter_id"],
                    value=json.dumps(voter_data),
                    on_delivery=delivery_report
                )

                print(f"Produced voter {i}, data: {voter_data}")

                # flush to ensure all messages are delivered
                producer.flush()

        cur.execute("SELECT COUNT(*) FROM voters")
        voters_count = cur.fetchone()
        print(f"Voters count after insert: {voters_count}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"Error connecting to db: {e}")
