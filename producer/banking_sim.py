import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'financial_transactions'
NUM_USERS = 1000

fake = Faker()

CITIES = ["Istanbul", "Ankara", "Izmir", "London", "New York", "Berlin", "Tokyo", "Paris"]
CATEGORIES = ["Food", "Electronics", "Clothing", "Travel", "Utilities", "Entertainment"]


class BankingSimulator:
    def __init__(self):
        self.producer = self._create_producer()
        self.users = self._generate_users()
        print(f"[INFO] Generated {NUM_USERS} customer profiles.")

    def _create_producer(self):
        """Establishes Kafka connection with retry mechanism."""
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                print("[INFO] Kafka connection established successfully.")
                return producer
            except Exception as e:
                print(f"[WARN] Waiting for Kafka... Error: {e}")
                time.sleep(3)

    def _generate_users(self):
        """Creates a static pool of users."""
        users = []
        for _ in range(NUM_USERS):
            users.append({
                "card_id": str(uuid.uuid4()),
                "name": fake.name(),
                "home_city": random.choice(CITIES[:3]),  # Local cities
                "avg_spending": random.uniform(50, 2000)
            })
        return users

    def generate_transaction(self):
        """Generates a single transaction (Normal or Fraud)."""
        user = random.choice(self.users)
        is_fraud = False

        # Default (Normal) Transaction
        amount = np.random.normal(user["avg_spending"], user["avg_spending"] * 0.2)
        amount = max(5, round(amount, 2))
        city = user["home_city"]
        merchant = fake.company()
        category = random.choice(CATEGORIES)

        # --- FRAUD SCENARIOS (1% Probability) ---
        if random.random() < 0.01:
            is_fraud = True
            fraud_type = random.choice(["HIGH_AMOUNT", "LOCATION_JUMP", "RAPID_TX"])

            if fraud_type == "HIGH_AMOUNT":
                # Sudden high amount spending
                amount = random.uniform(20000, 100000)
                category = "Electronics"

            elif fraud_type == "LOCATION_JUMP":
                # Transaction in a foreign city far from home
                city = random.choice(CITIES[3:])


            # We flag it here, Spark will detect the pattern.

        # Construct the message
        transaction = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "card_id": user["card_id"],
            "amount": amount,
            "city": city,
            "merchant": merchant,
            "category": category,
            
            "is_fraud_simulation": is_fraud
        }

        return transaction

    def run(self):
        print("[INFO] Financial transaction stream started...")
        try:
            while True:
                tx = self.generate_transaction()
                self.producer.send(TOPIC_NAME, value=tx)

                # Print only FRAUD or 5% of NORMAL transactions to keep logs clean
                if tx["is_fraud_simulation"] or random.random() < 0.05:
                    status = "[FRAUD]" if tx["is_fraud_simulation"] else "[NORMAL]"
                    print(f"{status} Amount: {tx['amount']:.2f} | City: {tx['city']} | ID: {tx['card_id'][:8]}...")

                # Approximately 10-50 transactions per second
                time.sleep(random.uniform(0.01, 0.1))

        except KeyboardInterrupt:
            print("\n[INFO] Simulation stopped by user.")
            self.producer.close()


if __name__ == "__main__":
    sim = BankingSimulator()
    sim.run()