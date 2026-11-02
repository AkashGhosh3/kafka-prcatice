
import json
import time
import argparse

from confluent_kafka import Producer


TOPIC = "customer-events"

producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "customer-producer"
}

producer = Producer(producer_config)


def delivery_report(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")

    else:
        print(
            f"Delivered: key={msg.key().decode('utf-8')}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


def send_event(customer_id, event_type, amount, malformed=False):

    event = {
        "customer_id": customer_id,
        "event_type": event_type,
        "amount": amount,
        "timestamp": time.time()
    }

    if malformed:
        value = '{"customer_id": "C999", invalid_json}'
    else:
        value = json.dumps(event)

    producer.produce(
        topic=TOPIC,
        key=str(customer_id),
        value=value,
        callback=delivery_report
    )

    producer.poll(0)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bad",
        action="store_true",
        help="Send one malformed JSON event"
    )

    args = parser.parse_args()

    customers = [
        ("C101", "PURCHASE", 500),
        ("C102", "REFUND", 200),
        ("C103", "PURCHASE", 800),
        ("C101", "PAYMENT", 500),
        ("C104", "PURCHASE", 1200),
        ("C102", "PAYMENT", 200),
        ("C105", "PURCHASE", 900),
        ("C103", "REFUND", 100),
        ("C101", "PURCHASE", 300),
        ("C106", "PAYMENT", 700)
    ]

    for customer_id, event_type, amount in customers:

        send_event(
            customer_id,
            event_type,
            amount
        )

        time.sleep(0.5)

    if args.bad:
        send_event(
            "C999",
            "INVALID",
            0,
            malformed=True
        )

    producer.flush()

    print("All messages sent.")
