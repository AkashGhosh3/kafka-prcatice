
import json
import time
import argparse

from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition,
    KafkaException
)

TOPIC = "customer-events"
DLQ_TOPIC = "customer-events-dlq"
GROUP_ID = "customer-group"

COMMIT_EVERY = 3


def create_consumer(reset_policy):
    return Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": reset_policy,
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000
    })


dlq_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "customer-dlq-producer"
})


def send_to_dlq(msg, error):
    raw = msg.value() or b""

    dlq_event = {
        "original_topic": msg.topic(),
        "original_partition": msg.partition(),
        "original_offset": msg.offset(),
        "original_key": (
            msg.key().decode("utf-8", errors="replace")
            if msg.key() else None
        ),
        "original_value": raw.decode(
            "utf-8", errors="replace"
        ),
        "error": str(error),
        "failed_at": time.time()
    }

    delivery_errors = []

    def delivery_callback(err, delivered_msg):
        if err:
            delivery_errors.append(err)

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=json.dumps(dlq_event).encode("utf-8"),
        callback=delivery_callback
    )

    remaining = dlq_producer.flush(10)

    if remaining or delivery_errors:
        raise RuntimeError(
            f"DLQ delivery failed: {delivery_errors}"
        )

    print(
        f"Sent to DLQ: partition={msg.partition()}, "
        f"offset={msg.offset()}"
    )


def run_consumer(reset_policy):
    consumer = create_consumer(reset_policy)

    # Track next offset independently for each partition
    pending_offsets = {}

    # Number of handled messages since last checkpoint
    processed_count = 0

    def commit_pending():
        nonlocal processed_count

        if not pending_offsets:
            return

        offsets = [
            TopicPartition(topic, partition, next_offset)
            for (topic, partition), next_offset
            in pending_offsets.items()
        ]

        print("\nSAVING CHECKPOINTS...")

        result = consumer.commit(
            offsets=offsets,
            asynchronous=False
        )

        failures = []

        for tp in result:
            key = (tp.topic, tp.partition)

            if tp.error:
                failures.append((key, tp.error))
                print(
                    f"COMMIT FAILED: partition={tp.partition}, "
                    f"error={tp.error}"
                )
            else:
                print(
                    f"CHECKPOINT SAVED: "
                    f"partition={tp.partition}, "
                    f"next offset={tp.offset}"
                )
                pending_offsets.pop(key, None)

        processed_count = len(pending_offsets)

        if failures:
            raise RuntimeError(
                f"Offset commit failed: {failures}"
            )

    def on_assign(c, partitions):
        print("\nPARTITIONS ASSIGNED:")
        for tp in partitions:
            print(
                f"Topic={tp.topic}, "
                f"Partition={tp.partition}"
            )

    def on_revoke(c, partitions):
        nonlocal processed_count

        print("\nPARTITIONS REVOKED:")
        for tp in partitions:
            print(
                f"Topic={tp.topic}, "
                f"Partition={tp.partition}"
            )

        # Save handled messages before giving up partitions.
        if pending_offsets:
            commit_pending()

    consumer.subscribe(
        [TOPIC],
        on_assign=on_assign,
        on_revoke=on_revoke
    )

    print(f"Consumer started. Group: {GROUP_ID}")
    print(f"Offset reset policy: {reset_policy}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                raw_value = msg.value().decode("utf-8")
                event = json.loads(raw_value)

                print("\nValid event received:")
                print(f"Data: {event}")
                print(f"Partition: {msg.partition()}")
                print(f"Offset: {msg.offset()}")

                # Add real business processing here.
                # Only mark handled after it succeeds.

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                send_to_dlq(msg, error)

            # Mark the successfully handled offset
            key = (msg.topic(), msg.partition())
            next_offset = msg.offset() + 1

            pending_offsets[key] = max(
                pending_offsets.get(key, next_offset),
                next_offset
            )

            processed_count += 1

            print(
                f"Pending checkpoint: "
                f"partition={msg.partition()}, "
                f"next offset={next_offset}"
            )

            if processed_count >= COMMIT_EVERY:
                commit_pending()

    except KeyboardInterrupt:
        print("\nConsumer interrupted.")
        print("Uncommitted messages may be replayed.")

    except (KafkaException, RuntimeError) as error:
        print(f"Consumer error: {error}")

    finally:
        # Intentionally no final commit:
        # allows testing crash/restart replay.
        consumer.close()
        dlq_producer.flush()
        print("Consumer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--reset",
        choices=["earliest", "latest"],
        default="earliest"
    )

    args = parser.parse_args()
    run_consumer(args.reset)
