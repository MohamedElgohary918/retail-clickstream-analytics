import argparse
import csv
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = PROJECT_ROOT / "data" / "2019-Oct.csv"
DEFAULT_CHECKPOINT = PROJECT_ROOT / "streaming" / "producer" / "checkpoint.json"

TOPIC = "clickstream_events"

CSV_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def parse_event(row, event_id):
    """Convert one CSV row into the Snowflake RAW event format."""

    event_time = datetime.strptime(
        row["event_time"], "%Y-%m-%d %H:%M:%S UTC"
    )

    return {
        "EVENT_ID": event_id,
        "EVENT_TIME": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "EVENT_TYPE": row["event_type"],
        "PRODUCT_ID": int(row["product_id"]) if row["product_id"] else None,
        "CATEGORY_ID": int(row["category_id"]) if row["category_id"] else None,
        "CATEGORY_CODE": row["category_code"] or None,
        "BRAND": row["brand"] or None,
        "PRICE": float(row["price"]) if row["price"] else None,
        "USER_ID": int(row["user_id"]) if row["user_id"] else None,
        "USER_SESSION": row["user_session"] or None,
        "SIMULATED_AT": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


def source_metadata(csv_path):
    """Identify the source file so a checkpoint cannot silently be reused."""
    stat = csv_path.stat()

    return {
        "source_file": str(csv_path.resolve()),
        "source_size_bytes": stat.st_size,
        "source_mtime_ns": stat.st_mtime_ns,
    }


def load_checkpoint(checkpoint_path, source_info, reset=False):
    """Load the saved position or initialize a new checkpoint."""

    if reset:
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            print("Checkpoint reset.")

    if not checkpoint_path.exists():
        return {
            **source_info,
            "last_processed_row": 0,
            "last_event_id": 0,
        }

    with checkpoint_path.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    for key, expected_value in source_info.items():
        if checkpoint.get(key) != expected_value:
            raise ValueError(
                f"Checkpoint does not match the current source file "
                f"({key} differs). Use --reset-checkpoint to start over."
            )

    last_row = checkpoint.get("last_processed_row")

    if not isinstance(last_row, int) or last_row < 0:
        raise ValueError("Invalid last_processed_row in checkpoint.")

    # Backward compatibility with checkpoints created before last_event_id
    # was introduced. This is valid because event_id equals the source row.
    last_event_id = checkpoint.get("last_event_id", last_row)

    if not isinstance(last_event_id, int) or last_event_id < 0:
        raise ValueError("Invalid last_event_id in checkpoint.")

    if last_event_id != last_row:
        raise ValueError(
            "Checkpoint inconsistency: last_event_id must equal "
            "last_processed_row for this producer."
        )

    checkpoint["last_event_id"] = last_event_id

    print(
        f"Resuming after source row {last_row:,} "
        f"(last event ID: {last_event_id:,})."
    )

    return checkpoint


def save_checkpoint(checkpoint_path, checkpoint):
    """Atomically save progress to avoid partially written JSON files."""

    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(
        dir=checkpoint_path.parent,
        prefix=".checkpoint-",
        suffix=".tmp",
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_name, checkpoint_path)

    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def main():
    parser = argparse.ArgumentParser(
        description="Replay clickstream CSV events to Kafka with checkpointing."
    )

    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=DEFAULT_CHECKPOINT,
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Start from the first source row and replace the checkpoint.",
    )

    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be greater than zero")

    if args.rate <= 0:
        parser.error("--rate must be greater than zero")

    if not args.csv.is_file():
        parser.error(f"CSV file not found: {args.csv}")

    csv_path = args.csv.resolve()
    checkpoint_path = args.checkpoint.resolve()
    source_info = source_metadata(csv_path)

    checkpoint = load_checkpoint(
        checkpoint_path,
        source_info,
        reset=args.reset_checkpoint,
    )

    last_processed_row = checkpoint["last_processed_row"]
    sent_this_run = 0
    interval = 1.0 / args.rate
    next_send_time = time.monotonic()

    producer = KafkaProducer(
        bootstrap_servers="localhost:9094",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)

            if reader.fieldnames != CSV_COLUMNS:
                raise ValueError(
                    f"Unexpected CSV columns: {reader.fieldnames}"
                )

            for row_number, row in enumerate(reader, start=1):

                # Skip rows already acknowledged in previous runs.
                if row_number <= last_processed_row:
                    continue

                # Generate a stable ID from the source row number.
                event_id = row_number

                event = parse_event(row, event_id)

                # Wait for Kafka to acknowledge the event.
                producer.send(TOPIC, value=event).get(timeout=30)

                # Save progress only after Kafka acknowledges the event.
                checkpoint["last_processed_row"] = row_number
                checkpoint["last_event_id"] = event_id
                save_checkpoint(checkpoint_path, checkpoint)

                sent_this_run += 1

                if sent_this_run % 100 == 0 or sent_this_run == args.limit:
                    print(
                        f"Published {sent_this_run:,}/{args.limit:,} "
                        f"events this run. "
                        f"Source row: {row_number:,}, "
                        f"Event ID: {event_id:,}"
                    )

                if sent_this_run >= args.limit:
                    break

                next_send_time += interval
                delay = next_send_time - time.monotonic()

                if delay > 0:
                    time.sleep(delay)
                else:
                    next_send_time = time.monotonic()

        producer.flush()

        print(f"Finished. Published {sent_this_run:,} events this run.")
        print(
            "Last acknowledged source row: "
            f"{checkpoint['last_processed_row']:,}"
        )
        print(f"Last event ID: {checkpoint['last_event_id']:,}")
        print(f"Checkpoint: {checkpoint_path}")

    except (KafkaError, ValueError, OSError) as error:
        print(f"Producer failed: {error}")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    main()