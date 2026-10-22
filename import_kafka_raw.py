
import json
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

# Update this path if your Kafka log is stored elsewhere.
KAFKA_FILE = Path(
    r"C:\Users\jagan\OneDrive\Desktop\capstone project\DE01Capstone\stream\pulsetrack_conversions.log"
)
ERROR_FILE = KAFKA_FILE.with_name("kafka_import_errors.log")
# Use the same PostgreSQL connection details as your MongoDB script.
DB_URL = "postgresql://postgres:Hdse171033@localhost:5432/abc_capstone"



INSERT_SQL = """
INSERT INTO pulsetrack_raw.kafka_pulsetrack_conversions (
    topic, partition_id, offset_id, message_timestamp,
    message_key, event_id, conversion_id, event_payload
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def parse_event(line):
    parts = line.rstrip("\r\n").split("|", 5)

    if len(parts) != 6:
        raise ValueError("Expected 6 fields")

    topic = parts[0]
    metadata = {}

    for item in parts[1:5]:
        key, value = item.split("=", 1)
        metadata[key] = value

    payload = json.loads(parts[5])

    if not isinstance(payload, dict):
        raise ValueError("JSON payload must be an object")

    return (
        topic,
        int(metadata["partition"]),
        int(metadata["offset"]),
        int(metadata["ts"]),
        metadata["key"],
        payload.get("event_id"),
        payload.get("conversion_id"),
        Jsonb(payload),
    )


def main():
    imported = 0
    rejected = 0

    with ERROR_FILE.open("w", encoding="utf-8") as errors:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                with KAFKA_FILE.open(
                    "r", encoding="utf-8"
                ) as source:
                    for line_number, line in enumerate(
                        source, start=1
                    ):
                        if not line.strip():
                            continue

                        try:
                            event = parse_event(line)
                        except (
                            ValueError,
                            json.JSONDecodeError,
                        ) as exc:
                            errors.write(
                                f"Line {line_number}: {exc}\n"
                                f"Raw record: {line}"
                                f"{'' if line.endswith(chr(10)) else chr(10)}"
                                "---\n"
                            )
                            rejected += 1
                            print(
                                f"Skipped malformed line "
                                f"{line_number}: {exc}"
                            )
                            continue

                        cur.execute(INSERT_SQL, event)
                        imported += 1

    print("\nKafka Raw import completed.")
    print(f"Imported events: {imported}")
    print(f"Malformed lines skipped: {rejected}")
    print(f"Error log: {ERROR_FILE}")


if __name__ == "__main__":
    main()

