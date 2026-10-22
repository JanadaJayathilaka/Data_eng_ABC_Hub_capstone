
import json
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

# Update these paths to your actual files.
PARTNER_FILE = Path(
    r"C:\Users\jagan\OneDrive\Desktop\capstone project\DE01Capstone\mongo\partner_profiles.jsonl"
)
ADVERTISER_FILE = Path(
    r"C:\Users\jagan\OneDrive\Desktop\capstone project\DE01Capstone\mongo\advertiser_accounts.jsonl"
)

# Update with your PostgreSQL credentials.
DB_URL = "postgresql://postgres:Hdse171033@localhost:5432/abc_capstone"


def get_mongo_id(document):
    mongo_id = document.get("_id")

    if isinstance(mongo_id, dict):
        mongo_id = mongo_id.get("$oid")

    return str(mongo_id) if mongo_id is not None else None


def import_jsonl(file_path, table, id_field):
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    sql = f"""
        INSERT INTO {table} (
            mongo_document_id,
            {id_field},
            document
        )
        VALUES (%s, %s, %s)
    """

    count = 0

    # Each file is loaded in a transaction.
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            with file_path.open("r", encoding="utf-8") as source:
                for line_number, line in enumerate(source, start=1):
                    if not line.strip():
                        continue

                    try:
                        doc = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"Invalid JSON in {file_path.name}, "
                            f"line {line_number}"
                        ) from exc

                    if not isinstance(doc, dict):
                        raise ValueError(
                            f"Expected a JSON object on line {line_number}"
                        )

                    mongo_id = get_mongo_id(doc)
                    business_id = doc.get(id_field)

                    if mongo_id is None:
                        raise ValueError(
                            f"Missing _id on line {line_number}"
                        )

                    if business_id is None:
                        raise ValueError(
                            f"Missing {id_field} on line {line_number}"
                        )

                    cur.execute(
                        sql,
                        (
                            mongo_id,
                            str(business_id),
                            Jsonb(doc),
                        ),
                    )

                    count += 1

    print(f"{table}: imported {count} documents")


if __name__ == "__main__":
    import_jsonl(
        PARTNER_FILE,
        "pulsetrack_raw.mongo_partner_profiles",
        "partner_id",
    )

    import_jsonl(
        ADVERTISER_FILE,
        "pulsetrack_raw.mongo_advertiser_accounts",
        "advertiser_id",
    )

    print("Both MongoDB imports completed successfully.")
