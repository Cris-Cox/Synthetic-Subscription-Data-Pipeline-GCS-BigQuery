# ── Notes ─────────────────────────────────────────────────────────────────────
# Dependencies:
#   pip install google-cloud-storage google-cloud-bigquery
#
# Setup (run once before executing this script):
#   gcloud auth application-default login
#   gcloud config set project "PROJECT_ID_HERE"
#
# Optional PowerShell environment variables:
#   $env:GCP_PROJECT_ID=""PROJECT_ID_HERE""
#   $env:GCS_BUCKET_NAME=""PROJECT_ID_HERE"-subscriptions"
#   $env:GCS_FOLDER="subscriptions"
#   $env:BQ_DATASET="subscriptions"
#   $env:BQ_TABLE="subscriptions"
#   $env:GCS_LOCATION="US"
#   $env:NUM_CUSTOMERS="10000"
#   $env:MAX_ROWS_PER_FILE="5000"
#   $env:CREATE_BUCKET_IF_MISSING="true"
#   $env:RECREATE_TABLE_IF_NEEDED="false"
#
# Run:
#   python random_subscription_data.py
#
# What this script does:
#   1. Generates fake subscription data in chunks of MAX_ROWS_PER_FILE
#   2. Writes each chunk to a temp local CSV file
#   3. Uploads each CSV to GCS
#   4. Triggers a BigQuery load job from GCS into the target table
#   5. Deletes the local temp file after upload
#
# BigQuery table is created on first load and appended to on subsequent loads.
# The script validates bucket / dataset / table setup before generating data.
# ─────────────────────────────────────────────────────────────────────────────

import csv
import hashlib
import logging
import os
import random
import re
import tempfile
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Tuple

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.cloud import storage


# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 4_000_000))
MAX_ROWS_PER_FILE = int(os.getenv("MAX_ROWS_PER_FILE", 250_000))

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", ""PROJECT_ID_HERE"")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", ""BUCKET_NAME_HERE"-subscriptions")
GCS_FOLDER = os.getenv("GCS_FOLDER", "subscriptions")
GCS_LOCATION = os.getenv("GCS_LOCATION", "US")

BQ_DATASET = os.getenv("BQ_DATASET", "subscriptions")
BQ_TABLE = os.getenv("BQ_TABLE", "subscriptions")

GROWTH_SKEW = float(os.getenv("GROWTH_SKEW", 0.4))
CREATE_BUCKET_IF_MISSING = os.getenv("CREATE_BUCKET_IF_MISSING", "true").lower() == "true"
RECREATE_TABLE_IF_NEEDED = os.getenv("RECREATE_TABLE_IF_NEEDED", "false").lower() == "true"

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 2, 1)
LOADED_AT = datetime.now(UTC)


# ── Product weights ───────────────────────────────────────────────────────────
PRODUCT_WEIGHTS = {
    "Basic monthly": 38,
    "Basic annual": 5,
    "WB monthly": 32,
    "WB annual": 4,
    "Pro monthly": 8,
    "Pro annual": 1,
}

PRODUCTS = list(PRODUCT_WEIGHTS.keys())
WEIGHTS = list(PRODUCT_WEIGHTS.values())

PERIOD_TYPE_MAP = {
    name: "annual" if "annual" in name else "monthly"
    for name in PRODUCTS
}

AMOUNT_MAP = {
    "Basic monthly": 749,
    "Basic annual": 7490,
    "Pro monthly": 1999,
    "Pro annual": 19990,
    "WB monthly": 1300,
    "WB annual": 13000,
}

DISCOUNT_DETAILS = [
    "Subscription alignment discount",
    "Promo code discount",
    "Loyalty discount",
    "",
    "",
    "",
]

COLUMNS = [
    "created_at",
    "subscription_item_id",
    "invoice_id",
    "transaction_id",
    "customer_id",
    "paid",
    "version_start_ts",
    "subscription_name",
    "subscription_amount_cents",
    "subscription_discount_cents",
    "subscription_discount_details",
    "version_start_date",
    "loaded_at",
    "signup_date",
    "standardized_product_name",
    "net_amount_cents",
    "period_type",
]

BQ_SCHEMA = [
    bigquery.SchemaField("created_at", "DATE"),
    bigquery.SchemaField("subscription_item_id", "STRING"),
    bigquery.SchemaField("invoice_id", "STRING"),
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("paid", "BOOLEAN"),
    bigquery.SchemaField("version_start_ts", "STRING"),
    bigquery.SchemaField("subscription_name", "STRING"),
    bigquery.SchemaField("subscription_amount_cents", "INTEGER"),
    bigquery.SchemaField("subscription_discount_cents", "INTEGER"),
    bigquery.SchemaField("subscription_discount_details", "STRING"),
    bigquery.SchemaField("version_start_date", "DATE"),
    bigquery.SchemaField("loaded_at", "STRING"),
    bigquery.SchemaField("signup_date", "STRING"),
    bigquery.SchemaField("standardized_product_name", "STRING"),
    bigquery.SchemaField("net_amount_cents", "INTEGER"),
    bigquery.SchemaField("period_type", "STRING"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def fake_id(prefix: str = "") -> str:
    return hashlib.md5((prefix + str(uuid.uuid4())).encode()).hexdigest()


def fake_transaction_id() -> str:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "3N" + "".join(random.choices(chars, k=20))


def random_date_skewed(start: datetime, end: datetime) -> datetime:
    t = random.random() ** GROWTH_SKEW
    return start + timedelta(seconds=int(t * (end - start).total_seconds()))


def open_new_temp_file() -> Tuple[tempfile.NamedTemporaryFile, csv.DictWriter]:
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        newline="",
        encoding="utf-8"
    )
    writer = csv.DictWriter(temp_file, fieldnames=COLUMNS)
    writer.writeheader()
    return temp_file, writer


def build_row(customer_id: str, signup_date: datetime, product: str, event_date: datetime) -> dict:
    period_type = PERIOD_TYPE_MAP[product]
    amount = AMOUNT_MAP[product]
    discount_desc = random.choice(DISCOUNT_DETAILS)
    discount_amt = random.randint(100, 600) if discount_desc else 0
    net_amount = amount - discount_amt
    paid = random.random() > 0.15
    version_start = event_date + timedelta(seconds=random.randint(0, 86400))

    return {
        "created_at": event_date.date(),
        "subscription_item_id": fake_id("item"),
        "invoice_id": fake_id("inv"),
        "transaction_id": fake_transaction_id() if paid else "",
        "customer_id": customer_id,
        "paid": paid,
        "version_start_ts": version_start.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC",
        "subscription_name": product,
        "subscription_amount_cents": amount,
        "subscription_discount_cents": discount_amt,
        "subscription_discount_details": discount_desc,
        "version_start_date": version_start.date(),
        "loaded_at": LOADED_AT.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC",
        "signup_date": signup_date.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC",
        "standardized_product_name": product,
        "net_amount_cents": net_amount,
        "period_type": period_type,
    }


# ── Validation / Setup ────────────────────────────────────────────────────────
def validate_config() -> None:
    if not GCP_PROJECT_ID or GCP_PROJECT_ID == "PROJECT_ID":
        raise ValueError(
            "GCP_PROJECT_ID is missing or still set to the placeholder 'PROJECT_ID'."
        )

    if not GCS_BUCKET_NAME or GCS_BUCKET_NAME == "PROJECT_ID-subscriptions":
        raise ValueError(
            "GCS_BUCKET_NAME is missing or still set to the placeholder "
            "'PROJECT_ID-subscriptions'."
        )

    if GCS_BUCKET_NAME.lower() != GCS_BUCKET_NAME:
        raise ValueError(f"GCS bucket names must be lowercase: {GCS_BUCKET_NAME}")

    bucket_regex = r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$"
    if not re.match(bucket_regex, GCS_BUCKET_NAME):
        raise ValueError(
            f"Invalid GCS bucket name: {GCS_BUCKET_NAME}. "
            "Use lowercase letters, numbers, dashes, underscores, or dots."
        )

    if NUM_CUSTOMERS <= 0:
        raise ValueError("NUM_CUSTOMERS must be greater than 0.")

    if MAX_ROWS_PER_FILE <= 0:
        raise ValueError("MAX_ROWS_PER_FILE must be greater than 0.")

    if START_DATE >= END_DATE:
        raise ValueError("START_DATE must be before END_DATE.")


def ensure_bucket(storage_client: storage.Client) -> None:
    try:
        storage_client.get_bucket(GCS_BUCKET_NAME)
        logger.info("Verified bucket exists: gs://%s", GCS_BUCKET_NAME)
    except NotFound:
        if not CREATE_BUCKET_IF_MISSING:
            raise ValueError(
                f"GCS bucket does not exist: gs://{GCS_BUCKET_NAME}. "
                "Create it manually or set CREATE_BUCKET_IF_MISSING=true."
            )

        logger.info("Bucket does not exist. Creating: gs://%s", GCS_BUCKET_NAME)
        bucket = storage.Bucket(storage_client, name=GCS_BUCKET_NAME)
        bucket.location = GCS_LOCATION
        storage_client.create_bucket(bucket, project=GCP_PROJECT_ID)
        logger.info("Created bucket: gs://%s in %s", GCS_BUCKET_NAME, GCS_LOCATION)


def ensure_dataset(bq_client: bigquery.Client) -> None:
    dataset_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

    try:
        bq_client.get_dataset(dataset_id)
        logger.info("Verified dataset exists: %s", dataset_id)
    except NotFound:
        logger.info("Dataset does not exist. Creating: %s", dataset_id)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = GCS_LOCATION
        bq_client.create_dataset(dataset, exists_ok=True)
        logger.info("Created dataset: %s", dataset_id)


def table_has_expected_layout(table: bigquery.Table) -> bool:
    time_partitioning_ok = (
        table.time_partitioning is not None
        and table.time_partitioning.type_ == "DAY"
        and table.time_partitioning.field == "created_at"
    )

    clustering_ok = table.clustering_fields == ["subscription_name", "customer_id"]

    return time_partitioning_ok and clustering_ok


def recreate_table(bq_client: bigquery.Client, table_ref: str) -> None:
    logger.warning("Deleting existing table so it can be recreated with the expected layout: %s", table_ref)
    bq_client.delete_table(table_ref, not_found_ok=True)
    logger.info("Deleted table: %s", table_ref)


def ensure_table_layout(bq_client: bigquery.Client) -> None:
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        table = bq_client.get_table(table_ref)
        logger.info("Verified table exists: %s", table_ref)

        if table_has_expected_layout(table):
            logger.info("Existing table has the expected partitioning and clustering.")
            return

        message = (
            f"Existing table {table_ref} does not match the expected layout. "
            "Expected daily partitioning on created_at and clustering on "
            "(subscription_name, customer_id)."
        )

        if RECREATE_TABLE_IF_NEEDED:
            logger.warning("%s RECREATE_TABLE_IF_NEEDED=true, so the table will be recreated.", message)
            recreate_table(bq_client, table_ref)
        else:
            raise ValueError(
                f"{message} Either delete the table manually, change BQ_TABLE to a new name, "
                "or set RECREATE_TABLE_IF_NEEDED=true."
            )

    except NotFound:
        logger.info("Table does not exist yet and will be created on first load: %s", table_ref)


# ── GCS Upload ────────────────────────────────────────────────────────────────
def upload_to_gcs(storage_client: storage.Client, local_path: str, part_index: int) -> str:
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob_name = f"{GCS_FOLDER}/subscriptions_part{part_index}.csv"
    blob = bucket.blob(blob_name)

    logger.info("Uploading file to gs://%s/%s", GCS_BUCKET_NAME, blob_name)
    blob.upload_from_filename(local_path, content_type="text/csv")

    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info("Upload complete: %s", gcs_uri)
    return gcs_uri


# ── BigQuery Load ─────────────────────────────────────────────────────────────
def load_into_bigquery(bq_client: bigquery.Client, gcs_uri: str, part_index: int) -> None:
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        ),
        clustering_fields=["subscription_name", "customer_id"],
    )

    logger.info("Loading part %s into BigQuery table %s", part_index, table_ref)
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()

    if load_job.errors:
        raise RuntimeError(f"BigQuery load failed for part {part_index}: {load_job.errors}")

    logger.info("BigQuery load complete for part %s", part_index)


# ── Process One File Chunk ────────────────────────────────────────────────────
def finalize_chunk(
    storage_client: storage.Client,
    bq_client: bigquery.Client,
    tmp_file_path: str,
    part_index: int
) -> None:
    try:
        gcs_uri = upload_to_gcs(storage_client, tmp_file_path, part_index)
        load_into_bigquery(bq_client, gcs_uri, part_index)
    finally:
        if os.path.exists(tmp_file_path):
            os.unlink(tmp_file_path)
            logger.info("Deleted temp file: %s", tmp_file_path)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    validate_config()

    logger.info("Starting synthetic subscription pipeline")
    logger.info("Project: %s", GCP_PROJECT_ID)
    logger.info("Bucket: %s", GCS_BUCKET_NAME)
    logger.info("BigQuery table: %s.%s.%s", GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE)
    logger.info("Customers to generate: %s", f"{NUM_CUSTOMERS:,}")
    logger.info("Max rows per file: %s", f"{MAX_ROWS_PER_FILE:,}")
    logger.info("Bucket auto-create enabled: %s", CREATE_BUCKET_IF_MISSING)
    logger.info("Table auto-recreate enabled: %s", RECREATE_TABLE_IF_NEEDED)

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    ensure_bucket(storage_client)
    ensure_dataset(bq_client)
    ensure_table_layout(bq_client)

    file_index = 1
    rows_in_file = 0
    total_rows = 0

    tmp_file, writer = open_new_temp_file()
    logger.info("Started file part %s: %s", file_index, tmp_file.name)

    try:
        for i in range(NUM_CUSTOMERS):
            if i % 500_000 == 0:
                logger.info(
                    "Generating customer %s of %s | current part: %s | rows in file: %s",
                    f"{i:,}",
                    f"{NUM_CUSTOMERS:,}",
                    file_index,
                    f"{rows_in_file:,}",
                )

            customer_id = fake_id("cust")
            signup_date = random_date_skewed(START_DATE, END_DATE - timedelta(days=30))
            product = random.choices(PRODUCTS, weights=WEIGHTS, k=1)[0]
            period_type = PERIOD_TYPE_MAP[product]
            renewal_gap = timedelta(days=365 if period_type == "annual" else 30)

            num_events = random.randint(1, 5)
            event_date = signup_date + timedelta(days=random.randint(0, 5))

            for _ in range(num_events):
                if event_date > END_DATE:
                    break

                writer.writerow(build_row(customer_id, signup_date, product, event_date))
                rows_in_file += 1
                total_rows += 1

                if rows_in_file >= MAX_ROWS_PER_FILE:
                    tmp_file.close()
                    logger.info("Finished part %s with %s rows", file_index, f"{rows_in_file:,}")
                    finalize_chunk(storage_client, bq_client, tmp_file.name, file_index)

                    file_index += 1
                    rows_in_file = 0
                    tmp_file, writer = open_new_temp_file()
                    logger.info("Started file part %s: %s", file_index, tmp_file.name)

                event_date += renewal_gap + timedelta(days=random.randint(-2, 2))

        tmp_file.close()
        logger.info("Finished final part %s with %s rows", file_index, f"{rows_in_file:,}")

        if rows_in_file > 0:
            finalize_chunk(storage_client, bq_client, tmp_file.name, file_index)
        else:
            if os.path.exists(tmp_file.name):
                os.unlink(tmp_file.name)
                logger.info("Deleted empty temp file: %s", tmp_file.name)

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        raise

    logger.info(
        "All done. %s total rows across %s file(s) loaded into %s.%s.%s",
        f"{total_rows:,}",
        file_index,
        GCP_PROJECT_ID,
        BQ_DATASET,
        BQ_TABLE,
    )


if __name__ == "__main__":
    main()
