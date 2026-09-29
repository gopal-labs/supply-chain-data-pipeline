"""
Supply chain pipeline DAG: ingest -> validate -> transform -> load.

Scheduled @daily (the realistic production pattern), but since the source
dataset is historical (2015-2018), trigger runs manually with a config
override to target a real partition:

    Airflow UI -> Trigger DAG w/ config -> {"batch_date": "2018-01-13"}

If no override is given, the run's own logical date (ds) is used, which is
what would happen automatically once this pipeline is fed real daily data.
"""
from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

RAW_DIR = Path("/opt/airflow/raw")
INGEST_SCRIPT = "/opt/airflow/ingest/ingest.py"
WAREHOUSE_CONN = "postgresql+psycopg2://warehouse:warehouse@warehouse:5432/warehouse"

REQUIRED_COLUMNS = [
    "Type",
    "Days for shipping (real)",
    "Late_delivery_risk",
    "order date (DateOrders)",
    "Order Id",
]

PII_COLUMNS = [
    "Customer Email",
    "Customer Password",
    "Customer Fname",
    "Customer Lname",
    "Customer Street",
]

default_args = {
    "owner": "gopal",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def resolve_batch_date() -> str:
    context = get_current_context()
    conf = context["dag_run"].conf or {}
    return conf.get("batch_date") or context["ds"]


def _sanitize_column(col: str) -> str:
    return (
        col.strip()
        .lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
    )


@dag(
    dag_id="supply_chain_pipeline",
    description="Daily ingest -> validate -> transform -> load pipeline for supply chain data",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    params={
        "batch_date": Param(
            None,
            type=["null", "string"],
            description="Override batch date, e.g. 2018-01-13. Leave empty to use the run's logical date.",
        )
    },
    tags=["supply-chain", "portfolio"],
)
def supply_chain_pipeline():

    @task
    def ingest() -> str:
        batch_date = resolve_batch_date()
        result = subprocess.run(
            ["python", INGEST_SCRIPT, "--batch-date", batch_date, "--no-overwrite"],
            capture_output=True,
            text=True,
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            logging.error(result.stderr)
            raise AirflowFailException(f"Ingestion failed for {batch_date}")
        return batch_date

    @task
    def validate(batch_date: str) -> str:
        partition_path = RAW_DIR / f"batch_date={batch_date}" / "data.csv"
        if not partition_path.exists():
            raise AirflowFailException(f"No partition found for {batch_date}")

        df = pd.read_csv(partition_path)
        if df.empty:
            raise AirflowFailException(f"Partition {batch_date} is empty")

        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            raise AirflowFailException(f"Partition {batch_date} missing columns: {missing_cols}")

        null_risk = int(df["Late_delivery_risk"].isna().sum())
        if null_risk:
            logging.warning(f"{null_risk} rows with null Late_delivery_risk in {batch_date}")

        logging.info(f"Validated {len(df)} rows for {batch_date}")
        return batch_date

    @task
    def transform(batch_date: str) -> str:
        partition_path = RAW_DIR / f"batch_date={batch_date}" / "data.csv"
        df = pd.read_csv(partition_path)

        df.columns = [c.strip() for c in df.columns]
        df["order_date"] = pd.to_datetime(df["order date (DateOrders)"], errors="coerce")
        df["shipping_date"] = pd.to_datetime(df.get("shipping date (DateOrders)"), errors="coerce")
        df["batch_date"] = batch_date
        df = df.drop(columns=PII_COLUMNS, errors="ignore")

        out_path = RAW_DIR / f"batch_date={batch_date}" / "staged.csv"
        df.to_csv(out_path, index=False)
        logging.info(f"Transformed {len(df)} rows -> {out_path}")
        return str(out_path)

    @task
    def load(staged_path: str) -> None:
        # Load as text: this is the raw/bronze layer, typing and casting
        # happens later in the dbt staging models (Phase 3).
        df = pd.read_csv(staged_path, dtype=str)
        df.columns = [_sanitize_column(c) for c in df.columns]
        batch_date = df["batch_date"].iloc[0]

        engine = create_engine(WAREHOUSE_CONN)
        metadata = MetaData(schema="raw")
        orders_raw = Table(
            "orders_raw",
            metadata,
            *[Column(col, String) for col in df.columns],
        )

        # Avoids pandas.to_sql entirely: across some pandas/SQLAlchemy
        # version combinations, pandas fails to detect an Engine or
        # Connection as a "SQLAlchemy connectable" and falls back to a
        # legacy raw-DBAPI2 path that doesn't work with either object
        # (AttributeError: '...' object has no attribute 'cursor').
        # Plain SQLAlchemy Core sidesteps that detection issue.
        with engine.begin() as conn:
            metadata.create_all(conn, checkfirst=True)
            conn.execute(
                text("DELETE FROM raw.orders_raw WHERE batch_date = :bd").bindparams(bd=batch_date)
            )
            records = df.where(pd.notnull(df), None).to_dict(orient="records")
            conn.execute(orders_raw.insert(), records)

        logging.info(f"Loaded {len(df)} rows into raw.orders_raw for batch_date={batch_date}")

    batch_date = ingest()
    validated_date = validate(batch_date)
    staged_path = transform(validated_date)
    load(staged_path)


supply_chain_pipeline()