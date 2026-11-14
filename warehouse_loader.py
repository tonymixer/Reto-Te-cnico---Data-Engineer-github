
import os
import pandas as pd
import numpy as np

from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import psycopg2
from datetime import datetime, timezone
from decimal import Decimal


load_dotenv()

PROCESSED_DIR = Path("./processed")

DATABASE_URL = (
    "postgresql+psycopg2://postgres:1234@localhost:5432/postgres"
)
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)





def prepare_dataframe(df):

    df = df.copy()

    # Compatibilidad con diferentes versiones del generador
    defaults = {
        "user_id": "UNKNOWN",
        "merchant_id": "UNKNOWN",
        "merchant_name": "UNKNOWN",
        "merchant_country": "UNKNOWN",
        "payment_method": "UNKNOWN",
        "user_country": "UNKNOWN",
        "country": "UNKNOWN",
        "currency": "USD",
        "status": "UNKNOWN",
        "response_code": "UNKNOWN",
        "is_suspicious": False,
        "suspicious_reason": "",
        "amount_outlier": False
    }

    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default

    if "transaction_id" not in df.columns:
        raise ValueError("Falta transaction_id")

    if "transaction_date" not in df.columns and "timestamp" in df.columns:
        df["transaction_date"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="coerce",
        utc=True
    )

    df["amount_original"] = pd.to_numeric(
        df.get("amount_original", df.get("amount")),
        errors="coerce"
    )

    if "amount_usd" not in df.columns:
        df["amount_usd"] = np.where(
            df["currency"].eq("USD"),
            df["amount_original"],
            np.nan
        )

    df["amount_usd"] = pd.to_numeric(
        df["amount_usd"],
        errors="coerce"
    )

    df["is_suspicious"] = (
        df["is_suspicious"]
        .fillna(False)
        .astype(bool)
    )

    df["amount_outlier"] = (
        df["amount_outlier"]
        .fillna(False)
        .astype(bool)
    )

    df = df.dropna(
        subset=[
            "transaction_id",
            "transaction_date",
            "amount_original"
        ]
    )

    df = df.drop_duplicates(
        subset=["transaction_id"],
        keep="last"
    )

    return df


def load_dimension_users(df):

    users = df[[
        "user_id",
        "user_country"
    ]].drop_duplicates("user_id")

    users.to_sql(
        "stg_users",
        engine,
        schema="fraud_dw",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO fraud_dw.dim_users (
                user_id,
                user_country
            )
            SELECT
                user_id,
                user_country
            FROM fraud_dw.stg_users
            ON CONFLICT (user_id)
            DO UPDATE SET
                user_country = EXCLUDED.user_country;
        """))


def load_dimension_merchants(df):

    merchants = df[[
        "merchant_id",
        "merchant_name",
        "merchant_country"
    ]].drop_duplicates("merchant_id")

    merchants.to_sql(
        "stg_merchants",
        engine,
        schema="fraud_dw",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO fraud_dw.dim_merchants (
                merchant_id,
                merchant_name,
                merchant_country
            )
            SELECT
                merchant_id,
                merchant_name,
                merchant_country
            FROM fraud_dw.stg_merchants
            ON CONFLICT (merchant_id)
            DO UPDATE SET
                merchant_name = EXCLUDED.merchant_name,
                merchant_country = EXCLUDED.merchant_country;
        """))


def load_dimension_payment_methods(df):

    methods = df[["payment_method"]].drop_duplicates()

    methods.to_sql(
        "stg_payment_methods",
        engine,
        schema="fraud_dw",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO fraud_dw.dim_payment_methods (
                payment_method
            )
            SELECT payment_method
            FROM fraud_dw.stg_payment_methods
            ON CONFLICT (payment_method)
            DO NOTHING;
        """))


def load_dimension_time(df):

    dates = pd.to_datetime(
        df["transaction_date"],
        utc=True
    )

    dim_time = pd.DataFrame({
        "time_key": dates.dt.strftime("%Y%m%d").astype(int),
        "full_date": dates.dt.date,
        "year": dates.dt.year,
        "quarter": dates.dt.quarter,
        "month": dates.dt.month,
        "month_name": dates.dt.month_name(),
        "day": dates.dt.day,
        "day_of_week": dates.dt.dayofweek,
        "day_name": dates.dt.day_name(),
        "hour": dates.dt.hour,
        "is_weekend": dates.dt.dayofweek >= 5
    }).drop_duplicates("time_key")

    dim_time.to_sql(
        "stg_time",
        engine,
        schema="fraud_dw",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO fraud_dw.dim_time (
                time_key,
                full_date,
                year,
                quarter,
                month,
                month_name,
                day,
                day_of_week,
                day_name,
                hour,
                is_weekend
            )
            SELECT
                time_key,
                full_date,
                year,
                quarter,
                month,
                month_name,
                day,
                day_of_week,
                day_name,
                hour,
                is_weekend
            FROM fraud_dw.stg_time
            ON CONFLICT (time_key)
            DO NOTHING;
        """))


def load_fact_transactions(df):

    df = df.copy()

    dates = pd.to_datetime(
        df["transaction_date"],
        utc=True
    )

    df["time_key"] = dates.dt.strftime(
        "%Y%m%d"
    ).astype(int)

    df["is_international"] = (
        df["country"] != df["user_country"]
    )

    columns = [
        "transaction_id",
        "user_id",
        "merchant_id",
        "payment_method",
        "time_key",
        "amount_original",
        "currency",
        "amount_usd",
        "status",
        "response_code",
        "country",
        "is_international",
        "is_suspicious",
        "suspicious_reason",
        "amount_outlier"
    ]

    df["suspicious_reason"] = (
        df["suspicious_reason"].fillna("")
    )

    df[columns].to_sql(
        "stg_transactions",
        engine,
        schema="fraud_dw",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO fraud_dw.fact_transactions (
                transaction_id,
                user_key,
                merchant_key,
                time_key,
                payment_method_key,
                amount_original,
                currency,
                amount_usd,
                status,
                response_code,
                country,
                is_international,
                is_suspicious,
                suspicious_reason,
                amount_outlier
            )
            SELECT
                s.transaction_id,
                u.user_key,
                m.merchant_key,
                s.time_key,
                p.payment_method_key,
                s.amount_original,
                s.currency,
                s.amount_usd,
                s.status,
                s.response_code,
                s.country,
                s.is_international,
                s.is_suspicious,
                s.suspicious_reason,
                s.amount_outlier
            FROM fraud_dw.stg_transactions s
            JOIN fraud_dw.dim_users u
                ON s.user_id = u.user_id
            JOIN fraud_dw.dim_merchants m
                ON s.merchant_id = m.merchant_id
            JOIN fraud_dw.dim_payment_methods p
                ON s.payment_method = p.payment_method
            ON CONFLICT (transaction_id)
            DO NOTHING;
        """))


def run_warehouse_etl():

    files = list(PROCESSED_DIR.glob("*.csv"))

    if not files:
        print("No existen archivos para cargar.")
        return

    for file in files:

        print(f"Procesando: {file.name}")

        df = pd.read_csv(file)

        df = prepare_dataframe(df)

        if df.empty:
            print("Archivo sin registros válidos.")
            continue

        load_dimension_users(df)
        load_dimension_merchants(df)
        load_dimension_payment_methods(df)
        load_dimension_time(df)

        load_fact_transactions(df)

        print(
            f"Carga completada: {len(df)} registros procesados."
        )


if __name__ == "__main__":
    run_warehouse_etl()
