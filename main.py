import os
import time
import logging
import boto3
import pandas as pd

from pathlib import Path
from datetime import datetime, timezone

from scripts.generate_transactions import generate_transactions
from kafka_producer import FraudProducer


# ============================================================
# CONFIGURACION
# ============================================================

TRANSACTIONS_FOLDER = Path("./transactions")
PROCESSED_FOLDER = Path("./processed")
SUSPICIOUS_FOLDER = Path("./suspicious")
DATA_FOLDER = Path("./data")

INTERVAL_SECONDS = 60
TRANSACTIONS_PER_BATCH = 100

BUCKET_NAME = os.getenv(
    "S3_BUCKET",
    "transactions-datalake-anthony-2026-001"
)

S3_RAW_PREFIX = "raw"
S3_PROCESSED_PREFIX = "processed"
S3_SUSPICIOUS_PREFIX = "suspicious"

# Cliente S3: utiliza las credenciales configuradas
# mediante AWS CLI o variables de entorno.
s3 = boto3.client("s3")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# PREPARACION DE CARPETAS
# ============================================================

def setup_folders():

    folders = [
        TRANSACTIONS_FOLDER,
        PROCESSED_FOLDER,
        SUSPICIOUS_FOLDER,
        DATA_FOLDER
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)

    logging.info("Carpetas locales verificadas.")


# ============================================================
# SUBIR ARCHIVOS A AMAZON S3
# ============================================================

def upload_to_s3(local_file, s3_key):

    try:

        s3.upload_file(
            str(local_file),
            BUCKET_NAME,
            s3_key
        )

        logging.info(
            f"Archivo subido a S3: s3://{BUCKET_NAME}/{s3_key}"
        )

        return True

    except Exception as error:

        logging.error(f"Error subiendo archivo a S3: {error}")
        raise


# ============================================================
# CARGAR DATASETS MAESTROS
# ============================================================

def upload_reference_data():

    reference_files = {
        "users.csv": "users.csv",
        "companies.csv": "companies.csv",
        "payment_methods.csv": "payment_methods.csv"
    }

    for local_name, s3_name in reference_files.items():

        local_file = DATA_FOLDER / local_name

        if local_file.exists():

            upload_to_s3(
                local_file,
                f"{S3_RAW_PREFIX}/dimensions/{s3_name}"
            )

        else:

            logging.warning(
                f"No se encontró el dataset: {local_file}"
            )


# ============================================================
# GENERAR TRANSACCIONES
# ============================================================

def generate_batch():

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    df = generate_transactions(TRANSACTIONS_PER_BATCH)

    filename = f"transactions_{timestamp}.csv"

    raw_file = TRANSACTIONS_FOLDER / filename

    df.to_csv(raw_file, index=False)

    # Guardar el archivo original en S3
    upload_to_s3(
        raw_file,
        f"{S3_RAW_PREFIX}/transactions/{filename}"
    )

    logging.info(
        f"Lote generado: {len(df)} transacciones"
    )

    return raw_file


# ============================================================
# LIMPIEZA DE DATOS
# ============================================================

def clean_data(df):

    df = df.copy()

    initial_count = len(df)

    # Normalizar textos
    text_columns = [
        "currency",
        "status",
        "payment_method",
        "payment_provider",
        "country",
        "response_code",
        "device_type",
        "category"
    ]

    for column in text_columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
                .str.upper()
            )

    # Conversión de tipos numéricos
    numeric_columns = [
        "user_id",
        "merchant_id",
        "amount",
        "attempt_number",
        "processing_time_ms",
        "transaction_fee",
        "net_amount",
        "installments"
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # Fecha
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    # Campos obligatorios
    required_columns = [
        "transaction_id",
        "user_id",
        "merchant_id",
        "amount",
        "currency",
        "timestamp"
    ]

    df = df.dropna(
        subset=required_columns
    )

    # Validaciones
    df = df[
        (df["amount"] > 0) &
        (df["user_id"] > 0) &
        (df["merchant_id"] > 0)
    ]

    if "attempt_number" in df.columns:

        df = df[
            df["attempt_number"].fillna(1) >= 1
        ]

    # Eliminar transacciones duplicadas
    df = df.drop_duplicates(
        subset=["transaction_id"]
    )

    # Limpiar direcciones IP faltantes
    if "ip_address" in df.columns:

        df["ip_address"] = (
            df["ip_address"]
            .fillna("UNKNOWN")
        )

    logging.info(
        f"Limpieza: {initial_count} registros iniciales, "
        f"{len(df)} registros válidos."
    )

    return df.reset_index(drop=True)


# ============================================================
# ENRIQUECIMIENTO CON DATASETS MAESTROS
# ============================================================

def enrich_transactions(df):

    df = df.copy()

    users_file = DATA_FOLDER / "users.csv"
    companies_file = DATA_FOLDER / "companies.csv"
    methods_file = DATA_FOLDER / "payment_methods.csv"

    # --------------------------------------------------------
    # USUARIOS
    # --------------------------------------------------------

    if users_file.exists():

        users = pd.read_csv(users_file)

        user_columns = [
            "user_id",
            "account_status",
            "kyc_level",
            "kyc_verified",
            "transaction_limit_daily",
            "risk_score"
        ]

        users = users[
            [column for column in user_columns
             if column in users.columns]
        ]

        users = users.drop_duplicates(
            subset=["user_id"]
        )

        df = df.merge(
            users,
            on="user_id",
            how="left",
            validate="many_to_one"
        )

    # --------------------------------------------------------
    # COMERCIOS
    # --------------------------------------------------------

    if companies_file.exists():

        companies = pd.read_csv(companies_file)

        company_columns = [
            "merchant_id",
            "merchant_status",
            "kyc_verified",
            "pci_compliant",
            "chargeback_rate",
            "risk_score"
        ]

        companies = companies[
            [column for column in company_columns
             if column in companies.columns]
        ]

        companies = companies.drop_duplicates(
            subset=["merchant_id"]
        )

        # Renombrar campos para distinguirlos
        companies = companies.rename(
            columns={
                "kyc_verified": "merchant_kyc_verified",
                "risk_score": "merchant_risk_score"
            }
        )

        df = df.merge(
            companies,
            on="merchant_id",
            how="left",
            validate="many_to_one"
        )

    # --------------------------------------------------------
    # METODOS DE PAGO
    # --------------------------------------------------------

    if methods_file.exists():

        methods = pd.read_csv(methods_file)

        # No hacemos un merge directo porque un usuario
        # puede tener varios métodos de pago.
        # Primero agregamos indicadores por usuario.

        if "user_id" in methods.columns:

            methods["is_blocked"] = (
                methods["status"]
                .astype("string")
                .str.lower()
                .eq("blocked")
            )

            methods["is_expired"] = (
                methods["status"]
                .astype("string")
                .str.lower()
                .eq("expired")
            )

            methods["risk_score"] = pd.to_numeric(
                methods["risk_score"],
                errors="coerce"
            )

            payment_summary = methods.groupby(
                "user_id"
            ).agg(
                payment_methods_total=("payment_method_id", "count"),
                blocked_methods=("is_blocked", "sum"),
                expired_methods=("is_expired", "sum"),
                average_payment_risk=("risk_score", "mean")
            ).reset_index()

            df = df.merge(
                payment_summary,
                on="user_id",
                how="left",
                validate="many_to_one"
            )

    logging.info(
        "Enriquecimiento de transacciones finalizado."
    )

    return df


# ============================================================
# DETECCION DE TRANSACCIONES SOSPECHOSAS
# ============================================================

def detect_suspicious_transactions(df):

    df = df.copy()

    # Indicador inicial
    df["fraud_reasons"] = ""

    def add_reason(condition, reason):

        condition = condition.fillna(False)

        df.loc[condition, "fraud_reasons"] = (
            df.loc[condition, "fraud_reasons"]
            .apply(
                lambda current:
                f"{current}; {reason}".strip("; ")
            )
        )

    # --------------------------------------------------------
    # REGLAS SOBRE TRANSACCIONES
    # --------------------------------------------------------

    add_reason(
        df["attempt_number"] >= 3,
        "MULTIPLE_ATTEMPTS"
    )

    add_reason(
        df["status"] == "DECLINED",
        "DECLINED_TRANSACTION_REVIEW"
    )

    add_reason(
        df["processing_time_ms"] >= 2500,
        "HIGH_PROCESSING_TIME"
    )

    # Verificación 3DS solo para tarjetas
    if "three_ds_verified" in df.columns:

        is_card = df["payment_method"].isin(
            ["CREDIT_CARD", "DEBIT_CARD"]
        )

        three_ds_failed = (
            df["three_ds_verified"]
            .astype("string")
            .str.lower()
            .eq("false")
        )

        add_reason(
            is_card & three_ds_failed,
            "UNVERIFIED_3DS"
        )

    # --------------------------------------------------------
    # REGLAS SOBRE USUARIOS
    # --------------------------------------------------------

    if "account_status" in df.columns:

        add_reason(
            df["account_status"]
            .astype("string")
            .str.lower()
            .isin(["suspended", "pending_verification"]),
            "USER_ACCOUNT_REVIEW"
        )

    if "kyc_verified" in df.columns:

        add_reason(
            ~df["kyc_verified"].fillna(False).astype(bool),
            "USER_KYC_NOT_VERIFIED"
        )

    if "risk_score" in df.columns:

        add_reason(
            pd.to_numeric(
                df["risk_score"],
                errors="coerce"
            ) >= 80,
            "HIGH_USER_RISK_SCORE"
        )

    # --------------------------------------------------------
    # REGLAS SOBRE COMERCIOS
    # --------------------------------------------------------

    if "merchant_status" in df.columns:

        add_reason(
            df["merchant_status"]
            .astype("string")
            .str.lower()
            .ne("active"),
            "MERCHANT_NOT_ACTIVE"
        )

    if "merchant_risk_score" in df.columns:

        add_reason(
            pd.to_numeric(
                df["merchant_risk_score"],
                errors="coerce"
            ) >= 80,
            "HIGH_MERCHANT_RISK_SCORE"
        )

    if "chargeback_rate" in df.columns:

        add_reason(
            pd.to_numeric(
                df["chargeback_rate"],
                errors="coerce"
            ) >= 2,
            "HIGH_MERCHANT_CHARGEBACK_RATE"
        )

    # --------------------------------------------------------
    # REGLAS SOBRE METODOS DE PAGO
    # --------------------------------------------------------

    if "blocked_methods" in df.columns:

        add_reason(
            df["blocked_methods"].fillna(0) > 0,
            "USER_HAS_BLOCKED_PAYMENT_METHOD"
        )

    if "average_payment_risk" in df.columns:

        add_reason(
            df["average_payment_risk"] >= 80,
            "HIGH_PAYMENT_METHOD_RISK"
        )

    # --------------------------------------------------------
    # CLASIFICACION FINAL
    # --------------------------------------------------------

    df["is_suspicious"] = (
        df["fraud_reasons"] != ""
    )

    suspicious = df[
        df["is_suspicious"]
    ].copy()

    normal = df[
        ~df["is_suspicious"]
    ].copy()

    logging.info(
        f"Detección finalizada: "
        f"{len(suspicious)} sospechosas y "
        f"{len(normal)} normales."
    )

    return normal, suspicious


# ============================================================
# PUBLICAR TRANSACCIONES SOSPECHOSAS EN KAFKA
# ============================================================

def publish_suspicious_transactions(df, producer):

    if df.empty:
        logging.info("No hay transacciones sospechosas para Kafka.")
        return

    published = 0

    for _, row in df.iterrows():

        # Convertir el timestamp local del equipo a UTC.
        detected_at = (
            pd.Timestamp(row["timestamp"])
            .to_pydatetime()
            .astimezone(timezone.utc)
            .isoformat()
        )

        transaction = {
            "transaction_id": str(row["transaction_id"]),
            "user_id": int(row["user_id"]),
            "merchant_id": int(row["merchant_id"]),
            "amount": float(row["amount"]),
            "currency": str(row["currency"]),
            "is_fraud": True,
            "fraud_reason": str(row["fraud_reasons"]),
            "detected_at": detected_at
        }

        producer.send_transaction(transaction)
        published += 1

    logging.info(
        f"Kafka: {published} transacciones sospechosas enviadas."
    )


# ============================================================
# PROCESAR LOTE
# ============================================================

def process_batch(raw_file, producer):

    logging.info(
        f"Procesando archivo: {raw_file.name}"
    )

    # Leer transacciones
    df = pd.read_csv(raw_file)

    # Limpiar
    df = clean_data(df)

    # Enriquecer
    df = enrich_transactions(df)

    # Detectar
    normal, suspicious = detect_suspicious_transactions(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Guardar normales
    processed_file = (
        PROCESSED_FOLDER /
        f"processed_{timestamp}.csv"
    )

    normal.to_csv(
        processed_file,
        index=False
    )

    # Guardar sospechosas
    suspicious_file = (
        SUSPICIOUS_FOLDER /
        f"suspicious_{timestamp}.csv"
    )

    suspicious.to_csv(
        suspicious_file,
        index=False
    )

    # Subir resultados a S3
    upload_to_s3(
        processed_file,
        f"{S3_PROCESSED_PREFIX}/{processed_file.name}"
    )

    upload_to_s3(
        suspicious_file,
        f"{S3_SUSPICIOUS_PREFIX}/{suspicious_file.name}"
    )

    # Publicar en Kafka las transacciones que detectó el pipeline.
    publish_suspicious_transactions(suspicious, producer)

    logging.info(
        f"Lote terminado. Normales: {len(normal)} | "
        f"Sospechosas: {len(suspicious)}"
    )


# ============================================================
# EJECUCION PRINCIPAL
# ============================================================

def main():

    setup_folders()

    logging.info(
        f"Conectando con Amazon S3: {BUCKET_NAME}"
    )

    # Verificar acceso al bucket sin listar todos
    # los buckets de la cuenta.
    s3.head_bucket(
        Bucket=BUCKET_NAME
    )

    logging.info("Conexión con Amazon S3 exitosa.")

    # Subir dimensiones una vez al iniciar.
    upload_reference_data()

    logging.info(
        "Iniciando pipeline de detección de fraude."
    )

    # Crear un único productor y reutilizarlo en todos los lotes.
    producer = FraudProducer()

    try:

        while True:

            raw_file = generate_batch()

            process_batch(raw_file, producer)

            logging.info(
                f"Esperando {INTERVAL_SECONDS} segundos..."
            )

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:

        logging.info(
            "Pipeline detenido por el usuario."
        )

    finally:

        # Esperar a que Kafka confirme los mensajes pendientes.
        producer.close()

        logging.info(
            "Kafka Producer cerrado correctamente."
        )


if __name__ == "__main__":

    main()
