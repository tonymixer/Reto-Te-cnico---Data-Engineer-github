
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from confluent_kafka import Consumer, KafkaException


KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-alerts-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

TOPIC = "suspicious-transactions"


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password=1234
    )


class FraudConsumer:

    def __init__(self):
        self.consumer = Consumer(KAFKA_CONFIG)
        self.consumer.subscribe([TOPIC])

        self.db = get_db_connection()

        self.running = True

    def generate_alert(self, transaction):

        print("\n" + "=" * 50)
        print(" ALERTA DE FRAUDE DETECTADO")
        print("=" * 50)

        print(f"Transaccion: {transaction.get('transaction_id')}")
        print(f"Usuario: {transaction.get('user_id')}")
        print(f"Comercio: {transaction.get('merchant_id')}")
        print(f"Monto: ${transaction.get('amount', 0)}")
        print(f"Motivo: {transaction.get('fraud_reason')}")

        print("=" * 50)

    def register_metrics(self, transaction, detected_at):

        amount = Decimal(str(
            transaction.get("amount_usd", transaction.get("amount", 0))
        ))

        # Agrupamos la fecha y hora al inicio del minuto.
        minute = detected_at.replace(second=0, microsecond=0)

        with self.db.cursor() as cursor:

            cursor.execute("""
                INSERT INTO fraud_dw.fraud_metrics_minute (
                    minute_timestamp,
                    fraud_count,
                    suspicious_amount,
                    updated_at
                )
                VALUES (%s, 1, %s, NOW())

                ON CONFLICT (minute_timestamp)
                DO UPDATE SET
                    fraud_count =
                        fraud_dw.fraud_metrics_minute.fraud_count + 1,

                    suspicious_amount =
                        fraud_dw.fraud_metrics_minute.suspicious_amount
                        + EXCLUDED.suspicious_amount,

                    updated_at = NOW()
            """, (minute, amount))

    def show_metrics(self):

        with self.db.cursor() as cursor:

            cursor.execute("""
                SELECT
                    minute_timestamp,
                    fraud_count,
                    suspicious_amount
                FROM fraud_dw.fraud_metrics_minute
                ORDER BY minute_timestamp DESC
                LIMIT 1
            """)

            result = cursor.fetchone()

        if result:
            print("\n" + "#" * 50)
            print("ESTADISTICAS DE FRAUDE POR MINUTO")
            print("#" * 50)
            print(f"Minuto: {result[0]}")
            print(f"Fraudes detectados: {result[1]}")
            print(f"Monto sospechoso: ${result[2]:,.2f}")
            print("#" * 50)

    def save_alert(self, transaction):

        transaction_id = transaction.get("transaction_id")

        if not transaction_id:
            raise ValueError("La transaccion no tiene transaction_id")

        amount = Decimal(str(
            transaction.get("amount_usd", transaction.get("amount", 0))
        ))

        user_id = transaction.get("user_id")
        merchant_id = transaction.get("merchant_id")

        if user_id is not None:
            user_id = int(user_id)

        if merchant_id is not None:
            merchant_id = int(merchant_id)

        # Usamos la fecha del evento si viene en el mensaje.
        # Si no viene, utilizamos la hora actual UTC.
        timestamp = transaction.get("detected_at")

        if timestamp:
            detected_at = datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )

            if detected_at.tzinfo is None:
                detected_at = detected_at.replace(
                    tzinfo=timezone.utc
                )
        else:
            detected_at = datetime.now(timezone.utc)

        # Primero insertamos la alerta.
        with self.db.cursor() as cursor:

            cursor.execute("""
                INSERT INTO fraud_dw.fraud_alerts (
                    transaction_id,
                    user_id,
                    merchant_id,
                    amount_usd,
                    fraud_reason,
                    detected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)

                ON CONFLICT (transaction_id) DO NOTHING

                RETURNING alert_id
            """, (
                transaction_id,
                user_id,
                merchant_id,
                amount,
                transaction.get("fraud_reason"),
                detected_at
            ))

            inserted = cursor.fetchone()

        # Solo acumulamos métricas si la alerta es nueva.
        if inserted:
            self.register_metrics(transaction, detected_at)
            self.db.commit()

            print("Alerta guardada correctamente en PostgreSQL.")
            self.show_metrics()

            return True

        else:
            self.db.commit()
            print(f"Alerta duplicada ignorada: {transaction_id}")
            return False

    def process_message(self, message):

        try:
            transaction = json.loads(
                message.value().decode("utf-8")
            )

        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            print(f"Mensaje invalido: {error}")

            # Evitamos bloquear el topic por un mensaje mal formado.
            self.consumer.commit(
                message=message,
                asynchronous=False
            )
            return

        try:

            if transaction.get("is_fraud") is True:

                self.generate_alert(transaction)

                # Guardamos alerta y métricas en una transacción SQL.
                self.save_alert(transaction)

            else:
                print("Transaccion no fraudulenta.")

            # Confirmamos Kafka solamente después de procesar
            # correctamente el mensaje.
            self.consumer.commit(
                message=message,
                asynchronous=False
            )

        except Exception as error:

            self.db.rollback()

            print(f"Error procesando transaccion: {error}")
            print("El mensaje no fue confirmado en Kafka.")

    def run(self):

        print("Kafka Consumer iniciado...")
        print(f"Escuchando topic: {TOPIC}")
        print("PostgreSQL conectado.")

        try:

            while self.running:

                message = self.consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                self.process_message(message)

        except KeyboardInterrupt:
            print("\nDeteniendo Consumer...")

        finally:

            self.consumer.close()
            self.db.close()

            print("Consumer detenido.")

    def stop(self):
        self.running = False


if __name__ == "__main__":

    fraud_consumer = FraudConsumer()
    fraud_consumer.run()
