import json
from confluent_kafka import Producer


KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "fraud-detection-producer",
    "acks": "all",
    "enable.idempotence": True
}

TOPIC = "suspicious-transactions"


class FraudProducer:

    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Error enviando mensaje: {err}")
        else:
            print(
                f"Fraude enviado correctamente | "
                f"Topic: {msg.topic()} | "
                f"Partition: {msg.partition()}"
            )

    def send_transaction(self, transaction):

        message = json.dumps(
            transaction,
            ensure_ascii=False,
            default=str
        )

        self.producer.produce(
            topic=TOPIC,
            key=str(transaction.get("transaction_id", "")),
            value=message,
            callback=self.delivery_report
        )

        self.producer.poll(0)

    def close(self):
        self.producer.flush()


if __name__ == "__main__":

    producer = FraudProducer()

    test_transaction = {
        "transaction_id": "TXN-TEST-001",
        "user_id": "1001",
        "merchant_id": "5001",
        "amount": 9500.00,
        "is_fraud": True,
        "fraud_reason": "Monto inusualmente elevado"
    }

    producer.send_transaction(test_transaction)
    producer.close()
