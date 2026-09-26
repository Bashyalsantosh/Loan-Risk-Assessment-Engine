#!/usr/bin/env python3
"""
Real-Time Loan Application Kafka Stream Simulator
Generates synthetic, high-throughput loan application streams to evaluate
Kafka ingestion pipelines, Spark Structured Streaming, and real-time inference.
"""

import json
import time
import uuid
import random
import logging
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel, Field, ValidationError
from confluent_kafka import Producer, KafkaError, KafkaException

# Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s"
)
logger = logging.getLogger("KafkaStreamSimulator")


# =====================================================================
# 1. Schema Validation (Pydantic Model)
# =====================================================================
class LoanApplicationSchema(BaseModel):
    application_id: str = Field(..., description="Unique Application UUID")
    client_id: str = Field(..., description="Anonymized Client Identifier")
    timestamp: str = Field(..., description="ISO 8601 UTC Event Timestamp")
    requested_amount: float = Field(..., ge=1000.0, le=1000000.0)
    dti: float = Field(..., ge=0.0, le=1.0, description="Debt-to-Income Ratio")
    ltv: float = Field(..., ge=0.0, le=2.0, description="Loan-to-Value Ratio")
    p_instances: int = Field(..., ge=0, le=20, description="Past-Due Instances")
    employment_years: int = Field(..., ge=0, le=50)
    credit_score: int = Field(..., ge=300, le=850)


# =====================================================================
# 2. Producer Callback & Delivery Delivery Tracking
# =====================================================================
class DeliveryTracker:
    def __init__(self):
        self.delivered_records = 0
        self.failed_records = 0

    def delivery_report(self, err, msg):
        """Asynchronous execution callback for Kafka broker delivery ACK."""
        if err is not None:
            self.failed_records += 1
            logger.error(f"Message delivery failed: {err}")
        else:
            self.delivered_records += 1


# =====================================================================
# 3. Stream Generator Core Engine
# =====================================================================
class KafkaStreamSimulator:
    def __init__(self, bootstrap_servers: str, topic: str, target_tps: int):
        self.topic = topic
        self.target_tps = target_tps
        self.tracker = DeliveryTracker()

        # Enterprise High-Throughput Confluent Kafka Producer Configuration
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'loan-stream-simulator-v2',
            'acks': 1,                            # Leader acknowledgment
            'compression.type': 'snappy',        # Low-latency compression
            'queue.buffering.max.messages': 500000,
            'queue.buffering.max.ms': 10,        # Batching threshold (10ms)
            'batch.num.messages': 500,          # Messages per batch
            'linger.ms': 10
        }
        
        try:
            self.producer = Producer(producer_config)
            logger.info(f"Connected to Kafka brokers: {bootstrap_servers}")
        except KafkaException as e:
            logger.critical(f"Failed to initialize Kafka Producer: {str(e)}")
            raise e

    @staticmethod
    def generate_synthetic_payload() -> dict:
        """Generates realistic banking client application data with varied risk profiles."""
        
        # Biased distribution to simulate real credit risk proportions (80% Low Risk, 15% Elevated, 5% High Risk)
        profile = random.choices(["low_risk", "elevated_risk", "high_risk"], weights=[0.80, 0.15, 0.05])[0]

        if profile == "low_risk":
            dti = round(random.uniform(0.10, 0.38), 4)
            ltv = round(random.uniform(0.30, 0.65), 4)
            p_instances = random.choice([0, 0, 0, 1])
            credit_score = random.randint(700, 850)
        elif profile == "elevated_risk":
            dti = round(random.uniform(0.39, 0.55), 4)
            ltv = round(random.uniform(0.66, 0.85), 4)
            p_instances = random.randint(1, 3)
            credit_score = random.randint(600, 699)
        else:  # High Risk / Default Prone
            dti = round(random.uniform(0.56, 0.95), 4)
            ltv = round(random.uniform(0.86, 1.45), 4)
            p_instances = random.randint(3, 8)
            credit_score = random.randint(300, 599)

        payload = {
            "application_id": f"APP-{uuid.uuid4()}",
            "client_id": f"CLT-{random.randint(100000, 999999)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requested_amount": round(random.uniform(5000.0, 500000.0), 2),
            "dti": dti,
            "ltv": ltv,
            "p_instances": p_instances,
            "employment_years": random.randint(0, 35),
            "credit_score": credit_score
        }
        return payload

    def produce_batch(self, batch_size: int):
        """Generates, validates, and dispatches a batch of events asynchronously."""
        for _ in range(batch_size):
            data = self.generate_synthetic_payload()

            # Enforce Contract Validation prior to ingestion
            try:
                validated_event = LoanApplicationSchema(**data)
                payload_bytes = json.dumps(validated_event.model_dump()).encode('utf-8')
                key_bytes = validated_event.client_id.encode('utf-8')

                # Non-blocking produce
                self.producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=payload_bytes,
                    on_delivery=self.tracker.delivery_report
                )
            except ValidationError as ve:
                logger.error(f"Payload contract validation error: {ve}")
            except BufferError:
                logger.warning("Local Kafka producer queue full! Polling...")
                self.producer.poll(100)

            # Serve delivery callbacks periodically
            self.producer.poll(0)

    def start_streaming(self, duration_seconds: int = 0):
        """Runs the event stream loop targeted to hit specific TPS throughputs."""
        logger.info(f"Starting stream generator targeting ~{self.target_tps} Transactions Per Second (TPS)...")
        start_time = time.time()
        batch_size = max(1, int(self.target_tps / 10))  # 10 execution windows per second
        interval = 0.1  # 100ms interval

        try:
            total_sent = 0
            while True:
                window_start = time.time()
                self.produce_batch(batch_size)
                total_sent += batch_size

                # Measure and throttle execution rate
                elapsed = time.time() - window_start
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Periodic Statistics Log
                if total_sent % (self.target_tps * 5) == 0:
                    elapsed_total = time.time() - start_time
                    current_tps = round(total_sent / elapsed_total, 2)
                    logger.info(
                        f"Telemetry -> Sent: {total_sent} | "
                        f"Delivered: {self.tracker.delivered_records} | "
                        f"Failed: {self.tracker.failed_records} | "
                        f"Actual Throughput: {current_tps} TPS"
                    )

                # Duration cutoff check
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    logger.info("Target stream duration reached. Shutting down...")
                    break

        except KeyboardInterrupt:
            logger.info("Manual interrupt received. Flushing buffer...")
        finally:
            # Block until outstanding messages are delivered
            self.producer.flush(timeout=10)
            logger.info("Kafka Producer successfully flushed and shut down.")


# =====================================================================
# 4. CLI Entry Point
# =====================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Loan Application Kafka Stream Simulator")
    parser.add_argument("--brokers", type=str, default="localhost:9092", help="Kafka Bootstrap Servers")
    parser.add_argument("--topic", type=str, default="loan.applications.raw", help="Target Kafka Topic")
    parser.add_argument("--tps", type=int, default=500, help="Target Transactions Per Second (TPS)")
    parser.add_argument("--duration", type=int, default=0, help="Execution duration in seconds (0 = Infinite)")

    args = parser.parse_args()

    simulator = KafkaStreamSimulator(
        bootstrap_servers=args.brokers,
        topic=args.topic,
        target_tps=args.tps
    )
    simulator.start_streaming(duration_seconds=args.duration)
