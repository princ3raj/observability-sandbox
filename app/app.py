import os
import random
import time
import asyncio
import uuid
import json
from datetime import datetime
import structlog
from fastapi import FastAPI, Response, status
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from aiokafka import AIOKafkaProducer

# 1. Setup Structured Logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

# 2. Setup OpenTelemetry Metrics (Push directly to VictoriaMetrics via OTLP/HTTP)
resource = Resource(attributes={"service.name": "payment-gateway"})
exporter = OTLPMetricExporter(endpoint="http://victoria-metrics:8428/opentelemetry/api/v1/push")
reader = PeriodicExportingMetricReader(exporter)
provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(provider)
meter = metrics.get_meter("payment.meter")

# Define Metrics
payment_counter = meter.create_counter(
    "payment_total", unit="1", description="Total number of payment attempts"
)
payment_latency = meter.create_histogram(
    "payment_latency", unit="ms", description="Latency of payment processing"
)
order_counter = meter.create_counter(
    "order_total", unit="1", description="Total number of orders created"
)

app = FastAPI(title="Payment Gateway")

# 3. Kafka Producer Setup
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Retry logic for Kafka connection
    max_retries = 10
    for i in range(max_retries):
        try:
            await producer.start()
            logger.info("Kafka Producer started", bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            return
        except Exception as e:
            if i == max_retries - 1:
                logger.error("Failed to start Kafka Producer after max retries", error=str(e))
                raise e
            logger.warning(f"Kafka not ready, retrying ({i+1}/{max_retries})...")
            await asyncio.sleep(5)

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
    logger.info("Kafka Producer stopped")

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/order")
async def create_order():
    request_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1000, 9999)}"
    
    # Record Metric
    order_counter.add(1)
    
    logger.info("Order created", 
                request_id=request_id, 
                user_id=user_id,
                level="INFO")
    
    return {"order_id": str(uuid.uuid4()), "status": "created"}

@app.post("/pay")
async def process_payment(response: Response):
    start_time = time.time()
    transaction_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1000, 9999)}"
    amount = round(random.uniform(10.0, 500.0), 2)
    
    # Simulate Random Latency (100ms to 2s)
    sim_latency = random.uniform(0.1, 2.0)
    await asyncio.sleep(sim_latency)
    
    # Simulate 10% failure rate
    success = random.random() > 0.1
    status_str = "success" if success else "failed"
    
    latency_ms = int((time.time() - start_time) * 1000)
    
    # Record Metrics
    payment_counter.add(1, {"status": status_str, "currency": "USD"})
    payment_latency.record(latency_ms, {"status": status_str})
    
    # Produce Transaction Event to Kafka
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "transaction_id": transaction_id,
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "currency": "USD",
        "status": status_str,
        "latency_ms": latency_ms
    }
    
    await producer.send_and_wait("transaction-events", event)
    
    # Log the transaction
    log_level = "INFO" if success else "ERROR"
    logger.info("Payment processed", 
                transaction_id=transaction_id,
                order_id=order_id,
                user_id=user_id,
                status=status_str,
                amount=amount,
                latency_ms=latency_ms,
                level=log_level)
    
    if not success:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        
    return event
