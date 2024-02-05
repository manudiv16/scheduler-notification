import asyncio
import os
import logging

from sender import NotificationSender
from consumer import NotificationConsumer
from detector import Notification_detector
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry._logs import set_logger_provider
from opentelemetry.metrics import set_meter_provider
from notification_repository import NotificationDBHandler
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter

from utils import RedisConfig


async def main() -> None:    
    #RABBITMQ
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_password = os.environ.get("RABBITMQ_PASSWORD", "guest")
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_port = os.environ.get("RABBITMQ_PORT", "5672")
    rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/"
    #POSTGRES 
    postgres_db = os.environ.get("POSTGRES_DB", "postgres")
    postgres_db_user = os.environ.get("POSTGRES_USER", "postgres")
    postgres_db_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    postgres_db_host = os.environ.get("POSTGRES_HOST", "localhost")
    postgres_db_port = int(os.environ.get("POSTGRES_PORT", 5432))

    #REDIS
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))
    redis_db = int(os.environ.get("REDIS_DB", 0))
    redis_config = RedisConfig(host=redis_host, port=redis_port, db=redis_db)


    logger = logging.getLogger("yoda.practice")
    logger.setLevel(logging.INFO)
    logger.info("Starting yoda practice")
    db = await NotificationDBHandler.create(
            dbname=postgres_db,
            user=postgres_db_user,
            password=postgres_db_password,
            host=postgres_db_host,
            port=postgres_db_port
        )
    
    rabbit = NotificationSender(
        "notification-senders",
        rabbitmq_url
    )
            
    consumer = NotificationConsumer("notification-request-exchange", "notification-request-queue", rabbitmq_url)
    detector = Notification_detector(clustering=True,redis_config=redis_config).run(db,rabbit)
    tasks = [consumer.consume(connection=db), detector]   

    await asyncio.gather(*tasks) 

if __name__ == "__main__":
    try:

        #OTLP
        opentelemetry_host= os.environ.get("OPENTELEMETRY_HOST", "localhost")
        opentelemetry_port = os.environ.get("OPENTELEMETRY_PORT", "4317")
        opentelemetry_endpoint = f"{opentelemetry_host}:{opentelemetry_port}"

        #OPENTELEMETRY INSTRUMENTATION
        exporter = OTLPMetricExporter(endpoint=opentelemetry_endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(exporter)
        provider = MeterProvider(metric_readers=[reader],resource=Resource.create({
            "service.name": "scheduler-notifications"
        }))
        set_meter_provider(provider)
        logger_provider = LoggerProvider(
            resource=Resource.create(
                {
                    "service.name": "train-the-telemetry",
                    "service.instance.id": os.uname().nodename,
                }
            ),
        )
        set_logger_provider(logger_provider)
        otlp_exporter = OTLPLogExporter(endpoint=opentelemetry_endpoint, insecure=True)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))
        console_exporter = ConsoleLogExporter()
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(console_exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)




        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")


