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
from notificatiton_repository import NotificationDBHandler
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter


async def main() -> None:
        logger = logging.getLogger("yoda.practice")
        logger.setLevel(logging.INFO)
        logger.info("Starting yoda practice")
        db = await NotificationDBHandler.create(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
        
        rabbit = NotificationSender(
            "notification-senders",
            "amqp://guest:guest@localhost/"
        )
             
        consumer = NotificationConsumer("notification-request-exchange", "notification-request-queue")
        detector = Notification_detector(clustering=True).run(db,rabbit)
        tasks = [consumer.consume(connection=db), detector]   

        await asyncio.gather(*tasks) 

if __name__ == "__main__":
    try:
        exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True)
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

        otlp_exporter = OTLPLogExporter(endpoint="http://localhost:4317", insecure=True)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))

        console_exporter = ConsoleLogExporter()
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(console_exporter))

        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")


