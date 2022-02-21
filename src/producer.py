#!/usr/bin/env python
#
# =============================================================================
#
# Produce Avro messages to Kafka and store the schema in the Confluent registry
# Using Confluent Python Client
#
# =============================================================================

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import os
from uuid import uuid4
from time import sleep

from messages import Message


class Producer:

    def __init__(
        self,
        topic,
        schema_file_path,
        schema_registry_url,
        bootstrap_servers
    ):
        self.topic = topic
        self.schema = self.get_schema(schema_file_path)

        # we need to serialise also the key of the message
        self.schema_key = avro.loads("{\"namespace\": \"io.confluent.schema.keys\", \"name\": \"Key\", \"type\": \"string\"}")

        self.producer = AvroProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "schema.registry.url": schema_registry_url
            },
            default_key_schema=self.schema_key,
            default_value_schema=self.schema
        )

    def get_schema(self, schema_file_path):
        """
        get the schema from the source file
        """

        with open(schema_file_path, "r") as schema_file:
            schema = schema_file.read()

        return avro.loads(schema)

    def on_delivery(self, err, msg, obj):
        """
        Handle delivery reports served from producer.poll.

        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (KafkaMessage): Metadata related to the message.
            obj (Message): The message that was produced or failed.
        """
        if err is not None:
            print(
                f"Message delivery failed for message {obj} with error {err}"
            )
        else:
            print(
                f"Message successfully produced to {msg.topic()}"
                f"[{msg.partition()}] at offset {msg.offset()}"
            )

    def produce(self, key, message):
        """
        wrapper of the `produce` method
        """

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=message.to_dict(),
            callback=lambda err, msg, obj=message: self.on_delivery(err, msg, obj)
        )

        # this call is needed to serve the `on_delivery` callback
        # from the calls of `produce` in the previous iterations
        # NOTE: timeout set to 0.
        self.producer.poll(0.0)

    def flush(self):
        """
        wrapper of the `flush` method
        """

        self.producer.flush()


if __name__ == "__main__":

    bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS")
    topic = os.environ.get("TOPIC")
    schema_file_path = os.environ.get("AVRO_SCHEMA_LOCATION")
    schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")

    producer = Producer(
        topic,
        schema_file_path,
        schema_registry_url,
        bootstrap_servers
    )

    # send messages in AVRO format, registering the schema in the
    # registry
    while True:
        sleep(2)
        try:
            message = Message(
                string_key="foo",
                int_key=42
            )
            producer.produce(
                key=str(uuid4()),
                message=message
            )

            # NOTE messages can be flushed in batch, depending
            # if performance is more important than persistance
            producer.flush()
        except KeyboardInterrupt:
            break
        # in case of inputs not expected from the Message class
        # (should never happen)
        except ValueError:
            print("Invalid input, discarding record...")
            continue
