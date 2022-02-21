#!/usr/bin/env python
#
# =============================================================================
#
# Consume Avro messages to Kafka getting the schema from the Confluent registry
# Using Confluent Python Client
#
# =============================================================================

from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

import os
from requests import get

from messages import Message


class Subscriber:

    def __init__(
        self,
        topic,
        schema_registry_url,
        bootstrap_servers,
        group_id
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id

        self.schema = self.get_last_schema_version()

        # also the message key is serialised in Avro
        self.schema_key = avro.loads(
            "{\"namespace\": \"io.confluent.schema.keys\", \"name\": \"Key\", \"type\": \"string\"}"
        )

        self.consumer = AvroConsumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "schema.registry.url": self.schema_registry_url,
                "group.id": self.group_id,
                "auto.offset.reset": "latest"
            },
            reader_key_schema=self.schema_key,
            reader_value_schema=self.schema
        )
        self.consumer.subscribe([self.topic])

    def get_last_schema_version(self):
        """
        get the schema from the schema registry
        """

        versions = get(
            url=f"{schema_registry_url}/subjects/{self.topic}-value/versions/"
        ).json()

        schema = get(
            url=(
                f"{schema_registry_url}/subjects/{self.topic}-value/"
                f"versions/{max(versions)}"
            )
        ).json()["schema"]

        return avro.loads(schema)

    def to_message(self, dict_obj):
        """
        messages are serialised in Avro using a dict representation of the
        messages (which need to be defined as data classes). Messages are
        then deserialised passing from dictionaries to the actual Message
        object.

        Args:
            dict_obj (dict): the message as a Python dictionary

        Returns:
            (Message): the user-defined message object
        """

        if not dict_obj:
            return None

        return Message(
            string_key=dict_obj["stringKey"],
            int_key=dict_obj["intKey"]
        )

    def poll(self, timeout=1.0, max_num_records_per_poll=5):
        """
        wrapper generator of the `poll` method

        NOTE: Confluent client doesn't have the batching
              system yet implemented, we need to implement
              it here

        Args:
           timeout (float): timeout in the poll operation
           max_num_records_per_poll (int): number of polled records
                                           required to commit
        """

        num_records = 1
        while True:
            try:
                # SIGINT can't be handled when polling, set a proper timeout
                msg = self.consumer.poll(timeout)

                if msg is None:
                    continue

                yield msg.key(), self.to_message(msg.value())

                num_records += 1
                if num_records == max_num_records_per_poll:
                    self.consumer.commit()
                    num_records = 1
            except SerializerError as e:
                # try to reload the consumer with the latest schema uploaded
                # in the schema registry
                self.consumer = AvroConsumer(
                    {
                        "bootstrap.servers": self.bootstrap_servers,
                        "schema.registry.url": self.schema_registry_url,
                        "group.id": self.group_id,
                        "auto.offset.reset": "latest"
                    },
                    reader_key_schema=self.schema_key,
                    reader_value_schema=self.schema
                )
                self.consumer.subscribe([self.topic])

                continue
            except KeyboardInterrupt:
                self.consumer.commit()
                break


if __name__ == "__main__":

    bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS")
    topic = os.environ.get("TOPIC")
    schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    group_id = os.environ.get("KAFKA_GROUP_ID")
    timeout = float(os.environ.get("KAFKA_POLL_TIMEOUT"))
    max_num_records_per_poll = int(os.environ.get("KAFKA_POLL_MAX_RECS_PER_POLL"))

    subscriber = Subscriber(
        topic,
        schema_registry_url,
        bootstrap_servers,
        group_id
    )

    for message in subscriber.poll(timeout, max_num_records_per_poll):
        print(
            f"Consumed record {message[0]}:\tstring: {message[1].string_key}"
            f", number: {message[1].int_key}"
        )
