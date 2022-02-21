# =============================================================================
#
# Avro messages are serialised/deserialised passing a data class to the Confluent
# Kafka clients. This definition needs to be imported then both on Producer and
# Consumer side.
#
# =============================================================================

from dataclasses import dataclass


@dataclass
class Message:

    string_key: str
    int_key: int

    def to_dict(self) -> dict:
        """
        messages are serialised in Avro using a dict representation of the
        messages (which need to be defined as data classes)

        Args:
            message (Message): Message instance

        Returns:
            (dict): representation of the message as a Python dictionary
        """

        return {
            "stringKey": self.string_key, 
            "intKey": self.int_key
        }

