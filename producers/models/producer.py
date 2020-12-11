"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=6,
        num_replicas=2,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            # TODO
            # TODO
            # TODO
            "bootstrap.servers":"PLAINTEXT://localhost:9092",
            "schema.registry.url":"http://localhost:8081"
        }
        self.client = AdminClient(
                            {
                              # 'debug': 'admin',                       
                             "bootstrap.servers": "localhost:9092"
                             }
                        )
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # self.producer = AvroProducer(
        # )
#         avroProducer = AvroProducer({
#     'bootstrap.servers': 'mybroker,mybroker2',
#     'on_delivery': delivery_report,
#     'schema.registry.url': 'http://schema_registry_host:port'
#     }, default_key_schema=key_schema, default_value_schema=value_schema)

        self.producer = AvroProducer(
            config = self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #avroProducer.produce(topic='my_topic', value=value, key=key)
#         a = AdminClient({'bootstrap.servers': 'mybroker'})

#         new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["topic1", "topic2"]]
#         # Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

#         # Call create_topics to asynchronously create topics. A dict
#         # of <topic,future> is returned.
#         fs = a.create_topics(new_topics)

#         # Wait for each operation to finish.
#         for topic, f in fs.items():
#             try:
#                 f.result()  # The result itself is None
#                 print("Topic {} created".format(topic))
#             except Exception as e:
#                 print("Failed to create topic {}: {}".format(topic, e))

        logger.info(f"creating topic {self.topic_name}")

        fs = self.client.create_topics(
                        [NewTopic(self.topic_name, 
                                  num_partitions=self.num_partitions,
                                  replication_factor=self.num_replicas)])

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.producer is not None:
            self.producer.flush()

