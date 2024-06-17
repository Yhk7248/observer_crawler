
import json
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError


from ob_util.log_util import printl
from ob_kafka.const import bootstrap_servers


class KafkaManager:

    def __init__(
            self, group_id, topic_name,
            enable_auto_commit=True, auto_offset_reset='earliest',
            consumer_serializer=lambda x: json.loads(x.decode('utf-8')),
            producer_serializer=lambda v: json.dumps(v).encode('utf-8')
    ):
        self.topic_name = topic_name
        self.gr_id = group_id
        self.consumer_serializer = consumer_serializer
        self.producer_serializer = producer_serializer
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            group_id=group_id
        )

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers
        )

        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic-creator'
        )

    def send_message(self, message: dict):
        self.producer.send(self.topic_name, self.producer_serializer(message))
        self.producer.flush()
        printl(f'Sent: {self.topic_name} / Message: {message}')
        time.sleep(1)

    def receive_message(self):
        for message in self.consumer:
            kafka_message = message.value
            print(f'Received: {kafka_message}')

    def create_topic(self):
        topic_list = [NewTopic(name=self.topic_name, num_partitions=3, replication_factor=3)]

        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic {self.topic_name} created successfully")
        except TopicAlreadyExistsError:
            print(f"Topic '{self.topic_name}' already exists.")
        except Exception as e:
            print(f"Failed to create topic '{self.topic_name}': {e}")

    def delete_topic(self):
        try:
            self.admin_client.delete_topics(topics=[self.topic_name])
            print(f"Topic '{self.topic_name}' has been deleted.")
        except UnknownTopicOrPartitionError:
            print(f"Topic '{self.topic_name}' does not exist.")
        except Exception as e:
            print(f"Failed to delete topic '{self.topic_name}': {e}")

    def close(self):
        self.admin_client.close()
        self.consumer.close()
        self.producer.close()


if __name__ == '__main__':
    manager = KafkaManager(group_id='krx', topic_name='stock_price')
    manager.receive_message()
