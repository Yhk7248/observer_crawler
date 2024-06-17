import argparse
from ob_kafka.kafka_manager import KafkaManager


def create_topic(topic_name):
    manager = KafkaManager(
        topic_name=topic_name,
        group_id=None
    )
    manager.create_topic()


def delete_topic(topic_name):
    manager = KafkaManager(
        topic_name=topic_name,
        group_id=None
    )
    manager.delete_topic()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka topic management script.')
    parser.add_argument('action', choices=['create', 'delete'], help='Action to perform: create or delete a topic.')
    parser.add_argument('--topic-name', dest='topic_name')

    args = parser.parse_args()

    if args.action == 'create':
        create_topic(args.topic_name)
    elif args.action == 'delete':
        delete_topic(args.topic_name)
