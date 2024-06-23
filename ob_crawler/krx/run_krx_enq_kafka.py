import argparse
from datetime import datetime, timedelta

from ob_kafka.kafka_manager import KafkaManager


def run_enqueue_krx(topic_name, start_date, limit=100):
    manager = KafkaManager(topic_name=topic_name, group_id='krx')
    cur_date = datetime.strptime(start_date, '%Y%m%d')

    for i in range(0, limit, 1):

        if cur_date.weekday() in (5, 6):
            cur_date += timedelta(days=1)
            continue

        cur_date_str = cur_date.strftime('%Y%m%d')
        cur_date += timedelta(days=1)
        manager.send_message({'date': cur_date_str})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='krx kafka enqueue')
    parser.add_argument('--topic-name', dest='topic_name', help='ex) stock_price')
    parser.add_argument('--start-date', dest='start_date', help='ex) 20240324')
    args = parser.parse_args()

    run_enqueue_krx(topic_name=args.topic_name, start_date=args.start_date)
