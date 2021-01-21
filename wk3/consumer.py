#!/usr/bin/env python

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
from factory import create_consumer
import json
import logging
import threading
import ccloud_lib
import sys


def consume_msg(consumer, topic, consumer_name):
    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0

    running = True
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            running = False
        elif msg.error():
            logging.info('error: {}'.format(msg.error()))
        else:
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            total_count += 1
            logging.info("[{}]   Consumed record with key {} and value {}, \
                    and updated total count to {}"
                    .format(consumer_name, record_key, record_value, total_count))

    consumer.close()

    logging.info("{} consumed {} message in total"
                 .format(consumer_name, total_count))


if __name__ == '__main__':

    #logging.basicConfig(filename='./consumer.log', level=logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    n_consumer = args.nthread

    # Create Consumer instance
    consumers = [create_consumer(config_file) for _ in range(0, n_consumer)]

    threads = [threading.Thread(
                   target=consume_msg, 
                   args=(consumers[i], 
                         topic,
                         "consumer{}".format(i))) for i in range(0, n_consumer)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

