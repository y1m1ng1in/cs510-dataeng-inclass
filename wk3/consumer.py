#!/usr/bin/env python

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer, TopicPartition
from factory import create_consumer
import json
import logging
import threading
import ccloud_lib
import sys


def consume_msg(consumer, topic, consumer_name, partition=-1):
    # Subscribe to topic
    consumer.subscribe([topic])

    if partition > 0:
        consumer.assign([TopicPartition(topic, partition)])

    # Process messages
    total_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logging.info("[{}]   Waiting for message or event/error in poll()"
                        .format(consumer_name))
            elif msg.error():
                logging.info('[{}]   error: {}'.format(consumer_name, msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                total_count += 1
                logging.info("[{}]   Consumed record with key {} and value {}, \
                        and updated total count to {}"
                        .format(consumer_name, record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        logging.info("{} consumed {} message in total"
            .format(consumer_name, total_count))
        consumer.close()


if __name__ == '__main__':

    #logging.basicConfig(filename='./consumer.log', level=logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    # Read arguments and configurations and initialize
    args         = ccloud_lib.parse_args()
    config_file  = args.config_file
    topic        = args.topic
    n_consumer   = args.nthread
    random_key   = args.random
    partition_id = -1
    if random_key:
        partition_id = args.key

    # Create Consumer instance
    consumers = [create_consumer(config_file) for _ in range(0, n_consumer)]

    threads   = [threading.Thread(
        target=consume_msg, 
        args=(consumers[i], topic, "consumer{}".format(i), partition_id)) 
        for i in range(0, n_consumer)]
        
    for t in threads:
        t.start()

    for t in threads:
        t.join()

