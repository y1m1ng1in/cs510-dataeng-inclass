#!/usr/bin/env python
#
# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
from time import sleep
from factory import create_producer
import json
import logging
import threading
import ccloud_lib
import sys


def produce_msg(producer, topic, record_key, breadcrumbs):
    def acked(err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            logging.info("Failed to deliver message: {}".format(err))
        else:
            logging.info("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    for breadcrumb in breadcrumbs:
        record_value = json.dumps(breadcrumb)
        logging.info("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        sleep(0.25)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    producer.flush()


if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)    
    #logging.basicConfig(filename="./producer.log", level=logging.DEBUG)

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    n_producer = args.nthread

    sample = open('bcsample.json')
    breadcrumbs = json.load(sample)
    sample.close()

    # Create Producer instance
    producers = [create_producer(config_file, topic) for _ in range(0, n_producer)]

    threads = [threading.Thread(target=produce_msg, 
                                args=(producers[i], 
                                      topic,
                                      "producer{}".format(i), 
                                      breadcrumbs)) for i in range(0, n_producer)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

