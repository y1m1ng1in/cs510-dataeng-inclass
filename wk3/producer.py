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
from random import randrange
from factory import create_producer
import json
import logging
import threading
import ccloud_lib
import sys


def produce_msg(producer, topic, record_key, breadcrumbs, 
                random_key=False, experiment_g4=True):
    def acked(err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            logging.info("Failed to deliver message: {}".format(err))
        else:
            logging.info("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    # user for experiment G.4
    experimental_counter = 0
    for breadcrumb in breadcrumbs:
        # If random_key is enabled, then generate a random number in [1,5]
        if random_key:
            record_key = randrange(1, 6)
        record_value = json.dumps(breadcrumb)
        logging.info("Producing record: {}\t{}".format(record_key, record_value))
        if not random_key:
            # If random_key is not enabled, then send to partition that is assigned 
            # by kafka 
            producer.produce(
                topic, key=record_key, value=record_value, on_delivery=acked)
        else:
            # If random_key is enabled, in order to let consumer consume message 
            # with specific key, we manually assign partition that is exactly 
            # the key value
            producer.produce(
                topic, key=str(record_key), value=record_value, on_delivery=acked, 
                partition=record_key)
        sleep(0.25)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        if experiment_g4:
            if experimental_counter > 0 and experimental_counter % 5 == 0:
                sleep(2)
            if experimental_counter > 0 and experimental_counter % 15 == 0:
                producer.flush()
        experimental_counter += 1
        if not experiment_g4:
            producer.poll(0)
    
    if not experiment_g4:
        producer.flush()


if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)    
    #logging.basicConfig(filename="./producer.log", level=logging.DEBUG)

    # Read arguments and configurations and initialize
    args          = ccloud_lib.parse_args()
    config_file   = args.config_file
    topic         = args.topic
    n_producer    = args.nthread
    random_key    = args.random
    experiment_g4 = args.g4

    sample = open('bcsample.json')
    breadcrumbs = json.load(sample)
    sample.close()

    # Create Producer instance
    producers = [create_producer(config_file, topic) for _ in range(0, n_producer)]

    threads = [threading.Thread(
        target=produce_msg, 
        args=(producers[i], topic, "producer{}".format(i), breadcrumbs, 
              random_key, experiment_g4)) 
        for i in range(0, n_producer)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


