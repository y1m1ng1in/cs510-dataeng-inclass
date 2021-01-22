from confluent_kafka import Consumer, Producer, KafkaError
import json
import ccloud_lib

def create_producer(config_file, topic):
    conf = ccloud_lib.read_ccloud_config(config_file)
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)
    return producer

def create_consumer(config_file, group_id):
    conf = ccloud_lib.read_ccloud_config(config_file)
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
    })
    return consumer
