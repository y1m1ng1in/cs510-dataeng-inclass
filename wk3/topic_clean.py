from confluent_kafka import Consumer
from factory import create_consumer
import json
import ccloud_lib

if __name__ == '__main__':
    
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic

    # Create Consumer instance
    consumer = create_consumer(config_file)

    # Subscribe to topic
    consumer.subscribe([topic])

    running = True
    consumed = 0
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            print("No more message to consume, " 
                  "{counter} messages are "
                  "discarded".format(counter=consumed))
            running = False
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            consumed += 1

    # Leave group and commit final offsets        
    consumer.close()