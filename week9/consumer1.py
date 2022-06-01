from confluent_kafka import Consumer
from datetime import date, timedelta
import json
import sys
import ccloud_lib

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)


    # Subscribe to topic
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
		continue 
            elif msg.error() or msg.value() is None:
                print('error: {}'.format(msg.error()), file=sys.stdout)
            else:
                # Check for Kafka message, add to dataframe
		print(msg.value())	
                total_count += 1
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and validate+reshape data
        consumer.close()
