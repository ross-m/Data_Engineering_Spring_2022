from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib


if __name__ == '__main__':

    fp = open('bcsample.json', 'r')
    data = fp.read()
    json_data = json.loads(data)
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    i = 1
    for bc in json_data:
        record_key = str(i)
        record_value = json.dumps(bc)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)
        i += 1

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
