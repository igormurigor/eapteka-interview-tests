from confluent_kafka import Consumer, KafkaError

# configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ConfluentTelemetryReporterSampler-7369545720169092266',
    'auto.offset.reset': 'earliest'
}

# Создание consumer
consumer = Consumer(consumer_config)

# Подписка на топики

consumer.subscribe(['messages', 'users'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print('Error while consuming message: {}'.format(msg.error()))
        else:
            # Print the received message key and value
            print('Received message: key={}, value={}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
