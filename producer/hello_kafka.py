from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

producer.produce("test", value="hello from python")
producer.flush()

print("Message sent!")