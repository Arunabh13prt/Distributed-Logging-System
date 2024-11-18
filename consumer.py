from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    topics=['logs', 'heartbeat'],  
    bootstrap_servers='192.168.56.104:9092',  
    value_deserializer=lambda message: json.loads(message.decode('utf-8'))  
)

print("Kafka Consumer is now listening for messages...")

try:
    for record in consumer:
        print("Received message:", record.value)
except KeyboardInterrupt:
    print("\nConsumer stopped.")
except Exception as e:
    print("An error occurred:", str(e))
finally:
    print("Closing the Kafka Consumer.")
    consumer.close()
