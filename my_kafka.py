from confluent_kafka import Consumer, Producer
import asyncio

# Задаємо конфігурації Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}


# Функція для асинхронного споживання повідомлень з Kafka
async def consume_messages():
    consumer = Consumer(kafka_config)

    # Підписка на топік Kafka
    consumer.subscribe(['my_topic'])

    while True:
        # Очікуємо на повідомлення
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Обробка отриманого повідомлення
        print(f"Received message: {msg.value().decode('utf-8')}")

# Функція для асинхронного відправлення повідомлень в Kafka
async def produce_messages():
    producer = Producer(kafka_config)

    for i in range(10):
        message = f"Message {i}"
        # Відправляємо повідомлення в топік Kafka
        producer.produce('your_topic', key=str(i), value=message)
        producer.flush()

        print(f"Sent message: {message}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Створення та запуск асинхронних процесів
    tasks = [
        loop.create_task(consume_messages()),
        loop.create_task(produce_messages())
    ]

    # Очікування завершення всіх задач
    loop.run_until_complete(asyncio.gather(*tasks))

    # Завершення циклу подій
    loop.close()
