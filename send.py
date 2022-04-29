#!/usr/bin/env python
import pika
import json
from pika.exchange_type import ExchangeType

product = input("Enter Proudct: ")
qty = int(input("Enter quantity: "))
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='pubsub',exchange_type=ExchangeType.fanout)

message = {'product': product,
            'quantity': qty}

channel.basic_publish(exchange='pubsub', routing_key='', body=json.dumps(message))

print(f" [x] Sent message: {message}")

connection.close()