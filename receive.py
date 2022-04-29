#!/usr/bin/env python
import json
import MySQLdb
import pika, sys, os

# MySQL configurations
username = 'root'
passwd = 'database'
hostname = '127.0.0.1'
db_name = "db"

def on_message_received(ch,method,properties,body): 
    data = json.loads(body)
    print(f"[x] Subscriber - received new message: {data}")
    conn = MySQLdb.connect(user=username, passwd=passwd,host=hostname, port=3306, db=db_name)
    cursor = conn.cursor()
    query = '''INSERT INTO product VALUES ('{}',{});'''.format(data['product'],int(data['quantity']))
    cursor.execute(query)
    conn.commit()
    cursor.close()

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.exchange_declare(exchange='pubsub',exchange_type='fanout')

    queue = channel.queue_declare(queue='',exclusive=True)

    channel.queue_bind(exchange='pubsub',queue = queue.method.queue)

    # def callback(ch, method, properties, body):
    #     print(" [x] Received %r" % body)

    channel.basic_consume(queue=queue.method.queue, on_message_callback=on_message_received, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)