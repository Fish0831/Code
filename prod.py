from dotenv import load_dotenv
from datetime import datetime
import time, os
import pika
import minio

load_dotenv()

RABBIRMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = (os.getenv("RABBITMQ_PORT"))

RABBITMQ_USERNAME = os.getenv("RABBITMQ_DEFAULT_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_DEFAULT_PASS")

RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE")


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

    parm1 = pika.ConnectionParameters(host=RABBIRMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

    all_parm = [parm1]

    connection = pika.BlockingConnection(all_parm)
    channel = connection.channel()

    channel.queue_declare(queue=RABBITMQ_QUEUE)

from time import perf_counter,sleep
from pymongo import MongoClient
client = MongoClient('mongodb://lab5f.usalab.org:27087/')
db = client['vehicle']  
collection = db['300 raw data']  
def fetch_data_from_mongodb():
    query = {} 
    result = collection.find(query)
    client = MongoClient('mongodb://vehicle:vehicle@mongodb://vehicle:27087/')
    for document in result:
        print(document) 
    print("資料抓取完成")

fetch_data_from_mongodb()

start=perf_counter
Runtimes=1
while True and Runtimes<=10:
    Runtimes+=1
    data = client.get_object('test', 'canmod-gps.dbc')
    content = data.read().decode('utf-8')
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=f"content, {datetime.now().strftime('%H:%M:%S')}")
    print(" [x] Message Sent")
        # time.sleep(0.0001)
    time.sleep(2.5)
    end = perf_counter()
    print(end-start)
   

if __name__ == '__main__':
    main()