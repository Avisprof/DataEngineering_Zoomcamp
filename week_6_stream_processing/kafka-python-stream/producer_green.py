import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_GREEN, PRODUCE_TOPIC_GREEN


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class GreenTripDataProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, green_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                lpep_pickup_datetime  = row[1]
                lpep_dropoff_datetime = row[2]
                PULocationID          = row[5]
                DOLocationID          = row[6]
                passenger_count       = row[7]
                trip_distance         = row[8]
                payment_type          = row[17] 
                total_amount          = row[16]
                records.append(f'{lpep_pickup_datetime}, {lpep_dropoff_datetime}, \
                                 {PULocationID}, {DOLocationID}, {passenger_count}, \
                                 {trip_distance}, {payment_type}, {total_amount}')
                green_keys.append(str(PULocationID))
                i += 1
                if i == 500:
                    break
        return zip(green_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for topic {topic} <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record for topic {topic} - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = GreenTripDataProducer(props=config)
    green_records = producer.read_records(resource_path=INPUT_DATA_PATH_GREEN)
    print(green_records)
    producer.publish(topic=PRODUCE_TOPIC_GREEN, records=green_records)
