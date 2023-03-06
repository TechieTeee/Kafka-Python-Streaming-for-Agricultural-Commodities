import csv
from kafka import KafkaProducer

#Set Up the Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x.encode('utf-8')  # Assuming the CSV file contains UTF-8 encoded strings
)

#Open the CSV File to Read in Data
with open('Agricultural_Commodities_Data.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Read the CSV records and send them to Kafka
    for row in reader:
        value = row[0]  # Assuming the CSV file has only one column
        producer.send('my-csv-topic', value=value)

#Wait for any outstanding messages to be sent and delivery reports received
producer.flush()

print("Data sent to Kafka successfully!")
