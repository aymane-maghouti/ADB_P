import time
import datetime
from producer import send_message
from consumer import consum
import threading

def producer_thread():
    while True:
        try:

            d ={"time":datetime.datetime.now(),"zone1":22,"zone2":17}

            # Produce data to Kafka topic
            message = d

            send_message(message)
            print("Message sent to Kafka topic")

            # Sleep for 5 seconds before collecting and sending the next set of data
            time.sleep(0.5)

        except Exception as e:
            print(f"Error in producer_thread: {e}")

def consumer_thread():
    while True:
        try:
            consum()
            # Sleep for a short interval before consuming the next message
            time.sleep(0.1)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

# Create separate threads for producer and consumer
producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

# Start the threads
producer_thread.start()
consumer_thread.start()

# Wait for the threads to finish (which will never happen in this case as they run infinitely)
producer_thread.join()
consumer_thread.join()
