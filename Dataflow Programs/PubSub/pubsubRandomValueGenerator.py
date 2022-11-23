# This File generates continuous streaming data into Pubsub Topic in a base64 encoded format.
# After then create the Dataflow Job to start importing data into BigQuery after decoding


# Standard Python Library Imports
import numpy as np
import logging
import time
import datetime
import json
import base64
import pytz
from google.cloud import pubsub

# Time Initialize

indiaTime = pytz.timezone("asia/calcutta")
timeInIndia = datetime.datetime.now(indiaTime)
CurrentTimeInIndia = timeInIndia.strftime("%H:%M:%S")

# Initialize logger
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)
log = logging.getLogger(__name__)


class StockGenerator:
    # Generates random-walk stock value from a provided mu, sigma,
    #    and starting stock value.

    def __init__(self, mu, sigma, starting_price=100):
        self.mu = mu
        self.sigma = sigma
        self.stock_value = starting_price
        self.dist = np.random.normal(mu, sigma, 100)

    def __next__(self):
        random_return = np.random.choice(self.dist, 1)
        self.stock_value = self.stock_value * random_return[0]
        log.info('New stock value: %s', self.stock_value)
        return self.stock_value


PROJECT = 'practice-id1'
TOPIC = 'pubsubtobigq'


def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())


def main():
    # Publishes the message 'Hello World'
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    stock_price = StockGenerator(mu=1.001, sigma=0.001, starting_price=100)

    while True:
        # Pause for 1 second to mimic second-by-second data
        time.sleep(1)
        price = next(stock_price)
        timestamp = str(timeInIndia)  # str to make json serializable

        # Create the body of the message we want to publish in the stream
        body = {
            'stock_price': price,
            'timestamp': timestamp,
        }

        # Dump to json and encode as string, as is required by Pubsub
        str_body = json.dumps(body)
        print(str_body)
        data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
        message_future = publisher.publish(
            topic,
            data=data,
        )
        message_future.add_done_callback(pub_callback)


if __name__ == '__main__':
    main()
