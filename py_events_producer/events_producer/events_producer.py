#!/opt/rh/rh-python36/root/usr/bin python3
# -*- coding: utf-8 -*-

import datetime
import random
import socket
import time
import random_word
from optparse import OptionParser
from scipy.stats import truncnorm
from typing import Text

PRODUCT_CATEGORIES = (
    'Automotive & Powersports',
    'Baby Products',
    'Beauty',
    'Books',
    'Camera & Photo',
    'Cell Phones & Accessories',
    'Collectible Coins',
    'Consumer Electronics',
    'Entertainment Collectibles',
    'Fine Art',
    'Grocery & Gourmet Food',
    'Health & Personal Care',
    'Home & Garden',
    'Independent Design',
    'Industrial & Scientific',
    'Kindle Accessories and Amazon Fire TV Accessories',
    'Major Appliances',
    'Music',
    'Musical Instruments',
    'Office Products',
    'Outdoors',
    'Personal Computers',
    'Pet Supplies',
    'Software',
    'Sports',
    'Sports Collectibles',
    'Tools & Home Improvement',
    'Toys & Games',
    'DVD & Blu-ray',
    'Video Games',
    'Watches'
)

words_generator = random_word.RandomWords()
PRODUCTS = words_generator.get_random_words(includePartOfSpeech="noun",
                                            minCorpusCount=15,
                                            minDictionaryCount=15,
                                            minLength=5,
                                            limit=400)

SECONDS_IN_DAY = 24 * 60 * 60
PRICE_BOUND = 300
EVENTS_PER_SECOND = 50
DATA_RECEIVED_RESPONSE = 'OK'


def get_random_ip_v4(random):
    return '.'.join(str(random.randint(0, 255)) for _ in range(4))


def get_random_date(rand: random.Random) -> Text:
    """
    Creates random date within last week where day is chosen uniformly but time within a day has a
    truncated normal distribution.
    :param rand: random.Random instance.
    :return: string timestamp
    """
    today = datetime.datetime.now().date()
    gaussian_seconds = int(truncnorm.rvs(0, 1, loc=0, scale=SECONDS_IN_DAY))
    uniform_days = rand.randint(1, 7)
    time = (datetime.datetime.min + datetime.timedelta(
            seconds=gaussian_seconds)).time()
    return str(datetime.datetime.combine(today - datetime.timedelta(days=uniform_days), time))


def generate_events_stream(host: Text, port: int) -> None:
    """
    Generates stream of events at the speed defined by the EVENTS_PER_SECOND constant. And sends them to provided
    address of the Flume netcat source.
    :param host:
    :param port:
    """
    rand = random.Random()
    sleep_timeout = 1.0 / EVENTS_PER_SECOND
    category_mapping = {p: rand.choice(PRODUCT_CATEGORIES) for p in set(PRODUCTS)}
    while True:
        product = rand.choice(PRODUCTS)
        ip = get_random_ip_v4(rand)
        date = get_random_date(rand)
        try:
            send_random_event_line_to_flume(host, port, product, category_mapping[product], date, ip)
            time.sleep(sleep_timeout)
        except Exception as e:
            print(f'Exception on sending event to flume: {e}')


def send_random_event_line_to_flume(host, port, product, category, date, ip):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        line = ','.join([product,
                         '{:,.2f}'.format(truncnorm.rvs(0, 1, loc=0, scale=PRICE_BOUND)),
                         date,
                         category,
                         ip]) + '\n'
        print("Sending event line: '%s' to %s:%d" % (line, host, port))
        sock.sendall(line.encode())
        response_parts = []
        while True:
            data = sock.recv(8192)
            response_parts.append(data)
            if data.endswith(b'\n'):
                break
        resp_bytes = b''.join(response_parts)
        response = resp_bytes.decode("utf-8").strip()
        if response == DATA_RECEIVED_RESPONSE:
            print('Event received by Flume')
        else:
            print(f'Unexpected response from Flume: {response}')


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-a", "--address", action="store", type=str, default='127.0.0.1')
    op.add_option("-p", "--port", action="store", type=int, default=44444)
    (opts, args) = op.parse_args()
    generate_events_stream(opts.address, opts.port)
