import tensorflow as tf
import requests
import time
import datetime
import signal
import sys
import numpy as np

# Dictionary to store the last prices
last_prices = {}




def serialize_example(timestamp, currency, price_change, bid_price_change, ask_price_change, bid_volume, ask_volume):
    """
    Creates a tf.Example message ready to be written to a file.
    Serializes 64-bit floats as bytes to preserve precision.
    """
    feature = {
        'timestamp': tf.train.Feature(int64_list=tf.train.Int64List(value=[timestamp])),
        'currency': tf.train.Feature(bytes_list=tf.train.BytesList(value=[currency.encode('utf-8')])),
        'price_change': tf.train.Feature(bytes_list=tf.train.BytesList(value=[np.float64(price_change).tobytes()])),
        'bid_price_change': tf.train.Feature(bytes_list=tf.train.BytesList(value=[np.float64(bid_price_change).tobytes()])),
        'ask_price_change': tf.train.Feature(bytes_list=tf.train.BytesList(value=[np.float64(ask_price_change).tobytes()])),
        'bid_volume': tf.train.Feature(bytes_list=tf.train.BytesList(value=[np.float64(bid_volume).tobytes()])),
        'ask_volume': tf.train.Feature(bytes_list=tf.train.BytesList(value=[np.float64(ask_volume).tobytes()]))
    }
    example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
    return example_proto.SerializeToString()

def _parse_function(proto):
    feature_description = {
        'timestamp': tf.io.FixedLenFeature([], tf.int64),
        'currency': tf.io.FixedLenFeature([], tf.string),
        'price_change': tf.io.FixedLenFeature([], tf.string),
        'bid_price_change': tf.io.FixedLenFeature([], tf.string),
        'ask_price_change': tf.io.FixedLenFeature([], tf.string),
        'bid_volume': tf.io.FixedLenFeature([], tf.string),
        'ask_volume': tf.io.FixedLenFeature([], tf.string),
    }
    
    # Load one example
    parsed_features = tf.io.parse_single_example(proto, feature_description)
    
    # Decode all byte-serialized floats back to np.float64
    parsed_features['price_change'] = np.frombuffer(parsed_features['price_change'].numpy(), dtype=np.float64)[0]
    parsed_features['bid_price_change'] = np.frombuffer(parsed_features['bid_price_change'].numpy(), dtype=np.float64)[0]
    parsed_features['ask_price_change'] = np.frombuffer(parsed_features['ask_price_change'].numpy(), dtype=np.float64)[0]
    parsed_features['bid_volume'] = np.frombuffer(parsed_features['bid_volume'].numpy(), dtype=np.float64)[0]
    parsed_features['ask_volume'] = np.frombuffer(parsed_features['ask_volume'].numpy(), dtype=np.float64)[0]

    return parsed_features


def fetch_data(writer):
    currencies = ['XXBTZUSD', 'XETHZUSD', 'USDTZUSD', 'XXRPZUSD', 'XLTCZUSD', 'XETHXXBT', 'XXMRZUSD', 'XXLMZUSD',
                  'DOTUSD', 'LINKUSD', 'ADAUSD', 'ATOMUSD', 'XTZUSD', 'AAVEUSD']
    api_url = 'https://api.kraken.com/0/public/Ticker?pair=' + ','.join(currencies)
    while True:
        response = requests.get(api_url)
        data = response.json()
        if 'result' not in data:
            print(f"Error in API response: {data}")  # Debugging line
            time.sleep(1)
            continue
        timestamp = int(datetime.datetime.now().timestamp())
        for cur in currencies:
            try:
                price_info = data['result'][cur]
                new_price = float(price_info['c'][0])
                new_bid_price = float(price_info['b'][0])
                new_ask_price = float(price_info['a'][0])
                bid_volume = float(price_info['b'][1])
                ask_volume = float(price_info['a'][1])
                
                # Calculate changes
                price_change = new_price - last_prices.get(cur, {'price': new_price})['price']
                bid_price_change = new_bid_price - last_prices.get(cur, {'bid_price': new_bid_price})['bid_price']
                ask_price_change = new_ask_price - last_prices.get(cur, {'ask_price': new_ask_price})['ask_price']

                # Update last prices
                last_prices[cur] = {'price': new_price, 'bid_price': new_bid_price, 'ask_price': new_ask_price}

                # Serialize and write to TFRecord
                example = serialize_example(timestamp, cur[:-6], price_change, bid_price_change, ask_price_change, bid_volume, ask_volume)
                writer.write(example)
            except KeyError as e:
                print(f"Missing data for {cur[:-6]}: {e}")
        time.sleep(1)  # Sleep for one second before the next fetch

def main():
    with tf.io.TFRecordWriter('crypto_data_changes.tfrecord') as writer:
        fetch_data(writer)

if __name__ == '__main__':
    main()
