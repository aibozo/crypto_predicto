import tensorflow as tf
import datetime

def _parse_function(proto):
    # Define your tfrecord again to match the new data structure, expecting bytes for floats
    feature_description = {
        'timestamp': tf.io.FixedLenFeature([], tf.int64, default_value=0),
        'currency': tf.io.FixedLenFeature([], tf.string, default_value=''),
        'price_change': tf.io.FixedLenFeature([], tf.string, default_value=b''),
        'bid_price_change': tf.io.FixedLenFeature([], tf.string, default_value=b''),
        'ask_price_change': tf.io.FixedLenFeature([], tf.string, default_value=b''),
        'bid_volume': tf.io.FixedLenFeature([], tf.string, default_value=b''),
        'ask_volume': tf.io.FixedLenFeature([], tf.string, default_value=b'')
    }
    
    # Load one example using the feature description
    parsed_features = tf.io.parse_single_example(proto, feature_description)
    
    # Convert byte arrays back to float64 using TensorFlow operations
    parsed_features['price_change'] = tf.io.decode_raw(parsed_features['price_change'], tf.float64)[0]
    parsed_features['bid_price_change'] = tf.io.decode_raw(parsed_features['bid_price_change'], tf.float64)[0]
    parsed_features['ask_price_change'] = tf.io.decode_raw(parsed_features['ask_price_change'], tf.float64)[0]
    parsed_features['bid_volume'] = tf.io.decode_raw(parsed_features['bid_volume'], tf.float64)[0]
    parsed_features['ask_volume'] = tf.io.decode_raw(parsed_features['ask_volume'], tf.float64)[0]

    # Ensure currency is decoded for usability
    parsed_features['currency'] = tf.strings.decode_utf8(parsed_features['currency'])

    return parsed_features

# Describe the dataset you are going to read
dataset = tf.data.TFRecordDataset('crypto_data_changes.tfrecord')
dataset = dataset.map(_parse_function)

for features in dataset.take(10):  # Only read the first 10 entries for demonstration
    # Convert TensorFlow tensors to Python types where necessary
    timestamp = datetime.datetime.fromtimestamp(features['timestamp'].numpy()).strftime('%Y-%m-%d %H:%M:%S')
    currency = features['currency'].numpy().decode('utf-8')
    price_change = features['price_change'].numpy()
    bid_price_change = features['bid_price_change'].numpy()
    ask_price_change = features['ask_price_change'].numpy()
    bid_volume = features['bid_volume'].numpy()
    ask_volume = features['ask_volume'].numpy()
    
    print(f"Timestamp: {timestamp}, Currency: {currency}, Price Change: {price_change}, "
          f"Bid Price Change: {bid_price_change}, Ask Price Change: {ask_price_change}, "
          f"Bid Volume: {bid_volume}, Ask Volume: {ask_volume}")
