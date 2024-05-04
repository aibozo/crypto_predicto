import requests
import time
import datetime
import csv
from threading import Thread

   # Set the filename for ongoing data storage
filename = 'crypto_data_collected.csv'

# Ensure the file has a header before we start appending data
def initialize_csv():
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        # Only write headers if the file is empty
        if file.tell() == 0:
            writer.writerow(['Timestamp', 'Currency', 'Price', 'Bid Price', 'Bid Volume', 'Ask Price', 'Ask Volume'])

# Function to write data to the CSV file
def write_to_csv(data):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        for entry in data:
            writer.writerow(entry)

   # Function to fetch data from Kraken
def fetch_data():
       # Updated currency list with correct Kraken identifiers
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
           timestamp = datetime.datetime.now()
           stored_data = []
           for cur in currencies:
            try:
                price_info = data['result'][cur]
                price = price_info['c'][0]  # Current price
                bid_price = price_info['b'][0]  # Bid price (lowest price a seller is willing to accept)
                bid_volume = price_info['b'][1]  # Quantity available at bid price
                ask_price = price_info['a'][0]  # Ask price (lowest price a seller is willing to accept)
                ask_volume = price_info['a'][1]  # Quantity available at ask price
                stored_data.append([timestamp, cur[:-6], price, bid_price, bid_volume, ask_price, ask_volume])
                print(stored_data)
            except KeyError as e:
                print(f"Missing data for {cur[:-6]}: {e}")
            write_to_csv(stored_data)
            time.sleep(1)  # Sleep for one second before the next fetch

   # Main function to start the thread
def main():
       initialize_csv()
       thread = Thread(target=fetch_data)
       thread.start()

if __name__ == '__main__':
       main()