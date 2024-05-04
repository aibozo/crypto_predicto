import requests
import time
import pandas as pd

def fetch_crypto_data(symbols):
    base_url = "https://api.kraken.com/0/public/Ticker"
    data = {}

    for symbol in symbols:
        try:
            response = requests.get(f"{base_url}?pair={symbol}")
            response.raise_for_status()
            result = response.json()['result']
            key = next(iter(result))  # The result is a dict with one key-value pair.
            data[symbol] = {
                'ask': result[key]['a'][0],   # Current ask price
                'bid': result[key]['b'][0],   # Current bid price
                'last': result[key]['c'][0],  # Last trade price
                # Additional indicators can be added here if available from the API
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
        except KeyError as e:
            print(f"Data parsing error for {symbol}: {e}")
    return data

def main():
    # List of major crypto pairs according to Kraken's naming convention
    symbols = ['XXBTZUSD', 'XETHZUSD', 'XLTCZUSD', 'XXRPZUSD', 'XEOSZUSD', 
               'XBCHZUSD', 'XXMRZUSD', 'XETCZUSD', 'XDASHZUSD', 'XZECZUSD',
               'XADAUSD', 'XQTUMUSD', 'XXLMZUSD', 'XXTZUSD', 'XLINKUSD']
    
    while True:
        data = fetch_crypto_data(symbols)
        df = pd.DataFrame(data).T  # Transpose to get symbols as rows
        print(df)
        time.sleep(1)  # Wait for one minute before fetching again

if __name__ == "__main__":
    main()
