# fetch_api_data.py
import requests
import pandas as pd
from datetime import datetime, timedelta

# This function fetches data from the NYC API for the last 15 days
def fetch_and_save_data(days_back=15):
    print("--> Fetching data from NYC API...")
    
    # This is the same URL your backend script uses
    query_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    query = f"https://data.cityofnewyork.us/resource/43nn-pn8j.json?$where=inspection_date >= '{query_date}T00:00:00.000'&$limit=50000"
    
    try:
        response = requests.get(query, timeout=90)
        response.raise_for_status()
        data = response.json()
        print(f"--> Successfully fetched {len(data)} records.")
        
        # Save the data to a CSV file for inspection
        if data:
            df = pd.DataFrame(data)
            output_filename = "api_output.csv"
            df.to_csv(output_filename, index=False)
            print(f"--> Data saved to {output_filename}")
        else:
            print("--> No data was returned from the API.")
            
    except requests.exceptions.RequestException as e:
        print(f"--> API fetch error: {e}")

# Run the function
if __name__ == '__main__':
    fetch_and_save_data()
