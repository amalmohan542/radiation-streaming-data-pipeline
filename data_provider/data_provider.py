from kafka import KafkaProducer
import pandas as pd
import time
import os


# Kafka topic to which data will be sent
topic = 'radiation-data'

# Path to the CSV file containing radiation data
csv_file_path = os.getenv('DATASET_FILE_PATH', '/app/safecast_data/measurements-out.csv')

# Number of rows to read at a time from the CSV file
chunk_size = 10000  # Adjust based on your system memory

# Read the CSV file in chunks to handle large datasets efficiently
for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
     # Sort the data by 'captured_time' to ensure proper replay order
    chunk = chunk.sort_values(by='Captured Time')

    # Iterate through each row in the sorted chunk
    for _, row in chunk.iterrows():
        # Skip rows with missing or invalid radiation values
        if pd.isna(row.get('Value')) or row.get('Value') <= 0:
            continue

        # Prepare the data dictionary
        data = {
            'captured_time': row.get('Captured Time', time.time()),
            'latitude': row.get('Latitude', 0),
            'longitude': row.get('Longitude', 0),
            'value': row.get('Value', 0),
            'unit': row.get('Unit', 'unknown'),
            'loader_id': row.get('Loader ID', 'unknown')
        }
        
        # Print the processed data for debugging or logging purposes
        print("Processed data : ", data)

        # Introduce a small delay to control the processing speed if needed
        time.sleep(0.01)


