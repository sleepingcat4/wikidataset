import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq

print("Reading Parquet file...")
df = pd.read_parquet('/storage/ammar/ammar_storage/dewiki_abstracts.parquet')
print(f"Loaded {len(df)} rows from dewiki_abstracts.parquet")

# Generate Embeddings and Create DataFrame
#
print("Generating embeddings...")
embeddings = []
for index, row in df.iterrows():
    text = row['Text']
    payload = {'inputs': text}
    headers = {'Content-Type': 'application/json'}
    response = requests.post('http://127.0.0.1:8080/embed', json=payload, headers=headers)

    if response.status_code == 200:
        try:
            embedding = response.json()['embeddings']
        except TypeError:
            embedding = response.json()  # If response is a list
        embeddings.append(embedding)
    else:
        embeddings.append(None)  # Handle error or missing data case

    if index % 100 == 0:  # Print progress every 100 rows
        print(f"Processed {index + 1} rows")

print("Adding embeddings to DataFrame...")
df['Embeddings'] = embeddings
print("Embeddings added to DataFrame")

print("Saving DataFrame to Parquet file...")
table = pa.Table.from_pandas(df)
pq.write_table(table, '/storage/ammar/ammar_storage/dewiki_abstracts_embed.parquet')
print("Saved to dewiki_abstracts_embed.parquet")
