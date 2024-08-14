import pyarrow.parquet as pq

# Path to your Parquet file
file_path = '/ammar_storage/dewiki_abstracts_embed.parquet'

# Open the Parquet file
parquet_file = pq.ParquetFile(file_path)

# Read a few rows from the first row group 
num_rows_to_read = 10 
df = parquet_file.read_row_group(0).to_pandas().head(num_rows_to_read)

# Display the first few rows
print(df)
