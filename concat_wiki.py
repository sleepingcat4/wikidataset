import os
import pyarrow as pa
import pyarrow.parquet as pq

def combine_title_abstract(input_file, output_file, checkpoint_folder, process_all, wiki_language):
    if not os.path.exists(checkpoint_folder):
        os.makedirs(checkpoint_folder)

    table = pq.read_table(input_file)
    if process_all != 'yes':
        table = table.slice(0, 10)

    batch_size = 1000000
    num_rows = table.num_rows
    rows_processed = 0
    checkpoint_counter = 0
    checkpoint_files = []

    while rows_processed < num_rows:
        chunk_end = min(rows_processed + batch_size, num_rows)
        chunk = table.slice(rows_processed, chunk_end - rows_processed)
        
        title_col = chunk.column('Title')
        abstract_col = chunk.column('Abstract')
        concat_col = pa.array([f"{title} {abstract}" for title, abstract in zip(title_col, abstract_col)])
        wiki_language_col = pa.array([wiki_language] * chunk.num_rows)
        
        chunk = chunk.append_column('Concat Abstract', concat_col)
        chunk = chunk.append_column('Wiki Language', wiki_language_col)
        chunk = chunk.select(['Title', 'Concat Abstract', 'Version Control', 'Wiki Language'])

        checkpoint_counter += 1
        checkpoint_file = os.path.join(checkpoint_folder, f"checkpoint_{checkpoint_counter}.parquet")
        pq.write_table(chunk, checkpoint_file)
        checkpoint_files.append(checkpoint_file)

        for i in range(rows_processed, chunk_end):
            print(f"Processed row {i + 1}")

        rows_processed = chunk_end

    combined_tables = [pq.read_table(f) for f in checkpoint_files]
    final_table = pa.concat_tables(combined_tables)
    pq.write_table(final_table, output_file)
    print(f"Final output saved: {output_file}")
    assert final_table.num_rows == table.num_rows, "Row count mismatch between input and output files."

if __name__ == "__main__":
    input_file = input("Enter the absolute path to the input Parquet file: ").strip()
    output_file = input("Enter the absolute path to the output Parquet file: ").strip()
    checkpoint_folder = input("Enter the name of the checkpoint folder: ").strip()
    process_all = input("Process entire file? (yes/no): ").strip().lower()
    wiki_language = input("Enter the Wiki Language value (e.g., dewiki): ").strip()

    combine_title_abstract(input_file, output_file, checkpoint_folder, process_all, wiki_language)
