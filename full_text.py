import json
import base64
import pyarrow as pa
import pyarrow.parquet as pq
import regex as re
import os
import gzip

def format_title(title):
    if title:
        return title.replace(' ', '_')
    return None

def clean_text(text):
    if text:
        return re.sub(r'[^\p{L}\s]', '', text, flags=re.UNICODE)
    return None

def validate_output_file(output_file_path):
    if not output_file_path.endswith(".parquet"):
        raise ValueError("Output file must have a .parquet extension.")

def open_file(file_path):
    if file_path.endswith('.json.gz'):
        return gzip.open(file_path, 'rt', encoding='utf-8')
    elif file_path.endswith('.json'):
        return open(file_path, 'r')
    else:
        raise ValueError("Unsupported file format. Only .json and .json.gz are allowed.")

def save_checkpoint(data, checkpoint_num, checkpoint_folder):
    if not os.path.exists(checkpoint_folder):
        os.makedirs(checkpoint_folder)
    
    checkpoint_path = os.path.join(checkpoint_folder, f"checkpoint_{checkpoint_num}.parquet")
    table = pa.Table.from_pydict({
        'URL': [d['URL'] for d in data],
        'Wiki': [d['Wiki'] for d in data],
        'Language': [d['Language'] for d in data],
        'Title': [d['Title'] for d in data],
        'Full Text': [d['Full Text'] for d in data],
        'Version Control': [d['Version Control'] for d in data],
        'Popularity Score': [d['Popularity Score'] for d in data]
    })
    pq.write_table(table, checkpoint_path)
    print(f"Checkpoint saved at {checkpoint_path}")

file_path = input("Enter the path of the JSON or JSON.GZ file: ")
output_file_path = input("Enter the path for the output Parquet file: ")
language_code = input("Enter the language code for the URL (e.g., 'en', 'de', etc.): ").strip()
checkpoint_folder = input("Enter the name of the checkpoint folder: ").strip()

absolute_input_path = os.path.abspath(file_path)
absolute_output_path = os.path.abspath(output_file_path)
absolute_checkpoint_folder = os.path.abspath(checkpoint_folder)

print(f"Input JSON file path: {absolute_input_path}")
print(f"Output Parquet file path: {absolute_output_path}")
print(f"Language code for URL: {language_code}")
print(f"Checkpoint folder path: {absolute_checkpoint_folder}")

validate_output_file(absolute_output_path)

data = []
checkpoint_data = []
extract_all = True
limit = 50
processed_count = 0
checkpoint_threshold = 10
checkpoint_num = 0

with open_file(absolute_input_path) as file:
    for i, line in enumerate(file):
        if not extract_all and processed_count >= limit:
            break
        
        entry = json.loads(line.strip())
        
        wiki = entry.get('wiki', None)
        language = entry.get('language', None)
        title = entry.get('title', None)
        full_text = entry.get('text', None)
        popularity_score = entry.get('popularity_score', None)
        
        if all([wiki, language, title, full_text]):
            formatted_title = format_title(title)
            url = f"https://{language_code}.wikipedia.org/wiki/{formatted_title}" if formatted_title else None
            
            cleaned_full_text = clean_text(full_text)
            
            version_control_value = "20240819" + str(processed_count + 1)
            version_control_bytes = version_control_value.encode('utf-8')
            version_control_base64 = base64.b64encode(version_control_bytes).decode('utf-8')
            
            entry_data = {
                'URL': url,
                'Wiki': wiki,
                'Language': language,
                'Title': title,
                'Full Text': cleaned_full_text,
                'Version Control': version_control_base64,
                'Popularity Score': popularity_score
            }
            
            data.append(entry_data)
            checkpoint_data.append(entry_data)
            processed_count += 1
            
            if processed_count % checkpoint_threshold == 0:
                checkpoint_num += 1
                save_checkpoint(checkpoint_data, checkpoint_num, absolute_checkpoint_folder)
                checkpoint_data = []
            
            print(f"Processed entry {processed_count}")
        
# Save any remaining data in checkpoint_data after the loop ends
if checkpoint_data:
    checkpoint_num += 1
    save_checkpoint(checkpoint_data, checkpoint_num, absolute_checkpoint_folder)

# Save all the data into a single Parquet file
table = pa.Table.from_pydict({
    'URL': [d['URL'] for d in data],
    'Wiki': [d['Wiki'] for d in data],
    'Language': [d['Language'] for d in data],
    'Title': [d['Title'] for d in data],
    'Full Text': [d['Full Text'] for d in data],
    'Version Control': [d['Version Control'] for d in data],
    'Popularity Score': [d['Popularity Score'] for d in data]
})

pq.write_table(table, absolute_output_path)

print(f"All data saved to {absolute_output_path}")
