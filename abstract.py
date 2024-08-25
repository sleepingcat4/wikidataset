import json
import base64
import pyarrow as pa
import pyarrow.parquet as pq
import regex as re
import os

def format_title(title):
    if title:
        return title.replace(' ', '_')
    return None

def clean_text(text):
    if text:
        return re.sub(r'[^\p{L}\s]', '', text, flags=re.UNICODE)
    return None

def create_fingerprint(index):
    fingerprint_value = f"ammar{index}"
    fingerprint_bytes = fingerprint_value.encode('utf-8')
    return base64.b64encode(fingerprint_bytes).decode('utf-8')

def validate_output_file(output_file_path):
    if not output_file_path.endswith(".parquet"):
        raise ValueError("Output file must have a .parquet extension.")

file_path = input("Enter the path of the JSON file: ")
output_file_path = input("Enter the path for the output Parquet file: ")

absolute_input_path = os.path.abspath(file_path)
absolute_output_path = os.path.abspath(output_file_path)

print(f"Input JSON file path: {absolute_input_path}")
print(f"Output Parquet file path: {absolute_output_path}")

validate_output_file(absolute_output_path)

data = []
extract_all = False
processed_count = 0

with open(absolute_input_path, 'r') as file:
    for i, line in enumerate(file):
        entry = json.loads(line.strip())
        
        wiki = entry.get('wiki', None)
        language = entry.get('language', None)
        title = entry.get('title', None)
        abstract = entry.get('opening_text', None)
        
        if all([wiki, language, title, abstract]):
            formatted_title = format_title(title)
            url = f"https://en.wikipedia.org/wiki/{formatted_title}" if formatted_title else None
            
            cleaned_abstract = clean_text(abstract)
            
            version_control_value = "20240819" + str(processed_count + 1)
            version_control_bytes = version_control_value.encode('utf-8')
            version_control_base64 = base64.b64encode(version_control_bytes).decode('utf-8')
            
            fingerprint_index = create_fingerprint(processed_count + 1)
            
            data.append({
                'URL': url,
                'Wiki': wiki,
                'Language': language,
                'Title': title,
                'Abstract': cleaned_abstract,
                'Version Control': version_control_base64,
                'Fingerprint index': fingerprint_index
            })
            
            processed_count += 1
            print(f"Processed entry {processed_count}")
            
            if not extract_all and processed_count >= 5:
                break

table = pa.Table.from_pydict({
    'URL': [d['URL'] for d in data],
    'Wiki': [d['Wiki'] for d in data],
    'Language': [d['Language'] for d in data],
    'Title': [d['Title'] for d in data],
    'Abstract': [d['Abstract'] for d in data],
    'Version Control': [d['Version Control'] for d in data],
    'Fingerprint index': [d['Fingerprint index'] for d in data]
})

pq.write_table(table, absolute_output_path)

print(f"Data saved to {absolute_output_path}")
