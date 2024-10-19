import json
import base64
import os
import gzip
import regex as re

def format_title(title):
    if title:
        return title.replace(' ', '_')
    return None

def clean_text(text):
    if text:
        return re.sub(r'[^\p{L}\s]', '', text, flags=re.UNICODE)
    return None

def validate_output_file(output_file_path):
    if not output_file_path.endswith(".json"):
        raise ValueError("Output file must have a .json extension.")

def open_file(file_path):
    if file_path.endswith('.json.gz'):
        return gzip.open(file_path, 'rt', encoding='utf-8')
    elif file_path.endswith('.json'):
        return open(file_path, 'r')
    else:
        raise ValueError("Unsupported file format. Only .json and .json.gz are allowed.")

file_path = input("Enter the path of the JSON or JSON.GZ file: ")
output_file_path = input("Enter the path for the output JSON file: ")
language_code = input("Enter the language code for the URL (e.g., 'en', 'de', etc.): ").strip()
clean_text_flag = input("Do you want to enable text cleaning? (yes/no): ").strip().lower()

absolute_input_path = os.path.abspath(file_path)
absolute_output_path = os.path.abspath(output_file_path)

print(f"Input JSON file path: {absolute_input_path}")
print(f"Output JSON file path: {absolute_output_path}")
print(f"Language code for URL: {language_code}")
print(f"Text cleaning enabled: {clean_text_flag == 'yes'}")

validate_output_file(absolute_output_path)

data = []
extract_all = True
limit = 50
processed_count = 0

with open_file(absolute_input_path) as file:
    for i, line in enumerate(file):
        if not extract_all and processed_count >= limit:
            break
        
        entry = json.loads(line.strip())
        
        wiki = entry.get('wiki', None)
        language = entry.get('language', None)
        title = entry.get('title', None)
        full_text = entry.get('text', None)
        abstract = entry.get('abstract', None)
        popularity_score = entry.get('popularity_score', None)
        
        if all([wiki, language, title, full_text]):
            formatted_title = format_title(title)
            url = f"https://{language_code}.wikipedia.org/wiki/{formatted_title}" if formatted_title else None
            
            if clean_text_flag == 'yes':
                full_text = clean_text(full_text)
                abstract = clean_text(abstract) if abstract else None
            
            version_control_value = "20241014" + str(processed_count + 1)
            version_control_bytes = version_control_value.encode('utf-8')
            version_control_base64 = base64.b64encode(version_control_bytes).decode('utf-8')
            
            entry_data = {
                'URL': url,
                'Wiki': wiki,
                'Language': language,
                'Title': title,
                'Abstract': abstract,
                'Full Text': full_text,
                'Version Control': version_control_base64,
                'Popularity Score': popularity_score
            }
            
            data.append(entry_data)
            processed_count += 1
            
            print(f"Processed entry {processed_count}")

with open(absolute_output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"All data saved to {absolute_output_path}")
