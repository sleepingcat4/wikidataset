# this file allows to remove info boxes and everything
#
#
#
import json
import re

def clean_text(text):
    lines = text.split('\n')
    try:
        first_empty_line_index = lines.index('')
        text = '\n'.join(lines[first_empty_line_index + 1:])
    except ValueError:
        pass  

    reference_section_start = re.search(r'==\s*References?\s*==', text, re.IGNORECASE)
    if reference_section_start:
        text = text[:reference_section_start.start()]
    
    return text.strip()

def process_json(input_file, output_file):
    with open(input_file, 'r') as json_file:
        data = json.load(json_file)

    for page in data:
        for revision in page['revisions']:
            revision['text'] = clean_text(revision['text'])

    with open(output_file, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Processed data saved to {output_file}")

input_file = "/content/Northern grasshopper mouse.json"
output_file = "/content/clean.json"
process_json(input_file, output_file)
