# this file allows to view the generated JSON files
# 
# 
# 
# 
import json

def print_content(input_file):
    with open(input_file, 'r') as json_file:
        data = json.load(json_file)

    for page in data:
        print(f"Title: {page['title']}")
        for revision in page['revisions']:
            print(f"Content: {revision['text']}")
        print()  # Print a blank line between pages

input_file = "/content/cleaned.json"
print_content(input_file)
