# cleaning the text from like brackets, punc and etc
#
#
import json
import string

def remove_punctuation(input_file, output_file):
    translator = str.maketrans('', '', string.punctuation)
    
    with open(input_file, 'r') as json_file:
        data = json.load(json_file)
    
    for page in data:
        for revision in page['revisions']:
            revision['text'] = revision['text'].translate(translator)
    
    with open(output_file, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Cleaned data saved to {output_file}")

input_file = "/content/clean.json"
output_file = "/content/cleaned.json"
remove_punctuation(input_file, output_file)
