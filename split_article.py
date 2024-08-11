# this file allows to split the wiki dump into individual articles
# all rights reserved to sleepingcat4
#
#
#
#
import mwxml
import json
import os

file_location = "/content/enwiki-latest-pages-articles-multistream15.xml-p14324603p15824602"

data = []

with open(file_location, 'rb') as file:
    dump = mwxml.Dump.from_file(file)
    
    count = 0
    max_count = 1
    
    for page in dump:
        if count >= max_count:
            break
        
        page_data = {
            "title": page.title,
            "revisions": []
        }
        
        for revision in page:
            # Collect the text of the first revision
            page_data["revisions"].append({
                "text": revision.text
            })
            
            break 
        
        data.append(page_data)
        count += 1

        output_file = f"/content/{page.title.replace('/', '_').replace(':', '_')}.json"
        
        with open(output_file, 'w') as json_file:
            json.dump(data, json_file, indent=4)

        print(f"Data saved to {output_file}")
