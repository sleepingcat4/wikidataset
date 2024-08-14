import xml.etree.ElementTree as ET
import pandas as pd

file_path = '/ammar_storage/dewiki-latest-abstract.xml'
output_path = '/ammar_storage/dewiki_abstracts.parquet'

def extract_text(element, tag):
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else ''

context = ET.iterparse(file_path, events=('start', 'end'))

data = []

for event, elem in context:
    if event == 'end' and elem.tag == 'doc':
        title = extract_text(elem, 'title')
        url = extract_text(elem, 'url')
        abstract = extract_text(elem, 'abstract')

        # Remove "Wikipedia: " from the title
        if title.startswith("Wikipedia: "):
            title = title.replace("Wikipedia: ", "")

        text = f"{title}\n\n{abstract}"

        data.append({'URL': url, 'Text': text})

        elem.clear()

df = pd.DataFrame(data)
df.to_parquet(output_path)
