import requests
from bs4 import BeautifulSoup

def fetch_abstract(title):
    title = title.replace(" ", "_")
    url = f"https://en.wikipedia.org/wiki/{title}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        content_div = soup.select_one('#mw-content-text > div.mw-content-ltr.mw-parser-output')
        
        if content_div:
            paragraphs = []
            for element in content_div.children:
                if element.name == 'meta':
                    break  # Stop when encountering the first <meta> tag
                
                if element.name == 'p':
                    paragraphs.append(element.get_text().strip())
            
            return "\n\n".join(paragraphs) if paragraphs else "No content found."
        else:
            return "No content found."
    else:
        return "Failed to fetch the page."