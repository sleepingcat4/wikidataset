Most relevent Wikipedia Dumps (English) is available here: https://dumps.wikimedia.org/enwiki/latest/

Install this library ```pip install mwxml```

Downloading Wiki files
======================
Downloading the Wiki abstract: https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz

Downloading all the article: https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2

What's this repo for?
===========================
Downloading and parsing Wikipedia dumps are exceptionally hard. Because it's in XML format, too much big in size, and malformated. Wiki dumps are rich in information but equally obnoxiously malformed. There are infoboxes, painful brackets and etc. These are structured in a fashion, it results in difficulty to remove them and compile a good chunch dataset. this particular pain is surfaced while we deal with full text. 

I have read dozens of repositories online and everyone offers something in advantage. Unfortunately, most are outdated/rely on parsing information directly from the Wikipedia API. Not an efficient method while you are deadling with 100GB data. That's in thie repo, I wrote scripts that can compile a full text wikipedia dataset. 

```print_content``` - function allows to display the JSON file. 

```splitting in articles``` - allows to split the dump into articles and creates one JSON file = one article 

```extracting text``` - a function to remove info boxes and everything from reference and below from trhe earlier generated JSON file. 
