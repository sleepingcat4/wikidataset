#### Downloading Wikipedia Dumps
Wikipedia dumps can be found: https://dumps.wikimedia.org/enwiki/

Changing en value is enough to download the relevant language’s dump. After going to the dump, check the latest wikipedia dumps and download the relevant file. 

I have provided multiple different scripts to generate both the abstracts and articles. For simplification purposes, I won’t go over creating articles and adhering to abstracts creation. 

### Code approach
I programmed them while keeping simplification in mind. ```abstract.py``` is a one-step file. When just feeding the xml path, it’ll automatically fetch all the abstracts from the dump and create a parquet file. 

Structure looks like this

URL | Text 

—-----------------------------
Text column is structured: Title (title of wikipedia page) + Abstract

Embeddings Column: Entire Text Column cell for each row’s input is taken and embeddings are generated. 

Running ```embed.py``` file will take the parquet file we previously created and generate the embeddings and add an embeddings column

URL | Text | Embeddings

—-----------------------------------------

Reading the file: ```read_parquet.py``` 

Reading our generated parquet file (I mean any). 
