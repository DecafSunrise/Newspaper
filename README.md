# Newspaper
### The least uniquely named News Article retrieval script

![image](https://user-images.githubusercontent.com/36832027/216478286-52434aa7-2ec0-4b7b-ba45-62899e63f475.png)

### What is it?
This repo holds ETL/ELT code to scrape news sites, do some light transforms, and save them to disk (and eventually a database).

### How do I use it?
Currently, you can just run the `newspaper_pull.py` script. Feel free to change the `saveDir` line to change where your files save off to, and update `sites.py` if you want to get different news sites. The default list is pretty lengthy (~65 sites); it takes 10-15 minutes to run.
