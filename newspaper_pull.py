import os, json, time, datetime
from urllib.parse import urlparse
start = time.time()

import newspaper
from newspaper import news_pool

## Load the sites from sites.py (in the same dir)
from sites import urls

# saveDir = "/mnt/nfs/newspaperPull"
saveDir = r"D:\Work\Data\Newspaper"

start = datetime.datetime.now()
print(f"Started at: {start.strftime('%m/%d/%Y, %H:%M:%S')}")
timestamp = datetime.datetime.now().isoformat("T").split(".")[0].replace(":","-").replace("-","-")[:16]
# print("Timestamp", timestamp)

# for paper in papers:
papers = [newspaper.build(url) for url in urls]

news_pool.set(papers, threads_per_source=2)
news_pool.join()

num_papers = len(papers)
current_number = 0
for p in papers:
    current_number += 1
    # print(current_number, "/", num_papers)
    for a in p.articles:
        if a.meta_lang == "":
            a.parse()

total_articles = sum([len(p.articles) for p in papers])
print("Total Articles:",total_articles)
articleEnd = time.time()

print("English articles:",sum([len([a for a in p.articles if a.meta_lang=='en']) for p in papers]))

for p in papers:
    p.articles = [a for a in p.articles if a.meta_lang=='en']

articleKeys = [ 'source_url',
 'url',
 'title',
 'text',
 'keywords',
 'meta_keywords',
 'tags',
 'authors',
 'publish_date',
 'summary',
 'article_html',
 'meta_description',
 'meta_lang',
 'meta_data',
 'canonical_link',
 'additional_data',
 'link_hash']

sourceKeys = [
 'url',
 'domain',
 'brand',
 'description']

flat = []
for p in papers:
    sourcePart = {"source_"+sk:p.__dict__[sk] for sk in sourceKeys}
    for a in p.articles:
        newArt = {ak:a.__dict__[ak] for ak in articleKeys}
        newArt.update(sourcePart)
        for k in newArt.keys():
            if type(newArt[k]) == set:
                newArt[k] = list(newArt[k])
            if type(newArt[k]) == datetime.datetime:
                newArt[k] = str(newArt[k])
        flat.append(newArt)

for f in flat:
    f['id'] = f['link_hash']

print("Writing to:", f"articles_raw-{timestamp}.json")

with open(os.path.join(saveDir,f"articles_raw-{timestamp}.json"), 'w') as fp:
    json.dump(flat, fp)

finish = datetime.datetime.now()

print("Done!")
print(f"Finished at: {finish.strftime('%m/%d/%Y, %H:%M:%S')}")
print(f"Processing time: {(finish - start)}")
