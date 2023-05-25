import os, json, time, datetime
from urllib.parse import urlparse
start = time.time()

import newspaper
from newspaper import news_pool

print("""
 _   _                                               
| \ | |                                              
|  \| | _____      _____ _ __   __ _ _ __   ___ _ __ 
| . ` |/ _ \ \ /\ / / __| '_ \ / _` | '_ \ / _ \ '__|
| |\  |  __/\ V  V /\__ \ |_) | (_| | |_) |  __/ |   
\_| \_/\___| \_/\_/ |___/ .__/ \__,_| .__/ \___|_|   
                        | |         | |              
                        |_|         |_|              """)


## Load the sites from sites.py (in the same dir)
# from sites import urls

urls = [
'http://www.huffingtonpost.com',
 'http://cnn.com',
 'http://www.time.com',
 'http://www.cnbc.com',
 'http://www.pcmag.com',
 'http://www.foxnews.com',
 'http://theatlantic.com',
 'http://www.bbc.co.uk',
 'http://www.vice.com',
 'http://espn.com',
 'http://www.npr.org'
"https://www.buzzfeednews.com/",
"https://www.thedailybeast.com/",
"https://www.motherjones.com/",
"https://www.msnbc.com/",
"https://www.newyorker.com/",
"https://www.nytimes.com/",
"https://slate.com/",
"https://www.vox.com/",
"https://abcnews.go.com/",
"https://apnews.com/",
"https://www.bloomberg.com/",
"https://www.cbsnews.com/",
"https://www.theguardian.com/us",
"https://www.insider.com/",
"https://www.nbcnews.com/",
"https://www.politico.com/",
"https://www.propublica.org/",
"https://www.washingtonpost.com/",
"https://www.usatoday.com/",
"https://news.yahoo.com/",
"https://www.axios.com/",
"https://www.csmonitor.com/",
"https://www.forbes.com/",
"https://www.marketwatch.com/",
"https://www.newsnationnow.com/",
"https://www.newsweek.com/",
"https://www.reuters.com/",
"https://thehill.com/",
"https://www.wsj.com/",
"https://thedispatch.com/",
"https://www.theepochtimes.com/",
"https://ijr.com/",
"https://nypost.com/",
"https://thepostmillennial.com/",
"https://reason.com/",
"https://www.washingtontimes.com/",
"https://spectator.org/",
"https://www.breitbart.com/",
"https://www.theblaze.com/",
"https://www1.cbn.com/",
"https://dailycaller.com/",
"https://www.dailymail.co.uk/ushome/index.html",
"https://www.dailywire.com/",
"https://thefederalist.com/",
"https://www.nationalreview.com/",
"https://www.newsmax.com/",
"https://freebeacon.com/",
"https://www.oann.com/",
 ]


# saveDir = "/mnt/nfs/newspaperPull"
saveDir = r"/home/dan/articles"

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
