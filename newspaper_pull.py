import os, json, time, datetime

start = time.time()

import newspaper
from newspaper import news_pool


# saveDir = "/mnt/nfs/newspaperPull"
saveDir = r"D:\Work\Data\Newspaper"
urls = ['http://www.huffingtonpost.com',
 'http://cnn.com',
 'http://www.time.com',
 'http://www.ted.com',
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
"https://theintercept.com/",
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
"https://www.realclearpolitics.com/",
"https://thehill.com/",
"https://www.wsj.com/",
"https://thedispatch.com/",
"https://www.theepochtimes.com/",
"https://ijr.com/",
"https://nypost.com/",
"https://thepostmillennial.com/",
"https://reason.com/",
"https://www.washingtontimes.com/",
"https://www.theamericanconservative.com/",
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


# urls = urls[:1]

timestamp = datetime.datetime.now().isoformat("T").split(".")[0].replace(":","-").replace("-","-")[:16]
print("Timestamp", timestamp)

papers = [newspaper.build(url) for url in urls]

news_pool.set(papers, threads_per_source=2)
news_pool.join()

num_papers = len(papers)
current_number = 0
for p in papers:
    current_number += 1
    print(current_number, "/", num_papers)
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



#### Here's where I pass the data to the next process, probably persist it to the FS and pass the filename along

print("Writing to:", f"articles_raw-{timestamp}.json")



with open(os.path.join(saveDir,f"articles_raw-{timestamp}.json"), 'w') as fp:
    json.dump(flat, fp)


##### This is really the start of a second process
#
# from nltk.tokenize import sent_tokenize
#
# sents = [{"id":a['id']+"--"+str(i), "sent":sent} for a in flat for i,sent in enumerate(sent_tokenize(a['text']))]
# del flat
# print("Total Sentences:", len(sents))
#
# # # Augment sentences with PoS info
# # That service is running on docker, and we'll hit it fast, so let's resolve the IP of that up front so we don't smash the DNS server
#
# ## Probably have things break when I separate this into different files/functions.  Also servicename is another configurable . . .
# import socket
#
# dockerIP = socket.gethostbyname("docker.proxmox.local")
# servicePort = 5100
#
# URL = f"http://{dockerIP}:{servicePort}/?"
#
# import requests
# def retreiveNouns(sentence):
#     nouns = None
#     try:
#         page = requests.get(URL, {"text": sentence})
#
#         nouns = json.loads(page.content.decode("utf-8"))['nouns']
#     except:
#         pass
#     return nouns
#
# # Intentionally set the number off, so it'll enter and continue the loop
# nnsCount = len(sents) + 1
#
# # Select work which needs to be completed, this is everything in this case
# nns = [s for s in sents if 'nouns' not in s or s['nouns'] is None]
#
# import time
# # At some point some either won't be completable or everything will get done.  In either case, you can bail as soon as the delta is 0.
# nnsCount += 1
# while len(nns) != nnsCount:
#     nnsCount = len(nns)
#     print(nnsCount, "left to do.")
#     time.sleep(5)
#     for s in nns:
#         if 'nouns' in s and s['nouns'] is not None:
#             continue
#         if len(s['sent'].strip()) >0:
#             s['nouns'] = retreiveNouns(s['sent'])
#     # The call occasionally fails, so we'll re-select and then check at the top of the loop.
#     nns = [s for s in sents if 'nouns' not in s or s['nouns'] is None]
print("Done!")

# for s in sents:
#     s['wordcount'] = len(s['sent'].split(" "))
#
# print("Writing to:", f"sentences_tagged-{timestamp}.json")
#
# with open(os.path.join(saveDir,f"sentences_tagged-{timestamp}.json"), 'w') as fp:
#     json.dump(sents, fp)
#
# end = time.time()
#
# ### Technically this metrics gathering is a third, meta task.
#
# print("Total Articles:",total_articles)
# print("English articles:",sum([len([a for a in p.articles if a.meta_lang=='en']) for p in papers]))
# print()
#
# print("Total Sentences:", len(sents))
#
# print(f"Total time: {end-start:0.2f} seconds")
# print(f"Time per article retrieval and parsing (total articles): {(articleEnd - start)/total_articles:0.2}")
# print(f"Time per sentence (total): {(end - start)/len(sents):0.2}")
#
# print("Record copy:")
# minutes = (end-start)//60
# seconds = int(round((end-start)%60,0))
# english_articles = sum([len([a for a in p.articles if a.meta_lang=='en']) for p in papers])
# print(f"{int(minutes)}:{seconds:02}, {total_articles}, {english_articles}, {len(sents)}")


# 5:29, 435, 276, 6308
# 2:34, 176, 132, 2561


# 5076 sentences
#     completed in ~5:07 when using DNS
#     completed in 2:18 with ip address
#

