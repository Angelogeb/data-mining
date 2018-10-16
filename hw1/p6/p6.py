#! /usr/bin/env python3

import argparse
from lxml import html
import requests
import datetime 
from multiprocessing import Process
import time


parser = argparse.ArgumentParser(description='Scrape kijiji.it Informatica/Grafica/Web category')
parser.add_argument('-f','--full_desc', help='Download full description', action='store_true')
parser.add_argument('-n', '--n_proc', help='Number of processes to run', type=int, default = 6)
args = parser.parse_args()


csv_keys = ["title", "href", "city", "timestamp", "description"]
if args.full_desc:
    csv_keys.append("full_description")

now = str(datetime.datetime.now().isoformat())
now = now[:now.rfind(":")]
now += ("-full_desc" if args.full_desc else "")
file_name = now + "-announcements.tsv"

top_url = "https://www.kijiji.it/offerte-di-lavoro/offerta/informatica-e-web/?top-ads="

reg_url = "https://www.kijiji.it/offerte-di-lavoro/offerta/informatica-e-web/?p="

def escape_string(s):
    return s.strip().replace("\n", " ").replace("\t", " ").replace("\r", "")

def get_announcement_from_elem(ann_elem):
    res = {}
    res["title"] = escape_string(ann_elem.find_class("title")[0].text_content())
    res["href"] = ann_elem.find_class("cta")[0].get("href")
    res["description"] = escape_string(ann_elem.find_class("description")[0].text_content())
    res["city"] = ann_elem.find_class("locale")[0].text_content()
    res["timestamp"] = ann_elem.find_class("timestamp")[0].text_content()

    if args.full_desc:
        full_desc_elem = html.fromstring(requests.get(res["href"]).content).find_class("vip__text-description")
        if full_desc_elem != []:
            res["full_description"] = escape_string(full_desc_elem[0].text_content())

    return res

def get_announcements(page):
    places = page.find_class("locale")
    regular_announcements = (place.getparent() for place in places if not "topad" in place.getparent().getparent().classes)
    return [get_announcement_from_elem(elem) for elem in regular_announcements]

def get_top_announcements(page):
    places = page.find_class("locale")
    announcements = (place.getparent() for place in places if not "extended-result" in place.getparent().getparent().classes)
    return [get_announcement_from_elem(elem) for elem in announcements]

def ann_to_csv(ann):
    res = []
    for k in ann:
        res.append(ann[k])
    return "\t".join(res) + "\n"

def crawl_and_save(prid, urls, get_content, f):
    n_items = 0
    for url in urls:
        bad_req = True
        while bad_req:
            try:
                req = requests.get(url)
                content = get_content(html.fromstring(req.content))
                f.writelines([ann_to_csv(ann) for ann in content])
                n_items += len(content)
                bad_req = False
            except (Exception, requests.exceptions.ConnectionError):
                print("p"+str(prid) + ": Entering sleep")
                time.sleep(60)
                print("p"+str(prid) + ": Awake")
    f.close()
    print("p" + str(prid) + ": ", n_items, " Retrieved")
    return n_items


def get_last_page(url):
    req = requests.get(url)
    page = html.fromstring(req.content)
    last_page = page.find_class("last-page")
    return int(last_page[0].text_content()) if last_page != [] else 1


top_last_page = get_last_page(top_url + str(1))
top_pages_urls = [top_url + str(i) for i in range(1, top_last_page + 1)]


with open("top-"+file_name, "w") as f:

    f.write("\t".join(csv_keys) + "\n")

    n_top = crawl_and_save(1, top_pages_urls, get_top_announcements, f)

    print(top_last_page, "pages", n_top, "announcements in top announcements")


reg_last_page = get_last_page(reg_url + str(1))
reg_pages_urls = [reg_url + str(i) for i in range(1, reg_last_page + 1)]

print("reg_last_page =", reg_last_page)

def split_in(k, l):
    rem = len(l) % k
    list_len = len(l) // k
    if (rem != 0): list_len += 1
    return [l[i * list_len : (i + 1) * list_len] for i in range(k)]

urls_partitions = split_in(args.n_proc, reg_pages_urls)

ps = []
for (i, par) in enumerate(urls_partitions):
    f = open(str(i) + "-" + file_name, "w")
    p = Process(target = crawl_and_save, args =(i + 1, par, get_announcements, f))
    p.start()
    ps.append((p, f))

for (p, f) in ps:
    p.join()
